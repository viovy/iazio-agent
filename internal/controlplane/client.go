// Package controlplane is the outbound client the supervisor uses to dial the harness API.
package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Default transient retry parameters for rolling restart / update resilience.
const (
	DefaultMaxRetries     = 3
	DefaultInitialBackoff = 100 * time.Millisecond
)

// IsTransientStatus reports whether a status code represents a temporary gateway, internal server reboot, or overload condition.
func IsTransientStatus(status int) bool {
	return status == http.StatusInternalServerError ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout ||
		status == http.StatusTooManyRequests ||
		status == http.StatusRequestTimeout
}

// IsTransientError reports whether an error represents a temporary network error or connection refused/reset.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof")
}

// CommandEnvelope describes an instruction dispatched from the control plane to the agent.
type CommandEnvelope struct {
	Type    string          `json:"type"` // e.g. "PING", "CHECK_FLEET_GIT_STATUS", "UPDATE_TOOLS"
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Client posts heartbeats and finish checks. It does not send IDE credentials.
type Client struct {
	BaseURL        string
	Token          string
	HTTP           *http.Client
	Presence       func() string
	Tools          func() []Tool
	ActiveJobs     func() []ActiveJob
	OnTick         func(ctx context.Context, hostID string) error
	OnToolUpdate   func(ctx context.Context, tools []string, force bool) error
	OnCommand      func(ctx context.Context, cmd CommandEnvelope) error
	MaxRetries     int
	InitialBackoff time.Duration
}

func (c Client) retryParams() (int, time.Duration) {
	max := c.MaxRetries
	if max < 0 {
		max = 0
	} else if max == 0 {
		max = DefaultMaxRetries
	}
	backoff := c.InitialBackoff
	if backoff <= 0 {
		backoff = DefaultInitialBackoff
	}
	return max, backoff
}

// ActiveJob describes one job currently running on this agent.
type ActiveJob struct {
	JobID        string `json:"job_id"`
	WorktreePath string `json:"worktree_path"`
	PID          int    `json:"pid"`
}

// Tool is one binary on a heartbeat.
type Tool struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Version string `json:"version"`
	Status  string `json:"status"`
}

// PendingToolUpdate describes a remote tool update command for the agent.
type PendingToolUpdate struct {
	Tools []string `json:"tools,omitempty"`
	Force bool     `json:"force,omitempty"`
}

// HeartbeatResponse is the payload returned by the control plane upon heartbeat.
type HeartbeatResponse struct {
	Status            string             `json:"status"`
	PendingToolUpdate *PendingToolUpdate `json:"pending_tool_update,omitempty"`
	PendingCommands   []CommandEnvelope  `json:"pending_commands,omitempty"`
}

// Heartbeat marks the host online, stores the tool inventory, and reports active jobs.
func (c Client) Heartbeat(ctx context.Context, hostID string, tools []Tool, fetchFailed bool, activeJobs ...ActiveJob) error {
	_, err := c.HeartbeatDetailed(ctx, hostID, tools, fetchFailed, activeJobs...)
	return err
}

// HeartbeatDetailed marks the host online and returns any pending instructions from the control plane.
func (c Client) HeartbeatDetailed(ctx context.Context, hostID string, tools []Tool, fetchFailed bool, activeJobs ...ActiveJob) (*HeartbeatResponse, error) {
	presence := "ONLINE"
	if c.Presence != nil {
		if p := c.Presence(); strings.TrimSpace(p) != "" {
			presence = strings.TrimSpace(p)
		}
	}
	body := map[string]any{
		"presence":     presence,
		"fetch_failed": fetchFailed,
		"tools":        tools,
		"active_jobs":  activeJobs,
	}
	var res HeartbeatResponse
	if err := c.postJSON(ctx, "/v1/hosts/"+hostID+"/heartbeat", body, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Register records the host kind and optional distribution profile.
func (c Client) Register(ctx context.Context, hostID, kind string, profile ...string) error {
	body := map[string]string{"id": hostID, "kind": kind}
	if len(profile) > 0 && strings.TrimSpace(profile[0]) != "" {
		body["profile"] = strings.TrimSpace(profile[0])
	}
	return c.post(ctx, "/v1/hosts/register", body)
}

// Assignment is one leased job returned by the control plane.
type Assignment struct {
	ID                   string   `json:"id"`
	Kind                 string   `json:"kind"`
	WorktreePath         string   `json:"worktree_path"`
	DocsHubPath          string   `json:"docs_hub_path"`
	StoryID              string   `json:"story_id,omitempty"`
	ScheduleID           string   `json:"schedule_id,omitempty"`
	Engine               string   `json:"engine,omitempty"`
	Prompt               string   `json:"prompt,omitempty"`
	ResumeConversationID string   `json:"resume_conversation_id,omitempty"`
	ConversationIDs      []string `json:"conversation_ids,omitempty"`
}

// NextLease asks for the next job for this host. ok is false when the queue is empty.
func (c Client) NextLease(ctx context.Context, hostID string) (Assignment, bool, error) {
	maxRetries, backoff := c.retryParams()
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/hosts/"+hostID+"/poll", strings.NewReader("{}"))
		if err != nil {
			return Assignment{}, false, err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		resp, err := c.http().Do(req)
		if err != nil {
			lastErr = err
			if !IsTransientError(err) || attempt == maxRetries {
				return Assignment{}, false, lastErr
			}
		} else {
			if resp.StatusCode == http.StatusNoContent {
				_ = resp.Body.Close()
				return Assignment{}, false, nil
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var job Assignment
				err := json.NewDecoder(resp.Body).Decode(&job)
				_ = resp.Body.Close()
				if err != nil && err != io.EOF {
					return Assignment{}, false, err
				}
				if job.ID == "" {
					return Assignment{}, false, nil
				}
				return job, true, nil
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("control plane poll: %s", resp.Status)
			if !IsTransientStatus(resp.StatusCode) || attempt == maxRetries {
				return Assignment{}, false, lastErr
			}
		}
		jitter := time.Duration(0)
		if backoff > 0 {
			jitter = time.Duration(rand.Int63n(int64(backoff/2 + 1)))
		}
		select {
		case <-ctx.Done():
			return Assignment{}, false, ctx.Err()
		case <-time.After(backoff + jitter):
		}
		backoff *= 2
	}
	return Assignment{}, false, lastErr
}

// HostDaemonLoop registers the host and executes periodic heartbeats, tool inventory synchronization,
// and process supervisor health checks without task dispatching or repository inspection (ADR-0118).
func (c Client) HostDaemonLoop(ctx context.Context, hostID, kind string, every time.Duration, profile ...string) error {
	return c.Loop(ctx, hostID, kind, every, nil, profile...)
}

// Loop registers the host, starts a decoupled heartbeat engine, and polls for a lease until ctx is cancelled.
func (c Client) Loop(ctx context.Context, hostID, kind string, every time.Duration, onJob func(Assignment) error, profile ...string) error {
	if every <= 0 {
		every = 30 * time.Second
	}
	prof := ""
	if len(profile) > 0 {
		prof = profile[0]
	}
	backoff := 500 * time.Millisecond
	for {
		if err := c.Register(ctx, hostID, kind, prof); err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < every {
			backoff *= 2
			if backoff > every {
				backoff = every
			}
		}
	}

	hbInterval := every
	if hbInterval > 3*time.Second {
		hbInterval = 3 * time.Second
	}
	if hbInterval < 1*time.Second {
		hbInterval = 1 * time.Second
	}

	sendHB := func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[SIR: PANIC_RECOVERED] recovered panic in sendHB: %v\n", r)
			}
		}()
		var tools []Tool
		if c.Tools != nil {
			tools = c.Tools()
		}
		var activeJobs []ActiveJob
		if c.ActiveJobs != nil {
			activeJobs = c.ActiveJobs()
		}
		hbRes, err := c.HeartbeatDetailed(ctx, hostID, tools, false, activeJobs...)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			_ = c.Register(ctx, hostID, kind, prof)
		} else if hbRes != nil {
			if hbRes.PendingToolUpdate != nil && c.OnToolUpdate != nil {
				go func(up PendingToolUpdate) {
					_ = c.OnToolUpdate(ctx, up.Tools, up.Force)
				}(*hbRes.PendingToolUpdate)
			}
			if len(hbRes.PendingCommands) > 0 && c.OnCommand != nil {
				for _, cmd := range hbRes.PendingCommands {
					go func(cmd CommandEnvelope) {
						_ = c.OnCommand(ctx, cmd)
					}(cmd)
				}
			}
		}
		if c.OnTick != nil {
			_ = c.OnTick(ctx, hostID)
		}
	}

	// Immediate initial heartbeat
	sendHB()

	// Decoupled heartbeat goroutine: keeps presence alive even during long job runs or update tasks
	go func() {
		hbTicker := time.NewTicker(hbInterval)
		defer hbTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-hbTicker.C:
				sendHB()
			}
		}
	}()

	if onJob == nil {
		<-ctx.Done()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if c.Presence != nil && c.Presence() == "ONLINE_UPDATING_TOOLS" {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
				continue
			}
		}

		job, ok, err := c.NextLease(ctx, hostID)
		if err != nil && ctx.Err() != nil {
			return nil
		}
		if ok {
			if err := onJob(job); err != nil {
				if ctx.Err() != nil {
					return nil
				}
			}
			// When a job just executed, immediately poll again to drain active queues
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(every):
		}
	}
}

// PreflightHalt is the response from the API preflight check.
type PreflightHalt struct {
	Reason     string `json:"Reason"`
	PauseQueue bool   `json:"PauseQueue"`
	Heal       bool   `json:"Heal"`
}

// FinishReport carries the post-cooling finish assessment.
type FinishReport struct {
	Kind            string `json:"Kind"`
	ASEComplete     bool   `json:"ASEComplete"`
	WorkPorcelain   string `json:"WorkPorcelain"`
	HubPorcelain    string `json:"HubPorcelain"`
	HubAhead        int    `json:"HubAhead"`
	HealingAttempts int    `json:"HealingAttempts"`
	StoryDraftOK    bool   `json:"StoryDraftOK"`
	HubPushOK       bool   `json:"HubPushOK"`
}

// FinishDecision is what the API applies to the repo queue.
type FinishDecision struct {
	Queue           string `json:"Queue"`
	Reason          string `json:"Reason"`
	HealingAttempts int    `json:"HealingAttempts"`
	LeaseResume     bool   `json:"LeaseResume"`
	Decrement       bool   `json:"Decrement"`
}

// PostPreflight posts the preflight check before spawning a harness.
func (c Client) PostPreflight(ctx context.Context, hostID, worktree string, pre any) (PreflightHalt, error) {
	body := map[string]any{
		"worktree_path": worktree,
		"preflight":     pre,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return PreflightHalt{}, err
	}
	maxRetries, backoff := c.retryParams()
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/repos/"+hostID+"/preflight", bytes.NewReader(raw))
		if err != nil {
			return PreflightHalt{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		resp, err := c.http().Do(req)
		if err != nil {
			lastErr = err
			if !IsTransientError(err) || attempt == maxRetries {
				return PreflightHalt{}, lastErr
			}
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var halt PreflightHalt
				decErr := json.NewDecoder(resp.Body).Decode(&halt)
				_ = resp.Body.Close()
				if decErr != nil {
					return PreflightHalt{}, decErr
				}
				return halt, nil
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("preflight post: %s", resp.Status)
			if !IsTransientStatus(resp.StatusCode) || attempt == maxRetries {
				return PreflightHalt{}, lastErr
			}
		}
		select {
		case <-ctx.Done():
			return PreflightHalt{}, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return PreflightHalt{}, lastErr
}

// PostFinish posts the post-cooling finish check.
func (c Client) PostFinish(ctx context.Context, hostID, worktree, jobID string, finish any) (FinishDecision, error) {
	body := map[string]any{
		"worktree_path": worktree,
		"job_id":        jobID,
		"finish":        finish,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return FinishDecision{}, err
	}
	maxRetries, backoff := c.retryParams()
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/repos/"+hostID+"/finish", bytes.NewReader(raw))
		if err != nil {
			return FinishDecision{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		resp, err := c.http().Do(req)
		if err != nil {
			lastErr = err
			if !IsTransientError(err) || attempt == maxRetries {
				return FinishDecision{}, lastErr
			}
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var decision FinishDecision
				decErr := json.NewDecoder(resp.Body).Decode(&decision)
				_ = resp.Body.Close()
				if decErr != nil {
					return FinishDecision{}, decErr
				}
				return decision, nil
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("finish post: %s", resp.Status)
			if !IsTransientStatus(resp.StatusCode) || attempt == maxRetries {
				return FinishDecision{}, lastErr
			}
		}
		select {
		case <-ctx.Done():
			return FinishDecision{}, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return FinishDecision{}, lastErr
}

// PostChunk stores one stripped output chunk for a job.
func (c Client) PostChunk(ctx context.Context, jobID, stream, text string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/chunks", map[string]string{
		"type": "OUTPUT_CHUNK", "stream": stream, "text": text,
	})
}

// DeclineJob rejects an assignment when the worktree is busy or cannot be executed.
func (c Client) DeclineJob(ctx context.Context, jobID, reason string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/decline", map[string]string{
		"reason": reason,
	})
}

// HostDetail contains full details of a registered host and its checkouts.
type HostDetail struct {
	Host  HostSummary   `json:"host"`
	Repos []RepoSummary `json:"repos"`
	Tools []Tool        `json:"tools"`
}

// HostSummary is high-level host metadata.
type HostSummary struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	Presence      string `json:"presence"`
	Profile       string `json:"profile,omitempty"`
	LastHeartbeat string `json:"last_heartbeat"`
	ReposPaused   int    `json:"repos_paused"`
	FetchFailed   bool   `json:"fetch_failed"`
}

// RepoSummary is repository status on a host.
type RepoSummary struct {
	Path                         string `json:"path"`
	WorktreePath                 string `json:"worktree_path"`
	Queue                        string `json:"queue"`
	Lock                         string `json:"lock"`
	Reason                       string `json:"reason"`
	DiscardPending               bool   `json:"discard_pending"`
	DocsHubPath                  string `json:"docs_hub_path"`
	CloneURL                     string `json:"clone_url"`
	RunningJobID                 string `json:"running_job_id,omitempty"`
	Alias                        string `json:"alias,omitempty"`
	RepoAlias                    string `json:"repo_alias,omitempty"`
	GlobalQueueAllocationEnabled bool   `json:"global_queue_allocation_enabled,omitempty"`
}

// GetHost returns host details and registered repos from the control plane.
func (c Client) GetHost(ctx context.Context, hostID string) (HostDetail, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/v1/hosts/"+hostID, nil)
	if err != nil {
		return HostDetail{}, err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return HostDetail{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return HostDetail{}, fmt.Errorf("control plane get host: %s", resp.Status)
	}
	var detail HostDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return HostDetail{}, err
	}
	return detail, nil
}

// ResumeRepo requests the control plane to unpause/resume a paused repo queue.
func (c Client) ResumeRepo(ctx context.Context, hostID, worktree string) error {
	return c.post(ctx, "/v1/repos/"+hostID+"/resume", map[string]string{
		"worktree_path": worktree,
	})
}

// AbandonJob marks a stalled or orphaned job as abandoned.
func (c Client) AbandonJob(ctx context.Context, jobID, reason string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/abandon", map[string]string{
		"reason": reason,
	})
}

// ResumeJob requests the control plane to resume a stalled or abandoned job using its conversation ID.
func (c Client) ResumeJob(ctx context.Context, jobID string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/resume", map[string]string{})
}

// RemediateRepo requests the control plane to remediate a stuck repository worktree.
func (c Client) RemediateRepo(ctx context.Context, hostID, worktree string) error {
	return c.post(ctx, "/v1/repos/"+hostID+"/remediate", map[string]string{
		"worktree_path": worktree,
	})
}


// RepoRuntimeStatus contains near real-time git and ASE telemetry for a repository.
type RepoRuntimeStatus struct {
	RepoPath             string    `json:"repo_path"`
	Alias                string    `json:"alias,omitempty"`
	IsClean              bool      `json:"is_clean"`
	Branch               string    `json:"branch,omitempty"`
	Porcelain            string    `json:"porcelain,omitempty"`
	SubmodulesClean      bool      `json:"submodules_clean"`
	DirtySubmodules      []string  `json:"dirty_submodules,omitempty"`
	ASEState             string    `json:"ase_state,omitempty"` // IDLE, RUNNING, SUSPENDED_HITL, FAILED_DIRTY, ORPHANED
	ActiveStoryID        string    `json:"active_story_id,omitempty"`
	ActiveTool           string    `json:"active_tool,omitempty"`
	ToolLiveness         string    `json:"tool_liveness,omitempty"`
	ExecutionProvenance  string    `json:"execution_provenance,omitempty"`
	ActiveConversationID string    `json:"active_conversation_id,omitempty"`
	LastCheckedAt        time.Time `json:"last_checked_at"`
}

// PostRepoStatus sends real-time repository runtime telemetry to the control plane.
func (c Client) PostRepoStatus(ctx context.Context, hostID string, statuses []RepoRuntimeStatus) error {
	return c.post(ctx, "/v1/hosts/"+hostID+"/repos/status", statuses)
}

// PostPingAck notifies the control plane that a live PING command was received and handled by the agent.
func (c Client) PostPingAck(ctx context.Context, hostID, pingID, agentVersion string) error {
	payload := map[string]string{
		"ping_id":       pingID,
		"agent_version": agentVersion,
		"acked_at":      time.Now().UTC().Format(time.RFC3339Nano),
	}
	return c.post(ctx, "/v1/hosts/"+hostID+"/ping-ack", payload)
}

func (c Client) post(ctx context.Context, path string, body any) error {
	return c.postJSON(ctx, path, body, nil)
}

func (c Client) postJSON(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	maxRetries, backoff := c.retryParams()
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		resp, err := c.http().Do(req)
		if err != nil {
			lastErr = err
			if !IsTransientError(err) || attempt == maxRetries {
				return lastErr
			}
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if out != nil {
					decErr := json.NewDecoder(resp.Body).Decode(out)
					_ = resp.Body.Close()
					if decErr != nil && decErr != io.EOF {
						return decErr
					}
				} else {
					_ = resp.Body.Close()
				}
				return nil
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("control plane %s: %s", path, resp.Status)
			if !IsTransientStatus(resp.StatusCode) || attempt == maxRetries {
				return lastErr
			}
		}
		jitter := time.Duration(0)
		if backoff > 0 {
			jitter = time.Duration(rand.Int63n(int64(backoff/2 + 1)))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
		backoff *= 2
	}
	return lastErr
}

var defaultHTTPClient = &http.Client{Timeout: 30 * time.Second}

func (c Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return defaultHTTPClient
}
