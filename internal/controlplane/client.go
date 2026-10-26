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
	BaseURL               string
	Token                 string
	HTTP                  *http.Client
	Presence              func() string
	Tools                 func() []Tool
	ActiveJobs            func() []ActiveJob
	OnTick                func(ctx context.Context, hostID string) error
	OnToolUpdate          func(ctx context.Context, tools []string, force bool, targetVersion ...string) error
	OnCommand             func(ctx context.Context, cmd CommandEnvelope) error
	SupportedCapabilities []string
	Version               string
	CommitSHA             string
	MaxRetries            int
	InitialBackoff        time.Duration
	Spooler               *OfflineReportSpooler
	EagerUpdateEnabled    bool
	LastEagerUpdateAt     func() *time.Time
	DiscoveredDevBinaries func() []string
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
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Version          string   `json:"version"`
	CommitSHA        string   `json:"commit_sha,omitempty"`
	Status           string   `json:"status"`
	ServiceInstalled bool     `json:"service_installed,omitempty"`
	ServiceType      string   `json:"service_type,omitempty"`
	ServiceStatus    string   `json:"service_status,omitempty"`
	Capabilities     []string `json:"capabilities,omitempty"`
	Installed        bool     `json:"installed,omitempty"`
	InstallationType string   `json:"installation_type,omitempty"`
	ActiveState      string   `json:"active_state,omitempty"`
	WatchdogEnabled  bool     `json:"watchdog_enabled,omitempty"`
	ConfigStatus     string   `json:"config_status,omitempty"`
}

// PendingToolUpdate describes a remote tool update command for the agent.
type PendingToolUpdate struct {
	Tools         []string `json:"tools,omitempty"`
	Force         bool     `json:"force,omitempty"`
	TargetVersion string   `json:"target_version,omitempty"`
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
	supportedCaps := c.SupportedCapabilities
	if len(supportedCaps) == 0 {
		supportedCaps = []string{
			"user_service_watchdog_v2",
			"deterministic_resume_4tuple_v1",
			"agy_conversation_pinning_v1",
			"workspace_recovery_modes_v1",
			"background_task_watchdog_v1",
		}
	}
	body := map[string]any{
		"presence":               presence,
		"fetch_failed":           fetchFailed,
		"tools":                  tools,
		"active_jobs":            activeJobs,
		"supported_capabilities": supportedCaps,
		"version":                c.Version,
		"commit_sha":             c.CommitSHA,
		"eager_update_enabled":   c.EagerUpdateEnabled,
	}
	if c.LastEagerUpdateAt != nil {
		if t := c.LastEagerUpdateAt(); t != nil && !t.IsZero() {
			body["last_eager_update_at"] = t.Format(time.RFC3339)
		}
	}
	if c.DiscoveredDevBinaries != nil {
		if devBins := c.DiscoveredDevBinaries(); len(devBins) > 0 {
			body["discovered_dev_binaries"] = devBins
		}
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
	ID                    string         `json:"id"`
	Kind                  string         `json:"kind"`
	WorktreePath          string         `json:"worktree_path"`
	DocsHubPath           string         `json:"docs_hub_path"`
	StoryID               string         `json:"story_id,omitempty"`
	ScheduleID            string         `json:"schedule_id,omitempty"`
	Engine                string         `json:"engine,omitempty"`
	Prompt                string         `json:"prompt,omitempty"`
	ResumeConversationID  string         `json:"resume_conversation_id,omitempty"`
	ConversationIDs       []string       `json:"conversation_ids,omitempty"`
	ParentJobID           string         `json:"parent_job_id,omitempty"`
	RootJobID             string         `json:"root_job_id,omitempty"`
	InitialConversationID string         `json:"initial_conversation_id,omitempty"`
	ActiveConversationID  string         `json:"active_conversation_id,omitempty"`
	ResumeMode            string         `json:"resume_mode,omitempty"`
	ResumeFallbackReason  string         `json:"resume_fallback_reason,omitempty"`
	InterruptedASEState   map[string]any `json:"interrupted_ase_state,omitempty"`
	LeaseToken            string         `json:"lease_token,omitempty"`
	LeaseAckDeadlineAt    string         `json:"lease_ack_deadline_at,omitempty"`
	ZipkinTraceID         string         `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL        string         `json:"zipkin_trace_url,omitempty"`
	Traceparent           string         `json:"traceparent,omitempty"`
	InfraRequeueCount     int            `json:"infra_requeue_count,omitempty"`
	FailureCategory       string         `json:"failure_category,omitempty"`
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
					if up.TargetVersion != "" {
						_ = c.OnToolUpdate(ctx, up.Tools, up.Force, up.TargetVersion)
					} else {
						_ = c.OnToolUpdate(ctx, up.Tools, up.Force)
					}
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

	if c.Spooler != nil {
		c.Spooler.StartAutoDrain(ctx, 10*time.Second)
	}

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

// PostJobReadiness reports pre-execution or resume readiness, reap results, and workspace manifest to iazio-harness-api.
func (c Client) PostJobReadiness(ctx context.Context, jobID string, reapResult, manifest, rehydrationStatus map[string]any) error {
	payload := map[string]any{}
	if reapResult != nil {
		payload["reap_result"] = reapResult
	}
	if manifest != nil {
		payload["workspace_state_manifest"] = manifest
	}
	if rehydrationStatus != nil {
		payload["rehydration_status"] = rehydrationStatus
	}
	return c.post(ctx, "/v1/jobs/"+jobID+"/readiness", payload)
}

// DeclineJob rejects an assignment when the worktree is busy or cannot be executed.
func (c Client) DeclineJob(ctx context.Context, jobID, reason string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/decline", map[string]string{
		"reason": reason,
	})
}

// AckLease completes Phase 2 of the two-phase lease handshake.
func (c Client) AckLease(ctx context.Context, jobID, leaseToken string, pid int, activeConvID string) error {
	payload := map[string]any{
		"lease_token":            leaseToken,
		"agent_pid":              pid,
		"active_conversation_id": activeConvID,
	}
	return c.post(ctx, "/v1/jobs/"+jobID+"/ack-lease", payload)
}

// HeartbeatJob sends an active lease heartbeat to prevent lease timeout during long tasks.
func (c Client) HeartbeatJob(ctx context.Context, jobID, leaseToken string) error {
	payload := map[string]any{
		"lease_token": leaseToken,
	}
	return c.post(ctx, "/v1/jobs/"+jobID+"/heartbeat", payload)
}

// AttemptFailure reports a tool startup or pre-progress fault for circuit breaking and failover.
type AttemptFailure struct {
	Host            string `json:"host"`
	Tool            string `json:"tool"`
	Workspace       string `json:"workspace"`
	ExitCode        int    `json:"exit_code"`
	StderrTail      string `json:"stderr_tail"`
	FailureCategory string `json:"failure_category"`
	FailureStage    string `json:"failure_stage"`
	Reason          string `json:"reason"`
	Timestamp       string `json:"timestamp"`
}

// FailoverJob reports early tool failure and triggers multi-tier priority failover.
func (c Client) FailoverJob(ctx context.Context, jobID string, req AttemptFailure) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/failover", req)
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
	HeadSHA              string    `json:"head_sha,omitempty"`
	DirtyFilesCount      int       `json:"dirty_files_count,omitempty"`
	UntrackedFilesCount  int       `json:"untracked_files_count,omitempty"`
	Ahead                int       `json:"ahead,omitempty"`
	Behind               int       `json:"behind,omitempty"`
	HasActiveGitLock     bool      `json:"has_active_git_lock,omitempty"`
	Porcelain            string    `json:"porcelain,omitempty"`
	SubmodulesClean      bool      `json:"submodules_clean"`
	DirtySubmodules      []string  `json:"dirty_submodules,omitempty"`
	NonMainSubmodules    []string  `json:"non_main_submodules,omitempty"`
	ASEState             string    `json:"ase_state,omitempty"` // IDLE, RUNNING, SUSPENDED_HITL, FAILED_DIRTY, ORPHANED
	ActiveStoryID        string    `json:"active_story_id,omitempty"`
	ActiveTool           string    `json:"active_tool,omitempty"`
	ToolLiveness         string    `json:"tool_liveness,omitempty"`
	ExecutionProvenance  string    `json:"execution_provenance,omitempty"`
	ActiveConversationID string    `json:"active_conversation_id,omitempty"`
	MetadataYmlRaw       string    `json:"metadata_yml_raw,omitempty"`
	MetadataYmlSHA       string    `json:"metadata_yml_sha,omitempty"`
	MetadataYmlModTime   string    `json:"metadata_yml_mod_time,omitempty"`
	GitVersion           string    `json:"git_version,omitempty"`
	LastCheckedAt        time.Time `json:"last_checked_at"`
}

// PostRepoStatus sends real-time repository runtime telemetry to the control plane.
func (c Client) PostRepoStatus(ctx context.Context, hostID string, statuses []RepoRuntimeStatus) error {
	return c.post(ctx, "/v1/hosts/"+hostID+"/repos/status", statuses)
}

// PostEagerReadiness sends eager tool update readiness report to the control plane.
func (c Client) PostEagerReadiness(ctx context.Context, hostID string, report any) error {
	return c.post(ctx, "/v1/hosts/"+hostID+"/eager-readiness", report)
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

// NotificationRecord captures system events, releases refreshes, and agent tool execution events.
type NotificationRecord struct {
	ID        string         `json:"id,omitempty"`
	Title     string         `json:"title"`
	Message   string         `json:"message"`
	Category  string         `json:"category"` // "info", "success", "warning", "error", "userAction"
	Timestamp time.Time      `json:"timestamp,omitempty"`
	Source    string         `json:"source,omitempty"`
	JobID     string         `json:"job_id,omitempty"`
	HostID    string         `json:"host_id,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// PostNotification records a notification event with the control plane.
func (c Client) PostNotification(ctx context.Context, rec NotificationRecord) error {
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}
	return c.post(ctx, "/v1/notifications", rec)
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
	if lastErr != nil && c.Spooler != nil && out == nil {
		headers := map[string]string{"Content-Type": "application/json"}
		if c.Token != "" {
			headers["Authorization"] = "Bearer " + c.Token
		}
		_ = c.Spooler.Spool(SpooledRequest{
			URL:     strings.TrimRight(c.BaseURL, "/") + path,
			Method:  http.MethodPost,
			Headers: headers,
			Body:    raw,
		})
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
