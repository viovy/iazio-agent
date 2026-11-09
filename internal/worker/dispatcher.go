// Package worker implements the job dispatcher and worker loop in iazio-agent.
// It claims jobs from iazio-harness-api and delegates execution strictly to iazio-harness.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/procutil"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/telemetry"
	"github.com/viovy/iazio-agent/internal/watchdog"
	"github.com/viovy/iazio-agent/internal/workspace"
)

// Supported job types.
const (
	JobTypeStoryCreate   = "story.create"
	JobTypeStoryExecute  = "story.execute"
	JobTypePromptExecute = "prompt.execute"
	JobTypeToolsUpdate   = "agent.tools.update"
)

var (
	// ErrJobDeferred is returned by ToolUpdater when an update is queued or deferred without failure (N3).
	ErrJobDeferred = errors.New("tool update queued/deferred")
)

// DefaultJobExecutionTimeout is the maximum duration an individual job can run before being terminated.
// Configured to 7 days for enterprise multi-day executions (STORY-GOV-3690).
const DefaultJobExecutionTimeout = 7 * 24 * time.Hour

// Job describes a unit of work received from the control plane.
type Job struct {
	ID                    string          `json:"id"`
	Type                  string          `json:"type"`
	Repo                  string          `json:"repo"`
	WorktreePath          string          `json:"worktree_path,omitempty"`
	TargetHost            string          `json:"target_host,omitempty"`
	TargetAgent           string          `json:"target_agent,omitempty"`
	Payload               json.RawMessage `json:"payload"`
	TraceID               string          `json:"trace_id,omitempty"`
	ParentID              string          `json:"parent_id,omitempty"`
	ParentJobID           string          `json:"parent_job_id,omitempty"`
	StoryID               string          `json:"story_id,omitempty"`
	FencingToken          int64           `json:"fencing_token,omitempty"`
	LeaseToken            string          `json:"lease_token,omitempty"`
	LeaseAckDeadlineAt    string          `json:"lease_ack_deadline_at,omitempty"`
	Traceparent           string          `json:"traceparent,omitempty"`
	ZipkinTraceID         string          `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL        string          `json:"zipkin_trace_url,omitempty"`
	Attempts              int             `json:"attempts,omitempty"`
	InfraRequeueCount     int             `json:"infra_requeue_count,omitempty"`
	FailureCategory       string          `json:"failure_category,omitempty"`
	ActiveConversationID  string          `json:"active_conversation_id,omitempty"`
	ConversationID        string          `json:"conversation_id,omitempty"`
	ResumeConversationID  string          `json:"resume_conversation_id,omitempty"`
	InitialConversationID string          `json:"initial_conversation_id,omitempty"`
	ConversationIDs       []string        `json:"conversation_ids,omitempty"`
}

// ResolvedConversationID returns the canonical conversation ID to resume for this job, if any.
func (j *Job) ResolvedConversationID() string {
	if j == nil {
		return ""
	}
	for _, cand := range []string{
		j.ResumeConversationID,
		j.ActiveConversationID,
		j.ConversationID,
		j.InitialConversationID,
	} {
		if trimmed := strings.TrimSpace(cand); trimmed != "" {
			return trimmed
		}
	}
	for _, cid := range j.ConversationIDs {
		if trimmed := strings.TrimSpace(cid); trimmed != "" {
			return trimmed
		}
	}
	if len(j.Payload) > 0 {
		var pMap map[string]any
		if json.Unmarshal(j.Payload, &pMap) == nil && pMap != nil {
			for _, key := range []string{"resume_conversation_id", "active_conversation_id", "conversation_id", "initial_conversation_id"} {
				if v, ok := pMap[key].(string); ok {
					if trimmed := strings.TrimSpace(v); trimmed != "" {
						return trimmed
					}
				}
			}
		}
	}
	return ""
}

// ClaimRequest is sent to claim pending jobs.
type ClaimRequest struct {
	HostID        string   `json:"host_id,omitempty"`
	JobTypes      []string `json:"job_types,omitempty"`
	IdleWorktrees []string `json:"idle_worktrees,omitempty"`
}

// CompleteRequest reports the final outcome of a job.
type CompleteRequest struct {
	FencingToken          int64           `json:"fencing_token,omitempty"`
	Result                json.RawMessage `json:"result,omitempty"`
	Error                 string          `json:"error,omitempty"`
	FinalTurnCount        int             `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256 string          `json:"final_transcript_sha256,omitempty"`
}

// HeartbeatRequest extends the lease of an active claimed job.
type HeartbeatRequest struct {
	FencingToken         int64  `json:"fencing_token,omitempty"`
	LeaseToken           string `json:"lease_token,omitempty"`
	StoryID              string `json:"story_id,omitempty"`
	WorktreePath         string `json:"worktree_path,omitempty"`
	AgentPID             int    `json:"agent_pid,omitempty"`
	ConversationID       string `json:"conversation_id,omitempty"`
	ActiveConversationID string `json:"active_conversation_id,omitempty"`
}

// StoryBindingRequest is sent to dynamically bind an allocated story ID to a running job.
type StoryBindingRequest struct {
	FencingToken int64  `json:"fencing_token,omitempty"`
	StoryID      string `json:"story_id"`
	StoryTitle   string `json:"story_title,omitempty"`
	StoryPath    string `json:"story_path,omitempty"`
}

// ControlPlaneClient defines communication with iazio-harness-api for agent jobs.
type ControlPlaneClient interface {
	ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error)
	CompleteJob(ctx context.Context, jobID string, req CompleteRequest) error
	HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64, storyID ...string) error
	BindStory(ctx context.Context, jobID string, req StoryBindingRequest) error
	AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error
	FailoverJob(ctx context.Context, jobID string, req controlplane.AttemptFailure) error
}

// HTTPControlPlaneClient is a concrete implementation of ControlPlaneClient over HTTP.
type HTTPControlPlaneClient struct {
	BaseURL        string
	AuthToken      string
	TokenProvider  func(ctx context.Context) string
	HTTPClient     *http.Client
	Spooler        *controlplane.OfflineReportSpooler
	MaxRetries     int
	InitialBackoff time.Duration
}

func (c *HTTPControlPlaneClient) bearerToken(ctx context.Context) string {
	if c.TokenProvider != nil {
		if tok := strings.TrimSpace(c.TokenProvider(ctx)); tok != "" {
			return tok
		}
	}
	return strings.TrimSpace(c.AuthToken)
}

// NewHTTPControlPlaneClient constructs an HTTPControlPlaneClient.
func NewHTTPControlPlaneClient(baseURL, authToken string, client *http.Client, spooler ...*controlplane.OfflineReportSpooler) *HTTPControlPlaneClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	baseURL = strings.TrimRight(baseURL, "/")
	var sp *controlplane.OfflineReportSpooler
	if len(spooler) > 0 {
		sp = spooler[0]
	}
	return &HTTPControlPlaneClient{
		BaseURL:        baseURL,
		AuthToken:      authToken,
		HTTPClient:     client,
		Spooler:        sp,
		MaxRetries:     controlplane.DefaultMaxRetries,
		InitialBackoff: controlplane.DefaultInitialBackoff,
	}
}

func (c *HTTPControlPlaneClient) doWithRetry(ctx context.Context, method, url string, bodyBytes []byte) (*http.Response, error) {
	maxRetries := c.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	} else if maxRetries == 0 {
		maxRetries = controlplane.DefaultMaxRetries
	}
	backoff := c.InitialBackoff
	if backoff <= 0 {
		backoff = controlplane.DefaultInitialBackoff
	}
	var lastResp *http.Response
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		httpReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if tok := c.bearerToken(ctx); tok != "" {
			httpReq.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := c.HTTPClient.Do(httpReq)
		if err != nil {
			lastErr = err
			if !controlplane.IsTransientError(err) || attempt == maxRetries {
				return nil, lastErr
			}
		} else {
			if !controlplane.IsTransientStatus(resp.StatusCode) || attempt == maxRetries {
				return resp, nil
			}
			// Drain and close transient error response before retrying
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			lastResp = resp
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return lastResp, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return lastResp, lastErr
}

// ClaimJob calls POST /api/v1/agent/jobs/claim.
func (c *HTTPControlPlaneClient) ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error) {
	url := c.BaseURL + "/api/v1/agent/jobs/claim"
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal claim request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("execute claim request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var job Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	if job.ID == "" {
		return nil, nil
	}
	return &job, nil
}

// CompleteJob calls POST /api/v1/agent/jobs/{job_id}/complete.
func (c *HTTPControlPlaneClient) CompleteJob(ctx context.Context, jobID string, req CompleteRequest) error {
	url := fmt.Sprintf("%s/api/v1/agent/jobs/%s/complete", c.BaseURL, jobID)
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal complete request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		if c.Spooler != nil {
			headers := map[string]string{"Content-Type": "application/json"}
			if tok := c.bearerToken(ctx); tok != "" {
				headers["Authorization"] = "Bearer " + tok
			}
			_ = c.Spooler.Spool(controlplane.SpooledRequest{
				URL:     url,
				Method:  http.MethodPost,
				Headers: headers,
				Body:    bodyBytes,
			})
		}
		return fmt.Errorf("execute complete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		if c.Spooler != nil && resp.StatusCode >= 500 {
			headers := map[string]string{"Content-Type": "application/json"}
			if tok := c.bearerToken(ctx); tok != "" {
				headers["Authorization"] = "Bearer " + tok
			}
			_ = c.Spooler.Spool(controlplane.SpooledRequest{
				URL:     url,
				Method:  http.MethodPost,
				Headers: headers,
				Body:    bodyBytes,
			})
		}
		return fmt.Errorf("complete returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// HeartbeatJob calls POST /api/v1/agent/jobs/{job_id}/heartbeat.
func (c *HTTPControlPlaneClient) HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64, storyID ...string) error {
	url := fmt.Sprintf("%s/api/v1/agent/jobs/%s/heartbeat", c.BaseURL, jobID)
	sid := ""
	wt := ""
	cid := ""
	if len(storyID) > 0 {
		sid = storyID[0]
	}
	if len(storyID) > 1 {
		wt = storyID[1]
	}
	if len(storyID) > 2 {
		cid = strings.TrimSpace(storyID[2])
	}
	bodyBytes, err := json.Marshal(HeartbeatRequest{
		FencingToken:         fencingToken,
		LeaseToken:           leaseToken,
		StoryID:              sid,
		WorktreePath:         wt,
		AgentPID:             os.Getpid(),
		ConversationID:       cid,
		ActiveConversationID: cid,
	})
	if err != nil {
		return fmt.Errorf("marshal heartbeat request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return fmt.Errorf("execute heartbeat request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("409 conflict: stale fencing token: %s", string(respBody))
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// BindStory calls PATCH /api/v1/jobs/{job_id}/story-binding.
func (c *HTTPControlPlaneClient) BindStory(ctx context.Context, jobID string, req StoryBindingRequest) error {
	url := fmt.Sprintf("%s/api/v1/jobs/%s/story-binding", c.BaseURL, jobID)
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal bind story request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPatch, url, bodyBytes)
	if err != nil {
		return fmt.Errorf("execute bind story request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("409 conflict: stale fencing token: %s", string(respBody))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bind story returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// AckLease calls POST /api/v1/agent/jobs/{job_id}/ack-lease.
func (c *HTTPControlPlaneClient) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error {
	return c.AckLeaseWithWorktree(ctx, jobID, leaseToken, agentPID, activeConvID, "")
}

// AckLeaseWithWorktree calls POST /api/v1/agent/jobs/{job_id}/ack-lease with optional worktree_path.
func (c *HTTPControlPlaneClient) AckLeaseWithWorktree(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID, worktreePath string) error {
	url := fmt.Sprintf("%s/api/v1/agent/jobs/%s/ack-lease", c.BaseURL, jobID)
	payload := map[string]any{
		"lease_token":            leaseToken,
		"agent_pid":              agentPID,
		"active_conversation_id": activeConvID,
	}
	if strings.TrimSpace(worktreePath) != "" && worktreePath != "." {
		payload["worktree_path"] = strings.TrimSpace(worktreePath)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ack lease request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return fmt.Errorf("execute ack lease request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ack lease returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// FailoverJob calls POST /api/v1/agent/jobs/{job_id}/failover.
func (c *HTTPControlPlaneClient) FailoverJob(ctx context.Context, jobID string, req controlplane.AttemptFailure) error {
	url := fmt.Sprintf("%s/api/v1/agent/jobs/%s/failover", c.BaseURL, jobID)
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal failover request: %w", err)
	}

	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return fmt.Errorf("execute failover request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failover returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// DeclineJob calls POST /v1/jobs/{job_id}/decline (falling back to /api/v1/agent/jobs/{job_id}/decline on 404).
func (c *HTTPControlPlaneClient) DeclineJob(ctx context.Context, jobID, reason string) error {
	bodyBytes, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return fmt.Errorf("marshal decline request: %w", err)
	}
	url := fmt.Sprintf("%s/v1/jobs/%s/decline", c.BaseURL, jobID)
	resp, err := c.doWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return fmt.Errorf("execute decline request: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		fallbackURL := fmt.Sprintf("%s/api/v1/agent/jobs/%s/decline", c.BaseURL, jobID)
		resp, err = c.doWithRetry(ctx, http.MethodPost, fallbackURL, bodyBytes)
		if err != nil {
			return fmt.Errorf("execute fallback decline request: %w", err)
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("decline returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// RepoSyncer defines the contract for triggering an on-demand codebase delta sync for a repository.
type RepoSyncer interface {
	TriggerRepoSync(repoName string)
}

// WorktreeResolver maps a repository name or relative submodule to an absolute worktree directory on the host.
type WorktreeResolver func(repo string) string

// ToolUpdater defines the callback to execute tool suite updates.
type ToolUpdater func(ctx context.Context, tools []string, force bool, targetVersion ...string) error

// PreDispatchHook defines a callback invoked before dispatching a claimed job.
// It enables eager tool updates between jobs to prevent queue starvation.
type PreDispatchHook func(ctx context.Context, job *Job) error

// PostJobHook defines a callback invoked after a job finishes execution and cleanup.
type PostJobHook func(ctx context.Context, job *Job)

// HarvestFlusher abstracts synchronous conversation flushing to iazio-harvester.
type HarvestFlusher func(ctx context.Context, conversationID string) (turnCount int, sha string, err error)

// HostExecutionGate coordinates shared execution leases and exclusive maintenance leases.
type HostExecutionGate interface {
	AcquireExecutionLease(ctx context.Context, jobID, repoPath string, fencingToken ...int64) (release func(), err error)
}

// Dispatcher coordinates claiming jobs from the control plane and routing them to iazio-harness.
type Dispatcher struct {
	client                ControlPlaneClient
	invoker               harness.Invoker
	hostID                string
	jobTypes              []string
	syncer                RepoSyncer
	worktreeResolver      WorktreeResolver
	toolUpdater           ToolUpdater
	preDispatchHook       PreDispatchHook
	postJobHook           PostJobHook
	hostGate              HostExecutionGate
	eagerReadinessGate    func(ctx context.Context) (bool, string)
	idleWorktreesProvider func() []string
	worktreeBusyDetector  func(worktreePath string) bool
	repoMap               map[string]string
	workspace             string
	jobTimeout            time.Duration
	flusher               HarvestFlusher
	activeJobsMu          sync.Mutex
	activeJobs            map[string]controlplane.ActiveJob
	inFlightPrepJobs      map[string]string
}

// ActiveJobs returns the list of jobs currently being processed by the dispatcher.
func (d *Dispatcher) ActiveJobs() []controlplane.ActiveJob {
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	out := make([]controlplane.ActiveJob, 0, len(d.activeJobs))
	for _, aj := range d.activeJobs {
		out = append(out, aj)
	}
	return out
}

func (d *Dispatcher) setActiveJob(jobID, worktreePath string, pid int) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	if d.activeJobs == nil {
		d.activeJobs = make(map[string]controlplane.ActiveJob)
	}
	d.activeJobs[jobID] = controlplane.ActiveJob{
		JobID:        jobID,
		WorktreePath: worktreePath,
		PID:          pid,
	}
}

func (d *Dispatcher) clearActiveJob(jobID string) {
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	delete(d.activeJobs, jobID)
}

const (
	reasonRepoBusy                = "REPO_BUSY"
	reasonRepoDirtyUncompletedASE = "repo_dirty_uncompleted_ase"
)

func normalizeWorktreeKey(wt string) string {
	trimmed := strings.TrimSpace(wt)
	if trimmed == "" || trimmed == "." {
		return ""
	}
	return filepath.Clean(trimmed)
}

func (d *Dispatcher) hasConflictingWorktreeLocked(jobID, cleanWT string) bool {
	if cleanWT == "" {
		return false
	}
	for existingJobID, wt := range d.inFlightPrepJobs {
		if existingJobID != jobID && normalizeWorktreeKey(wt) == cleanWT {
			return true
		}
	}
	for existingJobID, aj := range d.activeJobs {
		if existingJobID != jobID && normalizeWorktreeKey(aj.WorktreePath) == cleanWT {
			return true
		}
	}
	return false
}

func (d *Dispatcher) tryReserveInFlightWorktree(jobID, worktreePath string) bool {
	if strings.TrimSpace(jobID) == "" {
		return true
	}
	cleanWT := normalizeWorktreeKey(worktreePath)
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	if d.hasConflictingWorktreeLocked(jobID, cleanWT) {
		return false
	}
	if d.inFlightPrepJobs == nil {
		d.inFlightPrepJobs = make(map[string]string)
	}
	d.inFlightPrepJobs[jobID] = cleanWT
	return true
}

func (d *Dispatcher) clearInFlightWorktree(jobID string) {
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	delete(d.inFlightPrepJobs, jobID)
}

// IsWorktreeInFlight returns true if wt is currently used by any job in activeJobs or inFlightPrepJobs.
func (d *Dispatcher) IsWorktreeInFlight(wt string) bool {
	cleanWT := normalizeWorktreeKey(wt)
	if cleanWT == "" {
		return false
	}
	d.activeJobsMu.Lock()
	defer d.activeJobsMu.Unlock()
	return d.hasConflictingWorktreeLocked("", cleanWT)
}

func isDotLockHeldByOther(cleanWT, jobID string) bool {
	b, err := os.ReadFile(filepath.Join(cleanWT, ".iazio-repolock"))
	if err != nil {
		return false
	}
	var meta struct {
		HolderPID int    `json:"holder_pid"`
		PID       int    `json:"pid"`
		JobID     string `json:"job_id"`
	}
	if json.Unmarshal(b, &meta) != nil {
		return true
	}
	pid := meta.HolderPID
	if pid <= 0 {
		pid = meta.PID
	}
	if pid <= 0 {
		return true
	}
	return (meta.JobID == "" || meta.JobID != jobID) && watchdog.IsProcessAlive(pid)
}

func isRepoLockHeldByOther(cleanWT, jobID string) bool {
	info, err := repolock.Read(cleanWT)
	if err != nil {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(info.Status))
	if status == "COMPLETED" || status == "FAILED" || status == "IDLE" || repolock.IsExpiredMaintenanceLock(info) {
		return false
	}
	if info.JobID != "" && info.JobID == jobID {
		return false
	}
	return (info.PID > 0 && watchdog.IsProcessAlive(info.PID)) ||
		(info.HarnessPID > 0 && watchdog.IsProcessAlive(info.HarnessPID)) ||
		(info.ToolPID > 0 && watchdog.IsProcessAlive(info.ToolPID))
}

func hasActiveWorkspaceSentinel(cleanWT, jobID string) bool {
	sentinels := []string{
		filepath.Join(cleanWT, ".autopilot", "active_job.json"),
		filepath.Join(cleanWT, ".autopilot", "job.lock"),
		filepath.Join(cleanWT, ".autopilot", "lock"),
		filepath.Join(cleanWT, ".harness.lock"),
		filepath.Join(cleanWT, "tmp", "harness.lock"),
	}
	for _, sPath := range sentinels {
		fi, err := os.Stat(sPath)
		if err != nil {
			continue
		}
		if b, rErr := os.ReadFile(sPath); rErr == nil {
			var parsed struct {
				JobID string `json:"job_id"`
				PID   int    `json:"pid"`
			}
			if json.Unmarshal(b, &parsed) == nil {
				if parsed.JobID != "" && parsed.JobID == jobID {
					continue
				}
				if parsed.PID > 0 {
					if watchdog.IsProcessAlive(parsed.PID) {
						return true
					}
					continue
				}
			}
		}
		if time.Since(fi.ModTime()) < 15*time.Minute {
			return true
		}
	}
	return false
}

func isWorktreeLockedByOther(wt, jobID string) bool {
	cleanWT := normalizeWorktreeKey(wt)
	if cleanWT == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(cleanWT, ".git", "index.lock")); err == nil {
		return true
	}
	return isDotLockHeldByOther(cleanWT, jobID) || isRepoLockHeldByOther(cleanWT, jobID) || hasActiveWorkspaceSentinel(cleanWT, jobID)
}

// SetWorktreeBusyDetector registers an optional callback that reports whether a worktree
// has an active external CLI/harness process running inside it.
func (d *Dispatcher) SetWorktreeBusyDetector(fn func(worktreePath string) bool) {
	d.worktreeBusyDetector = fn
}

func (d *Dispatcher) defaultWorktreeCandidates() []string {
	var raw []string
	for _, p := range d.repoMap {
		if strings.TrimSpace(p) != "" {
			raw = append(raw, p)
		}
	}
	if strings.TrimSpace(d.workspace) != "" {
		raw = append(raw, d.workspace)
	}
	return raw
}

func (d *Dispatcher) isCandidateWorktreeIdle(clean string) bool {
	fi, err := os.Stat(clean)
	if err != nil || !fi.IsDir() {
		return false
	}
	if d.IsWorktreeInFlight(clean) || isWorktreeLockedByOther(clean, "") {
		return false
	}
	if b, err := os.ReadFile(filepath.Join(clean, ".session_story_id")); err == nil && strings.TrimSpace(string(b)) != "" {
		return false
	}
	if d.worktreeBusyDetector != nil && d.worktreeBusyDetector(clean) {
		return false
	}
	if ok, _ := repolock.IsWorktreeCleanForNewJob(clean); !ok {
		return false
	}
	return supervisor.InspectWorktreeGitState(clean).IsClean
}

// CollectIdleWorktrees filters candidate worktrees and returns genuinely IDLE, clean, non-locked worktree paths.
func (d *Dispatcher) CollectIdleWorktrees(candidates []string) []string {
	raw := candidates
	if len(raw) == 0 {
		raw = d.defaultWorktreeCandidates()
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, wt := range raw {
		clean := normalizeWorktreeKey(wt)
		if clean == "" || seen[clean] {
			continue
		}
		seen[clean] = true
		if d.isCandidateWorktreeIdle(clean) {
			out = append(out, clean)
		}
	}
	return out
}

func (d *Dispatcher) declineJob(ctx context.Context, jobID, reason string) error {
	if d.client == nil || strings.TrimSpace(jobID) == "" {
		return nil
	}
	if decliner, ok := d.client.(interface {
		DeclineJob(ctx context.Context, jobID, reason string) error
	}); ok {
		return decliner.DeclineJob(ctx, jobID, reason)
	}
	if abandoner, ok := d.client.(interface {
		AbandonJob(ctx context.Context, jobID, reason string) error
	}); ok {
		return abandoner.AbandonJob(ctx, jobID, reason)
	}
	return nil
}

// NewDispatcher constructs a new Dispatcher, optionally accepting a RepoSyncer for immediate post-job sync hooks.
func NewDispatcher(client ControlPlaneClient, invoker harness.Invoker, hostID string, syncer ...RepoSyncer) *Dispatcher {
	var s RepoSyncer
	if len(syncer) > 0 {
		s = syncer[0]
	}
	return &Dispatcher{
		client:           client,
		invoker:          invoker,
		hostID:           hostID,
		jobTypes:         []string{JobTypeStoryCreate, JobTypeStoryExecute, JobTypePromptExecute, JobTypeToolsUpdate},
		syncer:           s,
		jobTimeout:       0,
		activeJobs:       make(map[string]controlplane.ActiveJob),
		inFlightPrepJobs: make(map[string]string),
	}
}

// SetJobTimeout sets a custom per-job execution timeout for the Dispatcher.
func (d *Dispatcher) SetJobTimeout(timeout time.Duration) {
	d.jobTimeout = timeout
}

// SetHarvestFlusher configures the synchronous harvest flusher.
func (d *Dispatcher) SetHarvestFlusher(f HarvestFlusher) {
	d.flusher = f
}

// SetSyncer sets or updates the RepoSyncer on the Dispatcher.
func (d *Dispatcher) SetSyncer(syncer RepoSyncer) {
	d.syncer = syncer
}

// SetWorktreeResolver configures a custom worktree resolver on the Dispatcher.
func (d *Dispatcher) SetWorktreeResolver(r WorktreeResolver) {
	d.worktreeResolver = r
}

// SetToolUpdater configures an optional tool updater callback on the Dispatcher.
func (d *Dispatcher) SetToolUpdater(u ToolUpdater) {
	d.toolUpdater = u
}

// SetPreDispatchHook configures an optional pre-dispatch hook for starvation prevention.
func (d *Dispatcher) SetPreDispatchHook(h PreDispatchHook) {
	d.preDispatchHook = h
}

// SetPostJobHook configures an optional post-job hook called after job completion.
func (d *Dispatcher) SetPostJobHook(h PostJobHook) {
	d.postJobHook = h
}

// SetHostGate configures the host execution and update gate for lease coordination.
func (d *Dispatcher) SetHostGate(g HostExecutionGate) {
	d.hostGate = g
}

// SetEagerReadinessGate configures an optional pre-claim eager update readiness barrier.
func (d *Dispatcher) SetEagerReadinessGate(fn func(ctx context.Context) (bool, string)) {
	d.eagerReadinessGate = fn
}

// SetIdleWorktreesProvider configures an optional provider returning currently idle worktrees on this host.
func (d *Dispatcher) SetIdleWorktreesProvider(fn func() []string) {
	d.idleWorktreesProvider = fn
}

// SetRepoMap configures the known repo-to-worktree mapping on the Dispatcher.
func (d *Dispatcher) SetRepoMap(m map[string]string) {
	d.repoMap = m
}

// SetWorkspace configures the default host workspace path on the Dispatcher.
func (d *Dispatcher) SetWorkspace(ws string) {
	d.workspace = ws
}

func (d *Dispatcher) isHostMaintenanceActive() bool {
	if d.hostGate == nil {
		return false
	}
	if sg, ok := d.hostGate.(interface{ State() supervisor.GateState }); ok {
		return sg.State() == supervisor.GateStateUpdatingTools
	}
	if mg, ok := d.hostGate.(interface{ IsMaintenanceHeld() bool }); ok {
		return mg.IsMaintenanceHeld()
	}
	return false
}

func (d *Dispatcher) checkPreClaimBarrier(ctx context.Context) (ClaimRequest, bool) {
	if d.isHostMaintenanceActive() {
		return ClaimRequest{}, false
	}
	if d.eagerReadinessGate != nil {
		if ready, _ := d.eagerReadinessGate(ctx); !ready {
			if d.toolUpdater != nil {
				for _, jt := range d.jobTypes {
					if jt == JobTypeToolsUpdate {
						return ClaimRequest{
							HostID:   d.hostID,
							JobTypes: []string{JobTypeToolsUpdate},
						}, true
					}
				}
			}
			return ClaimRequest{}, false
		}
	}
	claimReq := ClaimRequest{
		HostID:   d.hostID,
		JobTypes: d.jobTypes,
	}
	if d.idleWorktreesProvider != nil {
		var idle []string
		for _, wt := range d.idleWorktreesProvider() {
			if trimmed := strings.TrimSpace(wt); trimmed != "" && !d.IsWorktreeInFlight(trimmed) {
				idle = append(idle, trimmed)
			}
		}
		if len(idle) == 0 {
			if d.toolUpdater != nil {
				for _, jt := range d.jobTypes {
					if jt == JobTypeToolsUpdate {
						return ClaimRequest{
							HostID:   d.hostID,
							JobTypes: []string{JobTypeToolsUpdate},
						}, true
					}
				}
			}
			return ClaimRequest{}, false
		}
		claimReq.IdleWorktrees = idle
	}
	return claimReq, true
}

func extractPayloadStringField(payload json.RawMessage, key string) string {
	if len(payload) == 0 {
		return ""
	}
	var pMap map[string]any
	if json.Unmarshal(payload, &pMap) != nil || pMap == nil {
		return ""
	}
	val, _ := pMap[key].(string)
	return strings.TrimSpace(val)
}

func isWorktreeBusyForJob(d *Dispatcher, wt, jobID string) bool {
	trimmed := strings.TrimSpace(wt)
	if trimmed == "" || trimmed == "." {
		return false
	}
	if d != nil && d.IsWorktreeInFlight(trimmed) {
		return true
	}
	cleanWT := filepath.Clean(trimmed)
	return isRepoLockHeldByOther(cleanWT, jobID) || hasActiveWorkspaceSentinel(cleanWT, jobID)
}

func (d *Dispatcher) selectIdleWorktreeFallback(jobID string) string {
	if d == nil || d.idleWorktreesProvider == nil {
		return ""
	}
	for _, wt := range d.idleWorktreesProvider() {
		trimmed := strings.TrimSpace(wt)
		if trimmed != "" && !isWorktreeBusyForJob(d, trimmed, jobID) {
			return trimmed
		}
	}
	return ""
}

func (d *Dispatcher) resolveJobWorktree(job *Job) (string, harness.Invoker) {
	invoker := d.invoker
	if job == nil || job.Type == JobTypeToolsUpdate {
		return ".", invoker
	}
	if explicitWT := strings.TrimSpace(job.WorktreePath); explicitWT != "" {
		if !isWorktreeBusyForJob(d, explicitWT, job.ID) {
			return explicitWT, invoker.WithWorkDir(explicitWT)
		}
		if fallbackWT := d.selectIdleWorktreeFallback(job.ID); fallbackWT != "" {
			return fallbackWT, invoker.WithWorkDir(fallbackWT)
		}
		return explicitWT, invoker.WithWorkDir(explicitWT)
	}
	var resolvedFromCallback string
	if d.worktreeResolver != nil {
		resolvedFromCallback = strings.TrimSpace(d.worktreeResolver(job.Repo))
	}
	if resolvedFromCallback != "" {
		if !isWorktreeBusyForJob(d, resolvedFromCallback, job.ID) {
			return resolvedFromCallback, invoker.WithWorkDir(resolvedFromCallback)
		}
		if fallbackWT := d.selectIdleWorktreeFallback(job.ID); fallbackWT != "" {
			return fallbackWT, invoker.WithWorkDir(fallbackWT)
		}
		return resolvedFromCallback, invoker.WithWorkDir(resolvedFromCallback)
	}
	if payloadWT := extractPayloadStringField(job.Payload, "worktree_path"); payloadWT != "" {
		return payloadWT, invoker.WithWorkDir(payloadWT)
	}
	if fallbackWT := d.selectIdleWorktreeFallback(job.ID); fallbackWT != "" {
		return fallbackWT, invoker.WithWorkDir(fallbackWT)
	}
	return ".", invoker
}

func (d *Dispatcher) acquireWorktreeRepoLock(ctx context.Context, job *Job, worktreeDir, resolvedConvID, initialStoryID string) (*repolock.LockHandle, bool) {
	if worktreeDir == "" || worktreeDir == "." {
		return nil, true
	}
	fi, statErr := os.Stat(worktreeDir)
	if statErr != nil || !fi.IsDir() {
		return nil, true
	}
	if isWorktreeLockedByOther(worktreeDir, job.ID) {
		fmt.Fprintf(os.Stderr, "[dispatcher] declining job %s: worktree %s locked by other\n", job.ID, worktreeDir)
		_ = d.declineJob(ctx, job.ID, reasonRepoBusy)
		return nil, false
	}
	if resolvedConvID == "" {
		if clean, reason := repolock.IsWorktreeCleanForNewJob(worktreeDir, initialStoryID); !clean {
			fmt.Fprintf(os.Stderr, "[dispatcher] declining job %s: worktree %s is quarantined (not clean for new job: %s)\n", job.ID, worktreeDir, reason)
			if span := telemetry.SpanFromContext(ctx); span != nil {
				span.SetTag("decline_reason", reasonRepoDirtyUncompletedASE)
				span.SetTag("worktree.quarantine_reason", reason)
			}
			_ = d.declineJob(ctx, job.ID, reasonRepoDirtyUncompletedASE)
			return nil, false
		}
	}
	h, lockErr := repolock.Acquire(worktreeDir, repolock.LockInfo{
		JobID:          job.ID,
		PID:            os.Getpid(),
		ConversationID: resolvedConvID,
		StoryID:        initialStoryID,
		Status:         "RUNNING",
	}, watchdog.IsProcessAlive)
	if lockErr != nil {
		fmt.Fprintf(os.Stderr, "[dispatcher] declining job %s: repolock.Acquire error on %s: %v\n", job.ID, worktreeDir, lockErr)
		_ = d.declineJob(ctx, job.ID, reasonRepoBusy)
		return nil, false
	}
	return h, true
}

// PollAndProcess claims one job and processes it. Returns (true, err) if a job was processed,
// or (false, nil) if no job was available.
func (d *Dispatcher) PollAndProcess(ctx context.Context) (bool, error) {
	if d.client == nil {
		return false, errors.New("control plane client not configured")
	}
	if d.invoker == nil {
		return false, errors.New("harness invoker not configured")
	}

	claimReq, canClaim := d.checkPreClaimBarrier(ctx)
	if !canClaim {
		return false, nil
	}

	job, err := d.client.ClaimJob(ctx, claimReq)
	if err != nil {
		return false, fmt.Errorf("claim job failed: %w", err)
	}
	if job == nil {
		return false, nil
	}

	var spanOpts []telemetry.SpanOption
	if job.TraceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(job.TraceID, job.ParentID))
	}
	spanOpts = append(spanOpts,
		telemetry.WithTag("job.id", job.ID),
		telemetry.WithTag("job.type", job.Type),
		telemetry.WithTag("job.repo", job.Repo),
		telemetry.WithTag("job.target_agent", job.TargetAgent),
	)
	ctx, span := telemetry.StartSpan(ctx, "dispatcher.processJob", spanOpts...)
	defer span.End()

	worktreeDir, invoker := d.resolveJobWorktree(job)
	if (worktreeDir == "" || worktreeDir == ".") && d.worktreeResolver != nil {
		if job.Repo != "" && job.Repo != "." && (job.Type == JobTypeStoryCreate || job.Type == JobTypeStoryExecute || job.Type == JobTypePromptExecute) {
			if filepath.IsAbs(job.Repo) {
				if fi, err := os.Stat(job.Repo); err == nil && fi.IsDir() {
					worktreeDir = job.Repo
					invoker = invoker.WithWorkDir(job.Repo)
				} else {
					errMsg := fmt.Sprintf("ERR_WORKSPACE_NOT_FOUND: requested repo %q cannot be resolved to any existing workspace on host %s", job.Repo, d.hostID)
					_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: errMsg})
					return true, fmt.Errorf("%s", errMsg)
				}
			} else {
				errMsg := fmt.Sprintf("ERR_WORKSPACE_NOT_FOUND: requested repo %q cannot be resolved to any existing workspace on host %s", job.Repo, d.hostID)
				_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: errMsg})
				return true, fmt.Errorf("%s", errMsg)
			}
		}
	}

	effectiveRepo := job.Repo
	if (effectiveRepo == "" || effectiveRepo == "." || effectiveRepo == "root") && worktreeDir != "" && worktreeDir != "." {
		effectiveRepo = worktreeDir
	}

	// Atomic local worktree reservation BEFORE AckLease:
	// 1. Reserve inFlightPrepJobs (verifying worktreeDir is not already in inFlightPrepJobs or activeJobs)
	// 2. Acquire hostGate execution lease
	// 3. Acquire repolock on worktreeDir
	if !d.tryReserveInFlightWorktree(job.ID, worktreeDir) {
		fmt.Fprintf(os.Stderr, "[dispatcher] declining job %s: worktree %s already in-flight\n", job.ID, worktreeDir)
		_ = d.declineJob(ctx, job.ID, reasonRepoBusy)
		return false, nil
	}
	defer d.clearInFlightWorktree(job.ID)

	var relGateLease func()
	if d.hostGate != nil && (job.Type == JobTypeStoryCreate || job.Type == JobTypeStoryExecute || job.Type == JobTypePromptExecute) {
		var leaseErr error
		relGateLease, leaseErr = d.hostGate.AcquireExecutionLease(ctx, job.ID, effectiveRepo, job.FencingToken)
		if leaseErr != nil {
			d.clearInFlightWorktree(job.ID)
			if job.LeaseToken != "" || (worktreeDir != "" && worktreeDir != ".") {
				fmt.Fprintf(os.Stderr, "[dispatcher] declining job %s: hostGate lease error: %v\n", job.ID, leaseErr)
				_ = d.declineJob(ctx, job.ID, reasonRepoBusy)
				return false, nil
			}
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: leaseErr.Error()})
			return true, leaseErr
		}
		defer func() {
			if relGateLease != nil {
				relGateLease()
			}
		}()
	}

	resolvedConvID := job.ResolvedConversationID()
	initialStoryID := strings.TrimSpace(job.StoryID)
	if initialStoryID == "" {
		initialStoryID = extractPayloadStringField(job.Payload, "story_id")
	}

	repoLockHandle, lockOK := d.acquireWorktreeRepoLock(ctx, job, worktreeDir, resolvedConvID, initialStoryID)
	if !lockOK {
		if relGateLease != nil {
			relGateLease()
			relGateLease = nil
		}
		d.clearInFlightWorktree(job.ID)
		return false, nil
	}
	if repoLockHandle != nil {
		defer func() {
			_ = repoLockHandle.Release()
		}()
	}

	d.setActiveJob(job.ID, worktreeDir, os.Getpid())
	defer d.clearActiveJob(job.ID)

	// Phase 2 of Two-Phase Lease Handshake: Acknowledge lease immediately after local worktree reservation
	// (before potentially long-running 52-submodule workspace sanitization or preDispatchHook) so the 20s
	// LeaseAckDeadlineAt never expires during workspace preparation.
	if job.LeaseToken != "" {
		var ackErr error
		if wtAcker, ok := d.client.(interface {
			AckLeaseWithWorktree(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID, worktreePath string) error
		}); ok {
			ackErr = wtAcker.AckLeaseWithWorktree(ctx, job.ID, job.LeaseToken, os.Getpid(), resolvedConvID, worktreeDir)
		} else {
			ackErr = d.client.AckLease(ctx, job.ID, job.LeaseToken, os.Getpid(), resolvedConvID)
		}
		if ackErr != nil {
			fmt.Fprintf(os.Stderr, "[dispatcher] ack lease warning for job %s: %v\n", job.ID, ackErr)
		}
	}

	var allocatedStoryMu sync.Mutex
	currentStoryID := initialStoryID
	currentConvID := resolvedConvID

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	if job.FencingToken > 0 || job.LeaseToken != "" {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-hbCtx.Done():
					return
				case <-ticker.C:
					allocatedStoryMu.Lock()
					sid := currentStoryID
					cid := currentConvID
					allocatedStoryMu.Unlock()
					if err := d.client.HeartbeatJob(hbCtx, job.ID, job.LeaseToken, job.FencingToken, sid, worktreeDir, cid); err != nil {
						// Upstream control-plane errors (including 409 LEASE_FENCED after a control-plane redeploy/restart,
						// 5xx, or transient network blips) must NEVER kill an active local agy/iazio-harness execution
						// (job_38eeeb536a5a7130 root cause). Re-ack the lease to re-synchronize with the control plane.
						_ = d.client.AckLease(hbCtx, job.ID, job.LeaseToken, os.Getpid(), cid)
					}
				}
			}
		}()
	}

	isResumeJob := job.ParentJobID != "" || resolvedConvID != "" || job.Attempts > 1 || job.InfraRequeueCount > 0 || job.Type == "resume" || strings.Contains(string(job.Payload), `"kind":"resume"`) || strings.Contains(string(job.Payload), `"Kind":"resume"`) || strings.Contains(string(job.Payload), `"parent_job_id"`) || strings.Contains(string(job.Payload), `"resume_conversation_id"`)

	// Pre-job workspace sanitization: non-destructive lock clearing, interrupted git ops recovery, dirty patch archiving, and line ending hygiene
	if worktreeDir != "" && worktreeDir != "." {
		_, wsPrepSpan := telemetry.StartSpan(ctx, "agent.workspace_prep",
			telemetry.WithTag("job.id", job.ID),
			telemetry.WithTag("workspace", worktreeDir),
		)
		if isResumeJob {
			// For resume / remediation jobs: preserve dirty worktree and branch, only reap stale locks and orphan processes
			if sanRes, sanErr := workspace.SanitizeWorkspace(ctx, worktreeDir); sanErr != nil {
				fmt.Fprintf(os.Stderr, "[dispatcher] pre-resume sanitize warning: %v\n", sanErr)
			} else if sanRes != nil && len(sanRes.RemovedLockPaths) > 0 {
				fmt.Fprintf(os.Stderr, "[dispatcher] pre-resume sanitize removed %d stale locks\n", len(sanRes.RemovedLockPaths))
			}
		} else {
			var targetBranch string
			if len(job.Payload) > 0 {
				var pMap map[string]any
				if json.Unmarshal(job.Payload, &pMap) == nil {
					if b, ok := pMap["branch"].(string); ok {
						targetBranch = strings.TrimSpace(b)
					}
				}
			}
			if sanRes, sanErr := workspace.ResetAndSanitizeFreshWorkspace(ctx, worktreeDir, job.ID, targetBranch); sanErr != nil {
				fmt.Fprintf(os.Stderr, "[dispatcher] pre-job sanitize warning: %v\n", sanErr)
			} else if sanRes != nil && len(sanRes.RemovedLockPaths) > 0 {
				fmt.Fprintf(os.Stderr, "[dispatcher] pre-job sanitize removed %d stale locks\n", len(sanRes.RemovedLockPaths))
			}
		}
		if wsPrepSpan != nil {
			wsPrepSpan.End()
		}
		if cfgErr := procutil.CommandContext(ctx, "git", "-C", worktreeDir, "config", "core.autocrlf", "input").Run(); cfgErr != nil {
			fmt.Fprintf(os.Stderr, "[dispatcher] git autocrlf config warning: %v\n", cfgErr)
		}
	}

	var execCtx context.Context
	var execCancel context.CancelFunc
	if d.jobTimeout > 0 {
		execCtx, execCancel = context.WithTimeout(ctx, d.jobTimeout)
	} else {
		execCtx, execCancel = context.WithCancel(ctx)
	}
	defer execCancel()

	onStoryDiscovered := func(storyID string) {
		clean := strings.TrimSpace(storyID)
		if clean == "" || clean == "pending-allocation" || clean == "pending_harness_creation" {
			return
		}
		allocatedStoryMu.Lock()
		if currentStoryID == clean {
			allocatedStoryMu.Unlock()
			return
		}
		currentStoryID = clean
		cid := currentConvID
		allocatedStoryMu.Unlock()

		_ = d.client.BindStory(ctx, job.ID, StoryBindingRequest{
			FencingToken: job.FencingToken,
			StoryID:      clean,
		})
		if job.FencingToken > 0 || job.LeaseToken != "" {
			_ = d.client.HeartbeatJob(ctx, job.ID, job.LeaseToken, job.FencingToken, clean, worktreeDir, cid)
		}
	}

	onConversationDiscovered := func(convID string) {
		clean := strings.TrimSpace(convID)
		if clean == "" || clean == "pending" || clean == "none" || clean == "new_conversation" || clean == "pending-allocation" {
			return
		}
		allocatedStoryMu.Lock()
		if currentConvID == clean {
			allocatedStoryMu.Unlock()
			return
		}
		currentConvID = clean
		sid := currentStoryID
		allocatedStoryMu.Unlock()

		_ = d.client.HeartbeatJob(ctx, job.ID, job.LeaseToken, job.FencingToken, sid, worktreeDir, clean)
		if job.LeaseToken != "" {
			_ = d.client.AckLease(ctx, job.ID, job.LeaseToken, os.Getpid(), clean)
		}
	}

	if aware, ok := invoker.(interface {
		WithStoryCallback(func(string)) harness.Invoker
	}); ok {
		invoker = aware.WithStoryCallback(onStoryDiscovered)
	}
	if convAware, ok := invoker.(interface {
		WithConversationCallback(func(string)) harness.Invoker
	}); ok {
		invoker = convAware.WithConversationCallback(onConversationDiscovered)
	}

	defer func() {
		if d.postJobHook != nil && job != nil {
			d.postJobHook(ctx, job)
		}
	}()

	// Pre-dispatch hook: evaluate eager tool update before launching next job (starvation prevention)
	if d.preDispatchHook != nil && job.Type != JobTypeToolsUpdate {
		if hookErr := d.preDispatchHook(execCtx, job); hookErr != nil {
			span.SetTag("pre_dispatch_hook.error", hookErr.Error())
		}
	}

	switch job.Type {
	case JobTypeStoryCreate:
		res, err := invoker.CreateStory(execCtx, job.Payload)
		var storyConvIDs []string
		if res != nil {
			storyConvIDs = append(storyConvIDs, res.ConversationID, res.ActiveConversationID)
		}
		finalTurns, finalSHA := d.flushConversationHarvest(ctx, job, storyConvIDs...)
		if err != nil {
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{
				FencingToken:          job.FencingToken,
				Error:                 err.Error(),
				FinalTurnCount:        finalTurns,
				FinalTranscriptSHA256: finalSHA,
			})
			return true, err
		}
		resBytes, _ := json.Marshal(res)
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{
			FencingToken:          job.FencingToken,
			Result:                resBytes,
			FinalTurnCount:        finalTurns,
			FinalTranscriptSHA256: finalSHA,
		})
		if compErr == nil && d.syncer != nil {
			targetRepo := effectiveRepo
			if targetRepo == "" && res != nil {
				targetRepo = res.Submodule
			}
			if targetRepo != "" {
				d.syncer.TriggerRepoSync(targetRepo)
			}
		}
		return true, compErr

	case JobTypePromptExecute, JobTypeStoryExecute:
		toolName := job.TargetAgent
		if toolName == "" {
			toolName = "default"
		}
		_, toolSpan := telemetry.StartSpan(ctx, "agent.tool_execution."+toolName,
			telemetry.WithTag("job.id", job.ID),
			telemetry.WithTag("tool", toolName),
		)
		allocatedStoryMu.Lock()
		sidForPayload := currentStoryID
		allocatedStoryMu.Unlock()
		payload := harness.EnsurePromptPayloadWithJob(job.ID, job.TraceID, job.TargetAgent, effectiveRepo, sidForPayload, job.Payload)
		var m map[string]any
		if json.Unmarshal(payload, &m) == nil && m != nil {
			mutated := false
			if worktreeDir != "" && worktreeDir != "." {
				m["worktree_path"] = worktreeDir
				mutated = true
			}
			if job.FencingToken > 0 {
				m["fencing_token"] = job.FencingToken
				mutated = true
			}
			if job.LeaseToken != "" {
				m["lease_token"] = job.LeaseToken
				mutated = true
			}
			if resolvedConvID != "" {
				if existing, _ := m["resume_conversation_id"].(string); strings.TrimSpace(existing) == "" {
					m["resume_conversation_id"] = resolvedConvID
					mutated = true
				}
				if existing, _ := m["conversation_id"].(string); strings.TrimSpace(existing) == "" {
					m["conversation_id"] = resolvedConvID
					mutated = true
				}
			}
			if httpCli, ok := d.client.(*HTTPControlPlaneClient); ok && httpCli != nil && httpCli.BaseURL != "" {
				if _, exists := m["api_url"]; !exists {
					m["api_url"] = httpCli.BaseURL
					mutated = true
				}
				if tok := httpCli.bearerToken(ctx); tok != "" {
					if _, exists := m["token"]; !exists {
						m["token"] = tok
						mutated = true
					}
				}
			}
			if mutated {
				if b, mErr := json.Marshal(m); mErr == nil {
					payload = b
				}
			}
		}
		startTime := time.Now()
		const maxSupervisorAttempts = 3
		var res *harness.PromptExecutionResult
		var err error
		for supAttempt := 1; supAttempt <= maxSupervisorAttempts; supAttempt++ {
			res, err = invoker.ExecutePrompt(execCtx, payload)
			if err == nil && res != nil {
				combinedLower := strings.ToLower(res.Summary + "\n" + res.Output)
				if strings.Contains(combinedLower, "jetski: no output produced") ||
					strings.Contains(combinedLower, "headless mode cannot prompt for") ||
					strings.Contains(combinedLower, "so it was auto-denied") {
					err = fmt.Errorf("PERMISSION_DENIED: headless agent tool permission was auto-denied: %s", strings.TrimSpace(res.Summary))
				} else if strings.EqualFold(res.Status, "failed") || res.ExitCode != 0 {
					err = fmt.Errorf("prompt execution failed (status=%s exit=%d): %s", res.Status, res.ExitCode, strings.TrimSpace(res.Summary))
				}
			}
			if err == nil || supAttempt == maxSupervisorAttempts || execCtx.Err() != nil {
				break
			}
			errLower := strings.ToLower(err.Error())
			if strings.Contains(err.Error(), "PERMISSION_DENIED") ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, context.Canceled) ||
				strings.Contains(errLower, "429") ||
				strings.Contains(errLower, "quota") {
				break
			}
			// Mutual supervision: iazio-harness or agy crashed/exited non-zero locally.
			// Re-ack/heartbeat the job lease and retry in-place (attaching to discovered conversation on attempt 2,
			// or falling back to a new conversation on attempt 3 if conversation attach also failed).
			var discoveredStory, discoveredConv string
			if rawSID, sErr := os.ReadFile(filepath.Join(worktreeDir, ".session_story_id")); sErr == nil {
				discoveredStory = strings.TrimSpace(string(rawSID))
			}
			if res != nil {
				if conIDs := extractConversationIDFromResult(res); len(conIDs) > 0 {
					discoveredConv = conIDs[0]
				}
				if s := strings.TrimSpace(res.StoryID); s != "" {
					discoveredStory = s
				} else if s := extractStoryID(res.Summary, res.Output); s != "" && discoveredStory == "" {
					discoveredStory = s
				}
			}
			if discoveredStory != "" {
				onStoryDiscovered(discoveredStory)
			}
			var retryMap map[string]any
			if json.Unmarshal(payload, &retryMap) == nil && retryMap != nil {
				if discoveredStory != "" {
					retryMap["story_id"] = discoveredStory
				}
				if supAttempt == 1 && discoveredConv != "" {
					resolvedConvID = discoveredConv
					retryMap["resume_conversation_id"] = discoveredConv
					retryMap["conversation_id"] = discoveredConv
				} else if supAttempt >= 2 {
					// Previous conversation resume also failed -> fallback to a new conversation in the ASE flow
					resolvedConvID = ""
					delete(retryMap, "resume_conversation_id")
					delete(retryMap, "conversation_id")
					if discoveredStory != "" {
						retryMap["prompt"] = fmt.Sprintf("Resume and complete /autonomous-story-execution for %s in a new conversation, inspecting existing workspace state, running verification gates, review scheme, and git-flow landing.", discoveredStory)
					}
				}
				if updatedPayload, mErr := json.Marshal(retryMap); mErr == nil {
					payload = updatedPayload
				}
			}
			_ = d.client.HeartbeatJob(ctx, job.ID, job.LeaseToken, job.FencingToken, discoveredStory, worktreeDir)
		}
		if toolSpan != nil {
			toolSpan.End()
		}
		if err != nil {
			elapsed := time.Since(startTime)
			if execCtx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && elapsed < 45*time.Second && job.TargetAgent != "" {
				category := "TOOL_CRASH"
				direction := "LOCAL_EXECUTION"
				service := "iazio-harness"
				errLower := strings.ToLower(err.Error())
				if strings.Contains(errLower, "429") || strings.Contains(errLower, "quota") || strings.Contains(errLower, "auth") || strings.Contains(errLower, "token") {
					category = "TOOL_QUOTA_OR_AUTH"
					direction = "UPSTREAM"
					service = "antigravity-llm-provider"
				}
				attemptReq := controlplane.AttemptFailure{
					Host:             d.hostID,
					Tool:             job.TargetAgent,
					Workspace:        job.Repo,
					ExitCode:         1,
					StderrTail:       err.Error(),
					FailureCategory:  category,
					FailureStage:     "TOOL_EXECUTION",
					FailureDirection: direction,
					FailureService:   service,
					Reason:           "PREFLIGHT_OR_EARLY_TOOL_EXIT",
					Timestamp:        time.Now().UTC().Format(time.RFC3339),
				}
				if foErr := d.client.FailoverJob(ctx, job.ID, attemptReq); foErr == nil {
					return true, err
				}
			}
			finalTurns, finalSHA := d.flushConversationHarvest(ctx, job, extractConversationIDFromResult(res)...)
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{
				FencingToken:          job.FencingToken,
				Error:                 err.Error(),
				FinalTurnCount:        finalTurns,
				FinalTranscriptSHA256: finalSHA,
			})
			return true, err
		}
		_, gateSpan := telemetry.StartSpan(ctx, "agent.ase_completion_gate",
			telemetry.WithTag("job.id", job.ID),
		)
		if res != nil {
			allocatedStoryMu.Lock()
			discoveredCID := currentConvID
			allocatedStoryMu.Unlock()
			if res.ConversationID == "" && discoveredCID != "" {
				res.ConversationID = discoveredCID
			}
			if res.ActiveConversationID == "" && discoveredCID != "" {
				res.ActiveConversationID = discoveredCID
			}
			storyID := res.StoryID
			if storyID == "" {
				storyID = extractStoryID(res.Summary, res.Output)
			}
			if storyID != "" {
				allocatedStoryMu.Lock()
				currentStoryID = storyID
				allocatedStoryMu.Unlock()
				_ = d.client.BindStory(ctx, job.ID, StoryBindingRequest{
					FencingToken: job.FencingToken,
					StoryID:      storyID,
					StoryTitle:   res.StoryTitle,
					StoryPath:    res.StoryPath,
				})
			}
		}
		finalTurns, finalSHA := d.flushConversationHarvest(ctx, job, extractConversationIDFromResult(res)...)

		// STORY-GOV-5232: Enforce strict ASE completion gate invariants.
		// A job executing an autonomous story execution flow MUST:
		// 1. Have bound a valid story ID
		// 2. Have zero dirty files in worktree
		// 3. Have zero unpushed commits in worktree
		// 4. Not remain on an unmerged feature branch
		var aseGateErr error
		if isASEJob(job) && worktreeDir != "" && worktreeDir != "." {
			targetStoryID := ""
			if res != nil {
				targetStoryID = res.StoryID
				if targetStoryID == "" {
					targetStoryID = extractStoryID(res.Summary, res.Output)
				}
			}
			if targetStoryID == "" {
				allocatedStoryMu.Lock()
				targetStoryID = currentStoryID
				allocatedStoryMu.Unlock()
			}
			if targetStoryID == "" {
				aseGateErr = fmt.Errorf("ASE_COMPLETION_GATE_VIOLATION: job %s executed an autonomous story execution flow but no story ID was bound or allocated", job.ID)
			} else {
				if repolock.IsWorktreeDirty(worktreeDir) {
					aseGateErr = fmt.Errorf("ASE_COMPLETION_GATE_VIOLATION: worktree %s is dirty with uncommitted changes after ASE run", worktreeDir)
				} else if onFeat, featBranch := repolock.IsOnFeatureBranch(worktreeDir); onFeat {
					aseGateErr = fmt.Errorf("ASE_COMPLETION_GATE_VIOLATION: worktree %s remains on unmerged feature branch %q after ASE run", worktreeDir, featBranch)
				} else if unpushed, unpushedErr := repolock.HasUnpushedCommits(worktreeDir); unpushedErr != nil {
					aseGateErr = fmt.Errorf("ASE_COMPLETION_GATE_VIOLATION: unpushed commit probe failed for worktree %s: %w", worktreeDir, unpushedErr)
				} else if unpushed {
					aseGateErr = fmt.Errorf("ASE_COMPLETION_GATE_VIOLATION: worktree %s has unpushed commits after ASE run; story was not landed or pushed", worktreeDir)
				}
			}
		}

		if aseGateErr != nil {
			if gateSpan != nil {
				gateSpan.SetTag("error", "true")
				gateSpan.SetTag("error.message", aseGateErr.Error())
				gateSpan.End()
			}
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{
				FencingToken:          job.FencingToken,
				Error:                 aseGateErr.Error(),
				FinalTurnCount:        finalTurns,
				FinalTranscriptSHA256: finalSHA,
			})
			return true, aseGateErr
		}

		// STORY-GOV-5232 / AC3: Enforce strict serial lifecycle sequence:
		// 1. ASE Flow Completion Gate passed (checked above).
		// 2. Synchronous Tool & Canonical AI Update and Repository Synchronization.
		// 3. Mark job complete via CompleteJob, releasing worktree for next-story dequeue.
		targetRepo := job.Repo
		if targetRepo == "" && res != nil {
			targetRepo = res.Repo
		}
		if isASEJob(job) {
			var postAseSyncErr error
			if d.toolUpdater != nil {
				_, tuSpan := telemetry.StartSpan(ctx, "agent.ase_tool_update",
					telemetry.WithTag("job.id", job.ID),
					telemetry.WithTag("worktree", worktreeDir),
				)
				updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 30*time.Second)
				tuErr := d.toolUpdater(updateCtx, nil, false)
				cancelUpdate()
				if tuErr != nil {
					postAseSyncErr = fmt.Errorf("tool update failed: %w", tuErr)
					if tuSpan != nil {
						tuSpan.SetTag("error", "true")
						tuSpan.SetTag("error.message", tuErr.Error())
					}
					fmt.Fprintf(os.Stderr, "[dispatcher] warning: post-ASE tool update failed for job %s: %v\n", job.ID, tuErr)
				}
				if tuSpan != nil {
					tuSpan.End()
				}
			}
			if d.syncer != nil && targetRepo != "" {
				_, syncSpan := telemetry.StartSpan(ctx, "agent.ase_repo_sync",
					telemetry.WithTag("job.id", job.ID),
					telemetry.WithTag("repo", targetRepo),
				)
				syncCtx, cancelSync := context.WithTimeout(context.Background(), 15*time.Second)
				if syncSyncer, ok := d.syncer.(interface {
					SyncRepoSync(context.Context, string) error
				}); ok {
					if syncErr := syncSyncer.SyncRepoSync(syncCtx, targetRepo); syncErr != nil {
						if postAseSyncErr == nil {
							postAseSyncErr = fmt.Errorf("repo sync failed: %w", syncErr)
						}
						if syncSpan != nil {
							syncSpan.SetTag("error", "true")
							syncSpan.SetTag("error.message", syncErr.Error())
						}
						fmt.Fprintf(os.Stderr, "[dispatcher] warning: synchronous post-ASE repo sync failed for %s: %v\n", targetRepo, syncErr)
					}
				} else {
					d.syncer.TriggerRepoSync(targetRepo)
				}
				cancelSync()
				if syncSpan != nil {
					syncSpan.End()
				}
			}

			// If tool update or repo sync failed, quarantine the worktree per Rule B90 §2
			if postAseSyncErr != nil {
				markerDir := filepath.Join(worktreeDir, ".iazio")
				_ = os.MkdirAll(markerDir, 0o755)
				_ = os.WriteFile(filepath.Join(markerDir, "post-ase-sync-failed"), []byte(postAseSyncErr.Error()+"\n"), 0o644)
			} else {
				_ = os.Remove(filepath.Join(worktreeDir, ".iazio", "post-ase-sync-failed"))
			}
		}

		resBytes, _ := json.Marshal(res)
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{
			FencingToken:          job.FencingToken,
			Result:                resBytes,
			FinalTurnCount:        finalTurns,
			FinalTranscriptSHA256: finalSHA,
		})
		if gateSpan != nil {
			gateSpan.End()
		}
		if compErr == nil && !isASEJob(job) && d.syncer != nil && targetRepo != "" {
			d.syncer.TriggerRepoSync(targetRepo)
		}
		return true, compErr

	case JobTypeToolsUpdate:
		start := time.Now()
		var p struct {
			Tools         []string `json:"tools"`
			Force         bool     `json:"force"`
			TargetVersion string   `json:"target_version,omitempty"`
		}
		_ = json.Unmarshal(job.Payload, &p)
		if d.toolUpdater != nil {
			if err := d.toolUpdater(execCtx, p.Tools, p.Force, p.TargetVersion); err != nil {
				if errors.Is(err, ErrJobDeferred) {
					compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{
						FencingToken: job.FencingToken,
						Result:       json.RawMessage(`{"status":"queued","deferred":true,"message":"tool update queued for execution when harness is idle","summary":"Tool update queued for execution when harness is idle"}`),
					})
					return true, compErr
				}
				_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: err.Error()})
				return true, err
			}
		}
		elapsedSec := int64(time.Since(start).Seconds())
		if elapsedSec <= 0 {
			elapsedSec = 1
		}
		toolsLabel := "all"
		if len(p.Tools) > 0 {
			toolsLabel = strings.Join(p.Tools, ", ")
		}
		summary := fmt.Sprintf("Completed agent.tools.update on host %s (tools=%s, force=%v)", d.hostID, toolsLabel, p.Force)
		resPayload, _ := json.Marshal(map[string]any{
			"status":          "completed",
			"updated":         true,
			"tools":           p.Tools,
			"force":           p.Force,
			"target_version":  p.TargetVersion,
			"host_id":         d.hostID,
			"agent_pid":       os.Getpid(),
			"harness_pid":     os.Getpid(),
			"elapsed_seconds": elapsedSec,
			"summary":         summary,
			"output":          summary,
		})
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{
			FencingToken: job.FencingToken,
			Result:       resPayload,
		})
		return true, compErr

	default:
		unsupportedErr := fmt.Sprintf("unsupported job type: %s", job.Type)
		_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: unsupportedErr})
		return true, errors.New(unsupportedErr)
	}
}

// EnsurePromptID ensures that a prompt payload JSON contains a valid prompt_id field.
func EnsurePromptID(jobID string, raw json.RawMessage) json.RawMessage {
	return harness.EnsurePromptPayload(jobID, raw)
}

// EnsurePromptIDWithTrace ensures that a prompt payload JSON contains a valid prompt_id and trace_id.
func EnsurePromptIDWithTrace(jobID, traceID string, raw json.RawMessage) json.RawMessage {
	return harness.EnsurePromptPayloadWithTrace(jobID, traceID, raw)
}

var storyIDRegex = regexp.MustCompile(`(?i)\b(STORY-[A-Z0-9]+(?:-[A-Z0-9]+)*)\b`)

func extractStoryID(texts ...string) string {
	for _, text := range texts {
		if text == "" {
			continue
		}
		if match := storyIDRegex.FindString(text); match != "" {
			return strings.ToUpper(match)
		}
	}
	return ""
}

func extractConversationID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil {
		for _, key := range []string{"conversation_id", "active_conversation_id", "resume_conversation_id", "initial_conversation_id", "session_id"} {
			if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

var convIDTextRegex = regexp.MustCompile(`(?i)"?(?:active_)?conversation_id"?\s*[:=]\s*"?([a-zA-Z0-9_-]{4,})"?`)

func extractConversationIDFromResult(res *harness.PromptExecutionResult) []string {
	if res == nil {
		return nil
	}
	var out []string
	for _, v := range []string{res.ConversationID, res.ActiveConversationID} {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	for _, txt := range []string{res.Output, res.Summary} {
		if trimmed := strings.TrimSpace(txt); trimmed != "" {
			if cid := extractConversationID([]byte(trimmed)); cid != "" {
				out = append(out, cid)
			} else if m := convIDTextRegex.FindStringSubmatch(trimmed); len(m) > 1 && strings.TrimSpace(m[1]) != "" {
				out = append(out, strings.TrimSpace(m[1]))
			}
		}
	}
	return out
}

func (d *Dispatcher) flushConversationHarvest(ctx context.Context, job *Job, extraConvIDs ...string) (int, string) {
	if d.flusher == nil || job == nil {
		return 0, ""
	}
	var convID string
	for _, cand := range extraConvIDs {
		if trimmed := strings.TrimSpace(cand); trimmed != "" {
			convID = trimmed
			break
		}
	}
	if convID == "" {
		convID = strings.TrimSpace(job.ActiveConversationID)
	}
	if convID == "" {
		convID = strings.TrimSpace(job.ConversationID)
	}
	if convID == "" {
		convID = extractConversationID(job.Payload)
	}
	if convID == "" {
		return 0, ""
	}
	turns, sha, _ := d.flusher(ctx, convID)
	return turns, sha
}

func isASEJob(job *Job) bool {
	if job == nil {
		return false
	}
	if job.Type == JobTypeStoryExecute {
		return true
	}
	var p struct {
		Prompt          string `json:"prompt"`
		EffectiveAction string `json:"effective_action"`
		Command         string `json:"command"`
	}
	if err := json.Unmarshal(job.Payload, &p); err == nil {
		if strings.TrimSpace(p.EffectiveAction) == "/autonomous-story-execution" {
			return true
		}
		trimmedPrompt := strings.TrimSpace(p.Prompt)
		if strings.HasPrefix(trimmedPrompt, "/autonomous-story-execution") || strings.HasPrefix(trimmedPrompt, "autopilot ase") {
			return true
		}
		trimmedCmd := strings.TrimSpace(p.Command)
		if strings.HasPrefix(trimmedCmd, "/autonomous-story-execution") || strings.HasPrefix(trimmedCmd, "autopilot ase") {
			return true
		}
		return false
	}
	raw := strings.TrimSpace(string(job.Payload))
	return strings.HasPrefix(raw, "/autonomous-story-execution") || strings.HasPrefix(raw, "autopilot ase")
}
