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
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/telemetry"
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
	ID                   string          `json:"id"`
	Type                 string          `json:"type"`
	Repo                 string          `json:"repo"`
	TargetHost           string          `json:"target_host,omitempty"`
	TargetAgent          string          `json:"target_agent,omitempty"`
	Payload              json.RawMessage `json:"payload"`
	TraceID              string          `json:"trace_id,omitempty"`
	ParentID             string          `json:"parent_id,omitempty"`
	StoryID              string          `json:"story_id,omitempty"`
	FencingToken         int64           `json:"fencing_token,omitempty"`
	LeaseToken           string          `json:"lease_token,omitempty"`
	LeaseAckDeadlineAt   string          `json:"lease_ack_deadline_at,omitempty"`
	Traceparent          string          `json:"traceparent,omitempty"`
	ZipkinTraceID        string          `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL       string          `json:"zipkin_trace_url,omitempty"`
	InfraRequeueCount    int             `json:"infra_requeue_count,omitempty"`
	FailureCategory      string          `json:"failure_category,omitempty"`
	ActiveConversationID string          `json:"active_conversation_id,omitempty"`
	ConversationID       string          `json:"conversation_id,omitempty"`
}

// ClaimRequest is sent to claim pending jobs.
type ClaimRequest struct {
	HostID   string   `json:"host_id,omitempty"`
	JobTypes []string `json:"job_types,omitempty"`
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
	FencingToken int64  `json:"fencing_token,omitempty"`
	LeaseToken   string `json:"lease_token,omitempty"`
	StoryID      string `json:"story_id,omitempty"`
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
	BaseURL    string
	AuthToken  string
	HTTPClient *http.Client
	Spooler    *controlplane.OfflineReportSpooler
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
		BaseURL:    baseURL,
		AuthToken:  authToken,
		HTTPClient: client,
		Spooler:    sp,
	}
}

// ClaimJob calls POST /api/v1/agent/jobs/claim.
func (c *HTTPControlPlaneClient) ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error) {
	url := c.BaseURL + "/api/v1/agent/jobs/claim"
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal claim request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create claim request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create complete request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		if c.Spooler != nil {
			headers := map[string]string{"Content-Type": "application/json"}
			if c.AuthToken != "" {
				headers["Authorization"] = "Bearer " + c.AuthToken
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
			if c.AuthToken != "" {
				headers["Authorization"] = "Bearer " + c.AuthToken
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
	if len(storyID) > 0 {
		sid = storyID[0]
	}
	bodyBytes, err := json.Marshal(HeartbeatRequest{FencingToken: fencingToken, LeaseToken: leaseToken, StoryID: sid})
	if err != nil {
		return fmt.Errorf("marshal heartbeat request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create heartbeat request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create bind story request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
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
	url := fmt.Sprintf("%s/api/v1/agent/jobs/%s/ack-lease", c.BaseURL, jobID)
	payload := map[string]any{
		"lease_token":            leaseToken,
		"agent_pid":              agentPID,
		"active_conversation_id": activeConvID,
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ack lease request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create ack lease request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create failover request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}

	resp, err := c.HTTPClient.Do(httpReq)
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
	client           ControlPlaneClient
	invoker          harness.Invoker
	hostID           string
	jobTypes         []string
	syncer           RepoSyncer
	worktreeResolver WorktreeResolver
	toolUpdater      ToolUpdater
	preDispatchHook  PreDispatchHook
	postJobHook      PostJobHook
	hostGate         HostExecutionGate
	jobTimeout       time.Duration
	flusher          HarvestFlusher
}

// NewDispatcher constructs a new Dispatcher, optionally accepting a RepoSyncer for immediate post-job sync hooks.
func NewDispatcher(client ControlPlaneClient, invoker harness.Invoker, hostID string, syncer ...RepoSyncer) *Dispatcher {
	var s RepoSyncer
	if len(syncer) > 0 {
		s = syncer[0]
	}
	return &Dispatcher{
		client:     client,
		invoker:    invoker,
		hostID:     hostID,
		jobTypes:   []string{JobTypeStoryCreate, JobTypeStoryExecute, JobTypePromptExecute, JobTypeToolsUpdate},
		syncer:     s,
		jobTimeout: 0,
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

// PollAndProcess claims one job and processes it. Returns (true, err) if a job was processed,
// or (false, nil) if no job was available.
func (d *Dispatcher) PollAndProcess(ctx context.Context) (bool, error) {
	if d.client == nil {
		return false, errors.New("control plane client not configured")
	}
	if d.invoker == nil {
		return false, errors.New("harness invoker not configured")
	}

	job, err := d.client.ClaimJob(ctx, ClaimRequest{
		HostID:   d.hostID,
		JobTypes: d.jobTypes,
	})
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

	invoker := d.invoker
	worktreeDir := "."
	if d.worktreeResolver != nil {
		if wt := d.worktreeResolver(job.Repo); wt != "" {
			worktreeDir = wt
			invoker = invoker.WithWorkDir(wt)
		} else if job.Repo != "" && job.Repo != "." && (job.Type == JobTypeStoryCreate || job.Type == JobTypeStoryExecute || job.Type == JobTypePromptExecute) {
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

	// Pre-job workspace sanitization: non-destructive lock clearing, interrupted git ops recovery, dirty patch archiving, and line ending hygiene
	if worktreeDir != "" && worktreeDir != "." {
		_, wsPrepSpan := telemetry.StartSpan(ctx, "agent.workspace_prep",
			telemetry.WithTag("job.id", job.ID),
			telemetry.WithTag("workspace", worktreeDir),
		)
		isResumeJob := job.ParentID != "" || job.Type == "resume" || strings.Contains(string(job.Payload), `"kind":"resume"`) || strings.Contains(string(job.Payload), `"Kind":"resume"`) || strings.Contains(string(job.Payload), `"parent_job_id"`)
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
		if cfgErr := exec.CommandContext(ctx, "git", "-C", worktreeDir, "config", "core.autocrlf", "input").Run(); cfgErr != nil {
			fmt.Fprintf(os.Stderr, "[dispatcher] git autocrlf config warning: %v\n", cfgErr)
		}
	}

	// Phase 2 of Two-Phase Lease Handshake: Acknowledge lease to transition from LEASED to RUNNING
	if job.LeaseToken != "" {
		if ackErr := d.client.AckLease(ctx, job.ID, job.LeaseToken, os.Getpid(), ""); ackErr != nil {
			fmt.Fprintf(os.Stderr, "[dispatcher] ack lease warning for job %s: %v\n", job.ID, ackErr)
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

	var allocatedStoryMu sync.Mutex
	currentStoryID := job.StoryID
	if currentStoryID == "" && len(job.Payload) > 0 {
		var pMap map[string]any
		if err := json.Unmarshal(job.Payload, &pMap); err == nil {
			if sid, ok := pMap["story_id"].(string); ok {
				currentStoryID = sid
			}
		}
	}

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
		allocatedStoryMu.Unlock()

		_ = d.client.BindStory(ctx, job.ID, StoryBindingRequest{
			FencingToken: job.FencingToken,
			StoryID:      clean,
		})
	}

	if aware, ok := invoker.(interface {
		WithStoryCallback(func(string)) harness.Invoker
	}); ok {
		invoker = aware.WithStoryCallback(onStoryDiscovered)
	}

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
					allocatedStoryMu.Unlock()
					if err := d.client.HeartbeatJob(hbCtx, job.ID, job.LeaseToken, job.FencingToken, sid); err != nil {
						if strings.Contains(err.Error(), "409") || strings.Contains(err.Error(), "LEASE_FENCED") {
							execCancel()
							return
						}
					}
				}
			}
		}()
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

	// Shared execution lease acquisition for harness jobs (prevents collision with binary updates)
	if d.hostGate != nil && (job.Type == JobTypeStoryCreate || job.Type == JobTypeStoryExecute || job.Type == JobTypePromptExecute) {
		relLease, leaseErr := d.hostGate.AcquireExecutionLease(execCtx, job.ID, job.Repo, job.FencingToken)
		if leaseErr != nil {
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: leaseErr.Error()})
			return true, leaseErr
		}
		defer relLease()
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
			targetRepo := job.Repo
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
		payload := harness.EnsurePromptPayloadWithJob(job.ID, job.TraceID, job.TargetAgent, job.Repo, sidForPayload, job.Payload)
		if httpCli, ok := d.client.(*HTTPControlPlaneClient); ok && httpCli != nil && httpCli.BaseURL != "" {
			var m map[string]any
			if json.Unmarshal(payload, &m) == nil && m != nil {
				if _, exists := m["api_url"]; !exists {
					m["api_url"] = httpCli.BaseURL
				}
				if httpCli.AuthToken != "" {
					if _, exists := m["token"]; !exists {
						m["token"] = httpCli.AuthToken
					}
				}
				if b, mErr := json.Marshal(m); mErr == nil {
					payload = b
				}
			}
		}
		startTime := time.Now()
		res, err := invoker.ExecutePrompt(execCtx, payload)
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
		if toolSpan != nil {
			toolSpan.End()
		}
		if err != nil {
			elapsed := time.Since(startTime)
			if execCtx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && elapsed < 45*time.Second && job.TargetAgent != "" {
				category := "TOOL_CRASH"
				errLower := strings.ToLower(err.Error())
				if strings.Contains(errLower, "429") || strings.Contains(errLower, "quota") || strings.Contains(errLower, "auth") || strings.Contains(errLower, "token") {
					category = "TOOL_QUOTA_OR_AUTH"
				}
				attemptReq := controlplane.AttemptFailure{
					Host:            d.hostID,
					Tool:            job.TargetAgent,
					Workspace:       job.Repo,
					ExitCode:        1,
					StderrTail:      err.Error(),
					FailureCategory: category,
					FailureStage:    "TOOL_EXECUTION",
					Reason:          "PREFLIGHT_OR_EARLY_TOOL_EXIT",
					Timestamp:       time.Now().UTC().Format(time.RFC3339),
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
		if compErr == nil && d.syncer != nil {
			targetRepo := job.Repo
			if targetRepo == "" && res != nil {
				targetRepo = res.Repo
			}
			if targetRepo != "" {
				d.syncer.TriggerRepoSync(targetRepo)
			}
		}
		return true, compErr

	case JobTypeToolsUpdate:
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
						Result:       json.RawMessage(`{"status":"queued","deferred":true,"message":"tool update queued for execution when harness is idle"}`),
					})
					return true, compErr
				}
				_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{FencingToken: job.FencingToken, Error: err.Error()})
				return true, err
			}
		}
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{
			FencingToken: job.FencingToken,
			Result:       json.RawMessage(`{"updated":true}`),
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
