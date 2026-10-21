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
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/telemetry"
)

// Supported job types.
const (
	JobTypeStoryCreate   = "story.create"
	JobTypePromptExecute = "prompt.execute"
)

// DefaultJobExecutionTimeout is the maximum duration an individual job can run before being terminated.
const DefaultJobExecutionTimeout = 30 * time.Minute

// Job describes a unit of work received from the control plane.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Repo        string          `json:"repo"`
	TargetHost  string          `json:"target_host,omitempty"`
	TargetAgent string          `json:"target_agent,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	TraceID     string          `json:"trace_id,omitempty"`
	ParentID    string          `json:"parent_id,omitempty"`
}

// ClaimRequest is sent to claim pending jobs.
type ClaimRequest struct {
	HostID   string   `json:"host_id,omitempty"`
	JobTypes []string `json:"job_types,omitempty"`
}

// CompleteRequest reports the final outcome of a job.
type CompleteRequest struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// ControlPlaneClient defines communication with iazio-harness-api for agent jobs.
type ControlPlaneClient interface {
	ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error)
	CompleteJob(ctx context.Context, jobID string, req CompleteRequest) error
}

// HTTPControlPlaneClient is a concrete implementation of ControlPlaneClient over HTTP.
type HTTPControlPlaneClient struct {
	BaseURL    string
	AuthToken  string
	HTTPClient *http.Client
}

// NewHTTPControlPlaneClient constructs an HTTPControlPlaneClient.
func NewHTTPControlPlaneClient(baseURL, authToken string, client *http.Client) *HTTPControlPlaneClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	baseURL = strings.TrimRight(baseURL, "/")
	return &HTTPControlPlaneClient{
		BaseURL:    baseURL,
		AuthToken:  authToken,
		HTTPClient: client,
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
		return fmt.Errorf("execute complete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("complete returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// RepoSyncer defines the contract for triggering an on-demand codebase delta sync for a repository.
type RepoSyncer interface {
	TriggerRepoSync(repoName string)
}

// WorktreeResolver maps a repository name or relative submodule to an absolute worktree directory on the host.
type WorktreeResolver func(repo string) string

// Dispatcher coordinates claiming jobs from the control plane and routing them to iazio-harness.
type Dispatcher struct {
	client           ControlPlaneClient
	invoker          harness.Invoker
	hostID           string
	jobTypes         []string
	syncer           RepoSyncer
	worktreeResolver WorktreeResolver
	jobTimeout       time.Duration
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
		jobTypes:   []string{JobTypeStoryCreate, JobTypePromptExecute},
		syncer:     s,
		jobTimeout: DefaultJobExecutionTimeout,
	}
}

// SetJobTimeout sets a custom per-job execution timeout for the Dispatcher.
func (d *Dispatcher) SetJobTimeout(timeout time.Duration) {
	d.jobTimeout = timeout
}

// SetSyncer sets or updates the RepoSyncer on the Dispatcher.
func (d *Dispatcher) SetSyncer(syncer RepoSyncer) {
	d.syncer = syncer
}

// SetWorktreeResolver configures a custom worktree resolver on the Dispatcher.
func (d *Dispatcher) SetWorktreeResolver(r WorktreeResolver) {
	d.worktreeResolver = r
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
	if d.worktreeResolver != nil {
		if wt := d.worktreeResolver(job.Repo); wt != "" {
			invoker = invoker.WithWorkDir(wt)
		}
	}

	timeout := d.jobTimeout
	if timeout <= 0 {
		timeout = DefaultJobExecutionTimeout
	}
	execCtx, execCancel := context.WithTimeout(ctx, timeout)
	defer execCancel()

	switch job.Type {
	case JobTypeStoryCreate:
		res, err := invoker.CreateStory(execCtx, job.Payload)
		if err != nil {
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{Error: err.Error()})
			return true, err
		}
		resBytes, _ := json.Marshal(res)
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{Result: resBytes})
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

	case JobTypePromptExecute:
		payload := EnsurePromptID(job.ID, job.Payload)
		res, err := invoker.ExecutePrompt(execCtx, payload)
		if err != nil {
			_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{Error: err.Error()})
			return true, err
		}
		resBytes, _ := json.Marshal(res)
		compErr := d.client.CompleteJob(ctx, job.ID, CompleteRequest{Result: resBytes})
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

	default:
		unsupportedErr := fmt.Sprintf("unsupported job type: %s", job.Type)
		_ = d.client.CompleteJob(ctx, job.ID, CompleteRequest{Error: unsupportedErr})
		return true, errors.New(unsupportedErr)
	}
}

// EnsurePromptID ensures that a prompt payload JSON contains a valid prompt_id field.
func EnsurePromptID(jobID string, raw json.RawMessage) json.RawMessage {
	return harness.EnsurePromptPayload(jobID, raw)
}
