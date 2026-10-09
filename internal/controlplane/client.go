// Package controlplane is the outbound client the supervisor uses to dial the harness API.
package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client posts heartbeats and finish checks. It does not send IDE credentials.
type Client struct {
	BaseURL    string
	Token      string
	HTTP       *http.Client
	Tools      func() []Tool
	ActiveJobs func() []ActiveJob
	OnTick     func(ctx context.Context, hostID string) error
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

// Heartbeat marks the host online, stores the tool inventory, and reports active jobs.
func (c Client) Heartbeat(ctx context.Context, hostID string, tools []Tool, fetchFailed bool, activeJobs ...ActiveJob) error {
	body := map[string]any{
		"fetch_failed": fetchFailed,
		"tools":        tools,
		"active_jobs":  activeJobs,
	}
	return c.post(ctx, "/v1/hosts/"+hostID+"/heartbeat", body)
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
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	WorktreePath string `json:"worktree_path"`
	DocsHubPath  string `json:"docs_hub_path"`
}

// NextLease asks for the next job for this host. ok is false when the queue is empty.
func (c Client) NextLease(ctx context.Context, hostID string) (Assignment, bool, error) {
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
		return Assignment{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return Assignment{}, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Assignment{}, false, fmt.Errorf("control plane poll: %s", resp.Status)
	}
	var job Assignment
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil && err != io.EOF {
		return Assignment{}, false, err
	}
	if job.ID == "" {
		return Assignment{}, false, nil
	}
	return job, true, nil
}

// Loop registers the host, then heartbeats and polls for a lease until ctx is cancelled.
func (c Client) Loop(ctx context.Context, hostID, kind string, every time.Duration, onJob func(Assignment) error, profile ...string) error {
	if every <= 0 {
		every = 30 * time.Second
	}
	prof := ""
	if len(profile) > 0 {
		prof = profile[0]
	}
	if err := c.Register(ctx, hostID, kind, prof); err != nil {
		return err
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		var tools []Tool
		if c.Tools != nil {
			tools = c.Tools()
		}
		var activeJobs []ActiveJob
		if c.ActiveJobs != nil {
			activeJobs = c.ActiveJobs()
		}
		if err := c.Heartbeat(ctx, hostID, tools, false, activeJobs...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			_ = c.Register(ctx, hostID, kind, prof)
		}
		if c.OnTick != nil {
			if err := c.OnTick(ctx, hostID); err != nil {
				if ctx.Err() != nil {
					return nil
				}
			}
		}
		job, ok, err := c.NextLease(ctx, hostID)
		if err != nil && ctx.Err() != nil {
			return nil
		}
		if ok && onJob != nil {
			if err := onJob(job); err != nil {
				if ctx.Err() != nil {
					return nil
				}
			}
			// When a job just executed, immediately poll again to drain active queues without 30s delay
			select {
			case <-ctx.Done():
				return nil
			default:
				tick.Reset(every)
				continue
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
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
		return PreflightHalt{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return PreflightHalt{}, fmt.Errorf("preflight post: %s", resp.Status)
	}
	var halt PreflightHalt
	if err := json.NewDecoder(resp.Body).Decode(&halt); err != nil {
		return PreflightHalt{}, err
	}
	return halt, nil
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
		return FinishDecision{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return FinishDecision{}, fmt.Errorf("finish post: %s", resp.Status)
	}
	var decision FinishDecision
	if err := json.NewDecoder(resp.Body).Decode(&decision); err != nil {
		return FinishDecision{}, err
	}
	return decision, nil
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
	Path           string `json:"path"`
	WorktreePath   string `json:"worktree_path"`
	Queue          string `json:"queue"`
	Lock           string `json:"lock"`
	Reason         string `json:"reason"`
	DiscardPending bool   `json:"discard_pending"`
	DocsHubPath    string `json:"docs_hub_path"`
	CloneURL       string `json:"clone_url"`
	RunningJobID   string `json:"running_job_id,omitempty"`
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


func (c Client) post(ctx context.Context, path string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
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
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("control plane %s: %s", path, resp.Status)
	}
	return nil
}

var defaultHTTPClient = &http.Client{Timeout: 30 * time.Second}

func (c Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return defaultHTTPClient
}
