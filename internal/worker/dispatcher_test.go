package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/supervisor"
)

type mockInvoker struct {
	createStoryFunc   func(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error)
	executePromptFunc func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error)
}

func (m *mockInvoker) CreateStory(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error) {
	if m.createStoryFunc != nil {
		return m.createStoryFunc(ctx, payload)
	}
	return &harness.StoryExecutionResult{
		StoryID:      "STORY-MOCK-001",
		Submodule:    "iazio-mcp",
		StoryPath:    "docs/stories/story.md",
		IndexUpdated: true,
	}, nil
}

func (m *mockInvoker) ExecutePrompt(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
	if m.executePromptFunc != nil {
		return m.executePromptFunc(ctx, payload)
	}
	return &harness.PromptExecutionResult{
		Status: "completed",
		Output: "success",
	}, nil
}

func (m *mockInvoker) ListRepos(ctx context.Context) (*harness.CodebaseReposResult, error) {
	return &harness.CodebaseReposResult{
		Repos: []harness.CodebaseRepoInfo{
			{Name: "iazio-mcp", Path: "iazio-mcp", DefaultBranch: "main"},
		},
	}, nil
}

func (m *mockInvoker) ExportCodebase(ctx context.Context, payload json.RawMessage) (*harness.CodebaseExportResult, error) {
	return &harness.CodebaseExportResult{
		RepoName: "iazio-mcp",
		UpToDate: true,
	}, nil
}

func (m *mockInvoker) WithWorkDir(workDir string) harness.Invoker {
	return m
}

type mockControlPlane struct {
	claimFunc    func(ctx context.Context, req ClaimRequest) (*Job, error)
	completeFunc func(ctx context.Context, jobID string, req CompleteRequest) error
	bindFunc     func(ctx context.Context, jobID string, req StoryBindingRequest) error
	ackLeaseFunc func(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error
	declineFunc  func(ctx context.Context, jobID, reason string) error

	completedJobs map[string]CompleteRequest
	boundStories  map[string]StoryBindingRequest
	ackedJobs     map[string]string
	declinedJobs  map[string]string
}

func newMockControlPlane() *mockControlPlane {
	return &mockControlPlane{
		completedJobs: make(map[string]CompleteRequest),
		boundStories:  make(map[string]StoryBindingRequest),
		ackedJobs:     make(map[string]string),
		declinedJobs:  make(map[string]string),
	}
}

func (m *mockControlPlane) ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx, req)
	}
	return nil, nil
}

func (m *mockControlPlane) CompleteJob(ctx context.Context, jobID string, req CompleteRequest) error {
	m.completedJobs[jobID] = req
	if m.completeFunc != nil {
		return m.completeFunc(ctx, jobID, req)
	}
	return nil
}

func (m *mockControlPlane) HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64, storyID ...string) error {
	return nil
}

func (m *mockControlPlane) BindStory(ctx context.Context, jobID string, req StoryBindingRequest) error {
	m.boundStories[jobID] = req
	if m.bindFunc != nil {
		return m.bindFunc(ctx, jobID, req)
	}
	return nil
}

func (m *mockControlPlane) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error {
	m.ackedJobs[jobID] = leaseToken
	if m.ackLeaseFunc != nil {
		return m.ackLeaseFunc(ctx, jobID, leaseToken, agentPID, activeConvID)
	}
	return nil
}

func (m *mockControlPlane) DeclineJob(ctx context.Context, jobID, reason string) error {
	m.declinedJobs[jobID] = reason
	if m.declineFunc != nil {
		return m.declineFunc(ctx, jobID, reason)
	}
	return nil
}

func (m *mockControlPlane) FailoverJob(ctx context.Context, jobID string, req controlplane.AttemptFailure) error {
	return nil
}

func TestDispatcher_StoryCreate_Success(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-101",
			Type:    JobTypeStoryCreate,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"title":"New Story"}`),
		}, nil
	}

	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected dispatcher error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	comp, ok := cp.completedJobs["job-101"]
	if !ok {
		t.Fatal("expected job-101 to be reported complete")
	}
	if comp.Error != "" {
		t.Fatalf("expected no error, got %s", comp.Error)
	}

	var res harness.StoryExecutionResult
	if err := json.Unmarshal(comp.Result, &res); err != nil {
		t.Fatalf("failed to decode result: %v", err)
	}
	if res.StoryID != "STORY-MOCK-001" {
		t.Errorf("expected story ID STORY-MOCK-001, got %s", res.StoryID)
	}
}

func TestDispatcher_StoryCreate_HarnessFailure(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-102",
			Type:    JobTypeStoryCreate,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"title":"Failing Story"}`),
		}, nil
	}

	inv := &mockInvoker{
		createStoryFunc: func(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error) {
			return nil, errors.New("harness lock collision")
		},
	}
	d := NewDispatcher(cp, inv, "host-1")

	processed, err := d.PollAndProcess(context.Background())
	if err == nil {
		t.Fatal("expected dispatcher to return error on harness failure")
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	comp, ok := cp.completedJobs["job-102"]
	if !ok {
		t.Fatal("expected job-102 completion to be reported")
	}
	if comp.Error != "harness lock collision" {
		t.Errorf("expected error 'harness lock collision', got %s", comp.Error)
	}
}

func TestDispatcher_PromptExecute_Success(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-201",
			Type:    JobTypePromptExecute,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"prompt":"Refactor stuff"}`),
		}, nil
	}

	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	comp, ok := cp.completedJobs["job-201"]
	if !ok {
		t.Fatal("expected job-201 completion to be reported")
	}
	if comp.Error != "" {
		t.Errorf("expected no error, got %s", comp.Error)
	}
}

func TestDispatcher_NoJobAvailable(t *testing.T) {
	cp := newMockControlPlane()
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed {
		t.Fatal("expected processed=false when no job available")
	}
}

func TestHTTPControlPlaneClient(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/jobs/claim", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Job{
			ID:   "job-http-1",
			Type: JobTypeStoryCreate,
			Repo: "iazio-mcp",
		})
	})

	var reportedComplete CompleteRequest
	mux.HandleFunc("/api/v1/agent/jobs/job-http-1/complete", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&reportedComplete)
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPControlPlaneClient(server.URL, "test-token", server.Client())

	job, err := client.ClaimJob(context.Background(), ClaimRequest{HostID: "h1"})
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}
	if job == nil || job.ID != "job-http-1" {
		t.Fatalf("unexpected job: %+v", job)
	}

	err = client.CompleteJob(context.Background(), job.ID, CompleteRequest{
		Result: json.RawMessage(`{"status":"done"}`),
	})
	if err != nil {
		t.Fatalf("complete error: %v", err)
	}

	if string(reportedComplete.Result) != `{"status":"done"}` {
		t.Errorf("unexpected complete payload received: %s", string(reportedComplete.Result))
	}
}

type mockRepoSyncer struct {
	triggeredRepos []string
}

func (m *mockRepoSyncer) TriggerRepoSync(repoName string) {
	m.triggeredRepos = append(m.triggeredRepos, repoName)
}

func TestPostJobSyncTrigger_StoryCreate(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-story-sync",
			Type:    JobTypeStoryCreate,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"title":"New Story"}`),
		}, nil
	}

	inv := &mockInvoker{
		createStoryFunc: func(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error) {
			return &harness.StoryExecutionResult{
				StoryID:   "STORY-123",
				Submodule: "iazio-mcp",
			}, nil
		},
	}

	syncer := &mockRepoSyncer{}
	d := NewDispatcher(cp, inv, "host-1", syncer)

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	if len(syncer.triggeredRepos) != 1 || syncer.triggeredRepos[0] != "iazio-mcp" {
		t.Fatalf("expected syncer trigger for iazio-mcp, got %v", syncer.triggeredRepos)
	}
}

func TestPostJobSyncTrigger_PromptExecute(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-prompt-sync",
			Type:    JobTypePromptExecute,
			Repo:    "",
			Payload: json.RawMessage(`{"repo":"iazio-harness"}`),
		}, nil
	}

	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return &harness.PromptExecutionResult{
				Status: "completed",
				Repo:   "iazio-harness",
			}, nil
		},
	}

	syncer := &mockRepoSyncer{}
	d := NewDispatcher(cp, inv, "host-1", syncer)

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	if len(syncer.triggeredRepos) != 1 || syncer.triggeredRepos[0] != "iazio-harness" {
		t.Fatalf("expected syncer trigger for iazio-harness, got %v", syncer.triggeredRepos)
	}
}

func TestPostJobSyncTrigger_NoTriggerOnFailure(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-fail",
			Type:    JobTypeStoryCreate,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{}`),
		}, nil
	}

	inv := &mockInvoker{
		createStoryFunc: func(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error) {
			return nil, errors.New("harness error")
		},
	}

	syncer := &mockRepoSyncer{}
	d := NewDispatcher(cp, inv, "host-1", syncer)

	_, err := d.PollAndProcess(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if len(syncer.triggeredRepos) != 0 {
		t.Fatalf("expected 0 syncer triggers on job failure, got %v", syncer.triggeredRepos)
	}
}

func TestDispatcher_SetSyncerAndNilValidation(t *testing.T) {
	d := NewDispatcher(nil, nil, "host-1")
	syncer := &mockRepoSyncer{}
	d.SetSyncer(syncer)
	if d.syncer != syncer {
		t.Fatal("expected syncer to be set")
	}

	// Nil client validation
	_, err := d.PollAndProcess(context.Background())
	if err == nil || !strings.Contains(err.Error(), "client not configured") {
		t.Fatalf("expected client not configured error, got %v", err)
	}

	// Nil invoker validation
	d2 := NewDispatcher(&mockControlPlane{}, nil, "host-1")
	_, err = d2.PollAndProcess(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invoker not configured") {
		t.Fatalf("expected invoker not configured error, got %v", err)
	}

	// Default HTTP client
	client := NewHTTPControlPlaneClient("http://localhost:8080/", "tok", nil)
	if client.HTTPClient == nil {
		t.Fatal("expected default http client")
	}
}

func TestHTTPControlPlaneClient_ClaimAndCompleteBranches(t *testing.T) {
	// 1. 204 No Content
	srv204 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv204.Close()

	cli := NewHTTPControlPlaneClient(srv204.URL, "", srv204.Client())
	job, err := cli.ClaimJob(context.Background(), ClaimRequest{HostID: "h1"})
	if err != nil || job != nil {
		t.Fatalf("expected nil job on 204: job=%+v err=%v", job, err)
	}

	// 2. 500 Server error
	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv500.Close()

	cli500 := NewHTTPControlPlaneClient(srv500.URL, "", srv500.Client())
	_, err = cli500.ClaimJob(context.Background(), ClaimRequest{HostID: "h1"})
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected status 500 error, got %v", err)
	}

	err = cli500.CompleteJob(context.Background(), "job-1", CompleteRequest{})
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("expected status 500 error on complete, got %v", err)
	}

	// 3. Decode error
	srvBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer srvBadJSON.Close()

	cliBad := NewHTTPControlPlaneClient(srvBadJSON.URL, "", srvBadJSON.Client())
	_, err = cliBad.ClaimJob(context.Background(), ClaimRequest{HostID: "h1"})
	if err == nil || !strings.Contains(err.Error(), "decode claim response") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestDispatcher_UnsupportedAndErrorJobs(t *testing.T) {
	ctx := context.Background()

	// 1. Claim job with empty ID returns nil, nil
	srvEmptyID := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Job{ID: ""})
	}))
	defer srvEmptyID.Close()

	cliEmpty := NewHTTPControlPlaneClient(srvEmptyID.URL, "", srvEmptyID.Client())
	job, err := cliEmpty.ClaimJob(ctx, ClaimRequest{HostID: "h1"})
	if err != nil || job != nil {
		t.Fatalf("expected nil job when ID is empty: %+v err=%v", job, err)
	}

	// 2. Unsupported job type
	invoker := &mockInvoker{}
	cli := newMockControlPlane()
	cli.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{ID: "j-unsupp", Type: "unknown_job_type"}, nil
	}
	d := NewDispatcher(cli, invoker, "h1")
	processed, err := d.PollAndProcess(ctx)
	if !processed || err == nil || !strings.Contains(err.Error(), "unsupported job type") {
		t.Fatalf("expected unsupported job type error, got processed=%v err=%v", processed, err)
	}

	// 3. CreateStory failure in PollAndProcess
	cliStory := newMockControlPlane()
	cliStory.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{ID: "j-story-err", Type: JobTypeStoryCreate}, nil
	}
	invStoryErr := &mockInvoker{
		createStoryFunc: func(ctx context.Context, payload json.RawMessage) (*harness.StoryExecutionResult, error) {
			return nil, errors.New("cannot create story")
		},
	}
	dStory := NewDispatcher(cliStory, invStoryErr, "h1")
	processed, err = dStory.PollAndProcess(ctx)
	if !processed || err == nil || !strings.Contains(err.Error(), "cannot create story") {
		t.Fatalf("expected create story error, got processed=%v err=%v", processed, err)
	}

	// 4. ExecutePrompt failure in PollAndProcess
	cliPrompt := newMockControlPlane()
	cliPrompt.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{ID: "j-prompt-err", Type: JobTypePromptExecute}, nil
	}
	invPromptErr := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return nil, errors.New("cannot execute prompt")
		},
	}
	dPrompt := NewDispatcher(cliPrompt, invPromptErr, "h1")
	processed, err = dPrompt.PollAndProcess(ctx)
	if !processed || err == nil || !strings.Contains(err.Error(), "cannot execute prompt") {
		t.Fatalf("expected execute prompt error, got processed=%v err=%v", processed, err)
	}
}

func TestDispatcher_WorktreeResolver(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-resolver",
			Type:    JobTypeStoryCreate,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{}`),
		}, nil
	}

	var capturedWorkDir string
	inv := &mockInvokerWithWorkDirCapture{
		onWithWorkDir: func(dir string) {
			capturedWorkDir = dir
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	d.SetWorktreeResolver(func(repo string) string {
		if repo == "iazio-mcp" {
			return "/custom/worktree/iazio-mcp"
		}
		return ""
	})

	processed, err := d.PollAndProcess(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}
	if capturedWorkDir != "/custom/worktree/iazio-mcp" {
		t.Fatalf("expected workDir '/custom/worktree/iazio-mcp', got %q", capturedWorkDir)
	}
}

func TestDispatcher_UnresolvableWorkspaceRefusal(t *testing.T) {
	ctx := context.Background()
	var completedError string
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-ghost-path",
			Type:    JobTypePromptExecute,
			Repo:    filepath.Join(t.TempDir(), "nonexistent-workspace-ghost"),
			Payload: json.RawMessage(`{}`),
		}, nil
	}
	cp.completeFunc = func(ctx context.Context, jobID string, req CompleteRequest) error {
		completedError = req.Error
		return nil
	}

	invokerCalled := false
	inv := &mockInvokerWithWorkDirCapture{
		onWithWorkDir: func(dir string) {
			invokerCalled = true
		},
	}

	d := NewDispatcher(cp, inv, "pc1-wsl")
	d.SetWorktreeResolver(func(repo string) string {
		return ""
	})

	processed, err := d.PollAndProcess(ctx)
	if err == nil {
		t.Fatal("expected error for unresolvable workspace, got nil")
	}
	if !processed {
		t.Fatal("expected job to be processed (handled)")
	}
	if invokerCalled {
		t.Fatal("invoker should not be called when workspace does not exist on disk")
	}
	if !strings.Contains(completedError, "ERR_WORKSPACE_NOT_FOUND") {
		t.Fatalf("expected ERR_WORKSPACE_NOT_FOUND, got %q", completedError)
	}
}

func TestDispatcher_UnresolvableRelativeWorkspaceRefusal(t *testing.T) {
	ctx := context.Background()
	var completedError string
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-relative-ghost-path",
			Type:    JobTypeStoryCreate,
			Repo:    "unknown-repo-relative",
			Payload: json.RawMessage(`{}`),
		}, nil
	}
	cp.completeFunc = func(ctx context.Context, jobID string, req CompleteRequest) error {
		completedError = req.Error
		return nil
	}

	invokerCalled := false
	inv := &mockInvokerWithWorkDirCapture{
		onWithWorkDir: func(dir string) {
			invokerCalled = true
		},
	}

	d := NewDispatcher(cp, inv, "pc1-wsl")
	d.SetWorktreeResolver(func(repo string) string {
		return ""
	})

	processed, err := d.PollAndProcess(ctx)
	if err == nil {
		t.Fatal("expected error for unresolvable relative workspace, got nil")
	}
	if !processed {
		t.Fatal("expected job to be processed (handled)")
	}
	if invokerCalled {
		t.Fatal("invoker should not be called when workspace does not exist on disk")
	}
	if !strings.Contains(completedError, "ERR_WORKSPACE_NOT_FOUND") {
		t.Fatalf("expected ERR_WORKSPACE_NOT_FOUND, got %q", completedError)
	}
}

func TestDispatcher_ValidExistingAbsoluteWorkspace(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-valid-abs-path",
			Type:    JobTypePromptExecute,
			Repo:    tmpDir,
			Payload: json.RawMessage(`{}`),
		}, nil
	}
	cp.completeFunc = func(ctx context.Context, jobID string, req CompleteRequest) error {
		return nil
	}

	var capturedWorkDir string
	inv := &mockInvokerWithWorkDirCapture{
		onWithWorkDir: func(dir string) {
			capturedWorkDir = dir
		},
	}

	d := NewDispatcher(cp, inv, "pc1-wsl")
	d.SetWorktreeResolver(func(repo string) string {
		return ""
	})

	processed, err := d.PollAndProcess(ctx)
	if err != nil {
		t.Fatalf("unexpected error for existing directory: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}
	if capturedWorkDir != tmpDir {
		t.Fatalf("expected workdir %q, got %q", tmpDir, capturedWorkDir)
	}
}

type mockInvokerWithWorkDirCapture struct {
	mockInvoker
	onWithWorkDir func(dir string)
}

func (m *mockInvokerWithWorkDirCapture) WithWorkDir(workDir string) harness.Invoker {
	if m.onWithWorkDir != nil {
		m.onWithWorkDir(workDir)
	}
	return m
}

func TestDispatcher_PromptExecute_AutoInjectsPromptID(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-exec-789",
			Type:    JobTypePromptExecute,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"story_id":"STORY-TEST-99","prompt":"implement feature"}`),
		}, nil
	}

	var capturedPayload json.RawMessage
	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			capturedPayload = payload
			return &harness.PromptExecutionResult{
				PromptID: "prompt-STORY-TEST-99",
				Status:   "completed",
				Output:   "done",
			}, nil
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	processed, err := d.PollAndProcess(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	var m map[string]any
	if err := json.Unmarshal(capturedPayload, &m); err != nil {
		t.Fatalf("failed unmarshaling captured payload: %v", err)
	}
	if m["prompt_id"] != "prompt-STORY-TEST-99" {
		t.Fatalf("expected injected prompt_id 'prompt-STORY-TEST-99', got %v", m["prompt_id"])
	}

	comp, ok := cp.completedJobs["job-exec-789"]
	if !ok {
		t.Fatal("expected job-exec-789 to be marked completed")
	}
	if comp.Error != "" {
		t.Fatalf("expected no error in completion, got: %s", comp.Error)
	}
}

func TestDispatcher_JobExecutionTimeout(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-timeout-1",
			Type:    JobTypePromptExecute,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"prompt":"hang forever"}`),
		}, nil
	}

	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return &harness.PromptExecutionResult{Status: "completed"}, nil
			}
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	d.SetJobTimeout(30 * time.Millisecond)

	processed, err := d.PollAndProcess(ctx)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	comp, ok := cp.completedJobs["job-timeout-1"]
	if !ok {
		t.Fatal("expected job-timeout-1 to be marked completed with error")
	}
	if !strings.Contains(comp.Error, "context deadline exceeded") && !strings.Contains(comp.Error, "deadline exceeded") {
		t.Fatalf("expected deadline exceeded error in completion report, got: %s", comp.Error)
	}
}

func TestDispatcher_ToolsUpdate_Success(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-tu-1",
			Type:         JobTypeToolsUpdate,
			Payload:      json.RawMessage(`{"tools":["autopilot"],"force":true,"target_version":"0.0.492"}`),
			FencingToken: 42,
		}, nil
	}

	toolUpdateCalled := false
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")
	d.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
		toolUpdateCalled = true
		if len(tools) != 1 || tools[0] != "autopilot" || !force {
			t.Errorf("unexpected tool update params: tools=%v force=%v", tools, force)
		}
		if len(targetVersion) == 0 || targetVersion[0] != "0.0.492" {
			t.Errorf("expected targetVersion 0.0.492 to propagate (F7), got %v", targetVersion)
		}
		return nil
	})

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}
	if !toolUpdateCalled {
		t.Fatal("expected tool updater to be called")
	}

	comp, ok := cp.completedJobs["job-tu-1"]
	if !ok {
		t.Fatal("expected completion report for job-tu-1")
	}
	if comp.FencingToken != 42 {
		t.Fatalf("expected fencing token 42, got %d", comp.FencingToken)
	}
}

func TestDispatcher_ToolsUpdate_Unforced(t *testing.T) {
	// N2: Verify that an unforced job payload passes force=false to ToolUpdater
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-tu-unforced",
			Type:         JobTypeToolsUpdate,
			Payload:      json.RawMessage(`{"tools":["autopilot"],"force":false}`),
			FencingToken: 43,
		}, nil
	}

	toolUpdateCalled := false
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")
	d.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
		toolUpdateCalled = true
		if len(tools) != 1 || tools[0] != "autopilot" {
			t.Errorf("unexpected tools: %v", tools)
		}
		if force {
			t.Errorf("expected force=false for unforced job (N2), got force=true")
		}
		return nil
	})

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed || !toolUpdateCalled {
		t.Fatal("expected unforced job to be processed and tool updater called")
	}
}

func TestDispatcher_ToolsUpdate_DeferredReportsNonTerminalSuccess(t *testing.T) {
	// N3: Verify that ErrJobDeferred reports non-terminal success with status=queued
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-tu-deferred",
			Type:         JobTypeToolsUpdate,
			Payload:      json.RawMessage(`{"tools":["autopilot"],"force":false}`),
			FencingToken: 44,
		}, nil
	}

	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")
	d.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
		return ErrJobDeferred
	})

	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on deferred job processing (N3), got: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	comp, ok := cp.completedJobs["job-tu-deferred"]
	if !ok {
		t.Fatal("expected completion report for job-tu-deferred")
	}
	if comp.Error != "" {
		t.Fatalf("expected empty error on deferred completion (N3), got error: %s", comp.Error)
	}
	var resMap map[string]any
	if err := json.Unmarshal(comp.Result, &resMap); err != nil {
		t.Fatalf("failed to unmarshal completion result: %v", err)
	}
	if resMap["status"] != "queued" || resMap["deferred"] != true {
		t.Fatalf("expected status=queued deferred=true in result (N3), got: %v", resMap)
	}
}

func TestHTTPControlPlaneClient_AllMethods(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/agent/jobs/claim", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req ClaimRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if req.HostID == "empty" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if req.HostID == "blank-job" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(Job{})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Job{
			ID:           "claimed-1",
			Type:         JobTypePromptExecute,
			FencingToken: 12,
		})
	})

	mux.HandleFunc("/api/v1/agent/jobs/job-1/complete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var req CompleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if req.FencingToken == 999 {
			http.Error(w, "fenced", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/api/v1/agent/jobs/job-1/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var req HeartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if req.FencingToken == 999 {
			http.Error(w, "fenced", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())

	ctx := context.Background()

	// 1. Claim success
	job, err := client.ClaimJob(ctx, ClaimRequest{HostID: "host-1"})
	if err != nil {
		t.Fatalf("ClaimJob error: %v", err)
	}
	if job == nil || job.ID != "claimed-1" || job.FencingToken != 12 {
		t.Fatalf("unexpected claimed job: %+v", job)
	}

	// 2. Claim NoContent
	jobNC, err := client.ClaimJob(ctx, ClaimRequest{HostID: "empty"})
	if err != nil {
		t.Fatalf("ClaimJob NoContent error: %v", err)
	}
	if jobNC != nil {
		t.Fatalf("expected nil job for NoContent, got %+v", jobNC)
	}

	// 3. Claim blank job
	jobBlank, err := client.ClaimJob(ctx, ClaimRequest{HostID: "blank-job"})
	if err != nil {
		t.Fatalf("ClaimJob blank error: %v", err)
	}
	if jobBlank != nil {
		t.Fatalf("expected nil job for blank, got %+v", jobBlank)
	}

	// 4. Complete success
	if err := client.CompleteJob(ctx, "job-1", CompleteRequest{FencingToken: 12, Result: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatalf("CompleteJob error: %v", err)
	}

	// 5. Complete failure (fenced)
	if err := client.CompleteJob(ctx, "job-1", CompleteRequest{FencingToken: 999, Result: json.RawMessage(`{"ok":true}`)}); err == nil {
		t.Fatal("expected error for fenced completion")
	}

	// 6. Heartbeat success
	if err := client.HeartbeatJob(ctx, "job-1", "lease-token-1", 12); err != nil {
		t.Fatalf("HeartbeatJob error: %v", err)
	}

	// 7. Heartbeat failure (fenced)
	if err := client.HeartbeatJob(ctx, "job-1", "lease-token-1", 999); err == nil {
		t.Fatal("expected error for fenced heartbeat")
	}
}

func TestEnsurePromptID_Helpers(t *testing.T) {
	raw := json.RawMessage(`{"prompt":"hello"}`)
	out := EnsurePromptID("job-123", raw)
	if !strings.Contains(string(out), "prompt_id") {
		t.Fatalf("expected prompt_id in output: %s", string(out))
	}
	outTrace := EnsurePromptIDWithTrace("job-123", "trace-456", raw)
	if !strings.Contains(string(outTrace), "trace_id") {
		t.Fatalf("expected trace_id in output: %s", string(outTrace))
	}
}

func TestDispatcher_PromptExecute_BindsStory(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-prompt-bind-1",
			Type:    JobTypePromptExecute,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"prompt":"/autonomous-story-execution implement feature"}`),
		}, nil
	}

	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return &harness.PromptExecutionResult{
				Status:     "completed",
				StoryID:    "STORY-GOV-3688",
				StoryTitle: "Gemini Web MCP ASE Tool Priority",
				StoryPath:  "docs/stories/2026-10-25-gov-3688.story.md",
				Summary:    "Successfully executed STORY-GOV-3688",
			}, nil
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	processed, err := d.PollAndProcess(context.Background())
	if err != nil {
		t.Fatalf("PollAndProcess failed: %v", err)
	}
	if !processed {
		t.Fatal("expected job to be processed")
	}

	bound, ok := cp.boundStories["job-prompt-bind-1"]
	if !ok {
		t.Fatal("expected story to be bound to job-prompt-bind-1")
	}
	if bound.StoryID != "STORY-GOV-3688" {
		t.Errorf("expected story ID STORY-GOV-3688, got %s", bound.StoryID)
	}
	if bound.StoryTitle != "Gemini Web MCP ASE Tool Priority" {
		t.Errorf("expected story title 'Gemini Web MCP ASE Tool Priority', got %s", bound.StoryTitle)
	}
}

func TestHTTPControlPlaneClient_BindStory(t *testing.T) {
	var receivedMethod string
	var receivedPath string
	var reqBody StoryBindingRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	client := NewHTTPControlPlaneClient(ts.URL, "test-token", ts.Client())
	err := client.BindStory(context.Background(), "job-999", StoryBindingRequest{
		FencingToken: 42,
		StoryID:      "STORY-GOV-3688",
		StoryTitle:   "Title",
		StoryPath:    "docs/stories/story.md",
	})
	if err != nil {
		t.Fatalf("BindStory failed: %v", err)
	}

	if reqBody.FencingToken != 42 {
		t.Errorf("expected fencing token 42, got %d", reqBody.FencingToken)
	}

	if receivedMethod != http.MethodPatch {
		t.Errorf("expected method PATCH, got %s", receivedMethod)
	}
	if receivedPath != "/api/v1/jobs/job-999/story-binding" {
		t.Errorf("expected path /api/v1/jobs/job-999/story-binding, got %s", receivedPath)
	}
	if reqBody.StoryID != "STORY-GOV-3688" {
		t.Errorf("expected StoryID STORY-GOV-3688, got %s", reqBody.StoryID)
	}
}

type mockHostGate struct {
	mu           sync.Mutex
	acquireCount int
	releaseCount int
	lastJobID    string
	block        bool
}

func (m *mockHostGate) AcquireExecutionLease(ctx context.Context, jobID, repoPath string, fencingToken ...int64) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.block {
		return nil, errors.New("gate locked by maintenance")
	}
	m.acquireCount++
	m.lastJobID = jobID
	return func() {
		m.mu.Lock()
		m.releaseCount++
		m.mu.Unlock()
	}, nil
}

func TestDispatcher_PreDispatchAndPostJobHooks(t *testing.T) {
	client := newMockControlPlane()
	client.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-hook-1",
			Type:    JobTypePromptExecute,
			Repo:    "workspace-fleet",
			Payload: json.RawMessage(`{"prompt":"test"}`),
		}, nil
	}
	invoker := &mockInvoker{}
	disp := NewDispatcher(client, invoker, "host-1")

	preCalled := false
	postCalled := false

	disp.SetPreDispatchHook(func(ctx context.Context, job *Job) error {
		preCalled = true
		if job.ID != "job-hook-1" {
			t.Errorf("preDispatchHook expected job-hook-1, got %s", job.ID)
		}
		return nil
	})
	disp.SetPostJobHook(func(ctx context.Context, job *Job) {
		postCalled = true
		if job.ID != "job-hook-1" {
			t.Errorf("postJobHook expected job-hook-1, got %s", job.ID)
		}
	})

	gate := &mockHostGate{}
	disp.SetHostGate(gate)

	processed, err := disp.PollAndProcess(context.Background())
	if err != nil || !processed {
		t.Fatalf("expected job processed successfully, got processed=%t err=%v", processed, err)
	}

	if !preCalled {
		t.Errorf("expected preDispatchHook to be called")
	}
	if !postCalled {
		t.Errorf("expected postJobHook to be called")
	}
	if gate.acquireCount != 1 || gate.releaseCount != 1 {
		t.Errorf("expected 1 acquire and 1 release on gate, got acquire=%d release=%d", gate.acquireCount, gate.releaseCount)
	}
}

func TestDispatcher_HostGateLockedBlocksDispatch(t *testing.T) {
	client := newMockControlPlane()
	client.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:      "job-blocked-1",
			Type:    JobTypePromptExecute,
			Repo:    "workspace-fleet",
			Payload: json.RawMessage(`{"prompt":"test"}`),
		}, nil
	}
	invoker := &mockInvoker{}
	disp := NewDispatcher(client, invoker, "host-1")

	gate := &mockHostGate{block: true}
	disp.SetHostGate(gate)

	processed, err := disp.PollAndProcess(context.Background())
	if !processed || err == nil {
		t.Fatalf("expected job to fail when gate fails to acquire lease")
	}
	if client.completedJobs["job-blocked-1"].Error == "" {
		t.Errorf("expected completedJob to record error")
	}
}

func TestDispatcher_SynchronousHarvestFlushOnCompletion(t *testing.T) {
	client := newMockControlPlane()
	client.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-harvest-1",
			Type:         JobTypePromptExecute,
			Repo:         "workspace-fleet",
			FencingToken: 42,
			Payload:      json.RawMessage(`{"prompt":"test","conversation_id":"conv-test-99"}`),
		}, nil
	}
	invoker := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return &harness.PromptExecutionResult{Status: "DONE", Summary: "all tests passed"}, nil
		},
	}
	disp := NewDispatcher(client, invoker, "host-1")

	flushedConv := ""
	disp.SetHarvestFlusher(func(ctx context.Context, conversationID string) (int, string, error) {
		flushedConv = conversationID
		return 15, "mock-sha256-hash", nil
	})

	processed, err := disp.PollAndProcess(context.Background())
	if !processed || err != nil {
		t.Fatalf("PollAndProcess failed: processed=%v, err=%v", processed, err)
	}

	if flushedConv != "conv-test-99" {
		t.Errorf("expected flushed conversation conv-test-99, got %s", flushedConv)
	}

	comp := client.completedJobs["job-harvest-1"]
	if comp.FinalTurnCount != 15 {
		t.Errorf("expected FinalTurnCount=15, got %d", comp.FinalTurnCount)
	}
	if comp.FinalTranscriptSHA256 != "mock-sha256-hash" {
		t.Errorf("expected FinalTranscriptSHA256=mock-sha256-hash, got %s", comp.FinalTranscriptSHA256)
	}
}

func TestHTTPControlPlaneClient_AckLeaseAndFailover(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/jobs/job-100/ack-lease", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/v1/agent/jobs/job-100/failover", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewHTTPControlPlaneClient(srv.URL, "test-tok", srv.Client())
	err := cli.AckLease(context.Background(), "job-100", "lease-abc", 1234, "conv-99")
	if err != nil {
		t.Fatalf("AckLease failed: %v", err)
	}

	err = cli.FailoverJob(context.Background(), "job-100", controlplane.AttemptFailure{
		Reason: "test-failure",
	})
	if err != nil {
		t.Fatalf("FailoverJob failed: %v", err)
	}

	// Test error cases
	badCli := NewHTTPControlPlaneClient(srv.URL, "wrong-tok", srv.Client())
	if err := badCli.AckLease(context.Background(), "job-100", "lease-abc", 1234, "conv-99"); err == nil {
		t.Fatalf("expected AckLease unauthorized error")
	}
	if err := badCli.FailoverJob(context.Background(), "job-100", controlplane.AttemptFailure{}); err == nil {
		t.Fatalf("expected FailoverJob unauthorized error")
	}
}

func TestGov3782_DispatcherStoryExecuteAndPostJobHookAfterComplete(t *testing.T) {
	cp := newMockControlPlane()
	var claimedTypes []string
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		claimedTypes = req.JobTypes
		return &Job{
			ID:      "job-story-exec-1",
			Type:    JobTypeStoryExecute,
			Repo:    "iazio-mcp",
			Payload: json.RawMessage(`{"story_id":"STORY-GOV-3782","prompt":"execute story"}`),
		}, nil
	}

	var completeCompletedBeforePostHook bool
	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return &harness.PromptExecutionResult{
				Status:  "completed",
				StoryID: "STORY-GOV-3782",
				Repo:    "iazio-mcp",
			}, nil
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	d.SetPostJobHook(func(ctx context.Context, job *Job) {
		if _, ok := cp.completedJobs["job-story-exec-1"]; ok {
			completeCompletedBeforePostHook = true
		}
	})

	processed, err := d.PollAndProcess(context.Background())
	if err != nil || !processed {
		t.Fatalf("expected JobTypeStoryExecute to be processed, got processed=%v err=%v", processed, err)
	}
	hasStoryExec := false
	for _, jt := range claimedTypes {
		if jt == JobTypeStoryExecute {
			hasStoryExec = true
		}
	}
	if !hasStoryExec {
		t.Fatalf("expected ClaimRequest.JobTypes to include %s, got %v", JobTypeStoryExecute, claimedTypes)
	}
	if !completeCompletedBeforePostHook {
		t.Fatalf("expected CompleteJob to finish BEFORE postJobHook executes")
	}
}

func TestDispatcher_SyncsFailoverTargetAgentIntoPayload(t *testing.T) {
	cp := newMockControlPlane()
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:          "job-failover-sync-1",
			Type:        JobTypePromptExecute,
			Repo:        "iazio-agent",
			TargetAgent: "opencode",
			StoryID:     "STORY-GOV-3799",
			TraceID:     "trace-failover-1",
			Payload:     json.RawMessage(`{"prompt":"fix bug","target_agent":"agy"}`),
		}, nil
	}

	var capturedPayload json.RawMessage
	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			capturedPayload = payload
			return &harness.PromptExecutionResult{Status: "completed"}, nil
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	processed, err := d.PollAndProcess(context.Background())
	if err != nil || !processed {
		t.Fatalf("PollAndProcess failed: processed=%v err=%v", processed, err)
	}

	var m map[string]any
	if err := json.Unmarshal(capturedPayload, &m); err != nil {
		t.Fatalf("failed to unmarshal captured payload: %v", err)
	}
	if m["target_agent"] != "opencode" {
		t.Fatalf("expected payload target_agent to be synchronized to 'opencode', got %v", m["target_agent"])
	}
	if m["story_id"] != "STORY-GOV-3799" {
		t.Fatalf("expected payload story_id to be 'STORY-GOV-3799', got %v", m["story_id"])
	}
	if m["repo"] != "iazio-agent" {
		t.Fatalf("expected payload repo to be 'iazio-agent', got %v", m["repo"])
	}
}

func TestExtractConversationID_ResumeAndInitialKeys(t *testing.T) {
	if got := extractConversationID(json.RawMessage(`{"resume_conversation_id":"conv-resume-123"}`)); got != "conv-resume-123" {
		t.Fatalf("expected conv-resume-123, got %q", got)
	}
	if got := extractConversationID(json.RawMessage(`{"initial_conversation_id":"conv-initial-456"}`)); got != "conv-initial-456" {
		t.Fatalf("expected conv-initial-456, got %q", got)
	}
}

func TestDispatcher_FlushHarvestFromResultAndSpoolerReachable(t *testing.T) {
	var flushedConvIDs []string
	cp := newMockControlPlane()
	jobs := []*Job{
		{
			ID:                   "job-harvest-active-conv",
			Type:                 JobTypePromptExecute,
			Repo:                 "iazio-agent",
			FencingToken:         11,
			ActiveConversationID: "conv-from-job-active-1",
			Payload:              json.RawMessage(`{"prompt":"hello"}`),
		},
		{
			ID:           "job-harvest-result-conv",
			Type:         JobTypePromptExecute,
			Repo:         "iazio-agent",
			FencingToken: 12,
			Payload:      json.RawMessage(`{"prompt":"hello"}`),
		},
	}
	idx := 0
	cp.claimFunc = func(ctx context.Context, req ClaimRequest) (*Job, error) {
		if idx >= len(jobs) {
			return nil, nil
		}
		j := jobs[idx]
		idx++
		return j, nil
	}

	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			if idx == 2 {
				return &harness.PromptExecutionResult{
					Status:         "completed",
					ConversationID: "conv-from-result-2",
				}, nil
			}
			return &harness.PromptExecutionResult{Status: "completed"}, nil
		},
	}

	d := NewDispatcher(cp, inv, "host-1")
	d.SetHarvestFlusher(func(ctx context.Context, convID string) (int, string, error) {
		flushedConvIDs = append(flushedConvIDs, convID)
		return 7, "sha256-harvest-ok", nil
	})

	for i := 0; i < 2; i++ {
		processed, err := d.PollAndProcess(context.Background())
		if err != nil || !processed {
			t.Fatalf("PollAndProcess pass %d failed: processed=%v err=%v", i, processed, err)
		}
	}

	if len(flushedConvIDs) != 2 {
		t.Fatalf("expected 2 harvest flushes, got %v", flushedConvIDs)
	}
	if flushedConvIDs[0] != "conv-from-job-active-1" {
		t.Fatalf("expected conv-from-job-active-1, got %q", flushedConvIDs[0])
	}
	if flushedConvIDs[1] != "conv-from-result-2" {
		t.Fatalf("expected conv-from-result-2, got %q", flushedConvIDs[1])
	}
}

func TestHTTPControlPlaneClient_TransientRetryAcrossRestartAndFailoverDirection(t *testing.T) {
	var completeAttempts int
	var hbAttempts int
	var failoverCaptured controlplane.AttemptFailure

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/jobs/job_110f47f09551baba/complete", func(w http.ResponseWriter, r *http.Request) {
		completeAttempts++
		if completeAttempts <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"upstream restarting"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"completed"}`))
	})
	mux.HandleFunc("/api/v1/agent/jobs/job_110f47f09551baba/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		hbAttempts++
		if hbAttempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/v1/agent/jobs/job_110f47f09551baba/failover", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&failoverCaptured)
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := NewHTTPControlPlaneClient(srv.URL, "token", srv.Client())
	cli.InitialBackoff = 5 * time.Millisecond
	cli.MaxRetries = 3

	ctx := context.Background()
	if err := cli.HeartbeatJob(ctx, "job_110f47f09551baba", "lease-1", 7, "STORY-GOV-3873"); err != nil {
		t.Fatalf("expected HeartbeatJob to succeed after transient 503, got: %v", err)
	}
	if hbAttempts != 2 {
		t.Fatalf("expected 2 heartbeat attempts, got %d", hbAttempts)
	}

	if err := cli.CompleteJob(ctx, "job_110f47f09551baba", CompleteRequest{
		FencingToken: 7,
		Result:       json.RawMessage(`{"status":"completed","story_id":"STORY-GOV-3873"}`),
	}); err != nil {
		t.Fatalf("expected CompleteJob to succeed after transient 502s, got: %v", err)
	}
	if completeAttempts != 3 {
		t.Fatalf("expected 3 complete attempts, got %d", completeAttempts)
	}

	// Verify FailoverJob carries FailureDirection and FailureService
	if err := cli.FailoverJob(ctx, "job_110f47f09551baba", controlplane.AttemptFailure{
		Host:             "debian",
		Tool:             "agy",
		FailureCategory:  "TOOL_QUOTA_OR_AUTH",
		FailureStage:     "TOOL_EXECUTION",
		FailureDirection: "UPSTREAM",
		FailureService:   "antigravity-llm-provider",
	}); err != nil {
		t.Fatalf("FailoverJob failed: %v", err)
	}
	if failoverCaptured.FailureDirection != "UPSTREAM" || failoverCaptured.FailureService != "antigravity-llm-provider" {
		t.Fatalf("expected FailureDirection=UPSTREAM and FailureService=antigravity-llm-provider, got %+v", failoverCaptured)
	}
}

func TestDispatcher_ForwardsResolvedConversationIDAndDynamicToken(t *testing.T) {
	var ackConvID string
	var claimed bool
	var mu sync.Mutex
	var capturedPayload json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/agent/jobs/claim":
			mu.Lock()
			already := claimed
			claimed = true
			mu.Unlock()
			if already {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(Job{
				ID:                   "job-resume-conv",
				Type:                 JobTypePromptExecute,
				Repo:                 ".",
				TargetAgent:          "agy",
				TraceID:              "trace-123",
				ParentID:             "span-456",
				FencingToken:         7,
				LeaseToken:           "lease-abc",
				ActiveConversationID: "29c18bab-aa8a-4d78-8426-4808d832582a",
				ConversationIDs:      []string{"29c18bab-aa8a-4d78-8426-4808d832582a"},
				Payload:              json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug"}`),
			})
		case "/api/v1/agent/jobs/job-resume-conv/ack-lease":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			ackConvID, _ = body["active_conversation_id"].(string)
			w.WriteHeader(http.StatusOK)
		case "/api/v1/agent/jobs/job-resume-conv/complete":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cli := NewHTTPControlPlaneClient(srv.URL, "", nil)
	cli.TokenProvider = func(context.Context) string {
		return "dynamic-oauth-token-999"
	}
	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			capturedPayload = payload
			return &harness.PromptExecutionResult{
				Status:         "completed",
				ExitCode:       0,
				ConversationID: "29c18bab-aa8a-4d78-8426-4808d832582a",
			}, nil
		},
	}
	disp := NewDispatcher(cli, inv, "host-1")
	processed, err := disp.PollAndProcess(context.Background())
	if err != nil || !processed {
		t.Fatalf("expected job to be processed cleanly, got processed=%v err=%v", processed, err)
	}
	if ackConvID != "29c18bab-aa8a-4d78-8426-4808d832582a" {
		t.Fatalf("expected AckLease to receive active_conversation_id, got %q", ackConvID)
	}
	var forwarded map[string]any
	if err := json.Unmarshal(capturedPayload, &forwarded); err != nil {
		t.Fatalf("unmarshal forwarded payload: %v", err)
	}
	if forwarded["resume_conversation_id"] != "29c18bab-aa8a-4d78-8426-4808d832582a" {
		t.Fatalf("expected resume_conversation_id to be forwarded, got %v", forwarded["resume_conversation_id"])
	}
	if forwarded["conversation_id"] != "29c18bab-aa8a-4d78-8426-4808d832582a" {
		t.Fatalf("expected conversation_id to be forwarded, got %v", forwarded["conversation_id"])
	}
	if forwarded["lease_token"] != "lease-abc" {
		t.Fatalf("expected lease_token=lease-abc, got %v", forwarded["lease_token"])
	}
	if forwarded["token"] != "dynamic-oauth-token-999" {
		t.Fatalf("expected dynamic token=dynamic-oauth-token-999, got %v", forwarded["token"])
	}
}

func TestDispatcher_RealTimeConversationDiscoveryPropagatesHeartbeatAndAckLeaseWhileRunning(t *testing.T) {
	var mu sync.Mutex
	var heartbeatConvIDs []string
	var ackConvIDs []string
	var completedConvID string
	var claimed bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/agent/jobs/claim":
			mu.Lock()
			already := claimed
			claimed = true
			mu.Unlock()
			if already {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(Job{
				ID:           "job-rt-conv-0190",
				Type:         JobTypePromptExecute,
				Repo:         ".",
				TargetAgent:  "agy",
				FencingToken: 27,
				LeaseToken:   "lt-job-rt-conv-0190",
				Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution STORY-IAZIO-0190"}`),
			})
		case "/api/v1/agent/jobs/job-rt-conv-0190/heartbeat":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			if c, _ := body["active_conversation_id"].(string); c != "" {
				heartbeatConvIDs = append(heartbeatConvIDs, c)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case "/api/v1/agent/jobs/job-rt-conv-0190/ack-lease":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			if c, _ := body["active_conversation_id"].(string); c != "" {
				ackConvIDs = append(ackConvIDs, c)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case "/api/v1/agent/jobs/job-rt-conv-0190/complete":
			var body CompleteRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			var res map[string]any
			_ = json.Unmarshal(body.Result, &res)
			mu.Lock()
			completedConvID, _ = res["conversation_id"].(string)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	fakeBin := filepath.Join(t.TempDir(), "fake-harness-rt-0190")
	script := `#!/bin/sh
echo "CONVERSATION_ID: aff9bb70-6c84-4121-ba1e-68745328700a" >&2
echo "ALLOCATED_STORY_ID: STORY-IAZIO-0190" >&2
printf '{"status":"completed","exit_code":0,"summary":"ok"}\n'
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake-harness-rt-0190: %v", err)
	}

	cli := NewHTTPControlPlaneClient(srv.URL, "tok", nil)
	inv := harness.NewCLIInvoker(fakeBin, t.TempDir())
	disp := NewDispatcher(cli, inv, "host-1")

	processed, err := disp.PollAndProcess(context.Background())
	if err != nil || !processed {
		t.Fatalf("expected PollAndProcess to succeed, got processed=%v err=%v", processed, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(heartbeatConvIDs) == 0 || heartbeatConvIDs[0] != "aff9bb70-6c84-4121-ba1e-68745328700a" {
		t.Fatalf("expected real-time HeartbeatJob with active_conversation_id=aff9bb70-6c84-4121-ba1e-68745328700a while running, got %v", heartbeatConvIDs)
	}
	if len(ackConvIDs) == 0 || ackConvIDs[len(ackConvIDs)-1] != "aff9bb70-6c84-4121-ba1e-68745328700a" {
		t.Fatalf("expected real-time AckLease with active_conversation_id=aff9bb70-6c84-4121-ba1e-68745328700a while running, got %v", ackConvIDs)
	}
	if completedConvID != "aff9bb70-6c84-4121-ba1e-68745328700a" {
		t.Fatalf("expected completed job result to retain discovered conversation_id=aff9bb70-6c84-4121-ba1e-68745328700a, got %q", completedConvID)
	}
}

func TestDispatcher_PreClaimBarrier_EagerReadinessAndIdleWorktrees(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()

	claimCalls := 0
	var lastClaimReq ClaimRequest
	cp.claimFunc = func(_ context.Context, req ClaimRequest) (*Job, error) {
		claimCalls++
		lastClaimReq = req
		return nil, nil
	}

	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")

	// 1. Eager readiness gate returning false blocks ClaimJob
	ready := false
	d.SetEagerReadinessGate(func(context.Context) (bool, string) {
		return ready, "DRIFT_DETECTED"
	})
	processed, err := d.PollAndProcess(ctx)
	if err != nil || processed {
		t.Fatalf("expected PollAndProcess to skip when eagerReadinessGate is false, got processed=%v err=%v", processed, err)
	}
	if claimCalls != 0 {
		t.Fatalf("expected 0 ClaimJob calls when eagerReadinessGate is false, got %d", claimCalls)
	}

	// 1b. When toolUpdater is configured and eagerReadinessGate is false (e.g. DRIFT_DETECTED),
	// checkPreClaimBarrier still claims JobTypeToolsUpdate to prevent deadlock.
	d.SetToolUpdater(func(context.Context, []string, bool, ...string) error { return nil })
	processed, err = d.PollAndProcess(ctx)
	if err != nil || processed {
		t.Fatalf("expected PollAndProcess to claim only JobTypeToolsUpdate when eagerReadinessGate is false and toolUpdater is set, got processed=%v err=%v", processed, err)
	}
	if claimCalls != 1 || len(lastClaimReq.JobTypes) != 1 || lastClaimReq.JobTypes[0] != JobTypeToolsUpdate {
		t.Fatalf("expected 1 ClaimJob call with [agent.tools.update], got calls=%d req=%+v", claimCalls, lastClaimReq)
	}
	d.SetToolUpdater(nil)
	claimCalls = 0

	// 2. HostGate in GateStateUpdatingTools blocks ClaimJob
	ready = true
	realGate := supervisor.NewHostExecutionAndUpdateGate()
	relMaint, ok := realGate.TryAcquireMaintenanceLease("test")
	if !ok {
		t.Fatal("expected TryAcquireMaintenanceLease to succeed")
	}
	d.SetHostGate(realGate)
	processed, err = d.PollAndProcess(ctx)
	if err != nil || processed || claimCalls != 0 {
		t.Fatalf("expected PollAndProcess to skip while hostGate is in GateStateUpdatingTools, got processed=%v err=%v claimCalls=%d", processed, err, claimCalls)
	}
	relMaint()

	// 3. IdleWorktreesProvider returning empty slice blocks ClaimJob
	var idleList []string
	d.SetIdleWorktreesProvider(func() []string {
		return idleList
	})
	processed, err = d.PollAndProcess(ctx)
	if err != nil || processed || claimCalls != 0 {
		t.Fatalf("expected PollAndProcess to skip when IdleWorktreesProvider returns empty, got processed=%v err=%v claimCalls=%d", processed, err, claimCalls)
	}

	// 4. IdleWorktreesProvider returning non-empty slice advertises IdleWorktrees in ClaimRequest
	idleList = []string{"/work/slot-1", "/work/slot-2"}
	processed, err = d.PollAndProcess(ctx)
	if err != nil || processed {
		t.Fatalf("expected PollAndProcess with no jobs to return false, nil; got processed=%v err=%v", processed, err)
	}
	if claimCalls != 1 {
		t.Fatalf("expected 1 ClaimJob call once barrier is ready and idle worktrees exist, got %d", claimCalls)
	}
	if len(lastClaimReq.IdleWorktrees) != 2 || lastClaimReq.IdleWorktrees[0] != "/work/slot-1" || lastClaimReq.IdleWorktrees[1] != "/work/slot-2" {
		t.Fatalf("expected IdleWorktrees=[/work/slot-1 /work/slot-2], got %v", lastClaimReq.IdleWorktrees)
	}
}

func TestDispatcher_CollectIdleWorktrees_FiltersBusyLockedAndDirty(t *testing.T) {
	cleanIdle := initTestFleetWorkspace(t)
	inFlightWT := initTestFleetWorkspace(t)
	lockedWT := initTestFleetWorkspace(t)
	sessionWT := initTestFleetWorkspace(t)
	dirtyWT := initTestFleetWorkspace(t)

	// Mark lockedWT with .iazio-repolock held by current process
	if err := os.WriteFile(filepath.Join(lockedWT, ".iazio-repolock"), []byte(`{"holder_pid":999999999}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// Mark lockedWT with live repolock
	h, err := repolock.Acquire(lockedWT, repolock.LockInfo{
		JobID:  "other-job",
		PID:    os.Getpid(),
		Status: "RUNNING",
	}, func(int) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Release() }()

	// Mark sessionWT with active .session_story_id
	if err := os.WriteFile(filepath.Join(sessionWT, ".session_story_id"), []byte("STORY-IAZIO-0196\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Mark dirtyWT with uncommitted tracked modification
	if err := os.WriteFile(filepath.Join(dirtyWT, "README.md"), []byte("dirty change\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	d := NewDispatcher(newMockControlPlane(), &mockInvoker{}, "host-1")
	d.tryReserveInFlightWorktree("in-flight-job", inFlightWT)
	defer d.clearInFlightWorktree("in-flight-job")

	got := d.CollectIdleWorktrees([]string{
		"",
		filepath.Join(t.TempDir(), "nonexistent-worktree-path"),
		cleanIdle,
		cleanIdle, // duplicate
		inFlightWT,
		lockedWT,
		sessionWT,
		dirtyWT,
	})
	if len(got) != 1 || got[0] != filepath.Clean(cleanIdle) {
		t.Fatalf("expected only [%s] to be returned as idle, got %v", filepath.Clean(cleanIdle), got)
	}
}

func TestDispatcher_AtomicLocalReservationBeforeAckLease_AndDeclineOnBusy(t *testing.T) {
	ctx := context.Background()
	wt := initTestFleetWorkspace(t)

	cp := newMockControlPlane()
	gate := supervisor.NewHostExecutionAndUpdateGate()

	var inv *mockInvoker
	var d *Dispatcher

	ackCalled := false
	cp.ackLeaseFunc = func(_ context.Context, jobID, _ string, _ int, _ string) error {
		ackCalled = true
		// Verify that BEFORE/DURING AckLease:
		// 1. inFlightPrepJobs has reserved wt
		if !d.IsWorktreeInFlight(wt) {
			t.Errorf("expected worktree %s to be reserved in inFlightPrepJobs before AckLease", wt)
		}
		// 2. hostGate holds execution lease
		if gate.State() != supervisor.GateStateExecutingJobs {
			t.Errorf("expected hostGate state=%s during AckLease, got %s", supervisor.GateStateExecutingJobs, gate.State())
		}
		// 3. repolock is held by jobID on wt
		info, err := repolock.Read(wt)
		if err != nil || info.JobID != jobID || info.Status != "RUNNING" {
			t.Errorf("expected repolock to be held by %s before AckLease, got info=%+v err=%v", jobID, info, err)
		}
		return nil
	}

	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-atomic-1",
			Type:         JobTypePromptExecute,
			Repo:         "fleet",
			WorktreePath: wt,
			LeaseToken:   "lease-atomic-1",
			FencingToken: 10,
			Payload:      json.RawMessage(`{"prompt":"test"}`),
		}, nil
	}

	inv = &mockInvoker{}
	d = NewDispatcher(cp, inv, "host-1")
	d.SetHostGate(gate)

	processed, err := d.PollAndProcess(ctx)
	if err != nil || !processed {
		t.Fatalf("expected PollAndProcess to succeed, got processed=%v err=%v", processed, err)
	}
	if !ackCalled {
		t.Fatal("expected AckLease to be called after local reservation")
	}
	if d.IsWorktreeInFlight(wt) {
		t.Fatal("expected inFlightPrepJobs to be cleaned up after PollAndProcess")
	}
	if gate.State() != supervisor.GateStateIdle {
		t.Fatalf("expected hostGate to be IDLE after PollAndProcess, got %s", gate.State())
	}

	// Now simulate wt already reserved in inFlightPrepJobs by another job -> must DeclineJob("REPO_BUSY") WITHOUT AckLease
	ackCalled = false
	delete(cp.ackedJobs, "job-busy-2")
	delete(cp.declinedJobs, "job-busy-2")
	d.tryReserveInFlightWorktree("other-active-job", wt)
	defer d.clearInFlightWorktree("other-active-job")

	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-busy-2",
			Type:         JobTypePromptExecute,
			Repo:         "fleet",
			WorktreePath: wt,
			LeaseToken:   "lease-busy-2",
			FencingToken: 11,
			Payload:      json.RawMessage(`{"prompt":"test"}`),
		}, nil
	}

	processed, err = d.PollAndProcess(ctx)
	if err != nil || processed {
		t.Fatalf("expected PollAndProcess to decline and return (false, nil) when worktree is busy, got processed=%v err=%v", processed, err)
	}
	if ackCalled {
		t.Fatal("expected AckLease NOT to be called when local worktree reservation fails")
	}
	if cp.declinedJobs["job-busy-2"] != "REPO_BUSY" {
		t.Fatalf("expected DeclineJob(job-busy-2, REPO_BUSY), got %q", cp.declinedJobs["job-busy-2"])
	}
}

type abandonOnlyControlPlane struct {
	*mockControlPlane
	abandoned map[string]string
}

func (a *abandonOnlyControlPlane) AbandonJob(_ context.Context, jobID, reason string) error {
	a.abandoned[jobID] = reason
	return nil
}

type plainControlPlane struct {
	delegate *mockControlPlane
}

func (p *plainControlPlane) ClaimJob(ctx context.Context, req ClaimRequest) (*Job, error) {
	return p.delegate.ClaimJob(ctx, req)
}
func (p *plainControlPlane) CompleteJob(ctx context.Context, jobID string, req CompleteRequest) error {
	return p.delegate.CompleteJob(ctx, jobID, req)
}
func (p *plainControlPlane) HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64, storyID ...string) error {
	return p.delegate.HeartbeatJob(ctx, jobID, leaseToken, fencingToken, storyID...)
}
func (p *plainControlPlane) BindStory(ctx context.Context, jobID string, req StoryBindingRequest) error {
	return p.delegate.BindStory(ctx, jobID, req)
}
func (p *plainControlPlane) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error {
	return p.delegate.AckLease(ctx, jobID, leaseToken, agentPID, activeConvID)
}
func (p *plainControlPlane) FailoverJob(ctx context.Context, jobID string, req controlplane.AttemptFailure) error {
	return p.delegate.FailoverJob(ctx, jobID, req)
}

type abandonWrapper struct {
	plainControlPlane
	abandoned map[string]string
}

func (a *abandonWrapper) AbandonJob(_ context.Context, jobID, reason string) error {
	a.abandoned[jobID] = reason
	return nil
}

func TestHTTPControlPlaneClient_DeclineJob_And_DispatcherHelpers(t *testing.T) {
	ctx := context.Background()

	var hitPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitPaths = append(hitPaths, r.URL.Path)
		switch r.URL.Path {
		case "/v1/jobs/job-200/decline":
			w.WriteHeader(http.StatusOK)
		case "/v1/jobs/job-404-fallback/decline":
			w.WriteHeader(http.StatusNotFound)
		case "/api/v1/agent/jobs/job-404-fallback/decline":
			w.WriteHeader(http.StatusNoContent)
		case "/v1/jobs/job-500/decline":
			http.Error(w, "boom", http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cli := NewHTTPControlPlaneClient(srv.URL, "tok", srv.Client())
	cli.MaxRetries = 0

	// 1. Direct 200 OK on /v1/jobs/{id}/decline
	if err := cli.DeclineJob(ctx, "job-200", "REPO_BUSY"); err != nil {
		t.Fatalf("DeclineJob 200 failed: %v", err)
	}
	// 2. 404 fallback to /api/v1/agent/jobs/{id}/decline
	if err := cli.DeclineJob(ctx, "job-404-fallback", "REPO_BUSY"); err != nil {
		t.Fatalf("DeclineJob 404 fallback failed: %v", err)
	}
	// 3. Error status
	if err := cli.DeclineJob(ctx, "job-500", "REPO_BUSY"); err == nil {
		t.Fatal("expected error from DeclineJob on 400 status")
	}

	// 4. Dispatcher.ActiveJobs, SetRepoMap, SetWorkspace, CollectIdleWorktrees with repoMap/workspace fallback
	wt1 := initTestFleetWorkspace(t)
	wt2 := initTestFleetWorkspace(t)
	d := NewDispatcher(newMockControlPlane(), &mockInvoker{}, "host-1")
	d.SetRepoMap(map[string]string{"r1": wt1})
	d.SetWorkspace(wt2)
	idle := d.CollectIdleWorktrees(nil)
	if len(idle) != 2 {
		t.Fatalf("expected 2 idle worktrees from repoMap+workspace fallback, got %v", idle)
	}

	d.setActiveJob("", wt1, 123) // empty jobID no-op
	d.setActiveJob("active-1", wt1, 1234)
	if len(d.ActiveJobs()) != 1 || d.ActiveJobs()[0].JobID != "active-1" {
		t.Fatalf("unexpected ActiveJobs: %+v", d.ActiveJobs())
	}
	if !d.IsWorktreeInFlight(wt1) {
		t.Fatal("expected active job worktree to be in-flight")
	}
	if d.IsWorktreeInFlight("") || d.IsWorktreeInFlight(".") {
		t.Fatal("expected empty or '.' worktree not to be in-flight")
	}
	if !d.tryReserveInFlightWorktree("", wt1) {
		t.Fatal("expected empty jobID in tryReserveInFlightWorktree to return true")
	}
	if d.tryReserveInFlightWorktree("other-job", wt1) {
		t.Fatal("expected tryReserveInFlightWorktree to fail when wt1 is in activeJobs")
	}
	d.clearActiveJob("active-1")

	// 5. isWorktreeLockedByOther edge cases
	if isWorktreeLockedByOther("", "j1") || isWorktreeLockedByOther(".", "j1") {
		t.Fatal("expected empty/dot worktree not to be locked")
	}
	wtLock := initTestFleetWorkspace(t)
	// .git/index.lock
	idxLock := filepath.Join(wtLock, ".git", "index.lock")
	_ = os.WriteFile(idxLock, []byte("lock"), 0o644)
	if !isWorktreeLockedByOther(wtLock, "j1") {
		t.Fatal("expected .git/index.lock to report locked")
	}
	_ = os.Remove(idxLock)

	// .iazio-repolock with alive PID
	dotLock := filepath.Join(wtLock, ".iazio-repolock")
	_ = os.WriteFile(dotLock, []byte(`{"pid":`+strings.TrimSpace(string(rune(0)))+`}`), 0o644)
	_ = os.WriteFile(dotLock, []byte("plain-lock"), 0o644)
	if !isWorktreeLockedByOther(wtLock, "j1") {
		t.Fatal("expected non-JSON .iazio-repolock to report locked")
	}
	_ = os.Remove(dotLock)

	// 6. declineJob with nil client, plain client, and AbandonJob wrapper
	dNil := &Dispatcher{}
	_ = dNil.declineJob(ctx, "j1", "REPO_BUSY")
	dPlain := NewDispatcher(&plainControlPlane{delegate: newMockControlPlane()}, &mockInvoker{}, "h1")
	_ = dPlain.declineJob(ctx, "j1", "REPO_BUSY")
	ab := &abandonWrapper{plainControlPlane: plainControlPlane{delegate: newMockControlPlane()}, abandoned: make(map[string]string)}
	dAb := NewDispatcher(ab, &mockInvoker{}, "h1")
	_ = dAb.declineJob(ctx, "j-ab", "REPO_BUSY")
	if ab.abandoned["j-ab"] != "REPO_BUSY" {
		t.Fatalf("expected AbandonJob fallback to record REPO_BUSY, got %v", ab.abandoned)
	}

	// 7. PollAndProcess declines when worktree has .git/index.lock or hostGate fails with LeaseToken != ""
	cp := newMockControlPlane()
	_ = os.WriteFile(idxLock, []byte("lock"), 0o644)
	defer os.Remove(idxLock)
	cp.claimFunc = func(context.Context, ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job-idx-locked",
			Type:         JobTypePromptExecute,
			WorktreePath: wtLock,
			LeaseToken:   "lt-idx",
			Payload:      json.RawMessage(`{"story_id":"STORY-1"}`),
		}, nil
	}
	dIdx := NewDispatcher(cp, &mockInvoker{}, "h1")
	dIdx.SetHostGate(&mockHostGate{})
	processed, err := dIdx.PollAndProcess(ctx)
	if err != nil || processed || cp.declinedJobs["job-idx-locked"] != "REPO_BUSY" {
		t.Fatalf("expected job-idx-locked to be declined with REPO_BUSY, got processed=%v err=%v declined=%v", processed, err, cp.declinedJobs)
	}

	// 8. PollAndProcess declines when hostGate.AcquireExecutionLease fails and job.LeaseToken != ""
	dGateFail := NewDispatcher(cp, &mockInvoker{}, "h1")
	dGateFail.SetHostGate(&mockHostGate{block: true})
	delete(cp.declinedJobs, "job-idx-locked")
	processed, err = dGateFail.PollAndProcess(ctx)
	if err != nil || processed || cp.declinedJobs["job-idx-locked"] != "REPO_BUSY" {
		t.Fatalf("expected hostGate block with LeaseToken to decline with REPO_BUSY, got processed=%v err=%v declined=%v", processed, err, cp.declinedJobs)
	}

	// 9. SetWorktreeBusyDetector and workspace sentinel (.autopilot/active_job.json) exclude busy worktrees
	d.SetWorktreeBusyDetector(func(p string) bool {
		return p == filepath.Clean(wt1)
	})
	idleAfterBusy := d.CollectIdleWorktrees(nil)
	if len(idleAfterBusy) != 1 || idleAfterBusy[0] != filepath.Clean(wt2) {
		t.Fatalf("expected only wt2 to be idle when wt1 is marked busy by worktreeBusyDetector, got %v", idleAfterBusy)
	}
	d.SetWorktreeBusyDetector(nil)

	sentinelPath := filepath.Join(wt2, ".autopilot", "active_job.json")
	_ = os.MkdirAll(filepath.Dir(sentinelPath), 0o755)
	_ = os.WriteFile(sentinelPath, []byte(`{"job_id":"job-ext"}`), 0o644)
	if !isWorktreeLockedByOther(wt2, "other-job") {
		t.Fatal("expected active workspace sentinel to mark worktree locked")
	}
	if isWorktreeLockedByOther(wt2, "job-ext") {
		t.Fatal("expected workspace sentinel with matching jobID not to lock against itself")
	}
	_ = os.Remove(sentinelPath)
}

func TestDispatcher_ToolsUpdateClaimableWhenAllWorktreesDirty_StoryGOV5195(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()

	var claimedReqs []ClaimRequest
	cp.claimFunc = func(_ context.Context, req ClaimRequest) (*Job, error) {
		claimedReqs = append(claimedReqs, req)
		return &Job{
			ID:           "job_tools_update_gov5195",
			Type:         JobTypeToolsUpdate,
			Repo:         "fleet",
			FencingToken: 99,
			Payload:      json.RawMessage(`{"tools":["codex"],"force":true}`),
		}, nil
	}

	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "docker1")
	d.SetIdleWorktreesProvider(func() []string {
		return nil // simulate all git worktrees dirty or busy
	})
	// When toolUpdater is configured, checkPreClaimBarrier still claims host-level agent.tools.update jobs
	// and resolveJobWorktree bypasses git worktree resolution for repo="fleet".
	d.SetWorktreeResolver(func(repo string) string {
		return "/nonexistent/locked/worktree"
	})
	updatedTools := 0
	d.SetToolUpdater(func(_ context.Context, tools []string, force bool, targetVersion ...string) error {
		updatedTools += len(tools)
		return nil
	})

	processed, err := d.PollAndProcess(ctx)
	if err != nil {
		t.Fatalf("PollAndProcess returned error: %v", err)
	}
	if !processed {
		t.Fatalf("expected PollAndProcess to claim and process agent.tools.update even when idle worktrees is empty")
	}
	if len(claimedReqs) != 1 {
		t.Fatalf("expected 1 ClaimJob request, got %d", len(claimedReqs))
	}
	if len(claimedReqs[0].JobTypes) != 1 || claimedReqs[0].JobTypes[0] != JobTypeToolsUpdate {
		t.Fatalf("expected ClaimRequest.JobTypes=[%s], got %v", JobTypeToolsUpdate, claimedReqs[0].JobTypes)
	}
	if updatedTools != 1 {
		t.Fatalf("expected toolUpdater to run for 1 tool, got %d", updatedTools)
	}
	if _, ok := cp.completedJobs["job_tools_update_gov5195"]; !ok {
		t.Fatalf("expected job_tools_update_gov5195 to be completed, completedJobs=%v", cp.completedJobs)
	}
}

func TestStoryGOV5196_ToolsUpdateWhenIdleWorktreesEmpty_AndExpiredMaintenanceLocks(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "pc1")

	var lastClaimReq ClaimRequest
	toolUpdated := false
	d.SetToolUpdater(func(_ context.Context, tools []string, force bool, targetVersion ...string) error {
		toolUpdated = true
		return nil
	})
	d.SetIdleWorktreesProvider(func() []string {
		return nil // simulate all worktrees busy or dirty
	})
	d.SetWorktreeResolver(func(repo string) string {
		t.Fatalf("expected worktreeResolver NOT to be called for agent.tools.update, got repo=%q", repo)
		return ""
	})

	cp.claimFunc = func(_ context.Context, req ClaimRequest) (*Job, error) {
		lastClaimReq = req
		return &Job{
			ID:         "job_f633dd5b34c16f08",
			Type:       JobTypeToolsUpdate,
			Repo:       "fleet",
			LeaseToken: "lease-tools-1",
			Payload:    json.RawMessage(`{"tools":["iazio-agent"],"force":false}`),
		}, nil
	}

	processed, err := d.PollAndProcess(ctx)
	if err != nil || !processed {
		t.Fatalf("expected PollAndProcess to claim and execute agent.tools.update even when idle worktrees is empty, got processed=%v err=%v", processed, err)
	}
	if len(lastClaimReq.JobTypes) != 1 || lastClaimReq.JobTypes[0] != JobTypeToolsUpdate {
		t.Fatalf("expected ClaimRequest.JobTypes=[%s], got %v", JobTypeToolsUpdate, lastClaimReq.JobTypes)
	}
	if !toolUpdated {
		t.Fatal("expected toolUpdater callback to execute")
	}

	// Verify expired autopilot-git-update maintenance lock does not block CollectIdleWorktrees or repolock.Acquire
	wt := initTestFleetWorkspace(t)
	lockDir := filepath.Join(wt, ".iazio")
	_ = os.MkdirAll(lockDir, 0o755)
	expiredAt := time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339)
	expiredLockJSON := `{"job_id":"autopilot-git-update","pid":` + jsonNumber(os.Getpid()) + `,"started_at":"` + expiredAt + `","status":"RUNNING"}`
	if err := os.WriteFile(filepath.Join(lockDir, "repo.lock"), []byte(expiredLockJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	idle := d.CollectIdleWorktrees([]string{wt})
	if len(idle) != 1 || idle[0] != filepath.Clean(wt) {
		t.Fatalf("expected worktree with expired autopilot-git-update lock to be collected as idle, got %v", idle)
	}
	h, err := repolock.Acquire(wt, repolock.LockInfo{
		JobID:  "job-new-1",
		PID:    os.Getpid(),
		Status: "RUNNING",
	}, func(int) bool { return true })
	if err != nil {
		t.Fatalf("expected repolock.Acquire to overwrite expired autopilot-git-update lock, got %v", err)
	}
	_ = h.Release()
}

func jsonNumber(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestStoryGOV5196_Integration_DriftToolsUpdateClaim_AckBeforePreDispatchHook_And_RichToolUpdateResult(t *testing.T) {
	ctx := context.Background()
	cp := newMockControlPlane()
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "debian")

	// Part 1: When eagerReadinessGate returns (false, "DRIFT_DETECTED"), PollAndProcess still claims
	// JobTypeToolsUpdate, executes toolUpdater, and completes with rich telemetry (summary, output, agent_pid, elapsed_seconds).
	d.SetEagerReadinessGate(func(context.Context) (bool, string) {
		return false, "DRIFT_DETECTED"
	})
	var updatedTools []string
	d.SetToolUpdater(func(_ context.Context, tools []string, force bool, targetVersion ...string) error {
		updatedTools = append(updatedTools, tools...)
		return nil
	})
	cp.claimFunc = func(_ context.Context, req ClaimRequest) (*Job, error) {
		if len(req.JobTypes) != 1 || req.JobTypes[0] != JobTypeToolsUpdate {
			t.Fatalf("expected ClaimRequest.JobTypes=[%s] during DRIFT_DETECTED, got %v", JobTypeToolsUpdate, req.JobTypes)
		}
		return &Job{
			ID:           "job_9224339206904244",
			Type:         JobTypeToolsUpdate,
			Repo:         "fleet",
			LeaseToken:   "lease-9224",
			FencingToken: 7,
			Payload:      json.RawMessage(`{"tools":["iazio-agent","iazio-harness"],"force":true}`),
		}, nil
	}

	processed, err := d.PollAndProcess(ctx)
	if err != nil || !processed {
		t.Fatalf("expected PollAndProcess to succeed for job_9224339206904244 during DRIFT_DETECTED, got processed=%v err=%v", processed, err)
	}
	if len(updatedTools) != 2 || updatedTools[0] != "iazio-agent" || updatedTools[1] != "iazio-harness" {
		t.Fatalf("expected updatedTools=[iazio-agent iazio-harness], got %v", updatedTools)
	}
	compReq, ok := cp.completedJobs["job_9224339206904244"]
	if !ok {
		t.Fatal("expected job_9224339206904244 in completedJobs")
	}
	var resMap map[string]any
	if err := json.Unmarshal(compReq.Result, &resMap); err != nil {
		t.Fatalf("failed to unmarshal tool update result: %v", err)
	}
	if resMap["status"] != "completed" || resMap["updated"] != true || resMap["host_id"] != "debian" {
		t.Fatalf("expected status=completed updated=true host_id=debian, got %+v", resMap)
	}
	if summary, _ := resMap["summary"].(string); !strings.Contains(summary, "Completed agent.tools.update on host debian") {
		t.Fatalf("expected non-empty summary in tool update result, got %q", summary)
	}
	if pid, _ := resMap["agent_pid"].(float64); int(pid) <= 0 {
		t.Fatalf("expected positive agent_pid in tool update result, got %v", resMap["agent_pid"])
	}

	// Part 2: Verify AckLease is invoked BEFORE preDispatchHook runs so slow pre-dispatch checks or workspace prep
	// never cause 20s LEASE_ACK_TIMEOUT_REQUEUED on the control plane.
	d.SetEagerReadinessGate(func(context.Context) (bool, string) {
		return true, "UP_TO_DATE"
	})
	var eventOrder []string
	cp.ackLeaseFunc = func(_ context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error {
		eventOrder = append(eventOrder, "ack_lease:"+jobID)
		return nil
	}
	d.SetPreDispatchHook(func(_ context.Context, job *Job) error {
		eventOrder = append(eventOrder, "pre_dispatch_hook:"+job.ID)
		return nil
	})
	cp.claimFunc = func(_ context.Context, req ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job_ack_before_prep",
			Type:         JobTypePromptExecute,
			Repo:         ".",
			LeaseToken:   "lease-ack-order",
			FencingToken: 8,
			Payload:      json.RawMessage(`{"prompt":"verify order"}`),
		}, nil
	}
	processed2, err2 := d.PollAndProcess(ctx)
	if err2 != nil || !processed2 {
		t.Fatalf("expected PollAndProcess for job_ack_before_prep to succeed, got processed=%v err=%v", processed2, err2)
	}
	if len(eventOrder) < 2 || eventOrder[0] != "ack_lease:job_ack_before_prep" || eventOrder[1] != "pre_dispatch_hook:job_ack_before_prep" {
		t.Fatalf("expected AckLease to fire before preDispatchHook, got order=%v", eventOrder)
	}
}

func TestASECompletionGate_Invariants_StoryGov5232(t *testing.T) {
	// 1. Missing story binding fails closed
	wt := t.TempDir()
	_ = exec.Command("git", "init", wt).Run()
	_ = exec.Command("git", "-C", wt, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", wt, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", wt, "checkout", "-b", "main").Run()
	f := filepath.Join(wt, "file.txt")
	_ = os.WriteFile(f, []byte("init\n"), 0o644)
	_ = exec.Command("git", "-C", wt, "add", "file.txt").Run()
	_ = exec.Command("git", "-C", wt, "commit", "-m", "init").Run()

	var lastCompleteReq CompleteRequest
	cp := newMockControlPlane()
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) {
			return &Job{
				ID:           "job_ase_missing_story",
				Type:         JobTypePromptExecute,
				Repo:         wt,
				FencingToken: 1,
				Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
			}, nil
		}
	cp.completeFunc = func(_ context.Context, _ string, req CompleteRequest) error {
		lastCompleteReq = req
		return nil
	}
	inv := &mockInvoker{
		executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
			return &harness.PromptExecutionResult{
				Status:   "completed",
				ExitCode: 0,
				// No StoryID set
			}, nil
		},
	}
	d := NewDispatcher(cp, inv, "host-1")
	processed, err := d.PollAndProcess(context.Background())
	if !processed || err == nil || !strings.Contains(err.Error(), "ASE_COMPLETION_GATE_VIOLATION") || !strings.Contains(err.Error(), "no story ID was bound") {
		t.Fatalf("expected missing story binding to fail completion gate, processed=%v err=%v", processed, err)
	}
	if !strings.Contains(lastCompleteReq.Error, "ASE_COMPLETION_GATE_VIOLATION") {
		t.Fatalf("expected CompleteRequest.Error to contain gate violation, got %q", lastCompleteReq.Error)
	}

	// 2. Dirty worktree fails closed
	dirtyFile := filepath.Join(wt, "dirty.txt")
	_ = os.WriteFile(dirtyFile, []byte("uncommitted\n"), 0o644)
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) {
		return &Job{
			ID:           "job_ase_dirty_wt",
			Type:         JobTypePromptExecute,
			Repo:         wt,
			StoryID:      "STORY-GOV-5232",
			FencingToken: 2,
			Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
		}, nil
	}
	inv.executePromptFunc = func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
		return &harness.PromptExecutionResult{
			Status:   "completed",
			ExitCode: 0,
			StoryID:  "STORY-GOV-5232",
		}, nil
	}
	// Resume mode so acquireWorktreeRepoLock doesn't decline
	jobDirty := &Job{
		ID:                   "job_ase_dirty_wt",
		Type:                 JobTypePromptExecute,
		Repo:                 wt,
		StoryID:              "STORY-GOV-5232",
		ActiveConversationID: "conv-dirty-1",
		FencingToken:         2,
		Payload:              json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobDirty, nil }

	processed, err = d.PollAndProcess(context.Background())
	if !processed || err == nil || !strings.Contains(err.Error(), "dirty with uncommitted changes") {
		t.Fatalf("expected dirty worktree to fail completion gate, processed=%v err=%v", processed, err)
	}
	_ = os.Remove(dirtyFile)

	// 3. Feature branch fails closed
	_ = exec.Command("git", "-C", wt, "checkout", "-b", "feat/STORY-GOV-5232").Run()
	jobFeat := &Job{
		ID:                   "job_ase_feat_wt",
		Type:                 JobTypePromptExecute,
		Repo:                 wt,
		StoryID:              "STORY-GOV-5232",
		ActiveConversationID: "conv-feat-1",
		FencingToken:         3,
		Payload:              json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobFeat, nil }

	processed, err = d.PollAndProcess(context.Background())
	if !processed || err == nil || !strings.Contains(err.Error(), "unmerged feature branch") {
		t.Fatalf("expected feature branch to fail completion gate, processed=%v err=%v", processed, err)
	}
	_ = exec.Command("git", "-C", wt, "checkout", "main").Run()

	// 4. Clean worktree passes and triggers syncer
	mockSync := &mockRepoSyncer{}
	dClean := NewDispatcher(cp, inv, "host-1", mockSync)

	jobClean := &Job{
		ID:           "job_ase_clean",
		Type:         JobTypePromptExecute,
		Repo:         "meta-repo",
		StoryID:      "STORY-GOV-5232",
		FencingToken: 4,
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobClean, nil }
	lastCompleteReq = CompleteRequest{}

	processed, err = dClean.PollAndProcess(context.Background())
	if !processed || err != nil {
		t.Fatalf("expected clean ASE job to succeed, processed=%v err=%v", processed, err)
	}
	if lastCompleteReq.Error != "" {
		t.Fatalf("expected no CompleteRequest error, got %q", lastCompleteReq.Error)
	}
	if len(mockSync.triggeredRepos) == 0 || mockSync.triggeredRepos[0] != "meta-repo" {
		t.Fatalf("expected syncer to be triggered for meta-repo, got %v", mockSync.triggeredRepos)
	}

	// 5. Unpushed commits fail closed in ASE completion gate (AC1 / finding F9)
	bareRemote := t.TempDir()
	_ = exec.Command("git", "init", "--bare", bareRemote).Run()
	_ = exec.Command("git", "-C", wt, "remote", "add", "origin", bareRemote).Run()
	_ = exec.Command("git", "-C", wt, "push", "-u", "origin", "main").Run()

	// Commit ahead without pushing
	_ = os.WriteFile(filepath.Join(wt, "file.txt"), []byte("ahead\n"), 0o644)
	_ = exec.Command("git", "-C", wt, "commit", "-am", "commit ahead of origin").Run()

	jobUnpushed := &Job{
		ID:                   "job_ase_unpushed",
		Type:                 JobTypePromptExecute,
		Repo:                 wt,
		StoryID:              "STORY-GOV-5232",
		ActiveConversationID: "conv-unpushed-1",
		FencingToken:         5,
		Payload:              json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobUnpushed, nil }
	processed, err = dClean.PollAndProcess(context.Background())
	if !processed || err == nil || !strings.Contains(err.Error(), "unpushed commits after ASE run") {
		t.Fatalf("expected unpushed commits to fail completion gate, processed=%v err=%v", processed, err)
	}
	// Push commits to restore clean state on origin/main
	_ = exec.Command("git", "-C", wt, "push", "origin", "main").Run()

	// 6. Worktree reservation decline with repo_dirty_uncompleted_ase (AC4 / finding F3 & F6)
	dirtyForDecline := filepath.Join(wt, "dirty-decline.txt")
	_ = os.WriteFile(dirtyForDecline, []byte("dirty content\n"), 0o644)
	jobDecline := &Job{
		ID:           "job_decline_dirty_wt",
		Type:         JobTypePromptExecute,
		Repo:         wt,
		StoryID:      "STORY-GOV-9999",
		FencingToken: 6,
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution new story","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobDecline, nil }
	processed, err = dClean.PollAndProcess(context.Background())
	if processed || err != nil {
		t.Fatalf("expected dirty worktree claim to be declined without error, processed=%v err=%v", processed, err)
	}
	if reason := cp.declinedJobs["job_decline_dirty_wt"]; reason != reasonRepoDirtyUncompletedASE {
		t.Fatalf("expected decline reason %q, got %q", reasonRepoDirtyUncompletedASE, reason)
	}
	_ = os.Remove(dirtyForDecline)

	// 7. AC3 Sequence Invariant: Tool Update -> Repo Syncer -> CompleteJob (finding F1)
	var callSequence []string
	dSeq := NewDispatcher(cp, inv, "host-1", &mockSequenceSyncer{
		syncCallback: func(repo string) {
			callSequence = append(callSequence, "repo_syncer:"+repo)
		},
	})
	dSeq.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
		callSequence = append(callSequence, "tool_updater")
		return nil
	})
	cp.completeFunc = func(_ context.Context, jobID string, req CompleteRequest) error {
		callSequence = append(callSequence, "complete_job:"+jobID)
		lastCompleteReq = req
		return nil
	}
	jobSeq := &Job{
		ID:           "job_ase_sequence",
		Type:         JobTypePromptExecute,
		Repo:         "meta-repo",
		StoryID:      "STORY-GOV-5232",
		FencingToken: 7,
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution sequence test","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	cp.claimFunc = func(_ context.Context, _ ClaimRequest) (*Job, error) { return jobSeq, nil }
	processed, err = dSeq.PollAndProcess(context.Background())
	if !processed || err != nil {
		t.Fatalf("expected sequence test job to succeed, processed=%v err=%v", processed, err)
	}
	expectedSeq := []string{"tool_updater", "repo_syncer:meta-repo", "complete_job:job_ase_sequence"}
	if len(callSequence) < 3 || callSequence[0] != expectedSeq[0] || callSequence[1] != expectedSeq[1] || callSequence[2] != expectedSeq[2] {
		t.Fatalf("expected call sequence %v, got %v", expectedSeq, callSequence)
	}

	// 8. isASEJob precision: mention in general prompt does not trigger ASE gate (finding F8)
	generalJob := &Job{
		ID:           "job_general_chat",
		Type:         JobTypePromptExecute,
		Repo:         "meta-repo",
		FencingToken: 8,
		Payload:      json.RawMessage(`{"prompt":"can you explain how /autonomous-story-execution works?","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	if isASEJob(generalJob) {
		t.Fatal("expected isASEJob to be false for question mentioning /autonomous-story-execution in body")
	}
	aseSlashJob := &Job{
		ID:           "job_ase_slash",
		Type:         JobTypePromptExecute,
		Repo:         "meta-repo",
		FencingToken: 9,
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution fix bug","worktree_path":"` + filepath.ToSlash(wt) + `"}`),
	}
	if !isASEJob(aseSlashJob) {
		t.Fatal("expected isASEJob to be true for leading /autonomous-story-execution")
	}
}

type mockSequenceSyncer struct {
	syncCallback func(repo string)
}

func (m *mockSequenceSyncer) TriggerRepoSync(repoName string) {
	if m.syncCallback != nil {
		m.syncCallback(repoName)
	}
}



