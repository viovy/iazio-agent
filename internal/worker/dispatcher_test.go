package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
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

	completedJobs map[string]CompleteRequest
	boundStories  map[string]StoryBindingRequest
}

func newMockControlPlane() *mockControlPlane {
	return &mockControlPlane{
		completedJobs: make(map[string]CompleteRequest),
		boundStories:  make(map[string]StoryBindingRequest),
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

func (m *mockControlPlane) HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64) error {
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
		StoryID:    "STORY-GOV-3688",
		StoryTitle: "Title",
		StoryPath:  "docs/stories/story.md",
	})
	if err != nil {
		t.Fatalf("BindStory failed: %v", err)
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
