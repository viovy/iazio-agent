package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	completedJobs map[string]CompleteRequest
}

func newMockControlPlane() *mockControlPlane {
	return &mockControlPlane{
		completedJobs: make(map[string]CompleteRequest),
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

func (m *mockControlPlane) HeartbeatJob(ctx context.Context, jobID string, fencingToken int64) error {
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
			Payload:      json.RawMessage(`{"tools":["autopilot"],"force":true}`),
			FencingToken: 42,
		}, nil
	}

	toolUpdateCalled := false
	inv := &mockInvoker{}
	d := NewDispatcher(cp, inv, "host-1")
	d.SetToolUpdater(func(ctx context.Context, tools []string, force bool) error {
		toolUpdateCalled = true
		if len(tools) != 1 || tools[0] != "autopilot" || !force {
			t.Errorf("unexpected tool update params: tools=%v force=%v", tools, force)
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
	if err := client.HeartbeatJob(ctx, "job-1", 12); err != nil {
		t.Fatalf("HeartbeatJob error: %v", err)
	}

	// 7. Heartbeat failure (fenced)
	if err := client.HeartbeatJob(ctx, "job-1", 999); err == nil {
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

