package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
