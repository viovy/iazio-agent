package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLoopPollsUntilCancel(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if strings.HasSuffix(r.URL.Path, "/poll") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	err := Client{BaseURL: srv.URL}.Loop(ctx, "runner-1", "permanent", 15*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("expected register and heartbeat, got %d", n)
	}
}

func TestHeartbeatAndChunk(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = append(seen, r.Method+" "+r.URL.Path+" "+string(b))
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Fatalf("auth %s", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := Client{BaseURL: srv.URL, Token: "tok"}
	ctx := context.Background()
	if err := c.Register(ctx, "runner-1", "permanent"); err != nil {
		t.Fatal(err)
	}
	if err := c.Heartbeat(ctx, "runner-1", []Tool{{Name: "iazio-harness", Path: "/bin/iazio-harness", Version: "1.0.0", Status: "OK"}}, false); err != nil {
		t.Fatal(err)
	}
	if err := c.PostChunk(ctx, "job-1", "stdout", "building"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(seen, "\n")
	if !strings.Contains(joined, "/v1/hosts/register") || !strings.Contains(joined, "/heartbeat") || !strings.Contains(joined, "/v1/jobs/job-1/chunks") {
		t.Fatalf("calls: %s", joined)
	}
	if strings.Contains(joined, "s3cret") {
		t.Fatal("secret leaked")
	}
}

func TestPostPreflightAndFinish(t *testing.T) {
	var seenPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPaths = append(seenPaths, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer test-tok" {
			t.Fatalf("auth %s", r.Header.Get("Authorization"))
		}
		switch r.URL.Path {
		case "/v1/repos/runner-1/preflight":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Reason":"","PauseQueue":false,"Heal":false}`))
		case "/v1/repos/runner-1/finish":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Queue":"OPEN","Reason":"","HealingAttempts":0,"LeaseResume":false,"Decrement":false}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := Client{BaseURL: srv.URL, Token: "test-tok"}
	ctx := context.Background()

	halt, err := c.PostPreflight(ctx, "runner-1", "/repos/work", map[string]any{"Kind": "ordinary"})
	if err != nil {
		t.Fatalf("PostPreflight failed: %v", err)
	}
	if halt.Reason != "" || halt.PauseQueue {
		t.Fatalf("unexpected halt: %+v", halt)
	}

	decision, err := c.PostFinish(ctx, "runner-1", "/repos/work", "job-1", FinishReport{Kind: "ordinary", ASEComplete: true})
	if err != nil {
		t.Fatalf("PostFinish failed: %v", err)
	}
	if decision.Queue != "OPEN" {
		t.Fatalf("unexpected decision: %+v", decision)
	}

	if len(seenPaths) != 2 || seenPaths[0] != "/v1/repos/runner-1/preflight" || seenPaths[1] != "/v1/repos/runner-1/finish" {
		t.Fatalf("unexpected seen paths: %v", seenPaths)
	}
}

func TestLoopWithTools(t *testing.T) {
	var heartbeatBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/heartbeat") {
			b, _ := io.ReadAll(r.Body)
			heartbeatBody = string(b)
		}
		if strings.HasSuffix(r.URL.Path, "/poll") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	c := Client{
		BaseURL: srv.URL,
		Tools: func() []Tool {
			return []Tool{{Name: "autopilot", Path: "/bin/autopilot", Version: "1.0.0", Status: "OK"}}
		},
	}
	_ = c.Loop(ctx, "runner-1", "permanent", 15*time.Millisecond, nil)

	if !strings.Contains(heartbeatBody, "autopilot") {
		t.Fatalf("heartbeat missing scanned tools: %s", heartbeatBody)
	}
}

func TestLoopContinuesWhenOnJobFails(t *testing.T) {
	var pollCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/poll") {
			pollCount++
			if pollCount == 1 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":"job-err","kind":"ordinary","worktree_path":"/repos/work"}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var jobsSeen int
	errOnJob := func(job Assignment) error {
		jobsSeen++
		return fmt.Errorf("simulated job execution error")
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	c := Client{BaseURL: srv.URL}
	err := c.Loop(ctx, "runner-1", "permanent", 10*time.Millisecond, errOnJob)
	if err != nil {
		t.Fatalf("expected Loop to ignore onJob error and not exit fatally, got: %v", err)
	}
	if jobsSeen != 1 {
		t.Fatalf("expected 1 job handled, got %d", jobsSeen)
	}
	if pollCount < 2 {
		t.Fatalf("expected Loop to continue polling after onJob error, pollCount: %d", pollCount)
	}
}

func TestDeclineJob(t *testing.T) {
	var declinedID string
	var declinedReason string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/decline") {
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 4 {
				declinedID = parts[len(parts)-2]
			}
			var body struct {
				Reason string `json:"reason"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			declinedReason = body.Reason
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := Client{BaseURL: srv.URL}
	if err := c.DeclineJob(context.Background(), "job-42", "worktree_busy"); err != nil {
		t.Fatalf("DeclineJob failed: %v", err)
	}
	if declinedID != "job-42" {
		t.Fatalf("expected declined job job-42, got: %s", declinedID)
	}
	if declinedReason != "worktree_busy" {
		t.Fatalf("expected reason worktree_busy, got: %s", declinedReason)
	}
}

func TestGetHostAndResumeRepo(t *testing.T) {
	var resumedHost, resumedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/hosts/runner-1" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"host": {"id":"runner-1","kind":"permanent","presence":"ONLINE","repos_paused":1},
				"repos": [{"path":"/repos/foo","worktree_path":"/repos/foo","queue":"PAUSED","lock":"IDLE","reason":"HALTED_DIRTY","discard_pending":true}]
			}`))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/repos/runner-1/resume" {
			resumedHost = "runner-1"
			var body struct {
				Path string `json:"worktree_path"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			resumedPath = body.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"queue":"OPEN"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := Client{BaseURL: srv.URL}
	ctx := context.Background()

	detail, err := c.GetHost(ctx, "runner-1")
	if err != nil {
		t.Fatalf("GetHost failed: %v", err)
	}
	if detail.Host.ID != "runner-1" || len(detail.Repos) != 1 {
		t.Fatalf("unexpected host detail: %+v", detail)
	}
	if !detail.Repos[0].DiscardPending || detail.Repos[0].Reason != "HALTED_DIRTY" {
		t.Fatalf("unexpected repo details: %+v", detail.Repos[0])
	}

	if err := c.ResumeRepo(ctx, "runner-1", "/repos/foo"); err != nil {
		t.Fatalf("ResumeRepo failed: %v", err)
	}
	if resumedHost != "runner-1" || resumedPath != "/repos/foo" {
		t.Fatalf("unexpected resumed host/path: %s %s", resumedHost, resumedPath)
	}
}

func TestLoopOnTick(t *testing.T) {
	var tickCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/poll") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := Client{
		BaseURL: srv.URL,
		OnTick: func(ctx context.Context, hostID string) error {
			tickCount++
			if tickCount >= 2 {
				cancel()
			}
			return nil
		},
	}

	err := c.Loop(ctx, "runner-1", "permanent", 5*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tickCount < 2 {
		t.Fatalf("expected at least 2 ticks, got %d", tickCount)
	}
}


