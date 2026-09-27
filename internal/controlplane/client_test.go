package controlplane

import (
	"context"
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
