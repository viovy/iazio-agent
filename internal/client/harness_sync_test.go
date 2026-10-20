package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/harness"
)

func TestHarnessSyncClient_SyncRepos_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/repos/sync" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			t.Errorf("missing or bad auth header: %s", r.Header.Get("Authorization"))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var req RepoSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.HostID != "mac-mini" || len(req.Repos) != 1 {
			t.Errorf("unexpected req: %+v", req)
		}

		resp := RepoSyncResponse{
			OutOfSync: []OutOfSyncRepo{
				{
					RepoName:       "iazio-mcp",
					IndexedTreeSHA: "old-sha",
					TargetTreeSHA:  "new-sha",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	c := NewHTTPHarnessSyncClient(ts.URL, "secret-token", ts.Client())
	res, err := c.SyncRepos(context.Background(), RepoSyncRequest{
		HostID: "mac-mini",
		Repos: []harness.RepoManifest{
			{Name: "iazio-mcp", GitTreeSHA: "new-sha"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(res.OutOfSync) != 1 || res.OutOfSync[0].RepoName != "iazio-mcp" {
		t.Fatalf("unexpected response: %+v", res)
	}
}

func TestHarnessSyncClient_SyncCodebase_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/codebase/sync" {
			http.NotFound(w, r)
			return
		}

		var req CodebaseSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.RepoName != "iazio-mcp" || !req.IsFinalBatch {
			t.Errorf("unexpected codebase sync req: %+v", req)
		}

		resp := CodebaseSyncResponse{
			Status:         "success",
			IndexedChunks:  len(req.Chunks),
			IndexedTreeSHA: req.GitTreeSHA,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	c := NewHTTPHarnessSyncClient(ts.URL, "secret-token", ts.Client())
	res, err := c.SyncCodebase(context.Background(), CodebaseSyncRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "old-sha",
		GitTreeSHA:   "new-sha",
		IsFinalBatch: true,
		Chunks: []harness.CodeChunk{
			{FilePath: "main.go", Content: "package main"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.IndexedChunks != 1 || res.IndexedTreeSHA != "new-sha" {
		t.Fatalf("unexpected response: %+v", res)
	}
}

func TestHarnessSyncClient_RetryOn503(t *testing.T) {
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attempts.Add(1)
		if count < 3 {
			http.Error(w, "server busy", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CodebaseSyncResponse{
			Status: "recovered",
		})
	}))
	defer ts.Close()

	c := NewHTTPHarnessSyncClient(ts.URL, "", ts.Client())
	c.InitialBackoff = 5 * time.Millisecond
	c.MaxRetries = 3

	res, err := c.SyncCodebase(context.Background(), CodebaseSyncRequest{
		RepoName: "iazio-mcp",
	})
	if err != nil {
		t.Fatalf("expected retry to succeed, got error: %v", err)
	}
	if res.Status != "recovered" {
		t.Fatalf("unexpected status: %s", res.Status)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestHarnessSyncClient_Exhausted503(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := NewHTTPHarnessSyncClient(ts.URL, "", ts.Client())
	c.InitialBackoff = 5 * time.Millisecond
	c.MaxRetries = 2

	_, err := c.SyncCodebase(context.Background(), CodebaseSyncRequest{
		RepoName: "iazio-mcp",
	})
	if err == nil {
		t.Fatal("expected error when 503 retries exhausted, got nil")
	}
}

func TestIsTransientNetError(t *testing.T) {
	if isTransientNetError(nil) {
		t.Fatal("nil error should not be transient")
	}
	if !isTransientNetError(errors.New("connection refused")) {
		t.Fatal("connection refused should be transient")
	}
	if !isTransientNetError(errors.New("read: connection reset by peer")) {
		t.Fatal("connection reset should be transient")
	}
	if !isTransientNetError(errors.New("write: broken pipe")) {
		t.Fatal("broken pipe should be transient")
	}
	if !isTransientNetError(errors.New("unexpected eof")) {
		t.Fatal("eof should be transient")
	}
	if isTransientNetError(errors.New("unauthorized 401")) {
		t.Fatal("regular error should not be transient")
	}
}

func TestNewHTTPHarnessSyncClient_Defaults(t *testing.T) {
	c := NewHTTPHarnessSyncClient("http://localhost:8080/", "tok", nil)
	if c.HTTPClient == nil {
		t.Fatal("expected default HTTPClient")
	}
	if c.BaseURL != "http://localhost:8080" {
		t.Fatalf("expected trimmed baseURL, got %s", c.BaseURL)
	}
}

func TestDoRequestWithRetry_NonTransient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer ts.Close()

	c := NewHTTPHarnessSyncClient(ts.URL, "tok", ts.Client())
	c.MaxRetries = 3
	_, err := c.SyncRepos(context.Background(), RepoSyncRequest{HostID: "test"})
	if err == nil {
		t.Fatal("expected error on 400 Bad Request")
	}
}

func TestDoRequestWithRetry_ContextCancelled_And_DecodeError(t *testing.T) {
	// 1. Context cancelled during retry
	ts503 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server busy", http.StatusServiceUnavailable)
	}))
	defer ts503.Close()

	c := NewHTTPHarnessSyncClient(ts503.URL, "tok", ts503.Client())
	c.InitialBackoff = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := c.SyncRepos(ctx, RepoSyncRequest{HostID: "test"})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 2. Decode error on malformed JSON
	tsBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer tsBadJSON.Close()

	cBad := NewHTTPHarnessSyncClient(tsBadJSON.URL, "tok", tsBadJSON.Client())
	_, err = cBad.SyncRepos(context.Background(), RepoSyncRequest{HostID: "test"})
	if err == nil || !strings.Contains(err.Error(), "decode repos sync response") {
		t.Fatalf("expected decode error, got %v", err)
	}

	_, err = cBad.SyncCodebase(context.Background(), CodebaseSyncRequest{RepoName: "test"})
	if err == nil || !strings.Contains(err.Error(), "decode codebase sync response") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestHarnessSyncClient_NegativeConfigAndSuccessStatusCodes(t *testing.T) {
	// 1. Negative config defaults
	c := NewHTTPHarnessSyncClient("http://localhost:1234", "", nil)
	c.MaxRetries = -1
	c.InitialBackoff = -1

	// 2. Status 201 Created and 204 No Content
	tsSuccess := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "repos") {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"host_id":"test","out_of_sync":[]}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer tsSuccess.Close()

	cSuccess := NewHTTPHarnessSyncClient(tsSuccess.URL, "", tsSuccess.Client())
	cSuccess.MaxRetries = -1
	cSuccess.InitialBackoff = -1

	res, err := cSuccess.SyncRepos(context.Background(), RepoSyncRequest{HostID: "test"})
	if err != nil || res == nil {
		t.Fatalf("expected success on 201 Created: res=%+v err=%v", res, err)
	}

	// 3. Transient net errors
	if isTransientNetError(nil) {
		t.Fatal("expected false for nil net error")
	}
	if !isTransientNetError(errors.New("connection reset by peer")) {
		t.Fatal("expected true for connection reset")
	}
	if !isTransientNetError(errors.New("connection refused")) {
		t.Fatal("expected true for connection refused")
	}
	if isTransientNetError(errors.New("some regular error")) {
		t.Fatal("expected false for regular error")
	}
}



