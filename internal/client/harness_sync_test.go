package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
