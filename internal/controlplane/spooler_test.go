package controlplane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

func TestOfflineReportSpooler_SpoolAndDrain(t *testing.T) {
	tempDir := t.TempDir()

	var receivedCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&receivedCount, 1)
		b, _ := io.ReadAll(r.Body)
		if string(b) == "fail" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	spooler, err := NewOfflineReportSpooler(tempDir, server.Client())
	if err != nil {
		t.Fatalf("NewOfflineReportSpooler: %v", err)
	}

	// 1. Spool 3 requests
	for i := 1; i <= 3; i++ {
		err := spooler.Spool(SpooledRequest{
			ID:      "req-success",
			URL:     server.URL + "/report",
			Method:  http.MethodPost,
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    []byte(`{"status":"ok"}`),
		})
		if err != nil {
			t.Fatalf("Spool %d failed: %v", i, err)
		}
	}

	// Verify 3 files exist
	entries, _ := os.ReadDir(tempDir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 spooled files, got %d", len(entries))
	}

	// 2. Drain all
	ctx := context.Background()
	drained, err := spooler.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if drained != 3 {
		t.Fatalf("expected 3 drained, got %d", drained)
	}
	if atomic.LoadInt32(&receivedCount) != 3 {
		t.Fatalf("expected 3 received by server, got %d", atomic.LoadInt32(&receivedCount))
	}

	// Verify spool is now empty
	entriesAfter, _ := os.ReadDir(tempDir)
	if len(entriesAfter) != 0 {
		t.Fatalf("expected 0 files remaining, got %d", len(entriesAfter))
	}
}

func TestOfflineReportSpooler_TransientFailureStopsDrain(t *testing.T) {
	tempDir := t.TempDir()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	spooler, err := NewOfflineReportSpooler(tempDir, server.Client())
	if err != nil {
		t.Fatalf("NewOfflineReportSpooler: %v", err)
	}

	_ = spooler.Spool(SpooledRequest{
		ID:     "req-1",
		URL:    server.URL + "/test",
		Method: http.MethodPost,
		Body:   []byte("test"),
	})

	ctx := context.Background()
	drained, err := spooler.Drain(ctx)
	if err == nil {
		t.Fatalf("expected error on 502, got nil")
	}
	if drained != 0 {
		t.Fatalf("expected 0 drained, got %d", drained)
	}

	// Verify file is preserved
	entries, _ := os.ReadDir(tempDir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 file preserved, got %d", len(entries))
	}
}

func TestOfflineReportSpooler_TokenProviderRefreshesAndRetries401(t *testing.T) {
	tempDir := t.TempDir()

	var currentToken atomic.Value
	currentToken.Store("expired-token")

	var seenAuth []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		seenAuth = append(seenAuth, authHeader)
		if authHeader != "Bearer refreshed-token-183" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	spooler, err := NewOfflineReportSpooler(tempDir, server.Client())
	if err != nil {
		t.Fatalf("NewOfflineReportSpooler: %v", err)
	}
	spooler.TokenProvider = func(context.Context) string {
		v, _ := currentToken.Load().(string)
		return v
	}

	if err := spooler.Spool(SpooledRequest{
		ID:      "req-auth-refresh",
		URL:     server.URL + "/v1/jobs/job-1/exit",
		Method:  http.MethodPost,
		Headers: map[string]string{"Authorization": "Bearer stale-initial-token"},
		Body:    []byte(`{"exit_code":0}`),
	}); err != nil {
		t.Fatalf("Spool failed: %v", err)
	}

	// First drain: server returns 401 Unauthorized; file must NOT be quarantined as .rejected
	ctx := context.Background()
	drained, err := spooler.Drain(ctx)
	if err == nil {
		t.Fatal("expected error on 401 Unauthorized during first drain")
	}
	if drained != 0 {
		t.Fatalf("expected 0 drained on 401, got %d", drained)
	}
	entries, _ := os.ReadDir(tempDir)
	if len(entries) != 1 || entries[0].Name()[len(entries[0].Name())-5:] != ".json" {
		t.Fatalf("expected spooled .json file preserved after 401 (not .rejected), got %v", entries)
	}

	// Rotate token in TokenProvider and drain again
	currentToken.Store("refreshed-token-183")
	drained2, err := spooler.Drain(ctx)
	if err != nil {
		t.Fatalf("expected second drain with refreshed token to succeed, got %v", err)
	}
	if drained2 != 1 {
		t.Fatalf("expected 1 drained, got %d", drained2)
	}
	if len(seenAuth) != 2 || seenAuth[1] != "Bearer refreshed-token-183" {
		t.Fatalf("expected refreshed token on second drain, got %v", seenAuth)
	}
}
