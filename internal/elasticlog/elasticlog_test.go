package elasticlog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestElasticSink_NonBlockingDropOnFull(t *testing.T) {
	// Tiny buffer size of 2, no worker running (URL empty)
	sink := New(Config{
		BufferSize: 2,
		URL:        "", // worker not started
	})
	defer func() { _ = sink.Close() }()

	sink.Info("msg1", nil)
	sink.Info("msg2", nil)
	sink.Info("msg3", nil) // should drop cleanly without blocking
	sink.Info("msg4", nil) // should drop cleanly without blocking

	if sink.DroppedCount() != 2 {
		t.Fatalf("expected 2 dropped messages, got %d", sink.DroppedCount())
	}
}

func TestElasticSink_SendBulk(t *testing.T) {
	var requestCount atomic.Int32
	var receivedBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if r.URL.Path != "/_bulk" {
			t.Errorf("expected path /_bulk, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("expected ndjson content type, got %s", r.Header.Get("Content-Type"))
		}
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	sink := New(Config{
		URL:           ts.URL,
		Index:         "test-iazio-logs",
		ServiceName:   "iazio-agent",
		Version:       "0.0.51",
		HostName:      "test-pc1",
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		HTTPClient:    ts.Client(),
	})

	sink.Info("agent started", map[string]any{"env": "test"})
	sink.Error("claim failed", errors.New("timeout"), map[string]any{"job_id": "job-123"})

	// Close forces flush
	_ = sink.Close()

	if requestCount.Load() == 0 {
		t.Fatalf("expected at least 1 request to mock Elasticsearch, got 0")
	}

	bodyStr := string(receivedBody)
	if !strings.Contains(bodyStr, `"test-iazio-logs"`) {
		t.Fatalf("expected index name in bulk body, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"agent started"`) {
		t.Fatalf("expected msg in bulk body, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"claim failed"`) {
		t.Fatalf("expected error msg in bulk body, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"timeout"`) {
		t.Fatalf("expected error string in bulk body, got: %s", bodyStr)
	}

	lines := strings.Split(strings.TrimSpace(bodyStr), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (2 action + 2 doc), got %d lines: %v", len(lines), lines)
	}

	var entry Entry
	if err := json.Unmarshal([]byte(lines[1]), &entry); err != nil {
		t.Fatalf("unmarshal doc failed: %v", err)
	}
	if entry.LogLevel != "INFO" || entry.HostName != "test-pc1" {
		t.Fatalf("unexpected entry fields: %+v", entry)
	}
}

func TestElasticSink_OfflineSpoolAndDrain(t *testing.T) {
	var serverOnline atomic.Bool
	var receivedCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serverOnline.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		receivedCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	spoolDir := t.TempDir()

	sink := New(Config{
		URL:           ts.URL,
		Index:         "test-resilient-logs",
		SpoolDir:      spoolDir,
		BatchSize:     1,
		FlushInterval: 10 * time.Millisecond,
		HTTPClient:    ts.Client(),
	})

	// Server is down initially
	serverOnline.Store(false)
	sink.Info("offline message 1", nil)

	// Wait for worker to attempt flush and fail
	time.Sleep(50 * time.Millisecond)

	if !sink.IsDown() {
		t.Fatalf("expected sink to detect Elasticsearch is down")
	}

	// Verify spooled to disk
	entries, err := os.ReadDir(spoolDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("expected spool file to be written on failure, got %v (err: %v)", entries, err)
	}

	// Server recovers online
	serverOnline.Store(true)

	// Reset failure timer for fast test execution
	sink.lastFailureTime.Store(0)

	sink.Info("online message 2", nil)
	time.Sleep(100 * time.Millisecond)

	_ = sink.Close()

	if receivedCount.Load() == 0 {
		t.Fatalf("expected recovered server to receive drained logs, got 0")
	}
}

func TestElasticlog_HelpersAndWarn(t *testing.T) {
	sink := New(Config{
		URL:         "http://127.0.0.1:9200",
		ServiceName: "test",
		SpoolDir:    t.TempDir(),
	})
	defer sink.Close()

	if sink.SpoolDir() == "" {
		t.Fatalf("expected non-empty spool dir")
	}
	if sink.ConsecutiveFailures() != 0 {
		t.Fatalf("expected 0 consecutive failures initially")
	}
	sink.Warn("warning message", errors.New("sample error"), map[string]any{"k": "v"})
	if sink.DroppedCount() != 0 {
		t.Fatalf("expected 0 dropped count")
	}
}

