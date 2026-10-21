package elasticlog

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
