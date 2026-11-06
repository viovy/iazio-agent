package elasticlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestElasticSink_SendCentralTelemetryGateway(t *testing.T) {
	var requestCount atomic.Int32
	var receivedBody []byte
	var receivedAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if r.URL.Path != "/v1/telemetry/logs" {
			t.Errorf("expected path /v1/telemetry/logs, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected json content type, got %s", r.Header.Get("Content-Type"))
		}
		receivedAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("{\"status\":\"accepted\",\"index\":\"iazio-logs\"}"))
	}))
	defer ts.Close()

	sink := New(Config{
		URL:           ts.URL,
		IsGateway:     true,
		AuthToken:     "test-token-123",
		ServiceName:   "iazio-agent",
		Version:       "0.0.51",
		HostName:      "test-pc1",
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
	})

	sink.Info("gateway log message", map[string]any{"key": "val"})
	time.Sleep(150 * time.Millisecond)
	_ = sink.Close()

	if requestCount.Load() == 0 {
		t.Fatalf("expected at least 1 request to mock gateway, got 0")
	}
	if receivedAuth != "Bearer test-token-123" {
		t.Fatalf("expected Authorization header Bearer test-token-123, got %s", receivedAuth)
	}
	if !strings.Contains(string(receivedBody), "gateway log message") {
		t.Fatalf("expected body to contain message, got: %s", string(receivedBody))
	}
}

func countSpoolFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	var names []string
	for _, de := range entries {
		if strings.HasSuffix(de.Name(), ".jsonl") {
			names = append(names, de.Name())
		}
	}
	return names
}

func TestElasticSink_BoundedDrainPerFlush(t *testing.T) {
	var receivedBatches atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBatches.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	spoolDir := t.TempDir()
	sink := New(Config{
		URL:        "", // manual drain control
		SpoolDir:   spoolDir,
		HTTPClient: ts.Client(),
	})
	defer func() { _ = sink.Close() }()
	sink.cfg.URL = ts.URL

	totalBatches := 12
	for i := 0; i < totalBatches; i++ {
		sink.spoolBatch([]*Entry{{
			Timestamp: time.Now().UTC(),
			LogLevel:  string(LevelInfo),
			Message:   fmt.Sprintf("spooled-%d", i),
		}})
	}

	if got := len(countSpoolFiles(t, spoolDir)); got != totalBatches {
		t.Fatalf("expected %d spooled files, got %d", totalBatches, got)
	}

	// First drain cycle must cap at maxDrainBatchesPerFlush (5)
	sink.drainSpool(context.Background())
	if got := int(receivedBatches.Load()); got != maxDrainBatchesPerFlush {
		t.Fatalf("expected %d batches drained on first flush, got %d", maxDrainBatchesPerFlush, got)
	}
	if rem := len(countSpoolFiles(t, spoolDir)); rem != totalBatches-maxDrainBatchesPerFlush {
		t.Fatalf("expected %d remaining spool files, got %d", totalBatches-maxDrainBatchesPerFlush, rem)
	}

	// Second drain cycle drains next 5
	sink.drainSpool(context.Background())
	if got := int(receivedBatches.Load()); got != 2*maxDrainBatchesPerFlush {
		t.Fatalf("expected %d total batches drained after second flush, got %d", 2*maxDrainBatchesPerFlush, got)
	}

	// Third drain cycle drains remaining 2
	sink.drainSpool(context.Background())
	if got := int(receivedBatches.Load()); got != totalBatches {
		t.Fatalf("expected %d total batches drained after third flush, got %d", totalBatches, got)
	}
	if rem := len(countSpoolFiles(t, spoolDir)); rem != 0 {
		t.Fatalf("expected 0 remaining spool files, got %d", rem)
	}
}

func TestElasticSink_PoisonPill400EvictionAndTransientRetention(t *testing.T) {
	var deliveredGood atomic.Int32
	var transientStatus atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodyStr := string(body)
		switch {
		case strings.Contains(bodyStr, "poison-400"):
			w.WriteHeader(http.StatusBadRequest)
		case strings.Contains(bodyStr, "poison-422"):
			w.WriteHeader(http.StatusUnprocessableEntity)
		case strings.Contains(bodyStr, "transient-err"):
			status := int(transientStatus.Load())
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			w.WriteHeader(status)
		default:
			deliveredGood.Add(1)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	spoolDir := t.TempDir()
	sink := New(Config{
		URL:        "",
		SpoolDir:   spoolDir,
		HTTPClient: ts.Client(),
	})
	defer func() { _ = sink.Close() }()
	sink.cfg.URL = ts.URL

	// Enqueue 2 poison-pill batches followed by a valid batch and a transient failure batch
	sink.spoolBatch([]*Entry{{Timestamp: time.Now().UTC(), LogLevel: "INFO", Message: "poison-400"}})
	sink.spoolBatch([]*Entry{{Timestamp: time.Now().UTC(), LogLevel: "INFO", Message: "poison-422"}})
	sink.spoolBatch([]*Entry{{Timestamp: time.Now().UTC(), LogLevel: "INFO", Message: "good-message-1"}})
	sink.spoolBatch([]*Entry{{Timestamp: time.Now().UTC(), LogLevel: "INFO", Message: "transient-err"}})
	sink.spoolBatch([]*Entry{{Timestamp: time.Now().UTC(), LogLevel: "INFO", Message: "good-message-2"}})

	transientStatus.Store(http.StatusTooManyRequests)
	sink.drainSpool(context.Background())

	// Both poison pills must be evicted and good-message-1 delivered before stopping at transient-err (429)
	if deliveredGood.Load() != 1 {
		t.Fatalf("expected good-message-1 to be delivered past poison pills, got %d", deliveredGood.Load())
	}
	remaining := countSpoolFiles(t, spoolDir)
	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining files (transient-err + good-message-2), got %d: %v", len(remaining), remaining)
	}

	// Verify 401 Unauthorized is also treated as transient (not evicted)
	transientStatus.Store(http.StatusUnauthorized)
	sink.drainSpool(context.Background())
	if len(countSpoolFiles(t, spoolDir)) != 2 {
		t.Fatalf("expected 401 batch to remain on disk for retry after token refresh")
	}

	// Now allow transient-err to succeed (200 OK)
	transientStatus.Store(http.StatusOK)
	sink.drainSpool(context.Background())
	if len(countSpoolFiles(t, spoolDir)) != 0 {
		t.Fatalf("expected all remaining spool files to drain once transient error resolved")
	}
	if deliveredGood.Load() != 2 {
		t.Fatalf("expected good-message-2 to be delivered, total good=%d", deliveredGood.Load())
	}
}

func TestElasticSink_SpoolRetentionCap(t *testing.T) {
	// 1. File count cap (evicts oldest first)
	spoolDirFiles := t.TempDir()
	sinkFiles := New(Config{
		URL:           "",
		SpoolDir:      spoolDirFiles,
		MaxSpoolFiles: 4,
	})
	defer func() { _ = sinkFiles.Close() }()

	for i := 1; i <= 10; i++ {
		sinkFiles.spoolBatch([]*Entry{{
			Timestamp: time.Now().UTC(),
			LogLevel:  "INFO",
			Message:   fmt.Sprintf("msg-%02d", i),
		}})
	}

	names := countSpoolFiles(t, spoolDirFiles)
	if len(names) != 4 {
		t.Fatalf("expected max 4 spool files retained, got %d: %v", len(names), names)
	}
	// Oldest (msg-01..msg-06) must be evicted; newest (msg-07..msg-10) must be retained
	firstData, err := os.ReadFile(sinkFiles.SpoolDir() + "/" + names[0])
	if err != nil {
		t.Fatalf("read oldest retained file: %v", err)
	}
	if !strings.Contains(string(firstData), "msg-07") {
		t.Fatalf("expected oldest retained file to contain msg-07, got %s", string(firstData))
	}

	// 2. Byte budget cap (evicts oldest first)
	spoolDirBytes := t.TempDir()
	sinkBytes := New(Config{
		URL:           "",
		SpoolDir:      spoolDirBytes,
		MaxSpoolFiles: 200,
		MaxSpoolBytes: 350, // enough for ~2 small batches
	})
	defer func() { _ = sinkBytes.Close() }()

	for i := 1; i <= 6; i++ {
		sinkBytes.spoolBatch([]*Entry{{
			Timestamp: time.Now().UTC(),
			LogLevel:  "INFO",
			Message:   fmt.Sprintf("byte-budget-msg-%02d", i),
		}})
	}

	byteFiles := countSpoolFiles(t, spoolDirBytes)
	if len(byteFiles) == 0 || len(byteFiles) >= 6 {
		t.Fatalf("expected byte cap to evict older files, got %d files", len(byteFiles))
	}
	var totalSize int64
	for _, name := range byteFiles {
		fi, err := os.Stat(spoolDirBytes + "/" + name)
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		totalSize += fi.Size()
	}
	if totalSize > 350 {
		t.Fatalf("expected total spool bytes <= 350, got %d", totalSize)
	}
}

func TestElasticSink_TLSConfigBehavior(t *testing.T) {
	t.Setenv("IAZIO_TLS_INSECURE_SKIP_VERIFY", "")

	// 1. Public HTTPS endpoint verifies certificates by default (InsecureSkipVerify == false)
	publicSink := New(Config{
		URL:      "https://central-telemetry-gateway.iazio.io/v1/telemetry/logs",
		SpoolDir: t.TempDir(),
	})
	_ = publicSink.Close()
	tr, ok := publicSink.client.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil {
		t.Fatalf("expected *http.Transport with TLSClientConfig")
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify=false for public HTTPS endpoint by default")
	}

	// 2. Explicit Config.InsecureSkipVerify=true enables skip verify
	explicitSink := New(Config{
		URL:                "https://central-telemetry-gateway.iazio.io/v1/telemetry/logs",
		InsecureSkipVerify: true,
		SpoolDir:           t.TempDir(),
	})
	_ = explicitSink.Close()
	if !explicitSink.client.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify=true when Config.InsecureSkipVerify=true")
	}

	// 3. Internal / local / loopback / RFC1918 private hosts allow self-signed homelab certs
	for _, internalURL := range []string{
		"https://es.homelab.internal:9200",
		"https://es.local:9200",
		"https://localhost:9200",
		"https://127.0.0.1:9200",
		"https://192.168.1.50:9200",
		"https://10.0.0.15:9200",
	} {
		if !shouldSkipTLSVerify(Config{URL: internalURL}) {
			t.Fatalf("expected shouldSkipTLSVerify=true for internal/private URL %s", internalURL)
		}
	}

	// 4. Environment override IAZIO_TLS_INSECURE_SKIP_VERIFY=1 enables skip verify
	t.Setenv("IAZIO_TLS_INSECURE_SKIP_VERIFY", "1")
	if !shouldSkipTLSVerify(Config{URL: "https://logs.example.com"}) {
		t.Fatalf("expected shouldSkipTLSVerify=true when IAZIO_TLS_INSECURE_SKIP_VERIFY=1")
	}
}

func TestElasticSink_RecoveryProbesDirectBeforeDrainSpool(t *testing.T) {
	var serverHealthy atomic.Bool
	var requestBodies []string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requestBodies = append(requestBodies, string(body))
		if !serverHealthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	spoolDir := t.TempDir()
	sink := New(Config{
		URL:        "",
		SpoolDir:   spoolDir,
		HTTPClient: ts.Client(),
	})
	defer func() { _ = sink.Close() }()
	sink.cfg.URL = ts.URL

	// 1. Pre-populate a spooled batch while sink is marked down and backoff has expired
	sink.spoolBatch([]*Entry{{
		Timestamp: time.Now().UTC(),
		LogLevel:  "INFO",
		Message:   "spooled-offline-entry",
	}})
	sink.isDown.Store(true)
	sink.lastFailureTime.Store(0)

	// 2. Endpoint is STILL unhealthy (503): sendBulk must probe live batch first and NOT touch spooled files
	serverHealthy.Store(false)
	sink.sendBulk(context.Background(), []*Entry{{
		Timestamp: time.Now().UTC(),
		LogLevel:  "INFO",
		Message:   "live-probe-while-down",
	}})

	if len(requestBodies) != 1 || !strings.Contains(requestBodies[0], "live-probe-while-down") {
		t.Fatalf("expected exactly 1 live probe request without draining spool when endpoint is still down, got %d requests: %v", len(requestBodies), requestBodies)
	}
	if got := len(countSpoolFiles(t, spoolDir)); got != 2 {
		t.Fatalf("expected original spool file + failed live probe (2 files) on disk, got %d", got)
	}

	// 3. Endpoint recovers (200 OK): sendBulk sends live batch first, then drains the 2 spooled files
	requestBodies = nil
	serverHealthy.Store(true)
	sink.lastFailureTime.Store(0)

	sink.sendBulk(context.Background(), []*Entry{{
		Timestamp: time.Now().UTC(),
		LogLevel:  "INFO",
		Message:   "live-probe-recovered",
	}})

	if len(requestBodies) != 3 {
		t.Fatalf("expected 3 requests (1 live probe + 2 drained spool files), got %d: %v", len(requestBodies), requestBodies)
	}
	if !strings.Contains(requestBodies[0], "live-probe-recovered") {
		t.Fatalf("expected first request on recovery to be live-probe-recovered, got %s", requestBodies[0])
	}
	if got := len(countSpoolFiles(t, spoolDir)); got != 0 {
		t.Fatalf("expected 0 remaining spool files after recovery drain, got %d", got)
	}
}

