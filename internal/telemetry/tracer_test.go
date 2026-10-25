package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func init() {
	_ = os.Setenv("ZIPKIN_SPOOL_DIR", "none")
}

func TestTracerSpanCreationAndPropagation(t *testing.T) {
	exp := NewZipkinExporter("http://localhost:9411")
	tracer := NewTracer("test-agent", exp)

	ctx, span := tracer.StartSpan(context.Background(), "test.job.claim", WithKind("CLIENT"), WithTag("host.id", "host-1"))
	if span.TraceID == "" || span.ID == "" {
		t.Fatalf("expected non-empty trace and span IDs, got trace=%s, id=%s", span.TraceID, span.ID)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://example.com/api", nil)
	InjectHTTP(span, req.Header)

	traceID, spanID := ExtractHTTP(req.Header)
	if traceID != span.TraceID || spanID != span.ID {
		t.Fatalf("expected extracted %s/%s, got %s/%s", span.TraceID, span.ID, traceID, spanID)
	}

	span.AddAnnotation("job claimed")
	span.SetTag("status", "success")
	span.End()
}

func TestZipkinExporterSendSpans(t *testing.T) {
	received := make(chan []ZipkinSpan, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var spans []ZipkinSpan
		_ = json.NewDecoder(r.Body).Decode(&spans)
		received <- spans
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	exp := NewZipkinExporter(ts.URL)
	defer func() { _ = exp.Close() }()

	tracer := NewTracer("test-agent", exp)
	_, span := tracer.StartSpan(context.Background(), "job.execute", WithTag("repo", "iazio"), WithKind("INTERNAL"))
	span.End()

	_ = exp.Flush(context.Background())

	select {
	case spans := <-received:
		if len(spans) == 0 {
			t.Fatalf("expected received spans")
		}
		if spans[0].Name != "job.execute" {
			t.Fatalf("expected span name job.execute, got %s", spans[0].Name)
		}
		if spans[0].LocalEndpoint.ServiceName != "test-agent" {
			t.Fatalf("expected service test-agent, got %s", spans[0].LocalEndpoint.ServiceName)
		}
		if spans[0].Kind != "" {
			t.Fatalf("expected internal kind to be stripped, got %q", spans[0].Kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for zipkin exporter payload")
	}
}

func TestSpanSetErrorAndNilSafety(t *testing.T) {
	var nilSpan *Span
	nilSpan.SetTag("k", "v")
	nilSpan.SetError(errors.New("err"))
	nilSpan.AddAnnotation("anno")
	nilSpan.End()

	s := &Span{
		TraceID:   "0123456789abcdef0123456789abcdef",
		ID:        "0123456789abcdef",
		StartTime: time.Now().UTC(),
	}
	s.SetError(nil)
	if s.Tags != nil && s.Tags["error"] == "true" {
		t.Fatalf("expected no error tag for nil error")
	}

	testErr := errors.New("something broke")
	s.SetError(testErr)
	if s.Tags["error"] != "true" || s.Tags["error.message"] != "something broke" {
		t.Fatalf("expected error tags, got %+v", s.Tags)
	}
	if len(s.Annotations) != 1 || s.Annotations[0].Value != "error: something broke" {
		t.Fatalf("expected error annotation, got %+v", s.Annotations)
	}

	s.AddAnnotation("")
	if len(s.Annotations) != 1 {
		t.Fatalf("expected empty annotation to be ignored")
	}
	s.AddAnnotation("custom-event")
	if len(s.Annotations) != 2 || s.Annotations[1].Value != "custom-event" {
		t.Fatalf("expected custom event annotation, got %+v", s.Annotations)
	}

	s.Duration = 0
	s.End()
	if !s.ended {
		t.Fatalf("expected span to be ended")
	}
	// Calling End second time should be no-op
	s.End()
}

func TestContextPropagationAndParentTrace(t *testing.T) {
	if s := SpanFromContext(nil); s != nil {
		t.Fatalf("expected nil span from nil context")
	}
	if s := SpanFromContext(context.Background()); s != nil {
		t.Fatalf("expected nil span from empty context")
	}

	tracer := NewTracer("test-agent", nil)
	ctx, rootSpan := tracer.StartSpan(context.Background(), "root.op", WithParentTrace("trace-abc", "parent-999"))
	if rootSpan.TraceID != "trace-abc" || rootSpan.ParentID != "parent-999" {
		t.Fatalf("expected trace-abc/parent-999, got %s/%s", rootSpan.TraceID, rootSpan.ParentID)
	}

	ctx = ContextWithSpan(ctx, rootSpan)
	if retrieved := SpanFromContext(ctx); retrieved != rootSpan {
		t.Fatalf("expected retrieved span to match rootSpan")
	}

	// Child span inherits TraceID and sets ParentID to rootSpan.ID
	_, childSpan := tracer.StartSpan(ctx, "child.op")
	if childSpan.TraceID != rootSpan.TraceID {
		t.Fatalf("expected child to inherit trace ID %s, got %s", rootSpan.TraceID, childSpan.TraceID)
	}
	if childSpan.ParentID != rootSpan.ID {
		t.Fatalf("expected child parent ID %s, got %s", rootSpan.ID, childSpan.ParentID)
	}
}

func TestHTTPHeadersB3FallbackAndEdgeCases(t *testing.T) {
	InjectHTTP(nil, nil)
	InjectHTTP(&Span{}, nil)

	s := &Span{
		TraceID:  "0123456789abcdef0123456789abcdef",
		ID:       "0123456789abcdef",
		ParentID: "fedcba9876543210",
	}
	hdr := http.Header{}
	InjectHTTP(s, hdr)
	if hdr.Get("X-B3-ParentSpanId") != "fedcba9876543210" {
		t.Fatalf("expected B3 parent span id fedcba9876543210, got %s", hdr.Get("X-B3-ParentSpanId"))
	}

	// Test extraction with nil header
	tID, pID := ExtractHTTP(nil)
	if tID != "" || pID != "" {
		t.Fatalf("expected empty for nil header")
	}

	// Test extraction with B3 fallback
	b3Hdr := http.Header{}
	b3Hdr.Set("X-B3-TraceId", "b3trace123")
	b3Hdr.Set("X-B3-SpanId", "b3span456")
	tID, pID = ExtractHTTP(b3Hdr)
	if tID != "b3trace123" || pID != "b3span456" {
		t.Fatalf("expected B3 fallback extract, got %s/%s", tID, pID)
	}

	// Test malformed traceparent falls back or returns empty
	badHdr := http.Header{}
	badHdr.Set("traceparent", "invalid-format")
	tID, pID = ExtractHTTP(badHdr)
	if tID != "" || pID != "" {
		t.Fatalf("expected empty for malformed traceparent without B3, got %s/%s", tID, pID)
	}
}

func TestGlobalTracerLifecycle(t *testing.T) {
	orig := GlobalTracer()
	if orig == nil {
		t.Fatalf("expected non-nil default global tracer")
	}

	customTracer := NewTracer("custom-service", nil)
	SetGlobalTracer(customTracer)
	if GlobalTracer() != customTracer {
		t.Fatalf("expected custom global tracer")
	}

	_, span := StartSpan(context.Background(), "global.op", WithTag("k", "v"))
	if span == nil || span.Name != "global.op" {
		t.Fatalf("expected valid span from package StartSpan")
	}
	span.End()

	// InitGlobalTracer with test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	os.Setenv("ZIPKIN_ENDPOINT", ts.URL)
	defer os.Unsetenv("ZIPKIN_ENDPOINT")

	cleanup := InitGlobalTracer("", "")
	if cleanup == nil {
		t.Fatalf("expected cleanup function")
	}
	cleanup()

	// Restore original
	SetGlobalTracer(orig)
}

func TestZipkinExporterErrorAndClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer ts.Close()

	exp := NewZipkinExporter(ts.URL)
	_ = exp.ExportSpan(&Span{
		TraceID:   "0123456789abcdef0123456789abcdef",
		ID:        "0123456789abcdef",
		StartTime: time.Now().UTC(),
		Duration:  -time.Second,
	})

	_ = exp.sendSpans(context.Background(), nil)

	_ = exp.Flush(context.Background())
	_ = exp.Close()
	// Second close is safe
	_ = exp.Close()

	// Export after close
	_ = exp.ExportSpan(&Span{})
}

func TestZipkinExporter_OfflineSpoolAndDrain(t *testing.T) {
	tempSpool := t.TempDir()
	exp := NewZipkinExporter("http://127.0.0.1:49999/api/v2/spans")
	exp.spoolDir = tempSpool
	defer exp.Close()

	span := &Span{
		TraceID:     "11112222333344445555666677778888",
		ID:          "1111222233334444",
		Name:        "offline.span",
		ServiceName: "test-agent",
		StartTime:   time.Now().UTC(),
		Duration:    10 * time.Millisecond,
	}

	// 1. Send while server is down -> should spool to disk
	err := exp.sendSpans(context.Background(), []*Span{span})
	if err == nil {
		t.Fatalf("expected error when zipkin is offline")
	}

	entries, err := os.ReadDir(tempSpool)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected 1 spooled span file in %s, got err=%v count=%d", tempSpool, err, len(entries))
	}

	// 2. Bring server up
	received := make(chan []ZipkinSpan, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var spans []ZipkinSpan
		_ = json.NewDecoder(r.Body).Decode(&spans)
		received <- spans
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	exp.endpoint = ts.URL + "/api/v2/spans"

	// 3. Drain spool
	exp.drainSpool(context.Background())

	select {
	case spans := <-received:
		if len(spans) != 1 || spans[0].Name != "offline.span" {
			t.Fatalf("unexpected drained span: %+v", spans)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for drained spans to be posted")
	}

	// Spool directory should now be empty
	remaining, _ := os.ReadDir(tempSpool)
	if len(remaining) != 0 {
		t.Fatalf("expected spool directory to be empty after drain, got %d files", len(remaining))
	}
}

func TestNewZipkinExporter_EnvBranches(t *testing.T) {
	t.Setenv("ZIPKIN_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_ZIPKIN_ENDPOINT", "http://otel.example.com:9411")
	t.Setenv("ZIPKIN_SPOOL_DIR", "none")

	exp := NewZipkinExporter("")
	defer exp.Close()

	if exp.endpoint != "http://otel.example.com:9411/api/v2/spans" {
		t.Errorf("unexpected endpoint: %s", exp.endpoint)
	}
	if exp.spoolDir != "" {
		t.Errorf("expected empty spoolDir with none, got: %s", exp.spoolDir)
	}
}

