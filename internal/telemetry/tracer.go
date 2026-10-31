package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type contextKey string

const activeSpanKey contextKey = "telemetry.active_span"

// Annotation represents a timed event within a span.
type Annotation struct {
	Timestamp time.Time `json:"timestamp"`
	Value     string    `json:"value"`
}

// Span represents a single unit of work in an execution trace.
type Span struct {
	TraceID     string            `json:"trace_id"`
	ID          string            `json:"id"`
	ParentID    string            `json:"parent_id,omitempty"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind,omitempty"`
	ServiceName string            `json:"service_name"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Duration    time.Duration     `json:"duration"`
	Tags        map[string]string `json:"tags,omitempty"`
	Annotations []Annotation      `json:"annotations,omitempty"`

	mu       sync.Mutex
	ended    bool
	exporter Exporter
}

// SetTag attaches a key/value tag to the span.
func (s *Span) SetTag(key, value string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Tags == nil {
		s.Tags = make(map[string]string)
	}
	s.Tags[key] = value
}

// SetError records an error tag and error annotation on the span.
func (s *Span) SetError(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Tags == nil {
		s.Tags = make(map[string]string)
	}
	s.Tags["error"] = "true"
	s.Tags["error.message"] = err.Error()
	s.Annotations = append(s.Annotations, Annotation{
		Timestamp: time.Now().UTC(),
		Value:     fmt.Sprintf("error: %v", err),
	})
}

// AddAnnotation records a timed milestone event on the span.
func (s *Span) AddAnnotation(value string) {
	if s == nil || value == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Annotations = append(s.Annotations, Annotation{
		Timestamp: time.Now().UTC(),
		Value:     value,
	})
}

// End finishes the span and forwards it to the configured exporter.
func (s *Span) End() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	if s.EndTime.IsZero() {
		s.EndTime = time.Now().UTC()
	}
	s.Duration = s.EndTime.Sub(s.StartTime)
	if s.Duration <= 0 {
		s.Duration = time.Microsecond
	}
	exp := s.exporter
	s.mu.Unlock()

	if exp != nil {
		_ = exp.ExportSpan(s)
	}
}

// Exporter sends finished spans to a backend collector.
type Exporter interface {
	ExportSpan(s *Span) error
	Flush(ctx context.Context) error
	Close() error
}

// ZipkinEndpoint matches the Zipkin v2 JSON endpoint model.
type ZipkinEndpoint struct {
	ServiceName string `json:"serviceName,omitempty"`
	IPv4        string `json:"ipv4,omitempty"`
	Port        int    `json:"port,omitempty"`
}

// ZipkinAnnotation matches the Zipkin v2 JSON annotation model.
type ZipkinAnnotation struct {
	Timestamp int64  `json:"timestamp"` // epoch microseconds
	Value     string `json:"value"`
}

// ZipkinSpan matches the standard Zipkin v2 JSON schema.
type ZipkinSpan struct {
	TraceID       string             `json:"traceId"`
	ID            string             `json:"id"`
	ParentID      string             `json:"parentId,omitempty"`
	Name          string             `json:"name"`
	Kind          string             `json:"kind,omitempty"`
	Timestamp     int64              `json:"timestamp"` // epoch microseconds
	Duration      int64              `json:"duration"`  // microseconds
	LocalEndpoint ZipkinEndpoint     `json:"localEndpoint"`
	Tags          map[string]string  `json:"tags,omitempty"`
	Annotations   []ZipkinAnnotation `json:"annotations,omitempty"`
}

// ZipkinExporter sends spans to a Zipkin HTTP collector endpoint (e.g. /api/v2/spans).
type ZipkinExporter struct {
	endpoint   string
	httpClient *http.Client
	ch         chan *Span
	done       chan struct{}
	wg         sync.WaitGroup
	mu         sync.Mutex
	closed     bool
	spoolDir   string
}

// NewZipkinExporter creates a background-batched Zipkin v2 HTTP exporter.
func NewZipkinExporter(endpoint string) *ZipkinExporter {
	clean := strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if clean == "" {
		if env := os.Getenv("ZIPKIN_ENDPOINT"); env != "" {
			clean = strings.TrimRight(strings.TrimSpace(env), "/")
		} else if env := os.Getenv("OTEL_EXPORTER_ZIPKIN_ENDPOINT"); env != "" {
			clean = strings.TrimRight(strings.TrimSpace(env), "/")
		} else if cfgURL := resolveZipkinFromConfig(); cfgURL != "" {
			clean = strings.TrimRight(strings.TrimSpace(cfgURL), "/")
		} else {
			clean = "http://localhost:9411"
		}
	}
	if !strings.HasSuffix(clean, "/api/v2/spans") {
		clean = clean + "/api/v2/spans"
	}

	var spoolDir string
	if env := os.Getenv("ZIPKIN_SPOOL_DIR"); env != "" {
		if env != "none" {
			spoolDir = env
		}
	} else if home, _ := os.UserHomeDir(); home != "" {
		spoolDir = filepath.Join(home, ".iazio", "zipkin-spool")
	}

	ze := &ZipkinExporter{
		endpoint: clean,
		httpClient: &http.Client{
			Timeout: 4 * time.Second,
		},
		ch:       make(chan *Span, 1024),
		done:     make(chan struct{}),
		spoolDir: spoolDir,
	}

	ze.wg.Add(1)
	go ze.batchWorker()
	return ze
}

func (ze *ZipkinExporter) spoolSpans(body []byte) {
	if ze.spoolDir == "" || len(body) == 0 {
		return
	}
	_ = os.MkdirAll(ze.spoolDir, 0o755)
	filePath := filepath.Join(ze.spoolDir, fmt.Sprintf("spans-%d.json", time.Now().UnixNano()))
	_ = os.WriteFile(filePath, body, 0o644)
}

func (ze *ZipkinExporter) drainSpool(ctx context.Context) {
	if ze.spoolDir == "" {
		return
	}
	entries, err := os.ReadDir(ze.spoolDir)
	if err != nil || len(entries) == 0 {
		return
	}
	drainCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(ze.spoolDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			_ = os.Remove(path)
			continue
		}
		req, err := http.NewRequestWithContext(drainCtx, http.MethodPost, ze.endpoint, bytes.NewReader(data))
		if err != nil {
			break
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := ze.httpClient.Do(req)
		if err != nil {
			break // Still offline, retain remaining spooled spans
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			_ = os.Remove(path)
		} else {
			break
		}
	}
}

func (ze *ZipkinExporter) batchWorker() {
	defer ze.wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var batch []*Span

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		toSend := batch
		batch = nil
		_ = ze.sendSpans(context.Background(), toSend)
	}

	for {
		select {
		case s, ok := <-ze.ch:
			if !ok {
				flushBatch()
				return
			}
			batch = append(batch, s)
			if len(batch) >= 50 {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		case <-ze.done:
			for {
				select {
				case s, ok := <-ze.ch:
					if ok {
						batch = append(batch, s)
					} else {
						flushBatch()
						return
					}
				default:
					flushBatch()
					return
				}
			}
		}
	}
}

func (ze *ZipkinExporter) sendSpans(ctx context.Context, spans []*Span) error {
	if len(spans) == 0 {
		return nil
	}

	zspans := make([]ZipkinSpan, 0, len(spans))
	for _, s := range spans {
		tsMicros := s.StartTime.UnixMicro()
		durMicros := s.Duration.Microseconds()
		if durMicros <= 0 {
			durMicros = 1
		}

		tags := make(map[string]string)
		for k, v := range s.Tags {
			tags[k] = v
		}

		var annos []ZipkinAnnotation
		for _, a := range s.Annotations {
			annos = append(annos, ZipkinAnnotation{
				Timestamp: a.Timestamp.UnixMicro(),
				Value:     a.Value,
			})
		}

		kind := strings.ToUpper(s.Kind)
		if kind == "INTERNAL" {
			kind = ""
		}

		zspans = append(zspans, ZipkinSpan{
			TraceID:   s.TraceID,
			ID:        s.ID,
			ParentID:  s.ParentID,
			Name:      s.Name,
			Kind:      kind,
			Timestamp: tsMicros,
			Duration:  durMicros,
			LocalEndpoint: ZipkinEndpoint{
				ServiceName: s.ServiceName,
			},
			Tags:        tags,
			Annotations: annos,
		})
	}

	body, err := json.Marshal(zspans)
	if err != nil {
		return err
	}

	// Drain previously spooled spans first
	ze.drainSpool(ctx)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ze.endpoint, bytes.NewReader(body))
	if err != nil {
		ze.spoolSpans(body)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ze.httpClient.Do(req)
	if err != nil {
		ze.spoolSpans(body)
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ze.spoolSpans(body)
		return fmt.Errorf("zipkin status: %s", resp.Status)
	}
	return nil
}

// ExportSpan enqueues a span for background transmission.
func (ze *ZipkinExporter) ExportSpan(s *Span) error {
	ze.mu.Lock()
	defer ze.mu.Unlock()
	if ze.closed {
		return fmt.Errorf("exporter closed")
	}
	select {
	case ze.ch <- s:
		return nil
	default:
		return fmt.Errorf("trace buffer full, dropped span")
	}
}

// Flush ensures all enqueued spans are processed.
func (ze *ZipkinExporter) Flush(ctx context.Context) error {
	ze.mu.Lock()
	if ze.closed {
		ze.mu.Unlock()
		return nil
	}
	ze.mu.Unlock()

	deadline := time.Now().Add(1 * time.Second)
	for len(ze.ch) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// Close shuts down the exporter worker.
func (ze *ZipkinExporter) Close() error {
	ze.mu.Lock()
	if ze.closed {
		ze.mu.Unlock()
		return nil
	}
	ze.closed = true
	close(ze.ch)
	ze.mu.Unlock()

	ze.wg.Wait()
	return nil
}

// SpanOption configures a new Span during StartSpan.
type SpanOption func(*Span)

// WithKind specifies the span kind (e.g. "CLIENT", "SERVER", "INTERNAL").
func WithKind(kind string) SpanOption {
	return func(s *Span) {
		s.Kind = kind
	}
}

// WithTag sets an initial tag on the span.
func WithTag(key, value string) SpanOption {
	return func(s *Span) {
		s.SetTag(key, value)
	}
}

// WithParentTrace links the span to an explicit parent trace ID and span ID.
func WithParentTrace(traceID, parentID string) SpanOption {
	return func(s *Span) {
		if traceID != "" {
			s.TraceID = traceID
		}
		if parentID != "" {
			s.ParentID = parentID
		}
	}
}

// Tracer creates and coordinates spans for a service.
type Tracer struct {
	ServiceName string
	Exporter    Exporter
}

// NewTracer creates a Tracer for a service.
func NewTracer(serviceName string, exp Exporter) *Tracer {
	if serviceName == "" {
		serviceName = "iazio-agent"
	}
	return &Tracer{
		ServiceName: serviceName,
		Exporter:    exp,
	}
}

// StartSpan starts a new Span and stores it in the returned Context.
func (t *Tracer) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	parent := SpanFromContext(ctx)

	s := &Span{
		ID:          randomHex(8), // 16-hex characters
		Name:        name,
		Kind:        "INTERNAL",
		ServiceName: t.ServiceName,
		StartTime:   time.Now().UTC(),
		Tags:        make(map[string]string),
		exporter:    t.Exporter,
	}

	if parent != nil {
		s.TraceID = parent.TraceID
		s.ParentID = parent.ID
	} else {
		s.TraceID = randomHex(16) // 32-hex characters
	}

	for _, opt := range opts {
		opt(s)
	}

	return context.WithValue(ctx, activeSpanKey, s), s
}

// ContextWithSpan stores a span as active in ctx.
func ContextWithSpan(ctx context.Context, s *Span) context.Context {
	return context.WithValue(ctx, activeSpanKey, s)
}

// SpanFromContext retrieves the currently active span from ctx, or nil if none exists.
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(activeSpanKey)
	if s, ok := val.(*Span); ok {
		return s
	}
	return nil
}

// InjectHTTP writes standard W3C traceparent and B3 headers into h.
func InjectHTTP(s *Span, h http.Header) {
	if s == nil || h == nil {
		return
	}
	traceparent := fmt.Sprintf("00-%s-%s-01", s.TraceID, s.ID)
	h.Set("traceparent", traceparent)
	h.Set("X-B3-TraceId", s.TraceID)
	h.Set("X-B3-SpanId", s.ID)
	if s.ParentID != "" {
		h.Set("X-B3-ParentSpanId", s.ParentID)
	}
}

// ExtractHTTP extracts trace and parent IDs from incoming request headers.
func ExtractHTTP(h http.Header) (traceID, parentID string) {
	if h == nil {
		return "", ""
	}
	if tp := h.Get("traceparent"); tp != "" {
		parts := strings.Split(tp, "-")
		if len(parts) >= 3 && len(parts[1]) == 32 && len(parts[2]) == 16 {
			return parts[1], parts[2]
		}
	}
	if b3Trace := h.Get("X-B3-TraceId"); b3Trace != "" {
		return b3Trace, h.Get("X-B3-SpanId")
	}
	return "", ""
}

func randomHex(byteLen int) string {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

var (
	globalTracer *Tracer
	tracerMu     sync.RWMutex
)

// GlobalTracer returns the package-level default tracer.
func GlobalTracer() *Tracer {
	tracerMu.RLock()
	defer tracerMu.RUnlock()
	if globalTracer == nil {
		return &Tracer{
			ServiceName: "iazio-agent",
			Exporter:    nil,
		}
	}
	return globalTracer
}

// SetGlobalTracer sets the package-level default tracer.
func SetGlobalTracer(t *Tracer) {
	tracerMu.Lock()
	defer tracerMu.Unlock()
	globalTracer = t
}

// StartSpan wraps GlobalTracer().StartSpan.
func StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	return GlobalTracer().StartSpan(ctx, name, opts...)
}

// InitGlobalTracer initializes the global tracer with a live Zipkin exporter.
func InitGlobalTracer(serviceName, zipkinURL string) func() {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "iazio-agent"
	}
	if strings.TrimSpace(zipkinURL) == "" {
		if env := os.Getenv("ZIPKIN_ENDPOINT"); env != "" {
			zipkinURL = env
		} else if env := os.Getenv("OTEL_EXPORTER_ZIPKIN_ENDPOINT"); env != "" {
			zipkinURL = env
		} else if cfgURL := resolveZipkinFromConfig(); cfgURL != "" {
			zipkinURL = cfgURL
		} else {
			zipkinURL = "http://localhost:9411"
		}
	}

	exp := NewZipkinExporter(zipkinURL)
	t := NewTracer(serviceName, exp)
	SetGlobalTracer(t)

	return func() {
		_ = exp.Flush(context.Background())
		_ = exp.Close()
	}
}

// DeriveZipkinURLFromAPI derives the Zipkin base URL from a control-plane API URL
// (e.g. https://tian.go.ro/iazio-api -> https://tian.go.ro/zipkin).
func DeriveZipkinURLFromAPI(apiURL string) string {
	u := strings.TrimSpace(apiURL)
	if u == "" {
		return ""
	}
	parsed, err := url.Parse(u)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	if parsed.Port() == "" || parsed.Port() == "80" || parsed.Port() == "443" {
		return fmt.Sprintf("%s://%s/zipkin", parsed.Scheme, parsed.Host)
	}
	return fmt.Sprintf("%s://%s:9411", parsed.Scheme, parsed.Hostname())
}

// ConfigureZipkinFromAPI updates the active ZipkinExporter endpoint if it was still
// pointing at the default localhost fallback and no explicit ZIPKIN_ENDPOINT env was set.
func ConfigureZipkinFromAPI(apiURL string) {
	if os.Getenv("ZIPKIN_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_ZIPKIN_ENDPOINT") != "" {
		return
	}
	derived := DeriveZipkinURLFromAPI(apiURL)
	if derived == "" {
		return
	}
	clean := strings.TrimRight(derived, "/") + "/api/v2/spans"
	ze, ok := GlobalTracer().Exporter.(*ZipkinExporter)
	if !ok || ze == nil {
		return
	}
	ze.mu.Lock()
	if strings.HasPrefix(ze.endpoint, "http://localhost:9411") || strings.HasPrefix(ze.endpoint, "http://127.0.0.1:9411") {
		ze.endpoint = clean
	}
	ze.mu.Unlock()
}

func resolveZipkinFromConfig() string {
	for _, envKey := range []string{"IAZIO_HARNESS_API_URL", "IAZIO_API_URL"} {
		if derived := DeriveZipkinURLFromAPI(os.Getenv(envKey)); derived != "" {
			return derived
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	if u := resolveZipkinFromIazioConfig(filepath.Join(home, ".iazio", "config.json")); u != "" {
		return u
	}
	if u := resolveZipkinFromAgentConfig(filepath.Join(home, ".iazio", "agent.json")); u != "" {
		return u
	}
	if data, err := os.ReadFile(filepath.Join(home, ".iazio", "api_url")); err == nil {
		return DeriveZipkinURLFromAPI(string(data))
	}
	return ""
}

func resolveZipkinFromIazioConfig(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var raw struct {
		Endpoints map[string]string `json:"endpoints"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return ""
	}
	if zipkin := strings.TrimSpace(raw.Endpoints["zipkin"]); zipkin != "" {
		return zipkin
	}
	for _, key := range []string{"harness_api", "iazio_api"} {
		if derived := DeriveZipkinURLFromAPI(raw.Endpoints[key]); derived != "" {
			return derived
		}
	}
	return ""
}

func resolveZipkinFromAgentConfig(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var raw struct {
		ZipkinURL string `json:"zipkin_url"`
		APIURL    string `json:"api_url"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return ""
	}
	if zipkin := strings.TrimSpace(raw.ZipkinURL); zipkin != "" {
		return zipkin
	}
	return DeriveZipkinURLFromAPI(raw.APIURL)
}
