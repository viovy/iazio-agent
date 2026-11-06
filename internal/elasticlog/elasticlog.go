package elasticlog

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxDrainBatchesPerFlush       = 5
	maxSpoolFiles                 = 200
	maxSpoolBytes           int64 = 50 << 20 // 50 MiB
)

// LogLevel represents severity level.
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
)

// Entry defines an ECS-compliant structured log event.
type Entry struct {
	Timestamp      time.Time      `json:"@timestamp"`
	LogLevel       string         `json:"log.level"`
	ServiceName    string         `json:"service.name"`
	ServiceVersion string         `json:"service.version,omitempty"`
	HostName       string         `json:"host.name"`
	HostOS         string         `json:"host.os"`
	Message        string         `json:"message"`
	ErrorMessage   string         `json:"error.message,omitempty"`
	ProcessPID     int            `json:"process.pid"`
	JobID          string         `json:"job.id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

// Config controls Elasticsearch log sink behavior.
type Config struct {
	URL                     string
	Index                   string
	Username                string
	Password                string
	ServiceName             string
	Version                 string
	HostName                string
	BufferSize              int
	BatchSize               int
	FlushInterval           time.Duration
	HTTPClient              *http.Client
	SpoolDir                string
	GatewayURL              string
	IsGateway               bool
	AuthToken               string
	TokenFunc               func() string
	InsecureSkipVerify      bool
	MaxDrainBatchesPerFlush int
	MaxSpoolFiles           int
	MaxSpoolBytes           int64
}

// Sink is an asynchronous, non-blocking Elasticsearch log shipper with offline spooling.
type Sink struct {
	cfg                 Config
	queue               chan *Entry
	wg                  sync.WaitGroup
	mu                  sync.RWMutex
	closed              bool
	droppedCount        atomic.Uint64
	hostOS              string
	pid                 int
	client              *http.Client
	spoolDir            string
	spoolMu             sync.Mutex
	spoolSeq            atomic.Uint64
	isDown              atomic.Bool
	consecutiveFailures atomic.Int32
	lastFailureTime     atomic.Int64
}

// New creates and starts an asynchronous Elasticsearch log Sink.
func New(cfg Config) *Sink {
	if cfg.Index == "" {
		cfg.Index = "iazio-tools-logs"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "iazio-agent"
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 2048
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.MaxDrainBatchesPerFlush <= 0 {
		cfg.MaxDrainBatchesPerFlush = maxDrainBatchesPerFlush
	}
	if cfg.MaxSpoolFiles <= 0 {
		cfg.MaxSpoolFiles = maxSpoolFiles
	}
	if cfg.MaxSpoolBytes <= 0 {
		cfg.MaxSpoolBytes = maxSpoolBytes
	}
	if cfg.HostName == "" {
		h, err := os.Hostname()
		if err == nil {
			cfg.HostName = h
		} else {
			cfg.HostName = "unknown"
		}
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: buildTLSConfig(cfg),
			},
		}
	}

	spoolDir := cfg.SpoolDir
	if spoolDir == "" {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			spoolDir = filepath.Join(home, ".iazio", "elastic-spool")
		} else {
			spoolDir = filepath.Join(os.TempDir(), "iazio-elastic-spool")
		}
	}
	_ = os.MkdirAll(spoolDir, 0755)

	s := &Sink{
		cfg:      cfg,
		queue:    make(chan *Entry, cfg.BufferSize),
		hostOS:   runtime.GOOS,
		pid:      os.Getpid(),
		client:   client,
		spoolDir: spoolDir,
	}

	if strings.TrimSpace(cfg.URL) != "" {
		s.wg.Add(1)
		go s.worker()
	}

	return s
}

// IsDown reports whether Elasticsearch is currently unreachable or degraded.
func (s *Sink) IsDown() bool {
	return s.isDown.Load()
}

// ConsecutiveFailures reports the number of consecutive failed push attempts.
func (s *Sink) ConsecutiveFailures() int32 {
	return s.consecutiveFailures.Load()
}

// SpoolDir returns the local offline spool directory.
func (s *Sink) SpoolDir() string {
	return s.spoolDir
}

// Log enqueues a log entry. It is completely non-blocking: if the queue is full
// or the sink is closed, the event is dropped and the dropped counter is incremented.
func (s *Sink) Log(level LogLevel, msg string, err error, jobID string, extra map[string]any) {
	if s == nil || s.queue == nil {
		return
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		s.droppedCount.Add(1)
		return
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	entry := &Entry{
		Timestamp:      time.Now().UTC(),
		LogLevel:       string(level),
		ServiceName:    s.cfg.ServiceName,
		ServiceVersion: s.cfg.Version,
		HostName:       s.cfg.HostName,
		HostOS:         s.hostOS,
		Message:        msg,
		ErrorMessage:   errMsg,
		ProcessPID:     s.pid,
		JobID:          jobID,
		Fields:         extra,
	}

	select {
	case s.queue <- entry:
	default:
		s.droppedCount.Add(1)
	}
	s.mu.RUnlock()
}

func (s *Sink) Info(msg string, extra map[string]any) {
	s.Log(LevelInfo, msg, nil, "", extra)
}

func (s *Sink) Warn(msg string, err error, extra map[string]any) {
	s.Log(LevelWarn, msg, err, "", extra)
}

func (s *Sink) Error(msg string, err error, extra map[string]any) {
	s.Log(LevelError, msg, err, "", extra)
}

func (s *Sink) DroppedCount() uint64 {
	return s.droppedCount.Load()
}

// Close flushes the queue and stops the worker cleanly.
func (s *Sink) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}

func (s *Sink) worker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	var batch []*Entry

	flush := func() {
		if len(batch) == 0 {
			return
		}
		toSend := batch
		batch = nil
		_ = s.sendBulk(context.Background(), toSend)
	}

	for {
		select {
		case entry, ok := <-s.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)
			if len(batch) >= s.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *Sink) sendBulk(ctx context.Context, entries []*Entry) error {
	if len(entries) == 0 || strings.TrimSpace(s.cfg.URL) == "" {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// If currently detected as down, check if backoff duration has passed
	if s.isDown.Load() {
		fails := s.consecutiveFailures.Load()
		backoffSec := 2 << min(fails, 5) // max 64s
		lastFail := s.lastFailureTime.Load()
		if time.Now().Unix()-lastFail < int64(backoffSec) {
			// Still in backoff window, spool entries directly
			s.spoolBatch(entries)
			return errors.New("elasticsearch down; entries spooled to disk")
		}
	}

	// Try to drain previously spooled entries first
	s.drainSpool(ctx)

	// Attempt direct push
	err := s.sendBulkDirect(ctx, entries)
	if err != nil {
		s.isDown.Store(true)
		s.consecutiveFailures.Add(1)
		s.lastFailureTime.Store(time.Now().Unix())
		// Persist entries to offline spool so they are never lost
		s.spoolBatch(entries)
		return err
	}

	// Success: restore healthy state
	s.isDown.Store(false)
	s.consecutiveFailures.Store(0)
	return nil
}

func (s *Sink) sendBulkDirect(ctx context.Context, entries []*Entry) error {
	isGateway := s.cfg.IsGateway || s.cfg.GatewayURL != "" || strings.Contains(s.cfg.URL, "central-telemetry-gateway") || strings.Contains(s.cfg.URL, "/telemetry/logs")
	var reqURL string
	var reqBody io.Reader
	var contentType string

	if isGateway {
		gw := s.cfg.GatewayURL
		if gw == "" {
			gw = s.cfg.URL
		}
		gw = strings.TrimRight(gw, "/")
		if !strings.HasSuffix(gw, "/v1/telemetry/logs") {
			reqURL = gw + "/v1/telemetry/logs"
		} else {
			reqURL = gw
		}
		data, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(data)
		contentType = "application/json"
	} else {
		reqURL = strings.TrimRight(s.cfg.URL, "/") + "/_bulk"
		var buf bytes.Buffer
		for _, e := range entries {
			meta := map[string]any{
				"index": map[string]any{
					"_index": s.cfg.Index,
				},
			}
			metaBytes, err := json.Marshal(meta)
			if err != nil {
				continue
			}
			entryBytes, err := json.Marshal(e)
			if err != nil {
				continue
			}
			buf.Write(metaBytes)
			buf.WriteByte('\n')
			buf.Write(entryBytes)
			buf.WriteByte('\n')
		}
		reqBody = &buf
		contentType = "application/x-ndjson"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	authToken := s.cfg.AuthToken
	if s.cfg.TokenFunc != nil {
		if refreshed := strings.TrimSpace(s.cfg.TokenFunc()); refreshed != "" {
			authToken = refreshed
		}
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	} else if s.cfg.Username != "" || s.cfg.Password != "" {
		req.SetBasicAuth(s.cfg.Username, s.cfg.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return &httpStatusError{StatusCode: resp.StatusCode}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

type httpStatusError struct {
	StatusCode int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("telemetry/elasticsearch error: status %d", e.StatusCode)
}

func isPermanentSpoolError(err error) bool {
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.StatusCode {
	case http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge,
		http.StatusUnprocessableEntity:
		return true
	default:
		return false
	}
}

func buildTLSConfig(cfg Config) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: shouldSkipTLSVerify(cfg), //nolint:gosec // controlled via explicit config, env, or internal/private host
	}
}

func shouldSkipTLSVerify(cfg Config) bool {
	if cfg.InsecureSkipVerify {
		return true
	}
	envVal := strings.TrimSpace(strings.ToLower(os.Getenv("IAZIO_TLS_INSECURE_SKIP_VERIFY")))
	if envVal == "1" || envVal == "true" {
		return true
	}
	rawURL := strings.TrimSpace(cfg.GatewayURL)
	if rawURL == "" {
		rawURL = strings.TrimSpace(cfg.URL)
	}
	if rawURL == "" {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return false
	}
	if host == "localhost" || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
	}
	return false
}

func (s *Sink) spoolBatch(entries []*Entry) {
	if len(entries) == 0 || s.spoolDir == "" {
		return
	}
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()

	seq := s.spoolSeq.Add(1)
	spoolPath := filepath.Join(s.spoolDir, fmt.Sprintf("batch-%019d-%06d.jsonl", time.Now().UnixNano(), seq))
	f, err := os.Create(spoolPath)
	if err != nil {
		return
	}

	for _, e := range entries {
		if data, err := json.Marshal(e); err == nil {
			_, _ = f.Write(data)
			_, _ = f.WriteString("\n")
		}
	}
	_ = f.Close()

	s.enforceSpoolLimitsLocked()
}

type spoolFileInfo struct {
	name    string
	path    string
	size    int64
	modTime time.Time
}

func (s *Sink) enforceSpoolLimitsLocked() {
	entries, err := os.ReadDir(s.spoolDir)
	if err != nil || len(entries) == 0 {
		return
	}

	maxFiles := s.cfg.MaxSpoolFiles
	if maxFiles <= 0 {
		maxFiles = maxSpoolFiles
	}
	maxBytes := s.cfg.MaxSpoolBytes
	if maxBytes <= 0 {
		maxBytes = maxSpoolBytes
	}

	files := make([]spoolFileInfo, 0, len(entries))
	var totalBytes int64
	for _, de := range entries {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".jsonl") {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		files = append(files, spoolFileInfo{
			name:    de.Name(),
			path:    filepath.Join(s.spoolDir, de.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		totalBytes += info.Size()
	}

	sort.Slice(files, func(i, j int) bool {
		if !files[i].modTime.Equal(files[j].modTime) {
			return files[i].modTime.Before(files[j].modTime)
		}
		return files[i].name < files[j].name
	})

	for len(files) > maxFiles || (totalBytes > maxBytes && len(files) > 0) {
		oldest := files[0]
		files = files[1:]
		_ = os.Remove(oldest.path)
		totalBytes -= oldest.size
	}
}

func parseSpoolBatch(path string) ([]*Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var batch []*Entry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err == nil {
			batch = append(batch, &e)
		}
	}
	return batch, nil
}

func (s *Sink) drainSpool(ctx context.Context) {
	if s.spoolDir == "" {
		return
	}
	s.spoolMu.Lock()
	defer s.spoolMu.Unlock()

	entries, err := os.ReadDir(s.spoolDir)
	if err != nil || len(entries) == 0 {
		return
	}

	maxDrain := s.cfg.MaxDrainBatchesPerFlush
	if maxDrain <= 0 {
		maxDrain = maxDrainBatchesPerFlush
	}

	drained := 0
	for _, de := range entries {
		if drained >= maxDrain {
			break
		}
		if !strings.HasSuffix(de.Name(), ".jsonl") {
			continue
		}
		p := filepath.Join(s.spoolDir, de.Name())
		batch, err := parseSpoolBatch(p)
		if err != nil {
			continue
		}
		drained++
		if len(batch) > 0 {
			if err := s.sendBulkDirect(ctx, batch); err != nil {
				if isPermanentSpoolError(err) {
					// Evict poison-pill batch so it does not cause permanent head-of-line blocking
					_ = os.Remove(p)
					continue
				}
				// Transient error / still down: stop draining and retry later
				return
			}
		}
		_ = os.Remove(p)
	}
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
