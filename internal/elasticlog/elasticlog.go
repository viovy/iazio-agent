package elasticlog

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	URL           string
	Index         string
	Username      string
	Password      string
	ServiceName   string
	Version       string
	HostName      string
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	HTTPClient    *http.Client
}

// Sink is an asynchronous, non-blocking Elasticsearch log shipper.
type Sink struct {
	cfg          Config
	queue        chan *Entry
	wg           sync.WaitGroup
	mu           sync.RWMutex
	closed       bool
	droppedCount atomic.Uint64
	hostOS       string
	pid          int
	client       *http.Client
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
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // homelab self-signed CA
			},
		}
	}

	s := &Sink{
		cfg:    cfg,
		queue:  make(chan *Entry, cfg.BufferSize),
		hostOS: runtime.GOOS,
		pid:    os.Getpid(),
		client: client,
	}

	if strings.TrimSpace(cfg.URL) != "" {
		s.wg.Add(1)
		go s.worker()
	}

	return s
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

	bulkURL := strings.TrimRight(s.cfg.URL, "/") + "/_bulk"
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, bulkURL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.cfg.Username != "" || s.cfg.Password != "" {
		req.SetBasicAuth(s.cfg.Username, s.cfg.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
