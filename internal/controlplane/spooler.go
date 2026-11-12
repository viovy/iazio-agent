// Package controlplane provides the outbound client and offline buffering spooler.
package controlplane

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

func newSecureHTTPTransport() *http.Transport {
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
		tr := base.Clone()
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			tr.TLSClientConfig = tr.TLSClientConfig.Clone()
			if tr.TLSClientConfig.MinVersion < tls.VersionTLS12 {
				tr.TLSClientConfig.MinVersion = tls.VersionTLS12
			}
			tr.TLSClientConfig.InsecureSkipVerify = false
		}
		return tr
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
}

// SpooledRequest represents a serialized outbound HTTP report or notification buffered on disk.
type SpooledRequest struct {
	ID        string            `json:"id"`
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	Body      []byte            `json:"body"`
	CreatedAt time.Time         `json:"created_at"`
	Attempts  int               `json:"attempts"`
}

// OfflineReportSpooler persists outbound telemetry and completion reports when the control plane is offline
// and automatically drains them chronologically when connectivity is restored.
type OfflineReportSpooler struct {
	mu                    sync.Mutex
	tokenMu               sync.RWMutex
	spoolDir              string
	client                *http.Client
	TokenProvider         func(ctx context.Context) string
	fallbackTokenProvider func(ctx context.Context) string
}

// SetFallbackTokenProvider registers a fallback token resolver (e.g. from Client or HTTPControlPlaneClient)
// used during Drain whenever TokenProvider is nil or returns an empty string.
func (s *OfflineReportSpooler) SetFallbackTokenProvider(fn func(ctx context.Context) string) {
	if s == nil || fn == nil {
		return
	}
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	if s.fallbackTokenProvider == nil {
		s.fallbackTokenProvider = fn
	}
}

func (s *OfflineReportSpooler) resolveBearerToken(ctx context.Context) string {
	if s == nil {
		return ""
	}
	if s.TokenProvider != nil {
		if tok := strings.TrimSpace(s.TokenProvider(ctx)); tok != "" {
			return tok
		}
	}
	s.tokenMu.RLock()
	fb := s.fallbackTokenProvider
	s.tokenMu.RUnlock()
	if fb != nil {
		if tok := strings.TrimSpace(fb(ctx)); tok != "" {
			return tok
		}
	}
	return ""
}

// NewOfflineReportSpooler initializes a spooler rooted at spoolDir (or ~/.iazio/reports-spool by default).
func NewOfflineReportSpooler(spoolDir string, client *http.Client) (*OfflineReportSpooler, error) {
	if strings.TrimSpace(spoolDir) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve home dir: %w", err)
		}
		spoolDir = filepath.Join(home, ".iazio", "reports-spool")
	}
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return nil, fmt.Errorf("create spool dir %s: %w", spoolDir, err)
	}
	if client == nil {
		client = &http.Client{
			Timeout:   30 * time.Second,
			Transport: newSecureHTTPTransport(),
		}
	}
	return &OfflineReportSpooler{
		spoolDir: spoolDir,
		client:   client,
	}, nil
}

// Spool serializes and writes a pending request to disk without crashing.
func (s *OfflineReportSpooler) Spool(req SpooledRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now().UTC()
	}
	if req.ID == "" {
		req.ID = fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	filename := fmt.Sprintf("%020d-%s.json", req.CreatedAt.UnixNano(), req.ID)
	fullPath := filepath.Join(s.spoolDir, filename)

	data, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal spooled request: %w", err)
	}

	tmpFile := fullPath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return fmt.Errorf("write spool tmp file: %w", err)
	}
	if err := os.Rename(tmpFile, fullPath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("commit spool file: %w", err)
	}
	return nil
}

// Drain processes buffered reports in FIFO order and flushes them to the destination.
func (s *OfflineReportSpooler) Drain(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.spoolDir)
	if err != nil {
		return 0, fmt.Errorf("read spool dir: %w", err)
	}

	var jsonFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			jsonFiles = append(jsonFiles, e.Name())
		}
	}
	sort.Strings(jsonFiles)

	drainedCount := 0
	for _, fName := range jsonFiles {
		select {
		case <-ctx.Done():
			return drainedCount, ctx.Err()
		default:
		}

		fPath := filepath.Join(s.spoolDir, fName)
		data, err := os.ReadFile(fPath)
		if err != nil {
			continue
		}

		var req SpooledRequest
		if err := json.Unmarshal(data, &req); err != nil {
			// Corrupted file: quarantine by renaming
			_ = os.Rename(fPath, fPath+".corrupt")
			continue
		}

		httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
		if err != nil {
			_ = os.Rename(fPath, fPath+".corrupt")
			continue
		}
		for k, v := range req.Headers {
			httpReq.Header.Set(k, v)
		}
		if freshTok := s.resolveBearerToken(ctx); freshTok != "" {
			httpReq.Header.Set("Authorization", "Bearer "+freshTok)
		}

		resp, err := s.client.Do(httpReq)
		if err != nil {
			// Connection refused or network down: retain remaining spooled reports for next cycle
			req.Attempts++
			_ = writeSpooledRequestAtomic(fPath, req)
			return drainedCount, err
		}
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Successfully drained
			_ = os.Remove(fPath)
			drainedCount++
		} else if IsTransientStatus(resp.StatusCode) || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// Remote server overload/restarting or token awaiting refresh: stop draining, wait for next tick
			req.Attempts++
			_ = writeSpooledRequestAtomic(fPath, req)
			return drainedCount, fmt.Errorf("transient status %d", resp.StatusCode)
		} else {
			// Unrecoverable 4xx client error: quarantine
			_ = os.Rename(fPath, fPath+".rejected")
		}
	}

	return drainedCount, nil
}

func writeSpooledRequestAtomic(fPath string, req SpooledRequest) error {
	updated, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return err
	}
	tmpFile := fPath + ".tmp"
	if err := os.WriteFile(tmpFile, updated, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpFile, fPath); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}
	return nil
}

// StartAutoDrain starts a background worker that periodically drains spooled requests until ctx is done.
func (s *OfflineReportSpooler) StartAutoDrain(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = s.Drain(ctx)
			}
		}
	}()
}
