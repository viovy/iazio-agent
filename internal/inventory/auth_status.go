package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

// DefaultAuthStatusInterval is the default periodic TTL for `<tool> auth status --json` probes.
const DefaultAuthStatusInterval = 30 * time.Second

// DefaultAuthCommandTimeout is the per-binary execution timeout for `<tool> auth status --json`.
const DefaultAuthCommandTimeout = 5 * time.Second

// AuthStatusEntry caches the result of a periodic `<tool> auth status --json` probe.
type AuthStatusEntry struct {
	AuthStatus      map[string]any
	AuthStatusError string
	AuthCheckedAt   string
	ExpiresAt       time.Time
}

// AuthStatusProber periodically executes `<tool> auth status --json` for profile-enabled tools
// and caches the parsed JSON payload across heartbeats.
type AuthStatusProber struct {
	mu              sync.Mutex
	cache           map[string]AuthStatusEntry
	enabledTools    map[string]bool
	manifestExpires time.Time
	Interval        time.Duration
	CommandTimeout  time.Duration
	FetchEnabled    func(ctx context.Context) (map[string]bool, error)
	Runner          func(ctx context.Context, binPath string, args ...string) ([]byte, error)
	Now             func() time.Time
}

// NewAuthStatusProber creates a periodic prober with the specified cache TTL and manifest fetcher.
func NewAuthStatusProber(interval time.Duration, fetchEnabled func(ctx context.Context) (map[string]bool, error)) *AuthStatusProber {
	if interval <= 0 {
		interval = DefaultAuthStatusInterval
	}
	return &AuthStatusProber{
		cache:          make(map[string]AuthStatusEntry),
		enabledTools:   make(map[string]bool),
		Interval:       interval,
		CommandTimeout: DefaultAuthCommandTimeout,
		FetchEnabled:   fetchEnabled,
		Now:            time.Now,
	}
}

func (p *AuthStatusProber) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func normalizeToolLookupKey(name string) string {
	clean := strings.ToLower(strings.TrimSpace(name))
	return strings.TrimSuffix(clean, ".exe")
}

// SetEnabledTools explicitly sets the enabled tool map (useful in tests or static overrides).
func (p *AuthStatusProber) SetEnabledTools(enabled map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabledTools = make(map[string]bool, len(enabled))
	for k, v := range enabled {
		if v {
			p.enabledTools[normalizeToolLookupKey(k)] = true
		}
	}
	p.manifestExpires = p.now().Add(p.Interval)
}

func (p *AuthStatusProber) refreshEnabledIfNeeded(ctx context.Context) {
	if p.FetchEnabled == nil {
		return
	}
	now := p.now()
	p.mu.Lock()
	expired := now.After(p.manifestExpires)
	p.mu.Unlock()
	if !expired {
		return
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	fetched, err := p.FetchEnabled(fetchCtx)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		// Retry sooner on transient manifest fetch failure while preserving prior enabledTools
		p.manifestExpires = p.now().Add(10 * time.Second)
		return
	}
	normalized := make(map[string]bool, len(fetched))
	for k, v := range fetched {
		if v {
			normalized[normalizeToolLookupKey(k)] = true
		}
	}
	p.enabledTools = normalized
	p.manifestExpires = p.now().Add(p.Interval)
}

func (p *AuthStatusProber) isToolEnabled(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enabledTools[normalizeToolLookupKey(name)]
}

func (p *AuthStatusProber) runAuthStatus(ctx context.Context, binPath string) ([]byte, error) {
	timeout := p.CommandTimeout
	if timeout <= 0 {
		timeout = DefaultAuthCommandTimeout
	}
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if p.Runner != nil {
		return p.Runner(cmdCtx, binPath, "auth", "status", "--json")
	}
	cmd := procutil.CommandContext(cmdCtx, binPath, "auth", "status", "--json")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	out := bytes.TrimSpace(stdout.Bytes())
	if len(out) == 0 && stderr.Len() > 0 {
		out = bytes.TrimSpace(stderr.Bytes())
	}
	return out, err
}

// ProbeTool executes `<binPath> auth status --json` and parses the JSON response.
func (p *AuthStatusProber) ProbeTool(ctx context.Context, binPath string) AuthStatusEntry {
	now := p.now()
	checkedAt := now.UTC().Format(time.RFC3339)
	entry := AuthStatusEntry{
		AuthCheckedAt: checkedAt,
		ExpiresAt:     now.Add(p.Interval),
	}

	if strings.TrimSpace(binPath) == "" {
		entry.AuthStatusError = "binary path is empty"
		return entry
	}

	rawOut, runErr := p.runAuthStatus(ctx, binPath)
	trimmed := bytes.TrimSpace(rawOut)

	if len(trimmed) > 0 {
		if parsed, ok := decodeAuthStatusJSON(trimmed); ok {
			entry.AuthStatus = parsed
			if errStr, ok := parsed["error"].(string); ok && strings.TrimSpace(errStr) != "" {
				entry.AuthStatusError = strings.TrimSpace(errStr)
			}
			return entry
		}
	}

	if runErr != nil {
		if len(trimmed) > 0 {
			msg := string(trimmed)
			if len(msg) > 240 {
				msg = msg[:240] + "..."
			}
			entry.AuthStatusError = fmt.Sprintf("%v: %s", runErr, msg)
		} else {
			entry.AuthStatusError = runErr.Error()
		}
		return entry
	}

	if len(trimmed) > 0 {
		msg := string(trimmed)
		if len(msg) > 240 {
			msg = msg[:240] + "..."
		}
		entry.AuthStatusError = fmt.Sprintf("invalid JSON from auth status --json: %s", msg)
	} else {
		entry.AuthStatusError = "empty output from auth status --json"
	}
	return entry
}

// EnrichTools attaches periodic `<tool> auth status --json` telemetry to tools enabled in the active profile.
func (p *AuthStatusProber) EnrichTools(ctx context.Context, tools []Tool) []Tool {
	if p == nil || len(tools) == 0 {
		return tools
	}
	p.refreshEnabledIfNeeded(ctx)

	out := make([]Tool, len(tools))
	copy(out, tools)

	now := p.now()
	for i := range out {
		t := &out[i]
		if !p.isToolEnabled(t.Name) {
			continue
		}
		t.AuthStatusEnabled = true
		if strings.TrimSpace(t.Path) == "" || (!t.Installed && t.Status != "OK") {
			continue
		}
		cacheKey := normalizeToolLookupKey(t.Name) + "|" + t.Path

		p.mu.Lock()
		cached, hasCached := p.cache[cacheKey]
		p.mu.Unlock()

		if hasCached && now.Before(cached.ExpiresAt) {
			t.AuthStatus = cached.AuthStatus
			t.AuthStatusError = cached.AuthStatusError
			t.AuthCheckedAt = cached.AuthCheckedAt
			continue
		}

		probed := p.ProbeTool(ctx, t.Path)
		p.mu.Lock()
		p.cache[cacheKey] = probed
		p.mu.Unlock()

		t.AuthStatus = probed.AuthStatus
		t.AuthStatusError = probed.AuthStatusError
		t.AuthCheckedAt = probed.AuthCheckedAt
	}
	return out
}

func decodeAuthStatusJSON(trimmed []byte) (map[string]any, bool) {
	var parsed map[string]any
	if err := json.Unmarshal(trimmed, &parsed); err == nil && len(parsed) > 0 {
		return parsed, true
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	var last map[string]any
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			break
		}
		if len(m) > 0 {
			last = m
		}
	}
	if len(last) > 0 {
		return last, true
	}

	if idx := bytes.LastIndexByte(trimmed, '{'); idx >= 0 {
		var tail map[string]any
		if err := json.Unmarshal(trimmed[idx:], &tail); err == nil && len(tail) > 0 {
			return tail, true
		}
	}
	return nil, false
}

