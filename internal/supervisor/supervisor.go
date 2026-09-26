// Package supervisor coordinates one harness child per checkout.
package supervisor

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	LockRunning = "RUNNING_HARNESS"
	LockIdle    = "IDLE"
	LockCooling = "COOLING_OFF"
)

// Runner executes a harness child. Tests substitute it.
type Runner interface {
	Start(argv []string) (pid int, err error)
	Signal(pid int, sig string) error
}

// Sup is the in-process repo lock table.
type Sup struct {
	mu    sync.Mutex
	locks map[string]string
	now   func() time.Time
}

// New returns a supervisor.
func New(now func() time.Time) *Sup {
	if now == nil {
		now = time.Now
	}
	return &Sup{locks: map[string]string{}, now: now}
}

// SpawnArgs is the only argv the agent passes. It does not include a token.
func SpawnArgs(jobID, apiURL, worktree, docsHub string) []string {
	return []string{
		"iazio-harness", "run",
		"--job-id", jobID,
		"--api-url", apiURL,
		"--worktree-path", worktree,
		"--docs-hub-path", docsHub,
	}
}

// TrySpawn records RUNNING_HARNESS. A second call for the same path is refused.
func (s *Sup) TrySpawn(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[path] == LockRunning {
		return errors.New("checkout busy")
	}
	s.locks[path] = LockRunning
	return nil
}

// MarkIdle clears the lock.
func (s *Sup) MarkIdle(path string) {
	s.mu.Lock()
	s.locks[path] = LockIdle
	s.mu.Unlock()
}

// Lock returns the current lock state.
func (s *Sup) Lock(path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[path] == "" {
		return LockIdle
	}
	return s.locks[path]
}

// SweepSpool deletes files older than 7 days unless the job is still active.
// A permission error is skipped and does not fail the sweep.
func SweepSpool(dir string, now time.Time, active map[string]bool, remove func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	cutoff := now.Add(-7 * 24 * time.Hour)
	for _, ent := range entries {
		info, err := ent.Info()
		if err != nil {
			continue
		}
		name := ent.Name()
		jobID := name
		if ext := filepath.Ext(name); ext != "" {
			jobID = name[:len(name)-len(ext)]
		}
		if active[jobID] || !info.ModTime().Before(cutoff) {
			continue
		}
		if err := remove(filepath.Join(dir, name)); err != nil {
			if errors.Is(err, os.ErrPermission) {
				continue
			}
			return err
		}
	}
	return nil
}

// DrainAction is reject or abort when the drain deadline hits.
type DrainAction string

const (
	DrainReject DrainAction = "reject"
	DrainAbort  DrainAction = "abort"
)

// DrainResult is the host transition after a suite update request.
type DrainResult struct {
	Presence string
	Cancel   bool
	Update   bool
}

// DecideDrain applies the timeout policy. reject is the default.
func DecideDrain(idle bool, onTimeout DrainAction) DrainResult {
	if idle {
		return DrainResult{Presence: "ONLINE", Update: true}
	}
	if onTimeout == DrainAbort {
		return DrainResult{Presence: "DRAINING", Cancel: true, Update: true}
	}
	return DrainResult{Presence: "ONLINE", Update: false}
}

// HostKind reads IAZIO_AGENT_HOST_KIND. Unset means permanent.
func HostKind(env string) string {
	if env == "ephemeral" {
		return "ephemeral"
	}
	return "permanent"
}
