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
	// LockRunning means a harness holds the checkout.
	LockRunning = "RUNNING_HARNESS"
	// LockIdle means the checkout lock is free.
	LockIdle = "IDLE"
	// LockCooling means the harness exited and the lock is still reserved.
	LockCooling = "COOLING_OFF"
	// LockPending means the job is waiting to be correlated.
	LockPending = "PENDING_CORRELATION"
	// LockFailed means the job will not be resumed.
	LockFailed = "FAILED"

	// TermGrace is the wait between TERM and KILL.
	TermGrace = 10 * time.Second
	// CoolingOff is how long a lock stays reserved after a harness exits.
	CoolingOff = 10 * time.Second
	// DefaultJobDuration is the execution timeout.
	DefaultJobDuration = 60 * time.Minute
	// HubLockTimeout is how long a docs-hub discard waits for the hub lock.
	HubLockTimeout = 30 * time.Second

	// ReasonCancelRequested is stored when a caller cancels a running harness.
	ReasonCancelRequested = "CANCEL_REQUESTED"
	// ReasonExecutionTimeout is stored when the harness exceeds its duration.
	ReasonExecutionTimeout = "execution_timeout"
	// ReasonDrainTimeout is stored when a drain abort cancels a running harness.
	ReasonDrainTimeout = "DRAIN_TIMEOUT"
	// ReasonHostRestart is stored when a lease is stranded after process start.
	ReasonHostRestart = "host_restart"
)

// Runner executes a harness child. Tests substitute it.
type Runner interface {
	Start(argv []string) (pid int, err error)
	Signal(pid int, sig string) error
	Wait(pid int) (exitCode int, err error)
}

// Sup is the in-process repo lock table.
type Sup struct {
	mu       sync.Mutex
	gates    map[string]*sync.Mutex
	locks    map[string]string
	until    map[string]time.Time
	deadline map[string]time.Time
	pids     map[string]int
	reasons  map[string]string
	now      func() time.Time
	sleep    func(time.Duration)
	signal   func(pid int, sig string) error
	alive    func(pid int) bool
	fetches  []string
	halted   bool
}

// New returns a supervisor. now injects the clock.
func New(now func() time.Time) *Sup {
	if now == nil {
		now = time.Now
	}
	return &Sup{
		gates:    map[string]*sync.Mutex{},
		locks:    map[string]string{},
		until:    map[string]time.Time{},
		deadline: map[string]time.Time{},
		pids:     map[string]int{},
		reasons:  map[string]string{},
		now:      now,
		sleep:    time.Sleep,
		alive:    isProcessAlive,
	}
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

// Spawn starts a harness when the worktree lock is free.
// token is accepted and ignored so it cannot be forwarded.
func (s *Sup) Spawn(r Runner, jobID, apiURL, worktree, docsHub, token string) error {
	if err := s.TrySpawn(worktree); err != nil {
		return err
	}
	argv := SpawnArgs(jobID, apiURL, worktree, docsHub)
	_ = token
	pid := 0
	if r != nil {
		var err error
		pid, err = r.Start(argv)
		if err != nil {
			s.MarkIdle(worktree)
			return err
		}
	}
	s.mu.Lock()
	s.pids[worktree] = pid
	s.deadline[worktree] = s.now().Add(DefaultJobDuration)
	if s.signal == nil && r != nil {
		s.signal = r.Signal
	}
	s.mu.Unlock()
	return nil
}

// PID returns the running process ID for the worktree.
func (s *Sup) PID(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pids[path]
}


// TrySpawn records RUNNING_HARNESS. A second call for the same path is refused.
func (s *Sup) TrySpawn(path string) error {
	gate := s.gate(path)
	gate.Lock()
	defer gate.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[path] == LockRunning {
		return errors.New("checkout busy")
	}
	s.locks[path] = LockRunning
	return nil
}

func (s *Sup) gate(path string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gates[path]
	if g == nil {
		g = &sync.Mutex{}
		s.gates[path] = g
	}
	return g
}

// MarkIdle clears the lock.
func (s *Sup) MarkIdle(path string) {
	s.mu.Lock()
	s.locks[path] = LockIdle
	delete(s.until, path)
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

// BeginCooling reserves the lock for CoolingOff after a harness exits.
func (s *Sup) BeginCooling(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.locks[path] = LockCooling
	s.until[path] = s.now().Add(CoolingOff)
}

// Promote moves cooled locks to IDLE.
func (s *Sup) Promote() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for path, state := range s.locks {
		if state != LockCooling {
			continue
		}
		until, ok := s.until[path]
		if ok && !now.Before(until) {
			s.locks[path] = LockIdle
			delete(s.until, path)
		}
	}
}

// Cancel sends TERM, then KILL after TermGrace, and starts cooling-off.
func (s *Sup) Cancel(path, reason string) {
	s.mu.Lock()
	pid := s.pids[path]
	state := s.locks[path]
	signal := s.signal
	alive := s.alive
	sleep := s.sleep
	s.reasons[path] = reason
	s.mu.Unlock()
	if state != LockRunning {
		return
	}
	GracefulStop(signal, alive, sleep, pid)
	s.BeginCooling(path)
}

// Reason returns the last stop reason for a checkout.
func (s *Sup) Reason(path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reasons[path]
}

// Tick cancels harnesses that have reached their execution timeout or died, and finishes cooling-off.
func (s *Sup) Tick() {
	now := s.now()
	s.mu.Lock()
	var due []string
	for path, state := range s.locks {
		if state == LockRunning {
			if deadline, ok := s.deadline[path]; ok && !deadline.After(now) {
				due = append(due, path)
			} else {
				pid := s.pids[path]
				if pid > 0 && s.alive != nil && !s.alive(pid) {
					due = append(due, path)
				}
			}
		}
	}
	s.mu.Unlock()
	for _, path := range due {
		s.Cancel(path, ReasonExecutionTimeout)
	}
	s.Promote()
}

// Deadline returns the execution deadline armed by Spawn.
func (s *Sup) Deadline(path string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadline[path]
}

// GracefulStop sends TERM and, if the process is still alive, KILL after 10s.
func GracefulStop(signal func(pid int, sig string) error, alive func(pid int) bool, sleep func(time.Duration), pid int) {
	if signal != nil {
		_ = signal(pid, "TERM")
	}
	if sleep != nil {
		sleep(TermGrace)
	}
	if alive != nil && alive(pid) && signal != nil {
		_ = signal(pid, "KILL")
	}
}

// SweepSpool deletes files older than 7 days unless the job is still active.
// A permission error is skipped and does not fail the sweep.
func SweepSpool(dir string, now time.Time, active map[string]bool, remove func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	cutoff := now.Add(-7 * 24 * time.Hour)
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
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
		if remove == nil {
			continue
		}
		if err := remove(filepath.Join(dir, name)); err != nil {
			continue
		}
	}
	return nil
}

// RecordFetch stores a fetch error and does not halt the host.
func (s *Sup) RecordFetch(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetches = append(s.fetches, err.Error())
}

// FetchErrors returns recorded fetch failures.
func (s *Sup) FetchErrors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.fetches))
	copy(out, s.fetches)
	return out
}

// Halted reports whether the host has been halted. Fetch failures do not halt it.
func (s *Sup) Halted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.halted
}

// DrainAction is reject or abort when the drain deadline hits.
type DrainAction string

const (
	// DrainReject leaves the host ONLINE and skips the update.
	DrainReject DrainAction = "reject"
	// DrainAbort cancels running harnesses and then updates.
	DrainAbort DrainAction = "abort"
)

// DrainResult is the host transition after a suite update request.
type DrainResult struct {
	Presence string
	Cancel   bool
	Update   bool
	Reason   string
}

// DecideDrain applies the timeout policy. reject is the default.
// reject returns ONLINE without updating. abort cancels with DRAIN_TIMEOUT and updates.
func DecideDrain(idle bool, onTimeout DrainAction) DrainResult {
	if idle {
		return DrainResult{Presence: "ONLINE", Update: true}
	}
	if onTimeout == DrainAbort {
		return DrainResult{Presence: "ONLINE", Cancel: true, Update: true, Reason: ReasonDrainTimeout}
	}
	return DrainResult{Presence: "ONLINE", Update: false}
}

// HostKind reads IAZIO_AGENT_HOST_KIND. Unset means permanent.
func HostKind(env string) string {
	if env == "" {
		return "permanent"
	}
	return env
}
