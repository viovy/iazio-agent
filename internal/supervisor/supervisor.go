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
	// HubLockTimeout is how long a docs-hub discard waits for the hub lock.
	HubLockTimeout = 30 * time.Second
)

// DefaultJobDuration is the execution timeout (0 = unbounded, no timeout on execution of jobs).
var DefaultJobDuration time.Duration = 0

const (
	// ReasonCancelRequested is stored when a caller cancels a running harness.
	ReasonCancelRequested = "CANCEL_REQUESTED"
	// ReasonExecutionTimeout is stored when the harness exceeds its duration.
	ReasonExecutionTimeout = "execution_timeout"
	// ReasonDrainTimeout is stored when a drain abort cancels a running harness.
	ReasonDrainTimeout = "DRAIN_TIMEOUT"
	// ReasonHostRestart is stored when a lease is stranded after process start.
	ReasonHostRestart = "host_restart"
	// ReasonProcessDied is stored when a child harness process terminates unexpectedly.
	ReasonProcessDied = "process_died"
)

// CoolingOff is how long a lock stays reserved after a harness exits.
var CoolingOff = 10 * time.Second

// Runner executes a harness child. Tests substitute it.
type Runner interface {
	Start(argv []string) (pid int, err error)
	Signal(pid int, sig string) error
	Wait(pid int) (exitCode int, err error)
}

// Sup is the in-process repo lock table.
type Sup struct {
	mu                  sync.Mutex
	gates               map[string]*sync.Mutex
	locks               map[string]string
	until               map[string]time.Time
	deadline            map[string]time.Time
	pids                map[string]int
	jobIDs              map[string]string
	reasons             map[string]string
	now                 func() time.Time
	sleep               func(time.Duration)
	signal              func(pid int, sig string) error
	alive               func(pid int) bool
	fetches             []string
	halted              bool
	loopDetector        *LoopDetector
	toolUpdating        bool
	toolUpdatingHolders int
	isProgressing       func(path string, pid int) bool
	lastProgress        map[string]time.Time
	stallTimeout        time.Duration
}

// New returns a supervisor. now injects the clock.
func New(now func() time.Time) *Sup {
	if now == nil {
		now = time.Now
	}
	return &Sup{
		gates:        map[string]*sync.Mutex{},
		locks:        map[string]string{},
		until:        map[string]time.Time{},
		deadline:     map[string]time.Time{},
		lastProgress: map[string]time.Time{},
		pids:         map[string]int{},
		jobIDs:       map[string]string{},
		reasons:      map[string]string{},
		now:          now,
		sleep:        time.Sleep,
		alive:        isProcessAlive,
	}
}

// SpawnArgs is the only argv the agent passes. It does not include a token.
func SpawnArgs(jobID, apiURL, worktree, docsHub string, extraArgs ...string) []string {
	argv := []string{
		"iazio-harness", "run",
		"--job-id", jobID,
		"--api-url", apiURL,
		"--worktree-path", worktree,
		"--docs-hub-path", docsHub,
	}
	if len(extraArgs) > 0 {
		argv = append(argv, extraArgs...)
	}
	return argv
}

// Spawn starts a harness when the worktree lock is free.
// token is accepted and ignored so it cannot be forwarded.
func (s *Sup) Spawn(r Runner, jobID, apiURL, worktree, docsHub, token string, extraArgs ...string) error {
	if err := s.TrySpawn(worktree); err != nil {
		return err
	}
	argv := SpawnArgs(jobID, apiURL, worktree, docsHub, extraArgs...)
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
	s.jobIDs[worktree] = jobID
	s.deadline[worktree] = s.now().Add(DefaultJobDuration)
	s.lastProgress[worktree] = s.now()
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

// JobID returns the active job ID for the worktree.
func (s *Sup) JobID(path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobIDs[path]
}

// TrySpawn records RUNNING_HARNESS. A second call for the same path is refused.
func (s *Sup) TrySpawn(path string) error {
	gate := s.gate(path)
	gate.Lock()
	defer gate.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolUpdating {
		return errors.New("host locked: tool update in progress")
	}
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
	delete(s.pids, path)
	delete(s.deadline, path)
	delete(s.jobIDs, path)
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
	delete(s.pids, path)
	delete(s.deadline, path)
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
			delete(s.jobIDs, path)
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

// ActiveJobs returns a list of actively running worktrees and their PIDs.
func (s *Sup) ActiveJobs() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make(map[string]int)
	for path, state := range s.locks {
		if state == LockRunning {
			res[path] = s.pids[path]
		}
	}
	return res
}

func (s *Sup) hasRunningHarnessLocked() bool {
	for _, state := range s.locks {
		if state == LockRunning || state == LockCooling || state == LockPending {
			return true
		}
	}
	return false
}

// HasRunningHarness reports whether any worktree is actively running, cooling, or pending correlation.
func (s *Sup) HasRunningHarness() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasRunningHarnessLocked()
}

// TryAcquireToolUpdate locks the host exclusively for tool update swapping.
// If any harness is running or cooling, or if an update is already running, it returns (nil, false).
func (s *Sup) TryAcquireToolUpdate() (release func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolUpdating || s.hasRunningHarnessLocked() {
		return nil, false
	}
	s.toolUpdatingHolders++
	s.toolUpdating = true
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.toolUpdatingHolders--
			if s.toolUpdatingHolders <= 0 {
				s.toolUpdatingHolders = 0
				s.toolUpdating = false
			}
			s.mu.Unlock()
		})
	}, true
}

// AcquireToolUpdateForce sets toolUpdating = true unconditionally, returning a release callback.
// Used when an update is forced, ensuring TrySpawn still refuses new child executions during the swap.
func (s *Sup) AcquireToolUpdateForce() func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolUpdatingHolders++
	s.toolUpdating = true
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.toolUpdatingHolders--
			if s.toolUpdatingHolders <= 0 {
				s.toolUpdatingHolders = 0
				s.toolUpdating = false
			}
			s.mu.Unlock()
		})
	}
}

// IsToolUpdating reports whether an exclusive tool update lock is currently held.
func (s *Sup) IsToolUpdating() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.toolUpdating
}

// RunningJob describes an active harness job.
type RunningJob struct {
	JobID        string
	WorktreePath string
	PID          int
}

// RunningJobs returns a list of actively running worktrees, their job IDs, and PIDs.
func (s *Sup) RunningJobs() []RunningJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	var res []RunningJob
	for path, state := range s.locks {
		if state == LockRunning {
			res = append(res, RunningJob{
				JobID:        s.jobIDs[path],
				WorktreePath: path,
				PID:          s.pids[path],
			})
		}
	}
	return res
}

// ReapedJob holds details about a reaped job.
type ReapedJob struct {
	JobID        string
	WorktreePath string
	Reason       string
}

// SetStallTimeout overrides the default stall timeout for progress-aware supervision.
func (s *Sup) SetStallTimeout(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stallTimeout = timeout
}

// Tick cancels harnesses that have reached their execution timeout or died, and finishes cooling-off.
func (s *Sup) Tick() []ReapedJob {
	now := s.now()
	s.mu.Lock()
	var dueTimeout []string
	var dueDied []string
	for path, state := range s.locks {
		if state == LockRunning {
			timedOut := false
			pid := s.pids[path]
			if s.isActivelyProgressingLocked(path, pid) {
				s.lastProgress[path] = now
				if DefaultJobDuration > 0 {
					s.deadline[path] = now.Add(DefaultJobDuration)
				}
			} else {
				lastProg := s.lastProgress[path]
				if lastProg.IsZero() {
					lastProg = now
					s.lastProgress[path] = now
				}
				stallLimit := s.stallTimeout
				if stallLimit <= 0 {
					stallLimit = 45 * time.Minute
				}
				if now.Sub(lastProg) >= stallLimit {
					dueTimeout = append(dueTimeout, path)
					timedOut = true
				} else if DefaultJobDuration > 0 {
					if deadline, ok := s.deadline[path]; ok && !deadline.After(now) {
						dueTimeout = append(dueTimeout, path)
						timedOut = true
					}
				}
			}
			if !timedOut {
				if pid > 0 && s.alive != nil && !s.alive(pid) {
					dueDied = append(dueDied, path)
				}
			}
		}
	}
	s.mu.Unlock()
	var reaped []ReapedJob
	for _, path := range dueTimeout {
		jobID := s.JobID(path)
		s.Cancel(path, ReasonExecutionTimeout)
		reaped = append(reaped, ReapedJob{
			JobID:        jobID,
			WorktreePath: path,
			Reason:       ReasonExecutionTimeout,
		})
	}
	for _, path := range dueDied {
		jobID := s.JobID(path)
		s.Cancel(path, ReasonProcessDied)
		reaped = append(reaped, ReapedJob{
			JobID:        jobID,
			WorktreePath: path,
			Reason:       ReasonProcessDied,
		})
	}
	s.Promote()
	return reaped
}

// Deadline returns the execution deadline armed by Spawn.
func (s *Sup) Deadline(path string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadline[path]
}

// SetProgressChecker sets a custom function to determine if a job in a given worktree is actively progressing.
func (s *Sup) SetProgressChecker(fn func(path string, pid int) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isProgressing = fn
}

func (s *Sup) isActivelyProgressingLocked(path string, pid int) bool {
	if s.isProgressing != nil {
		return s.isProgressing(path, pid)
	}
	if pid > 0 && s.alive != nil && s.alive(pid) {
		return true
	}
	return false
}

// GracefulStop sends TERM and forcefully reaps the process tree/group with KILL after 10s.
func GracefulStop(signal func(pid int, sig string) error, alive func(pid int) bool, sleep func(time.Duration), pid int) {
	if pid <= 0 {
		return
	}
	if signal != nil {
		_ = signal(pid, "TERM")
	}
	if sleep != nil {
		sleep(TermGrace)
	}
	if signal != nil {
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

// CheckLoop checks an action/output pair against the loop detector.
func (s *Sup) CheckLoop(action, output string) LoopDetectionResult {
	s.mu.Lock()
	if s.loopDetector == nil {
		s.loopDetector = NewLoopDetector(20, 3, 2)
	}
	ld := s.loopDetector
	s.mu.Unlock()
	return ld.Record(action, output)
}

// ResetLoopDetector clears supervisor loop history.
func (s *Sup) ResetLoopDetector() {
	s.mu.Lock()
	if s.loopDetector != nil {
		s.loopDetector.Reset()
	}
	s.mu.Unlock()
}
