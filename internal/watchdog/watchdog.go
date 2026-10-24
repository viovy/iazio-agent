// Package watchdog monitors in-flight job executions and provides autonomous crash recovery and resumption.
package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/sessionstore"
	"github.com/viovy/iazio-agent/internal/supervisor"
)

const (
	StatusRunning                = "RUNNING"
	StatusCompleted              = "COMPLETED"
	StatusFailed                 = "FAILED"
	StatusTerminatedUnexpectedly = "TERMINATED_UNEXPECTEDLY"
	StatusResumed                = "RESUMED"

	DefaultMaxResumes = 5
	DefaultTick       = 5 * time.Second
)

// TrackedJob records state of an in-flight or interrupted harness job.
type TrackedJob struct {
	JobID          string    `json:"job_id"`
	ScheduleID     string    `json:"schedule_id,omitempty"`
	WorktreePath   string    `json:"worktree_path"`
	DocsHubPath    string    `json:"docs_hub_path,omitempty"`
	APIURL         string    `json:"api_url"`
	Engine         string    `json:"engine"`
	Prompt         string    `json:"prompt"`
	StoryID        string    `json:"story_id,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	PID            int       `json:"pid"`
	StartedAt      time.Time `json:"started_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	ResumeCount    int       `json:"resume_count"`
	Status         string    `json:"status"`
	ExitCode       int       `json:"exit_code"`
	LastError      string    `json:"last_error,omitempty"`
}

// Spawner restarts a terminated job.
type Spawner func(ctx context.Context, job TrackedJob) (newPID int, err error)

// Watchdog tracks running harness jobs and autonomously resumes crashed or terminated jobs.
type Watchdog struct {
	mu         sync.Mutex
	stateFile  string
	jobs       map[string]*TrackedJob
	runner     supervisor.Runner
	spawner    Spawner
	maxResumes int
	isAlive    func(pid int) bool
	now        func() time.Time
}

// Options configures a Watchdog.
type Options struct {
	StateFile  string
	Runner     supervisor.Runner
	Spawner    Spawner
	MaxResumes int
	IsAlive    func(pid int) bool
	Now        func() time.Time
}

// New creates a Watchdog.
func New(opts Options) *Watchdog {
	if opts.StateFile == "" {
		home, _ := os.UserHomeDir()
		if home != "" {
			opts.StateFile = filepath.Join(home, ".iazio", "watchdog-active-jobs.json")
		} else {
			opts.StateFile = filepath.Join(os.TempDir(), "watchdog-active-jobs.json")
		}
	}
	if opts.MaxResumes <= 0 {
		opts.MaxResumes = DefaultMaxResumes
	}
	if opts.IsAlive == nil {
		opts.IsAlive = defaultIsProcessAlive
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	w := &Watchdog{
		stateFile:  opts.StateFile,
		jobs:       make(map[string]*TrackedJob),
		runner:     opts.Runner,
		spawner:    opts.Spawner,
		maxResumes: opts.MaxResumes,
		isAlive:    opts.IsAlive,
		now:        opts.Now,
	}

	// Load existing persistent state if present
	w.loadState()
	return w
}

func defaultIsProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func (w *Watchdog) loadState() {
	if w.stateFile == "" {
		return
	}
	data, err := os.ReadFile(w.stateFile)
	if err != nil {
		return
	}
	var list []*TrackedJob
	if err := json.Unmarshal(data, &list); err == nil {
		for _, j := range list {
			if j != nil && j.JobID != "" {
				w.jobs[j.JobID] = j
			}
		}
	}
}

func (w *Watchdog) persistStateLocked() {
	if w.stateFile == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(w.stateFile), 0755)
	list := make([]*TrackedJob, 0, len(w.jobs))
	for _, j := range w.jobs {
		list = append(list, j)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	tmp := fmt.Sprintf("%s.tmp.%d", w.stateFile, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0600); err == nil {
		_ = os.Rename(tmp, w.stateFile)
	}
}

// Track registers a newly spawned job and starts a supervisor reaper.
func (w *Watchdog) Track(job TrackedJob) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now().UTC()
	if job.StartedAt.IsZero() {
		job.StartedAt = now
	}
	job.UpdatedAt = now
	if job.Status == "" {
		job.Status = StatusRunning
	}

	jobCopy := job
	w.jobs[job.JobID] = &jobCopy
	w.persistStateLocked()

	// Launch background reaper if runner is available
	if w.runner != nil && job.PID > 0 {
		go w.reapProcess(job.JobID, job.PID)
	}
}

func (w *Watchdog) reapProcess(jobID string, pid int) {
	exitCode, err := w.runner.Wait(pid)
	w.OnProcessExit(jobID, pid, exitCode, err)
}

// OnProcessExit handles termination of a supervised child process.
func (w *Watchdog) OnProcessExit(jobID string, pid, exitCode int, waitErr error) {
	w.mu.Lock()
	job, ok := w.jobs[jobID]
	if !ok {
		w.mu.Unlock()
		return
	}

	now := w.now().UTC()
	job.UpdatedAt = now
	job.ExitCode = exitCode
	if waitErr != nil {
		job.LastError = waitErr.Error()
	}

	// Clean completion
	if exitCode == 0 && waitErr == nil {
		job.Status = StatusCompleted
		w.persistStateLocked()
		w.mu.Unlock()
		return
	}

	// Unexpected termination or crash
	job.Status = StatusTerminatedUnexpectedly
	w.persistStateLocked()
	w.mu.Unlock()

	// Trigger autonomous resume if budget remains
	w.attemptResume(jobID)
}

func (w *Watchdog) attemptResume(jobID string) {
	w.mu.Lock()
	job, ok := w.jobs[jobID]
	if !ok || job == nil {
		w.mu.Unlock()
		return
	}

	if job.Status == StatusCompleted || job.Status == StatusFailed {
		w.mu.Unlock()
		return
	}

	if job.ResumeCount >= w.maxResumes {
		job.Status = StatusFailed
		job.LastError = fmt.Sprintf("WATCHDOG_MAX_RETRIES_EXCEEDED: failed after %d resume attempts (last exit code: %d)", job.ResumeCount, job.ExitCode)
		w.persistStateLocked()
		w.mu.Unlock()
		return
	}

	// Early and eager conversation ID capture before resuming
	if job.ConversationID == "" && job.WorktreePath != "" {
		if lockInfo, err := repolock.Read(job.WorktreePath); err == nil && lockInfo.ConversationID != "" {
			job.ConversationID = lockInfo.ConversationID
		}
	}
	if job.ConversationID == "" && job.WorktreePath != "" {
		cpFile := filepath.Join(job.WorktreePath, ".iazio", "execution-checkpoint.json")
		if data, err := os.ReadFile(cpFile); err == nil {
			var cp struct {
				ConversationID string `json:"conversation_id"`
			}
			if err := json.Unmarshal(data, &cp); err == nil && cp.ConversationID != "" {
				job.ConversationID = cp.ConversationID
			}
		}
	}
	if job.ConversationID == "" && job.WorktreePath != "" && job.StoryID != "" {
		resolver := sessionstore.DefaultResolver()
		if sess, err := resolver.ResolveForStory(job.WorktreePath, job.StoryID); err == nil && sess != nil && sess.ConversationID != "" {
			job.ConversationID = sess.ConversationID
		}
	}

	job.ResumeCount++
	job.Status = StatusResumed
	jobCopy := *job
	spawner := w.spawner
	w.persistStateLocked()
	w.mu.Unlock()

	// Append diagnostic chunk to spool log
	w.appendResumeLog(jobCopy)

	if spawner != nil {
		newPID, err := spawner(context.Background(), jobCopy)
		w.mu.Lock()
		if err != nil {
			job.LastError = fmt.Sprintf("resume spawn error: %v", err)
			w.persistStateLocked()
			w.mu.Unlock()
			return
		}
		job.PID = newPID
		job.Status = StatusRunning
		job.UpdatedAt = w.now().UTC()
		w.persistStateLocked()
		w.mu.Unlock()

		if w.runner != nil && newPID > 0 {
			go w.reapProcess(jobID, newPID)
		}
	}
}

func (w *Watchdog) appendResumeLog(job TrackedJob) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return
	}
	logPath := filepath.Join(home, ".iazio", "spool", fmt.Sprintf("%s.log", job.JobID))
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	msg := fmt.Sprintf("\n[iazio-watchdog] Detected unexpected termination of job %s (PID %d, exit code %d). Autonomously resuming execution in-place (attempt %d/%d) with conversation %s...\n",
		job.JobID, job.PID, job.ExitCode, job.ResumeCount, w.maxResumes, job.ConversationID)
	_, _ = f.WriteString(msg)
}

// CheckInterrupted inspects all tracked jobs and restarts any dead or stranded jobs.
func (w *Watchdog) CheckInterrupted(ctx context.Context) []TrackedJob {
	w.mu.Lock()
	var toResume []string
	now := w.now().UTC()

	for id, job := range w.jobs {
		if job.Status == StatusCompleted || job.Status == StatusFailed {
			continue
		}
		// Eagerly refresh conversation ID while process is still alive
		if job.ConversationID == "" && job.WorktreePath != "" {
			if lockInfo, err := repolock.Read(job.WorktreePath); err == nil && lockInfo.ConversationID != "" {
				job.ConversationID = lockInfo.ConversationID
			} else {
				cpFile := filepath.Join(job.WorktreePath, ".iazio", "execution-checkpoint.json")
				if data, err := os.ReadFile(cpFile); err == nil {
					var cp struct {
						ConversationID string `json:"conversation_id"`
					}
					if err := json.Unmarshal(data, &cp); err == nil && cp.ConversationID != "" {
						job.ConversationID = cp.ConversationID
					}
				}
			}
		}
		if job.PID > 0 && !w.isAlive(job.PID) {
			job.Status = StatusTerminatedUnexpectedly
			job.UpdatedAt = now
			toResume = append(toResume, id)
		}
	}
	w.persistStateLocked()
	w.mu.Unlock()

	for _, id := range toResume {
		w.attemptResume(id)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	res := make([]TrackedJob, 0, len(w.jobs))
	for _, j := range w.jobs {
		res = append(res, *j)
	}
	return res
}

// StartWatchguard starts a background goroutine that periodically scans and resumes interrupted jobs.
func (w *Watchdog) StartWatchguard(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultTick
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = w.CheckInterrupted(ctx)
			}
		}
	}()
}

// GetJob returns a copy of a tracked job.
func (w *Watchdog) GetJob(jobID string) (TrackedJob, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	j, ok := w.jobs[jobID]
	if !ok || j == nil {
		return TrackedJob{}, errors.New("job not found")
	}
	return *j, nil
}

// MarkCompleted marks a job cleanly finished.
func (w *Watchdog) MarkCompleted(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if j, ok := w.jobs[jobID]; ok && j != nil {
		j.Status = StatusCompleted
		j.UpdatedAt = w.now().UTC()
		w.persistStateLocked()
	}
}

// ActiveJobs returns a snapshot of all jobs currently running or in resume flow.
func (w *Watchdog) ActiveJobs() []TrackedJob {
	w.mu.Lock()
	defer w.mu.Unlock()
	var res []TrackedJob
	for _, j := range w.jobs {
		if j.Status == StatusRunning || j.Status == StatusResumed || j.Status == StatusTerminatedUnexpectedly {
			res = append(res, *j)
		}
	}
	return res
}

// IsTracked reports whether a job is currently tracked and in-flight.
func (w *Watchdog) IsTracked(jobID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	j, ok := w.jobs[jobID]
	return ok && j != nil && j.Status != StatusCompleted && j.Status != StatusFailed
}

// NotifyWatchdog sends a WATCHDOG=1 heartbeat notification to systemd via $NOTIFY_SOCKET.
func NotifyWatchdog() error {
	socket := os.Getenv("NOTIFY_SOCKET")
	if socket == "" {
		return nil
	}
	addr := &net.UnixAddr{
		Name: socket,
		Net:  "unixgram",
	}
	conn, err := net.DialUnix("unixgram", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte("WATCHDOG=1\n"))
	return err
}

// StartSystemdWatchdog starts a background goroutine that periodically sends WATCHDOG=1
// heartbeats to systemd as long as the context remains active.
func StartSystemdWatchdog(ctx context.Context) {
	socket := os.Getenv("NOTIFY_SOCKET")
	if socket == "" {
		return
	}
	interval := 10 * time.Second
	if usecStr := os.Getenv("WATCHDOG_USEC"); usecStr != "" {
		if usec, err := strconv.ParseInt(usecStr, 10, 64); err == nil && usec > 0 {
			interval = time.Duration(usec/2) * time.Microsecond
			if interval < 1*time.Second {
				interval = 1 * time.Second
			}
		}
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		_ = NotifyWatchdog()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = NotifyWatchdog()
			}
		}
	}()
}
