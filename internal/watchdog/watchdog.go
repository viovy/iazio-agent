// Package watchdog monitors in-flight job executions and provides autonomous crash recovery and resumption.
package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/ase/gate"
	"github.com/viovy/iazio-agent/internal/reaper"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/sessionstore"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/workspace"
)

const (
	StatusRunning                = "RUNNING"
	StatusCompleted              = "COMPLETED"
	StatusFailed                 = "FAILED"
	StatusTerminatedUnexpectedly = "TERMINATED_UNEXPECTEDLY"
	StatusResumed                = "RESUMED"
	StatusAutoResuming           = "AUTO_RESUMING"
	StatusCircuitBreakerTripped  = "CIRCUIT_BREAKER_TRIPPED"

	DefaultMaxResumes = 5
	DefaultTick       = 5 * time.Second
)

// AutoResumePolicy defines the bounded retry and circuit-breaker configuration for autonomous jobs.
type AutoResumePolicy struct {
	Enabled                bool `json:"enabled"`
	MaxAutoResumes         int  `json:"max_auto_resumes"`
	BackoffInitialSec      int  `json:"backoff_initial_sec"`
	BackoffMaxSec          int  `json:"backoff_max_sec"`
	RequireForwardProgress bool `json:"require_forward_progress"`
	RequireTestsPassed     bool `json:"require_tests_passed,omitempty"`
	RequireReleaseVerified bool `json:"require_release_verified,omitempty"`
}

// DefaultAutoResumePolicy returns enterprise default values for autonomous story execution.
func DefaultAutoResumePolicy() AutoResumePolicy {
	return AutoResumePolicy{
		Enabled:                true,
		MaxAutoResumes:         5,
		BackoffInitialSec:      10,
		BackoffMaxSec:          120,
		RequireForwardProgress: true,
	}
}

// TrackedJob records state of an in-flight or interrupted harness job.
type TrackedJob struct {
	JobID                   string           `json:"job_id"`
	ScheduleID              string           `json:"schedule_id,omitempty"`
	WorktreePath            string           `json:"worktree_path"`
	DocsHubPath             string           `json:"docs_hub_path,omitempty"`
	APIURL                  string           `json:"api_url"`
	Engine                  string           `json:"engine"`
	Prompt                  string           `json:"prompt"`
	StoryID                 string           `json:"story_id,omitempty"`
	ConversationID          string           `json:"conversation_id,omitempty"`
	PID                     int              `json:"pid"`
	HarnessPID              int              `json:"harness_pid,omitempty"`
	ToolPID                 int              `json:"tool_pid,omitempty"`
	AttachToolPID           int              `json:"attach_tool_pid,omitempty"`
	StartedAt               time.Time        `json:"started_at"`
	UpdatedAt               time.Time        `json:"updated_at"`
	ResumeCount             int              `json:"resume_count"`
	Status                  string           `json:"status"`
	ExitCode                int              `json:"exit_code"`
	LastError               string           `json:"last_error,omitempty"`
	AutoResumePolicy        AutoResumePolicy `json:"auto_resume_policy"`
	LastProgressFingerprint string           `json:"last_progress_fingerprint,omitempty"`
	ConsecutiveZeroProgress int              `json:"consecutive_zero_progress,omitempty"`
	CircuitBreakerTripped   bool             `json:"circuit_breaker_tripped,omitempty"`
	ASEGateStatus           map[string]any   `json:"ase_gate_status,omitempty"`
	StallTimeoutSec         int              `json:"stall_timeout_sec,omitempty"`
	LastActivityAt          time.Time        `json:"last_activity_at,omitempty"`
	IdleSeconds             float64          `json:"idle_seconds,omitempty"`
}

// Spawner restarts a terminated job.
type Spawner func(ctx context.Context, job TrackedJob) (newPID int, err error)

// Watchdog tracks running harness jobs and autonomously resumes crashed or terminated jobs.
type Watchdog struct {
	mu          sync.Mutex
	guardWG     sync.WaitGroup
	stateFile   string
	jobs        map[string]*TrackedJob
	runner      supervisor.Runner
	spawner     Spawner
	maxResumes  int
	isAlive     func(pid int) bool
	now         func() time.Time
	monitor     *ConversationLivenessMonitor
	killProcess  func(pid int) error
	sleep        func(time.Duration)
	onOrphanTool func(job TrackedJob)
	onAutoResume func(job TrackedJob, reason string, attempt, max int)
}

// Options configures a Watchdog.
type Options struct {
	StateFile    string
	Runner       supervisor.Runner
	Spawner      Spawner
	MaxResumes   int
	IsAlive      func(pid int) bool
	Now          func() time.Time
	Monitor      *ConversationLivenessMonitor
	KillProcess  func(pid int) error
	Sleep        func(time.Duration)
	OnOrphanTool func(job TrackedJob)
	OnAutoResume func(job TrackedJob, reason string, attempt, max int)
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
	if opts.Monitor == nil {
		opts.Monitor = NewConversationLivenessMonitor(300 * time.Second)
	}
	if opts.KillProcess == nil {
		opts.KillProcess = defaultKillProcess
	}

	w := &Watchdog{
		stateFile:   opts.StateFile,
		jobs:        make(map[string]*TrackedJob),
		runner:      opts.Runner,
		spawner:     opts.Spawner,
		maxResumes:  opts.MaxResumes,
		isAlive:     opts.IsAlive,
		now:         opts.Now,
		monitor:     opts.Monitor,
		killProcess:  opts.KillProcess,
		sleep:        opts.Sleep,
		onOrphanTool: opts.OnOrphanTool,
		onAutoResume: opts.OnAutoResume,
	}

	// Load existing persistent state if present
	w.loadState()
	return w
}

// IsProcessAlive tests whether a process with the given PID is actively running.
func IsProcessAlive(pid int) bool {
	return defaultIsProcessAlive(pid)
}

func defaultKillProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	return reaper.TerminateProcessGroup(pid, 2*time.Second)
}

// ComputeProgressFingerprint generates a SHA-256 fingerprint of the workspace progress.
func ComputeProgressFingerprint(worktreePath, asePhase string, passingTestCount int, completedDoDItems []string) string {
	if worktreePath == "" {
		return ""
	}
	gitDir := filepath.Join(worktreePath, ".git")
	if _, err := os.Stat(gitDir); err != nil {
		return ""
	}

	h := sha256.New()
	h.Write([]byte(asePhase))
	h.Write([]byte{0})

	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = worktreePath
	if out, err := cmd.CombinedOutput(); err == nil {
		h.Write([]byte(strings.TrimSpace(string(out))))
	}
	h.Write([]byte{0})

	diffCmd := exec.Command("git", "diff", "--stat")
	diffCmd.Dir = worktreePath
	if diffOut, err := diffCmd.CombinedOutput(); err == nil {
		h.Write([]byte(strings.TrimSpace(string(diffOut))))
	}
	h.Write([]byte{0})

	h.Write([]byte(strconv.Itoa(passingTestCount)))
	h.Write([]byte{0})
	for _, item := range completedDoDItems {
		h.Write([]byte(item))
		h.Write([]byte(","))
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
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
	if job.AutoResumePolicy.MaxAutoResumes <= 0 {
		if w.maxResumes > 0 {
			job.AutoResumePolicy.MaxAutoResumes = w.maxResumes
		} else {
			job.AutoResumePolicy.MaxAutoResumes = DefaultMaxResumes
		}
		job.AutoResumePolicy.Enabled = true
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

	// H2: Guard against double-resume race condition on stall termination
	if job.PID != pid && job.HarnessPID != pid {
		w.mu.Unlock()
		return
	}
	if job.Status == StatusAutoResuming {
		// Process exit was already anticipated and handled by stall detector / auto-resumer
		w.mu.Unlock()
		return
	}

	now := w.now().UTC()
	job.UpdatedAt = now
	job.ExitCode = exitCode
	if waitErr != nil {
		job.LastError = waitErr.Error()
	}

	// First inspect if AI tool process (ToolPID) is still running while harness terminated
	if job.ToolPID <= 0 && job.WorktreePath != "" {
		if lockInfo, err := repolock.Read(job.WorktreePath); err == nil && lockInfo.ToolPID > 0 {
			job.ToolPID = lockInfo.ToolPID
		}
	}
	if job.ToolPID > 0 && w.isAlive(job.ToolPID) {
		w.appendOrphanToolLog(*job)
		w.persistStateLocked()
		w.mu.Unlock()
		w.attemptResumeWithReason(jobID, "attach_running_tool", nil)
		return
	}

	// Clean completion check with "Don't Stop Till Done" ASE Completion Gate
	if exitCode == 0 && waitErr == nil {
		gateRes := gate.Verify(gate.GateOptions{
			WorktreePath:           job.WorktreePath,
			StoryID:                job.StoryID,
			RequireTestsPassed:     job.AutoResumePolicy.RequireTestsPassed || (job.StoryID != "" && job.AutoResumePolicy.Enabled),
			RequireReleaseVerified: job.AutoResumePolicy.RequireReleaseVerified || (job.StoryID != "" && job.AutoResumePolicy.Enabled),
		})
		job.ASEGateStatus = map[string]any{
			"passed":           gateRes.Passed,
			"unmet_gates":      gateRes.UnmetGates,
			"git_clean":        gateRes.GitClean,
			"branch_clean":     gateRes.BranchClean,
			"submodules_clean": gateRes.SubmodulesClean,
			"story_done":       gateRes.StoryDone,
			"tests_passed":     gateRes.TestsPassed,
			"release_verified": gateRes.ReleaseVerified,
		}

		if !gateRes.Passed {
			job.Status = StatusAutoResuming
			job.LastError = fmt.Sprintf("incomplete_ase_flow: unmet gates: %v", gateRes.UnmetGates)
			w.persistStateLocked()
			w.mu.Unlock()
			w.attemptResumeWithReason(jobID, "incomplete_ase_flow", gateRes.UnmetGates)
			return
		}

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
	w.attemptResumeWithReason(jobID, "unexpected_termination", nil)
}

func (w *Watchdog) attemptResumeWithReason(jobID, reason string, unmetGates []string) {
	w.mu.Lock()
	job, ok := w.jobs[jobID]
	if !ok || job == nil {
		w.mu.Unlock()
		return
	}

	if job.Status == StatusCompleted || job.Status == StatusFailed || job.Status == StatusCircuitBreakerTripped {
		w.mu.Unlock()
		return
	}

	if reason == "attach_running_tool" {
		job.AttachToolPID = job.ToolPID
		job.Status = StatusAutoResuming
		jobCopy := *job
		spawner := w.spawner
		w.persistStateLocked()
		w.mu.Unlock()

		w.appendOrphanToolLog(jobCopy)
		if spawner != nil {
			newPID, err := spawner(context.Background(), jobCopy)
			w.mu.Lock()
			if err != nil {
				job.LastError = fmt.Sprintf("attach spawn error: %v", err)
				w.persistStateLocked()
				w.mu.Unlock()
				return
			}
			job.PID = newPID
			job.HarnessPID = newPID
			job.Status = StatusRunning
			job.UpdatedAt = w.now().UTC()
			w.persistStateLocked()
			w.mu.Unlock()

			if w.runner != nil && newPID > 0 {
				go w.reapProcess(jobID, newPID)
			}
		}
		return
	}

	policy := job.AutoResumePolicy
	if !policy.Enabled && job.ResumeCount > 0 {
		job.Status = StatusFailed
		job.LastError = "AUTO_RESUME_DISABLED: auto resume is not enabled for this job"
		w.persistStateLocked()
		w.mu.Unlock()
		return
	}

	effectiveMaxResumes := policy.MaxAutoResumes
	if effectiveMaxResumes <= 0 {
		effectiveMaxResumes = w.maxResumes
	}

	if job.ResumeCount >= effectiveMaxResumes {
		job.Status = StatusFailed
		job.CircuitBreakerTripped = true
		job.LastError = fmt.Sprintf("CIRCUIT_BREAKER_TRIPPED: max auto resumes reached (%d attempts, last reason: %s, last exit code: %d)", job.ResumeCount, reason, job.ExitCode)
		w.persistStateLocked()
		w.mu.Unlock()
		return
	}

	// Compute progress fingerprint and enforce circuit breaker
	fp := ComputeProgressFingerprint(job.WorktreePath, "IN_PROGRESS", 0, nil)
	if policy.RequireForwardProgress && fp != "" {
		if job.LastProgressFingerprint != "" && fp == job.LastProgressFingerprint {
			job.ConsecutiveZeroProgress++
		} else {
			job.LastProgressFingerprint = fp
			job.ConsecutiveZeroProgress = 1
		}
		if job.ConsecutiveZeroProgress >= 2 {
			job.Status = StatusFailed
			job.CircuitBreakerTripped = true
			job.LastError = fmt.Sprintf("CIRCUIT_BREAKER_TRIPPED: zero forward progress detected across %d consecutive auto-resumes (fingerprint: %s, reason: %s)", job.ConsecutiveZeroProgress, fp, reason)
			w.persistStateLocked()
			w.mu.Unlock()
			return
		}
	} else if fp != "" {
		job.LastProgressFingerprint = fp
		job.ConsecutiveZeroProgress = 0
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

	if job.ConversationID == "" {
		isResumePrompt := strings.EqualFold(strings.TrimSpace(job.Prompt), "resume") ||
			strings.HasPrefix(strings.TrimSpace(job.Prompt), "/autonomous-story-execution resume")
		if isResumePrompt {
			job.Status = StatusFailed
			job.CircuitBreakerTripped = true
			job.LastError = fmt.Sprintf("RESUME_ABORTED_EMPTY_CONVERSATION: cannot resume job %s without captured conversation ID", job.JobID)
			w.persistStateLocked()
			w.mu.Unlock()
			return
		}
	}

	job.ResumeCount++
	job.Status = StatusAutoResuming
	jobCopy := *job
	spawner := w.spawner
	sleeper := w.sleep
	w.persistStateLocked()
	w.mu.Unlock()

	// Append diagnostic chunk to spool log
	w.appendResumeLog(jobCopy)

	// Apply exponential backoff if configured
	if policy.BackoffInitialSec > 0 && sleeper != nil {
		backoff := time.Duration(policy.BackoffInitialSec*(1<<(jobCopy.ResumeCount-1))) * time.Second
		if policy.BackoffMaxSec > 0 && backoff > time.Duration(policy.BackoffMaxSec)*time.Second {
			backoff = time.Duration(policy.BackoffMaxSec) * time.Second
		}
		sleeper(backoff)
	}

	// Inject structured continuation prompt
	continuationDirective := fmt.Sprintf("\n[ASE AUTO-RESUME CONTINUATION: Attempt %d/%d on conversation %s. Interrupted Reason: %s. Unmet Gates: %v. Continue execution from current workspace state without repeating completed steps.]\n\n",
		jobCopy.ResumeCount, effectiveMaxResumes, jobCopy.ConversationID, reason, unmetGates)
	jobCopy.Prompt = continuationDirective + jobCopy.Prompt

	// Sanitize workspace and release orphan locks before spawning resume
	if jobCopy.WorktreePath != "" {
		_, _ = reaper.ReapWorkspaceOrphansAndLocks(context.Background(), jobCopy.WorktreePath, jobCopy.ConversationID)
		_, _ = workspace.SanitizeWorkspace(context.Background(), jobCopy.WorktreePath)
	}

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

func (w *Watchdog) appendOrphanToolLog(job TrackedJob) {
	if w.onOrphanTool != nil {
		w.onOrphanTool(job)
	}
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

	msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.orphan_tool_detected] Harness process (PID %d) terminated, but AI tool process (ToolPID %d, engine: %s) is STILL RUNNING for conversation %s in %s. Attaching new supervisor...\n",
		job.PID, job.ToolPID, job.Engine, job.ConversationID, job.WorktreePath)
	_, _ = f.WriteString(msg)
}

func (w *Watchdog) appendResumeLog(job TrackedJob) {
	if w.onAutoResume != nil {
		w.onAutoResume(job, job.LastError, job.ResumeCount, w.maxResumes)
	}
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

	prevErr := job.LastError
	if prevErr == "" {
		prevErr = "none"
	}
	cid := job.ConversationID
	if cid == "" {
		cid = "none"
	}
	msg := fmt.Sprintf("\n================================================================================\n"+
		"[iazio] Starting Attempt #%d (trigger_reason: watchdog_auto_resume, previous_exit_code: %d, previous_error: %s, conversation_id: %s, timestamp: %s)\n"+
		"================================================================================\n"+
		"[iazio-watchdog] Detected interruption/stall of job %s (PID %d, exit code %d). Autonomously resuming execution in-place (attempt %d/%d) with conversation %s...\n",
		job.ResumeCount+1, job.ExitCode, prevErr, cid, time.Now().UTC().Format(time.RFC3339),
		job.JobID, job.PID, job.ExitCode, job.ResumeCount+1, w.maxResumes, job.ConversationID)
	_, _ = f.WriteString(msg)
}

// CheckInterrupted inspects all tracked jobs and restarts any dead or stranded jobs.
func (w *Watchdog) CheckInterrupted(ctx context.Context) []TrackedJob {
	if ctx != nil && ctx.Err() != nil {
		return nil
	}
	type resumeReq struct {
		jobID  string
		reason string
	}
	w.mu.Lock()
	var toResume []resumeReq
	now := w.now().UTC()

	for id, job := range w.jobs {
		if job.Status == StatusCompleted || job.Status == StatusFailed || job.Status == StatusCircuitBreakerTripped {
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

		// Liveness and stall detection for active processes
		if job.PID > 0 && w.isAlive(job.PID) && w.monitor != nil {
			stallSec := job.StallTimeoutSec
			if stallSec <= 0 {
				stallSec = int(w.monitor.StallTimeout.Seconds())
			}
			mon := *w.monitor
			mon.StallTimeout = time.Duration(stallSec) * time.Second

			snap := mon.CheckLiveness(job.WorktreePath, job.ConversationID, job.PID, job.StartedAt)
			job.IdleSeconds = snap.IdleDurationSeconds
			job.LastActivityAt = snap.LastActivityTime

			if snap.IsStalled {
				// Capture diagnostics and gracefully terminate hung process
				if w.killProcess != nil {
					_ = w.killProcess(job.PID)
				}
				job.Status = StatusAutoResuming
				job.LastError = fmt.Sprintf("conversation_stalled: no activity for %.0fs (exceeded %ds stall timeout)", snap.IdleDurationSeconds, stallSec)
				job.UpdatedAt = now
				toResume = append(toResume, resumeReq{jobID: id, reason: "conversation_stalled"})
				continue
			}
		}

		if job.PID > 0 && !w.isAlive(job.PID) {
			job.Status = StatusTerminatedUnexpectedly
			job.UpdatedAt = now

			// Check if AI tool (ToolPID) is still running while harness died
			if job.ToolPID <= 0 && job.WorktreePath != "" {
				if lockInfo, err := repolock.Read(job.WorktreePath); err == nil && lockInfo.ToolPID > 0 {
					job.ToolPID = lockInfo.ToolPID
				}
			}
			if job.ToolPID > 0 && w.isAlive(job.ToolPID) {
				w.appendOrphanToolLog(*job)
				toResume = append(toResume, resumeReq{jobID: id, reason: "attach_running_tool"})
			} else {
				toResume = append(toResume, resumeReq{jobID: id, reason: "unexpected_termination"})
			}
		}
	}
	w.persistStateLocked()
	w.mu.Unlock()

	for _, req := range toResume {
		w.attemptResumeWithReason(req.jobID, req.reason, nil)
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
	w.guardWG.Add(1)
	go func() {
		defer w.guardWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				_ = w.CheckInterrupted(ctx)
			}
		}
	}()
}

// WaitWatchguard blocks until all active watchguard background goroutines have terminated.
func (w *Watchdog) WaitWatchguard() {
	w.guardWG.Wait()
}

// GetJob returns a copy of a tracked job.
func (w *Watchdog) GetJob(jobID string) (TrackedJob, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	j, ok := w.jobs[jobID]
	if !ok {
		return TrackedJob{}, errors.New("job not found")
	}
	return *j, nil
}

// MarkCompleted marks a job as cleanly completed and saves state.
func (w *Watchdog) MarkCompleted(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if j, ok := w.jobs[jobID]; ok {
		j.Status = StatusCompleted
		j.UpdatedAt = w.now().UTC()
		w.persistStateLocked()
	}
}

// ActiveJobs returns a list of all currently active or interrupted jobs.
func (w *Watchdog) ActiveJobs() []TrackedJob {
	w.mu.Lock()
	defer w.mu.Unlock()
	var list []TrackedJob
	for _, j := range w.jobs {
		if j.Status == StatusRunning || j.Status == StatusResumed || j.Status == StatusTerminatedUnexpectedly || j.Status == StatusAutoResuming {
			list = append(list, *j)
		}
	}
	return list
}

// IsTracked reports whether a job is currently tracked and in-flight.
func (w *Watchdog) IsTracked(jobID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	j, ok := w.jobs[jobID]
	return ok && j != nil && j.Status != StatusCompleted && j.Status != StatusFailed
}

// NotifyWatchdog pings systemd sd_notify WATCHDOG=1 if NOTIFY_SOCKET is set.
func NotifyWatchdog() error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return nil
	}
	conn, err := net.Dial("unixgram", sock)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte("WATCHDOG=1\n"))
	return err
}

// StartSystemdWatchdog sends regular WATCHDOG=1 keepalives to systemd --user service manager.
func StartSystemdWatchdog(ctx context.Context) {
	usec := os.Getenv("WATCHDOG_USEC")
	if usec == "" {
		return
	}
	val, err := strconv.ParseInt(usec, 10, 64)
	if err != nil || val <= 0 {
		return
	}
	interval := time.Duration(val/2) * time.Microsecond
	if interval <= 0 {
		interval = 5 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
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
