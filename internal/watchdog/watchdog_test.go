package watchdog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/repolock"
)

type mockRunner struct {
	mu       sync.Mutex
	waitChan map[int]chan struct {
		exit int
		err  error
	}
}

func newMockRunner() *mockRunner {
	return &mockRunner{
		waitChan: make(map[int]chan struct {
			exit int
			err  error
		}),
	}
}

func (m *mockRunner) Start(argv []string) (int, error) {
	return 1000, nil
}

func (m *mockRunner) Signal(pid int, sig string) error {
	return nil
}

func (m *mockRunner) getChan(pid int) chan struct {
	exit int
	err  error
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.waitChan[pid]
	if !ok {
		ch = make(chan struct {
			exit int
			err  error
		}, 1)
		m.waitChan[pid] = ch
	}
	return ch
}

func (m *mockRunner) Wait(pid int) (int, error) {
	ch := m.getChan(pid)
	res := <-ch
	return res.exit, res.err
}

func (m *mockRunner) complete(pid, exit int, err error) {
	ch := m.getChan(pid)
	ch <- struct {
		exit int
		err  error
	}{exit: exit, err: err}
}

func TestWatchdog_CleanCompletion(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog.json")
	runner := newMockRunner()

	w := New(Options{
		StateFile: stateFile,
		Runner:    runner,
	})

	job := TrackedJob{
		JobID:        "job-101",
		WorktreePath: "/tmp/test",
		PID:          1001,
		Engine:       "agy",
		Prompt:       "hello",
	}

	w.Track(job)

	// Verify tracked
	got, err := w.GetJob("job-101")
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("expected running job, got %+v, err: %v", got, err)
	}

	// Process finishes cleanly
	runner.complete(1001, 0, nil)
	time.Sleep(50 * time.Millisecond)

	got, err = w.GetJob("job-101")
	if err != nil || got.Status != StatusCompleted {
		t.Fatalf("expected completed job, got %+v, err: %v", got, err)
	}
}

func TestWatchdog_UnexpectedTerminationAutoResume(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog.json")
	runner := newMockRunner()

	var resumeSpawns atomic.Int32
	spawner := func(ctx context.Context, job TrackedJob) (int, error) {
		resumeSpawns.Add(1)
		return 2001, nil
	}

	w := New(Options{
		StateFile: stateFile,
		Runner:    runner,
		Spawner:   spawner,
	})

	job := TrackedJob{
		JobID:          "job-102",
		WorktreePath:   "/tmp/test",
		PID:            1002,
		Engine:         "agy",
		Prompt:         "vigo review",
		ConversationID: "conv-1234",
	}

	w.Track(job)

	// Process crashes with exit 99 (like binary update or crash)
	runner.complete(1002, 99, errors.New("child killed"))
	for i := 0; i < 20 && resumeSpawns.Load() == 0; i++ {
		time.Sleep(25 * time.Millisecond)
	}

	got, err := w.GetJob("job-102")
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if got.ResumeCount != 1 {
		t.Fatalf("expected resume count 1, got %d", got.ResumeCount)
	}
	if resumeSpawns.Load() != 1 {
		t.Fatalf("expected 1 resume spawn call, got %d", resumeSpawns.Load())
	}
	if got.PID != 2001 {
		t.Fatalf("expected resumed PID 2001, got %d", got.PID)
	}
}

func TestWatchdog_MaxResumesExceeded(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog.json")
	runner := newMockRunner()

	spawner := func(ctx context.Context, job TrackedJob) (int, error) {
		return 3000 + job.ResumeCount, nil
	}

	w := New(Options{
		StateFile:  stateFile,
		Runner:     runner,
		Spawner:    spawner,
		MaxResumes: 2,
	})

	job := TrackedJob{
		JobID:        "job-103",
		WorktreePath: "/tmp/test",
		PID:          1003,
		Engine:       "agy",
	}

	w.Track(job)

	// Fail 1 -> Resumed (count 1, pid 3001)
	runner.complete(1003, 1, nil)
	time.Sleep(50 * time.Millisecond)

	// Fail 2 -> Resumed (count 2, pid 3002)
	runner.complete(3001, 1, nil)
	time.Sleep(50 * time.Millisecond)

	// Fail 3 -> Exceeds max resumes (2) -> FAILED
	runner.complete(3002, 1, nil)
	var got TrackedJob
	var err error
	for i := 0; i < 20; i++ {
		time.Sleep(25 * time.Millisecond)
		got, err = w.GetJob("job-103")
		if err == nil && got.Status == StatusFailed {
			break
		}
	}
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if got.Status != StatusFailed {
		t.Fatalf("expected status FAILED, got %s", got.Status)
	}
	if got.ResumeCount != 2 {
		t.Fatalf("expected resume count 2, got %d", got.ResumeCount)
	}
}

func TestWatchdog_DeadPIDDetection(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog.json")

	var alivePID atomic.Int32
	alivePID.Store(9999)

	var resumeCalled atomic.Bool
	spawner := func(ctx context.Context, job TrackedJob) (int, error) {
		resumeCalled.Store(true)
		return 1111, nil
	}

	w := New(Options{
		StateFile: stateFile,
		Spawner:   spawner,
		IsAlive: func(pid int) bool {
			return pid == int(alivePID.Load())
		},
	})

	job := TrackedJob{
		JobID:        "job-104",
		WorktreePath: "/tmp/test",
		PID:          9999,
		Status:       StatusRunning,
	}

	w.Track(job)

	// Process abruptly dies (e.g. killed by binary rebuild / system update)
	alivePID.Store(0)

	// Watchguard periodic check
	w.CheckInterrupted(context.Background())

	if !resumeCalled.Load() {
		t.Fatalf("expected dead PID to trigger watchdog resume")
	}

	got, _ := w.GetJob("job-104")
	if got.ResumeCount != 1 || got.PID != 1111 {
		t.Fatalf("expected job to be resumed with PID 1111, got: %+v", got)
	}
}

func TestWatchdog_StatePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog.json")

	w1 := New(Options{
		StateFile: stateFile,
	})

	w1.Track(TrackedJob{
		JobID:          "job-persist",
		WorktreePath:   "/tmp/persist",
		ConversationID: "conv-persist-1",
		PID:            4321,
	})

	// Recreate Watchdog from disk (simulating agent restart or binary update)
	w2 := New(Options{
		StateFile: stateFile,
	})

	got, err := w2.GetJob("job-persist")
	if err != nil {
		t.Fatalf("failed to recover persisted job: %v", err)
	}
	if got.ConversationID != "conv-persist-1" || got.PID != 4321 {
		t.Fatalf("persisted job fields mismatch: %+v", got)
	}
}

func TestWatchdog_AdoptsConversationFromRepoLock(t *testing.T) {
	wt := t.TempDir()
	info := repolock.LockInfo{
		JobID:          "job-recover",
		PID:            9999,
		ConversationID: "conv-from-repolock-456",
		StoryID:        "STORY-RECOVER-1",
		Status:         "RUNNING",
	}
	handle, err := repolock.Acquire(wt, info, func(pid int) bool { return pid == 9999 })
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer handle.Release()

	var spawnedWithConv string
	w := New(Options{
		StateFile: filepath.Join(t.TempDir(), "watchdog.json"),
		IsAlive:   func(pid int) bool { return false }, // simulate harness crashed
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnedWithConv = job.ConversationID
			return 1234, nil
		},
	})

	w.Track(TrackedJob{
		JobID:          "job-recover",
		WorktreePath:   wt,
		ConversationID: "", // Initially empty!
		PID:            9999,
	})

	// CheckInterrupted runs
	res := w.CheckInterrupted(context.Background())
	if len(res) == 0 {
		t.Fatal("expected interrupted job to be detected")
	}

	if spawnedWithConv != "conv-from-repolock-456" {
		t.Fatalf("expected spawner to receive adopted conversation 'conv-from-repolock-456', got: %q", spawnedWithConv)
	}
	got, _ := w.GetJob("job-recover")
	if got.ConversationID != "conv-from-repolock-456" {
		t.Fatalf("expected tracked job conversation updated to 'conv-from-repolock-456', got: %q", got.ConversationID)
	}
}

func TestWatchdog_HelperMethods(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "watchdog-helpers.json")
	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return true },
	})

	w.Track(TrackedJob{
		JobID:        "job-active-1",
		WorktreePath: "/tmp/wt1",
		PID:          1001,
	})
	w.Track(TrackedJob{
		JobID:        "job-active-2",
		WorktreePath: "/tmp/wt2",
		PID:          1002,
	})

	if !w.IsTracked("job-active-1") {
		t.Fatalf("expected job-active-1 to be tracked")
	}

	active := w.ActiveJobs()
	if len(active) != 2 {
		t.Fatalf("expected 2 active jobs, got %d", len(active))
	}

	w.MarkCompleted("job-active-1")
	if w.IsTracked("job-active-1") {
		t.Fatalf("expected completed job-active-1 not to be tracked as active")
	}

	j, err := w.GetJob("job-active-1")
	if err != nil || j.Status != StatusCompleted {
		t.Fatalf("expected job-active-1 to be StatusCompleted, got err=%v, status=%s", err, j.Status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.StartWatchguard(ctx, 10*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	cancel()
}

func TestDefaultIsProcessAlive(t *testing.T) {
	w := New(Options{})
	if !w.isAlive(os.Getpid()) {
		t.Fatalf("expected current pid to be alive")
	}
	if w.isAlive(-1) {
		t.Fatalf("expected pid -1 to be dead")
	}
}

func TestWatchdog_AdoptsConversationFromExecutionCheckpoint(t *testing.T) {
	wt := t.TempDir()
	iazioDir := filepath.Join(wt, ".iazio")
	_ = os.MkdirAll(iazioDir, 0755)
	_ = os.WriteFile(filepath.Join(iazioDir, "execution-checkpoint.json"), []byte(`{"conversation_id":"cp-conv-999"}`), 0644)

	stateFile := filepath.Join(t.TempDir(), "watchdog.json")
	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return false },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			if job.ConversationID != "cp-conv-999" {
				return 0, fmt.Errorf("wrong conversation: %s", job.ConversationID)
			}
			return 7777, nil
		},
	})

	w.Track(TrackedJob{
		JobID:        "job-cp-1",
		WorktreePath: wt,
		PID:          1001,
	})

	_ = w.CheckInterrupted(context.Background())

	j, err := w.GetJob("job-cp-1")
	if err != nil || j.PID != 7777 || j.ConversationID != "cp-conv-999" {
		t.Fatalf("unexpected resumed job: %+v, err=%v", j, err)
	}
}

func TestWatchdog_SpawnerError(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "watchdog-err.json")
	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return false },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			return 0, errors.New("spawn failed")
		},
	})

	w.Track(TrackedJob{
		JobID: "job-err-1",
		PID:   1001,
	})

	_ = w.CheckInterrupted(context.Background())

	j, _ := w.GetJob("job-err-1")
	if !strings.Contains(j.LastError, "spawn failed") {
		t.Fatalf("expected LastError to contain 'spawn failed', got %q", j.LastError)
	}
}

func TestWatchdog_MidFlowCrashAutoResume(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "watchdog-crash.json")
	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	var spawnedJobs []TrackedJob

	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return false },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnedJobs = append(spawnedJobs, job)
			return 5555, nil
		},
	})

	w.Track(TrackedJob{
		JobID:          "job-1311",
		PID:            1311,
		ConversationID: convID,
		Prompt:         "implement feature xyz",
		AutoResumePolicy: AutoResumePolicy{
			Enabled:                true,
			MaxAutoResumes:         5,
			BackoffInitialSec:      0, // immediate in test
			RequireForwardProgress: false,
		},
	})

	// Process crashes with exit code 1
	w.OnProcessExit("job-1311", 1311, 1, errors.New("upstream LLM stream stall"))

	if len(spawnedJobs) != 1 {
		t.Fatalf("expected 1 resumed job, got %d", len(spawnedJobs))
	}
	resumed := spawnedJobs[0]
	if resumed.ConversationID != convID {
		t.Errorf("expected resumed conversation %s, got %s", convID, resumed.ConversationID)
	}
	if !strings.Contains(resumed.Prompt, "[ASE AUTO-RESUME CONTINUATION") {
		t.Errorf("expected prompt to contain continuation directive, got %s", resumed.Prompt)
	}
	if resumed.ResumeCount != 1 {
		t.Errorf("expected resume count 1, got %d", resumed.ResumeCount)
	}

	// Next run completes with exit code 0
	w.OnProcessExit("job-1311", 5555, 0, nil)
	j, _ := w.GetJob("job-1311")
	if j.Status != StatusCompleted {
		t.Errorf("expected job to be completed after second run, got %s", j.Status)
	}
}

func TestWatchdog_SilentHangStallDetection(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog-stall.json")
	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"

	var killedPIDs []int
	var spawnedJobs []TrackedJob
	isAlivePID := true

	now := time.Now().UTC()
	pastTime := now.Add(-400 * time.Second)

	monitor := &ConversationLivenessMonitor{
		StallTimeout: 300 * time.Second,
		Now:          func() time.Time { return now },
		LivenessDir:  filepath.Join(tmpDir, "liveness"),
		BrainDir:     filepath.Join(tmpDir, "brain"),
	}

	w := New(Options{
		StateFile: stateFile,
		IsAlive: func(pid int) bool {
			return isAlivePID
		},
		KillProcess: func(pid int) error {
			killedPIDs = append(killedPIDs, pid)
			isAlivePID = false // process terminated
			return nil
		},
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnedJobs = append(spawnedJobs, job)
			return 6666, nil
		},
		Monitor: monitor,
	})

	w.Track(TrackedJob{
		JobID:          "job-1311",
		PID:            8888,
		ConversationID: convID,
		WorktreePath:   tmpDir,
		StartedAt:      pastTime,
		StallTimeoutSec: 300,
		AutoResumePolicy: AutoResumePolicy{
			Enabled:        true,
			MaxAutoResumes: 5,
		},
	})

	// Run periodic check while process is silently hung
	_ = w.CheckInterrupted(context.Background())

	if len(killedPIDs) != 1 || killedPIDs[0] != 8888 {
		t.Fatalf("expected PID 8888 to be killed, got %v", killedPIDs)
	}
	if len(spawnedJobs) != 1 {
		t.Fatalf("expected 1 auto-resumed job spawned, got %d", len(spawnedJobs))
	}
	if spawnedJobs[0].ConversationID != convID {
		t.Errorf("expected resumed conversation %s, got %s", convID, spawnedJobs[0].ConversationID)
	}
	if !strings.Contains(spawnedJobs[0].Prompt, "conversation_stalled") {
		t.Errorf("expected continuation prompt to reference conversation_stalled, got %s", spawnedJobs[0].Prompt)
	}
}

func TestWatchdog_PrematureExit0GateEnforcement(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog-gate.json")
	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"

	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	_ = os.MkdirAll(storiesDir, 0755)
	storyFile := filepath.Join(storiesDir, "2026-10-25-gov-3684-premature.story.md")
	// Story is still IN_PROGRESS (not DONE)
	_ = os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-3684\nstatus: IN_PROGRESS\n---\n"), 0644)

	var spawnedJobs []TrackedJob
	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return false },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnedJobs = append(spawnedJobs, job)
			return 7777, nil
		},
	})

	w.Track(TrackedJob{
		JobID:          "job-1311",
		PID:            1311,
		ConversationID: convID,
		WorktreePath:   tmpDir,
		StoryID:        "STORY-GOV-3684",
		Prompt:         "finish release",
		AutoResumePolicy: AutoResumePolicy{
			Enabled:                true,
			MaxAutoResumes:         5,
			BackoffInitialSec:      0,
			RequireForwardProgress: false,
		},
	})

	// Process prematurely exits with code 0 before story is completed
	w.OnProcessExit("job-1311", 1311, 0, nil)

	if len(spawnedJobs) != 1 {
		t.Fatalf("expected 1 auto-resumed job due to incomplete gate, got %d", len(spawnedJobs))
	}
	if spawnedJobs[0].ConversationID != convID {
		t.Errorf("expected resumed on conversation %s, got %s", convID, spawnedJobs[0].ConversationID)
	}
	if !strings.Contains(spawnedJobs[0].Prompt, "incomplete_ase_flow") {
		t.Errorf("expected prompt to mention incomplete_ase_flow, got %s", spawnedJobs[0].Prompt)
	}

	// Now mark story as completed and provide test/release stamps and simulate second exit 0
	_ = os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-3684\nstatus: DONE\n---\n"), 0644)
	_ = os.MkdirAll(filepath.Join(tmpDir, "tmp", "reports"), 0755)
	_ = os.WriteFile(filepath.Join(tmpDir, "tmp", "reports", "validate-local-ok.stamp"), []byte("ok"), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "tmp", "reports", "verify-live-release-ok.stamp"), []byte("ok"), 0644)
	w.OnProcessExit("job-1311", 7777, 0, nil)

	j, _ := w.GetJob("job-1311")
	if j.Status != StatusCompleted {
		t.Errorf("expected job to be completed once gate passes, got %s", j.Status)
	}
}

func TestWatchdog_CircuitBreakerOnZeroProgress(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog-circuit.json")
	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	var spawnCount int

	// Initialize git repo in tmpDir so fingerprinting is active
	_ = os.MkdirAll(filepath.Join(tmpDir, ".git"), 0755)

	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return false },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnCount++
			return 1000 + spawnCount, nil
		},
	})

	w.Track(TrackedJob{
		JobID:          "job-loop",
		PID:            1000,
		ConversationID: convID,
		WorktreePath:   tmpDir,
		AutoResumePolicy: AutoResumePolicy{
			Enabled:                true,
			MaxAutoResumes:         5,
			BackoffInitialSec:      0,
			RequireForwardProgress: true, // Circuit breaker active!
		},
	})

	// Attempt 1: fails
	w.OnProcessExit("job-loop", 1000, 1, errors.New("repeatable error"))
	if spawnCount != 1 {
		t.Fatalf("expected 1 spawn, got %d", spawnCount)
	}

	// Attempt 2: fails again with identical progress fingerprint
	w.OnProcessExit("job-loop", 1001, 1, errors.New("repeatable error"))

	// Verify circuit breaker tripped and no infinite looping
	j, _ := w.GetJob("job-loop")
	if j.Status != StatusFailed {
		t.Errorf("expected job StatusFailed, got %s", j.Status)
	}
	if !j.CircuitBreakerTripped {
		t.Errorf("expected CircuitBreakerTripped=true")
	}
	if !strings.Contains(j.LastError, "CIRCUIT_BREAKER_TRIPPED") {
		t.Errorf("expected LastError to contain CIRCUIT_BREAKER_TRIPPED, got %s", j.LastError)
	}
}

func TestWatchdog_NoDoubleResumeOnStallTermination(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "watchdog-stall-race.json")
	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	var spawnCount int

	w := New(Options{
		StateFile: stateFile,
		IsAlive:   func(pid int) bool { return pid == 1000 },
		Spawner: func(ctx context.Context, job TrackedJob) (int, error) {
			spawnCount++
			return 2000 + spawnCount, nil
		},
		Monitor: &ConversationLivenessMonitor{
			StallTimeout: 10 * time.Millisecond,
		},
		KillProcess: func(pid int) error {
			return nil
		},
	})

	w.Track(TrackedJob{
		JobID:          "job-stall-race",
		PID:            1000,
		ConversationID: convID,
		WorktreePath:   tmpDir,
		StartedAt:      time.Now().Add(-1 * time.Hour), // Way beyond stall timeout
		AutoResumePolicy: AutoResumePolicy{
			Enabled:                true,
			MaxAutoResumes:         5,
			BackoffInitialSec:      0,
			RequireForwardProgress: false,
		},
	})

	// CheckInterrupted detects stall and initiates auto-resume
	w.CheckInterrupted(context.Background())

	if spawnCount != 1 {
		t.Fatalf("expected 1 auto-resume spawn on stall detection, got %d", spawnCount)
	}

	// Now simulate reapProcess waking up from process kill and calling OnProcessExit for the killed PID 1000
	w.OnProcessExit("job-stall-race", 1000, 15, errors.New("signal: terminated"))

	// Ensure NO second resume was triggered and spawn count is STILL 1
	if spawnCount != 1 {
		t.Fatalf("expected spawn count to remain 1 after reapProcess exit, got %d (double resume race condition!)", spawnCount)
	}
}

func TestWatchdog_CoverageHelpers(t *testing.T) {
	// 1. DefaultAutoResumePolicy
	pol := DefaultAutoResumePolicy()
	if !pol.Enabled || pol.MaxAutoResumes != 5 {
		t.Errorf("unexpected default policy: %+v", pol)
	}

	// 2. IsProcessAlive & defaultKillProcess
	_ = IsProcessAlive(os.Getpid())
	_ = IsProcessAlive(99999999)

	// 3. appendOrphanToolLog
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	spoolDir := filepath.Join(tmpDir, ".iazio", "spool")
	_ = os.MkdirAll(spoolDir, 0755)

	var orphanNotified bool
	w := New(Options{
		StateFile: filepath.Join(tmpDir, "watchdog.json"),
		OnOrphanTool: func(j TrackedJob) {
			orphanNotified = true
		},
	})
	job := TrackedJob{
		JobID:          "job-orphan-cov",
		PID:            123,
		ToolPID:        456,
		Engine:         "agy",
		ConversationID: "conv-orphan",
		WorktreePath:   tmpDir,
	}
	w.appendOrphanToolLog(job)
	if !orphanNotified {
		t.Errorf("expected orphan callback called")
	}

	// 4. NotifyWatchdog & StartSystemdWatchdog
	NotifyWatchdog()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartSystemdWatchdog(ctx)
}

