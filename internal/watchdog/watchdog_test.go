package watchdog

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	time.Sleep(50 * time.Millisecond)

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
	time.Sleep(50 * time.Millisecond)

	got, err := w.GetJob("job-103")
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
