package supervisor

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunState_SaveLoadList(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("IAZIO_RUNS_DIR", tmpDir)

	st1 := RunState{
		JobID:        "job-test-1",
		PID:          1234,
		WorktreePath: "/test/worktree/1",
		Status:       RunStatusRunning,
		StartedAt:    time.Now().UTC().Truncate(time.Second),
		UpdatedAt:    time.Now().UTC().Truncate(time.Second),
	}

	if err := SaveRunState(st1); err != nil {
		t.Fatalf("SaveRunState failed: %v", err)
	}

	loaded, err := LoadRunState("job-test-1")
	if err != nil {
		t.Fatalf("LoadRunState failed: %v", err)
	}
	if loaded.JobID != st1.JobID || loaded.PID != st1.PID || loaded.Status != st1.Status {
		t.Fatalf("loaded state mismatch: got %+v, want %+v", loaded, st1)
	}

	st2 := RunState{
		JobID:        "job-test-2",
		PID:          5678,
		WorktreePath: "/test/worktree/2",
		Status:       RunStatusCompleted,
		ExitCode:     0,
		StartedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	if err := SaveRunState(st2); err != nil {
		t.Fatalf("SaveRunState 2 failed: %v", err)
	}

	list, err := ListRunStates()
	if err != nil {
		t.Fatalf("ListRunStates failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 states, got %d", len(list))
	}
}

func TestOSRunner_StartWithRunDirAndOutputLog(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("IAZIO_RUNS_DIR", tmpDir)

	r := NewOSRunner()
	jobID := "job-out-1"

	// Spawn a quick echo process with --job-id
	pid, err := r.Start([]string{"echo", "--job-id", jobID, "hello world"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if pid <= 0 {
		t.Fatalf("expected positive pid, got %d", pid)
	}

	code, err := r.Wait(pid)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}

	// Verify output.log exists in run directory
	logFile := filepath.Join(tmpDir, jobID, "output.log")
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed reading output.log: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected non-empty output.log")
	}
}

func TestSup_ScanAndReattachRuns(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("IAZIO_RUNS_DIR", tmpDir)

	sup := New(time.Now)

	liveJobID := "job-reattach-live"
	liveWorktree := "/test/repo/live"

	// Start a real harness process that will stay alive during reattach test
	cmd := exec.Command("sh", "-c", "sleep 10", "iazio-harness-"+liveJobID)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed starting sleep process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	livePID := cmd.Process.Pid

	// Write running state for live process
	_ = SaveRunState(RunState{
		JobID:        liveJobID,
		PID:          livePID,
		WorktreePath: liveWorktree,
		Status:       RunStatusRunning,
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	})

	// Write running state for dead process
	deadJobID := "job-reattach-dead"
	deadWorktree := "/test/repo/dead"
	_ = SaveRunState(RunState{
		JobID:        deadJobID,
		PID:          99999999, // non-existent PID
		WorktreePath: deadWorktree,
		Status:       RunStatusRunning,
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	})

	runner := NewOSRunner()
	reattached := sup.ScanAndReattachRuns(runner)

	if len(reattached) != 1 {
		t.Fatalf("expected 1 reattached run, got %d", len(reattached))
	}
	if reattached[0].JobID != liveJobID {
		t.Fatalf("expected reattached job to be %s, got %s", liveJobID, reattached[0].JobID)
	}

	// Verify supervisor lock table has reattached live job
	if sup.Lock(liveWorktree) != LockRunning {
		t.Fatalf("expected live worktree lock to be RUNNING, got %s", sup.Lock(liveWorktree))
	}
	if sup.PID(liveWorktree) != livePID {
		t.Fatalf("expected live worktree PID %d, got %d", livePID, sup.PID(liveWorktree))
	}

	// Verify dead job was reconciled to FAILED (since exit status was unknown) and cooling off
	deadState, err := LoadRunState(deadJobID)
	if err != nil {
		t.Fatalf("failed loading dead run state: %v", err)
	}
	if deadState.Status != RunStatusFailed {
		t.Fatalf("expected dead job status to be FAILED, got %s", deadState.Status)
	}
	if deadState.Error == "" {
		t.Fatalf("expected error message for dead job")
	}
	if sup.Lock(deadWorktree) != LockCooling {
		t.Fatalf("expected dead worktree lock to be COOLING_OFF, got %s", sup.Lock(deadWorktree))
	}
}

func TestSup_ScanAndReattachRuns_PIDRecycleFenced(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("IAZIO_RUNS_DIR", tmpDir)

	sup := New(time.Now)

	// Start an unrelated process (sleep 10) whose command line has no harness/job affinity
	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed starting process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	recycledPID := cmd.Process.Pid
	recycledJobID := "job-recycled-unrelated"
	recycledWorktree := "/test/repo/recycled"

	_ = SaveRunState(RunState{
		JobID:        recycledJobID,
		PID:          recycledPID,
		WorktreePath: recycledWorktree,
		Status:       RunStatusRunning,
		StartedAt:    time.Now().Add(-1 * time.Hour),
		UpdatedAt:    time.Now().Add(-1 * time.Hour),
	})

	runner := NewOSRunner()
	reattached := sup.ScanAndReattachRuns(runner)

	if len(reattached) != 0 {
		t.Fatalf("expected 0 reattached runs due to PID recycle identity fence, got %d", len(reattached))
	}

	st, err := LoadRunState(recycledJobID)
	if err != nil {
		t.Fatalf("failed loading run state: %v", err)
	}
	if st.Status != RunStatusFailed {
		t.Fatalf("expected recycled job status to be FAILED, got %s", st.Status)
	}
	if !strings.Contains(st.Error, "recycled") {
		t.Fatalf("expected error mentioning recycled, got %q", st.Error)
	}
}
