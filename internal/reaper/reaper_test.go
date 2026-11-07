//go:build !windows

package reaper

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestTerminateProcessGroup(t *testing.T) {
	cmd := exec.Command("sleep", "10")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep command: %v", err)
	}
	pid := cmd.Process.Pid

	if !isPIDAlive(pid) {
		t.Fatalf("expected pid %d to be alive", pid)
	}

	err := TerminateProcessGroup(pid, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("TerminateProcessGroup failed: %v", err)
	}

	if isPIDAlive(pid) {
		t.Fatalf("expected pid %d to be terminated", pid)
	}
}

func TestReapWorkspaceOrphansAndLocks(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	convID := "test-conv-aad902c8"
	brainDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID)
	if err := os.MkdirAll(brainDir, 0755); err != nil {
		t.Fatalf("failed to create brain dir: %v", err)
	}
	lockFile := filepath.Join(brainDir, "LOCK")
	if err := os.WriteFile(lockFile, []byte("exclusive-lock"), 0644); err != nil {
		t.Fatalf("failed to write lock file: %v", err)
	}

	convDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "conversations")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("failed to create conv dir: %v", err)
	}
	dbLock := filepath.Join(convDir, convID+".lock")
	if err := os.WriteFile(dbLock, []byte("db-lock"), 0644); err != nil {
		t.Fatalf("failed to write db lock: %v", err)
	}

	res, err := ReapWorkspaceOrphansAndLocks(context.Background(), "/tmp/non-existent-worktree", convID)
	if err != nil {
		t.Fatalf("ReapWorkspaceOrphansAndLocks failed: %v", err)
	}

	if res.LocksReleased < 2 {
		t.Fatalf("expected at least 2 locks released, got %d (paths: %v)", res.LocksReleased, res.ReleasedPaths)
	}

	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatalf("expected lock file %s to be removed", lockFile)
	}
	if _, err := os.Stat(dbLock); !os.IsNotExist(err) {
		t.Fatalf("expected db lock file %s to be removed", dbLock)
	}
}

func TestFindOrphanPIDs(t *testing.T) {
	tempWorkspace := t.TempDir()
	cmd := exec.Command("sleep", "10")
	cmd.Dir = tempWorkspace
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep command: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	pids, err := findOrphanPIDs(tempWorkspace, "")
	if err != nil {
		t.Fatalf("findOrphanPIDs failed: %v", err)
	}
	found := false
	for _, p := range pids {
		if p == cmd.Process.Pid {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected sleep process %d (cwd %s) to be discovered by findOrphanPIDs, found: %v", cmd.Process.Pid, tempWorkspace, pids)
	}
}

func TestReleaseConversationSessionLocks_PreservesHeldLock(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	convID := "test-conv-held-1234"
	brainDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID)
	if err := os.MkdirAll(brainDir, 0755); err != nil {
		t.Fatalf("failed to create brain dir: %v", err)
	}
	lockFile := filepath.Join(brainDir, "LOCK")
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open lock file: %v", err)
	}
	defer f.Close()

	// An active file descriptor is open by this process, so isSessionLockHeld must be true
	if !isSessionLockHeld(lockFile) {
		t.Skip("environment cannot detect open file descriptor via /proc or lsof")
	}

	released, err := releaseConversationSessionLocks(convID)
	if err == nil {
		t.Fatalf("expected error for held session lock, got nil")
	}
	if len(released) != 0 {
		t.Fatalf("expected 0 locks released, got %d", len(released))
	}
	if _, statErr := os.Stat(lockFile); statErr != nil {
		t.Fatalf("expected held lock file %s to be preserved, but got stat error: %v", lockFile, statErr)
	}

	// Test ReapWorkspaceOrphansAndLocks surfaces this error
	res, reapErr := ReapWorkspaceOrphansAndLocks(context.Background(), "", convID)
	if reapErr == nil {
		t.Fatalf("expected ReapWorkspaceOrphansAndLocks to surface held lock error, got nil")
	}
	if res.LocksReleased != 0 {
		t.Fatalf("expected 0 locks released in reap result, got %d", res.LocksReleased)
	}
}

func TestTerminateProcessGroup_InvalidPID(t *testing.T) {
	if err := TerminateProcessGroup(0, time.Second); err != nil {
		t.Errorf("expected nil for pid 0, got %v", err)
	}
	if err := TerminateProcessGroup(-1, time.Second); err != nil {
		t.Errorf("expected nil for pid -1, got %v", err)
	}
}

func TestTerminateProcessGroup_LiveChild(t *testing.T) {
	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	err := TerminateProcessGroup(pid, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("expected clean termination, got %v", err)
	}
	_ = cmd.Wait()
}

func TestTerminateProcessGroup_SigkillEscalationAndDeadPID(t *testing.T) {
	if isPIDAlive(0) || isPIDAlive(-5) || isPIDAlive(99999999) {
		t.Fatal("expected non-positive or non-existent PID to report not alive")
	}

	// Start a shell process that traps SIGTERM and ignores it so TerminateProcessGroup escalates to SIGKILL
	cmd := exec.Command("sh", "-c", "trap '' TERM; sleep 30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sh: %v", err)
	}
	pid := cmd.Process.Pid
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	// Give the subshell a brief moment to install the trap
	time.Sleep(50 * time.Millisecond)

	if err := TerminateProcessGroup(pid, 150*time.Millisecond); err != nil {
		t.Fatalf("expected SIGKILL escalation to terminate process %d, got: %v", pid, err)
	}
	_ = cmd.Wait()
}

func TestReapWorkspaceOrphansAndLocks_EmptyAndProcessReap(t *testing.T) {
	ctx := context.Background()
	res, err := ReapWorkspaceOrphansAndLocks(ctx, "   ", "   ")
	if err != nil || res == nil || res.OrphansReaped != 0 || res.LocksReleased != 0 {
		t.Fatalf("expected no-op for empty workspace and conversationID, got res=%+v err=%v", res, err)
	}

	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	convID := "conv-reap-full-999"

	// Create locks in .iazio/conversations and .gemini/brain/<convID>
	iazioConvDir := filepath.Join(tempHome, ".iazio", "conversations")
	if err := os.MkdirAll(iazioConvDir, 0o755); err != nil {
		t.Fatal(err)
	}
	iazioLock := filepath.Join(iazioConvDir, convID+"-extra.lock")
	if err := os.WriteFile(iazioLock, []byte("lock"), 0o644); err != nil {
		t.Fatal(err)
	}

	legacyBrainDir := filepath.Join(tempHome, ".gemini", "brain", convID)
	if err := os.MkdirAll(legacyBrainDir, 0o755); err != nil {
		t.Fatal(err)
	}
	legacyLock := filepath.Join(legacyBrainDir, ".lock")
	if err := os.WriteFile(legacyLock, []byte("lock"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Spawn an orphan process in tempWorkspace whose command line also mentions convID
	tempWorkspace := t.TempDir()
	cmd := exec.Command("sh", "-c", "sleep 10 # "+convID)
	cmd.Dir = tempWorkspace
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sh: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	reapRes, reapErr := ReapWorkspaceOrphansAndLocks(ctx, tempWorkspace, convID)
	if reapErr != nil {
		t.Fatalf("ReapWorkspaceOrphansAndLocks failed: %v", reapErr)
	}
	if reapRes.OrphansReaped < 1 {
		t.Fatalf("expected at least 1 orphan process reaped, got %+v", reapRes)
	}
	if reapRes.LocksReleased < 2 {
		t.Fatalf("expected at least 2 locks released, got %+v", reapRes)
	}
	_ = cmd.Wait()
}

