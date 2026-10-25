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
