//go:build windows

package reaper

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTerminateProcessGroup_InvalidPID(t *testing.T) {
	if err := TerminateProcessGroup(0, time.Second); err != nil {
		t.Errorf("expected nil for pid 0, got %v", err)
	}
	if err := TerminateProcessGroup(-1, time.Second); err != nil {
		t.Errorf("expected nil for pid -1, got %v", err)
	}
}

func TestReapWorkspaceOrphansAndLocks(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	t.Setenv("USERPROFILE", tempHome)

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

	res, err := ReapWorkspaceOrphansAndLocks(context.Background(), tempHome, convID)
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
