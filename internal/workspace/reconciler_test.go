package workspace

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSanitizeWorkspaceLocksAndInterruptedState(t *testing.T) {
	tempDir := t.TempDir()

	// Initialize a temporary git repository
	cmd := exec.Command("git", "init", tempDir)
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git init: %v", err)
	}

	gitDir := filepath.Join(tempDir, ".git")

	// 1. Create a stale index.lock and HEAD.lock
	indexLock := filepath.Join(gitDir, "index.lock")
	if err := os.WriteFile(indexLock, []byte("lock"), 0644); err != nil {
		t.Fatalf("failed to create index.lock: %v", err)
	}
	headLock := filepath.Join(gitDir, "HEAD.lock")
	if err := os.WriteFile(headLock, []byte("lock"), 0644); err != nil {
		t.Fatalf("failed to create HEAD.lock: %v", err)
	}

	// 2. Create a submodule lock in modules/sub1/index.lock
	subLockDir := filepath.Join(gitDir, "modules", "sub1")
	if err := os.MkdirAll(subLockDir, 0755); err != nil {
		t.Fatalf("failed to create modules dir: %v", err)
	}
	subLock := filepath.Join(subLockDir, "index.lock")
	if err := os.WriteFile(subLock, []byte("lock"), 0644); err != nil {
		t.Fatalf("failed to create sub lock: %v", err)
	}

	// 3. Create interrupted rebase state
	rebaseDir := filepath.Join(gitDir, "rebase-merge")
	if err := os.MkdirAll(rebaseDir, 0755); err != nil {
		t.Fatalf("failed to create rebase-merge dir: %v", err)
	}

	// 4. Create an uncommitted file to verify work preservation
	uncommittedFile := filepath.Join(tempDir, "feature.go")
	if err := os.WriteFile(uncommittedFile, []byte("package main\n"), 0644); err != nil {
		t.Fatalf("failed to create uncommitted file: %v", err)
	}

	res, err := SanitizeWorkspace(context.Background(), tempDir)
	if err != nil {
		t.Fatalf("SanitizeWorkspace failed: %v", err)
	}

	if res.StaleLocksRemoved < 3 {
		t.Fatalf("expected at least 3 stale locks removed, got %d (paths: %v)", res.StaleLocksRemoved, res.RemovedLockPaths)
	}

	if _, err := os.Stat(indexLock); !os.IsNotExist(err) {
		t.Errorf("expected index.lock to be removed")
	}
	if _, err := os.Stat(headLock); !os.IsNotExist(err) {
		t.Errorf("expected HEAD.lock to be removed")
	}
	if _, err := os.Stat(subLock); !os.IsNotExist(err) {
		t.Errorf("expected sub lock to be removed")
	}

	if res.InterruptedGitOperation != "rebase-merge" {
		t.Errorf("expected interrupted git operation 'rebase-merge', got %q", res.InterruptedGitOperation)
	}

	// Verify uncommitted work was preserved
	if _, err := os.Stat(uncommittedFile); err != nil {
		t.Fatalf("expected uncommitted file to be preserved, but it was lost: %v", err)
	}

	if res.Manifest.PreservedFilesCount < 1 {
		t.Errorf("expected at least 1 preserved file in manifest, got %d", res.Manifest.PreservedFilesCount)
	}
	if len(res.Manifest.UntrackedFiles) < 1 {
		t.Errorf("expected untracked files to contain feature.go, got: %v", res.Manifest.UntrackedFiles)
	}
}

func TestSanitizeWorkspace_HeldLock_SetsCleanLocksFalse(t *testing.T) {
	tempDir := t.TempDir()

	cmd := exec.Command("git", "init", tempDir)
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to git init: %v", err)
	}

	gitDir := filepath.Join(tempDir, ".git")
	indexLock := filepath.Join(gitDir, "index.lock")
	f, err := os.OpenFile(indexLock, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open index.lock: %v", err)
	}
	defer f.Close()

	if !isLockFileHeld(indexLock) {
		t.Skip("environment cannot detect open file descriptor via /proc or lsof")
	}

	res, err := SanitizeWorkspace(context.Background(), tempDir)
	if err != nil {
		t.Fatalf("SanitizeWorkspace failed: %v", err)
	}

	if _, statErr := os.Stat(indexLock); statErr != nil {
		t.Fatalf("expected held index.lock to be preserved, but got stat error: %v", statErr)
	}

	if res.Manifest.CleanLocks {
		t.Fatalf("expected Manifest.CleanLocks to be false when index.lock is held and present on disk")
	}
}
