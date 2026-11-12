package repolock

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAcquireAndRelease(t *testing.T) {
	wt := t.TempDir()
	info := LockInfo{
		JobID:          "job-1",
		PID:            1001,
		Engine:         "agy",
		ConversationID: "conv-1",
		StoryID:        "STORY-1",
		Prompt:         "test prompt",
	}

	handle, err := Acquire(wt, info, func(pid int) bool { return pid == 1001 })
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	readInfo, err := Read(wt)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if readInfo.JobID != "job-1" || readInfo.ConversationID != "conv-1" || readInfo.Status != "RUNNING" {
		t.Fatalf("unexpected read info: %+v", readInfo)
	}
	if handle.Worktree() != wt {
		t.Fatalf("expected wt, got %s", handle.Worktree())
	}
	if handle.Info().JobID != "job-1" {
		t.Fatalf("expected job-1, got %s", handle.Info().JobID)
	}

	// Update conversation ID
	info.ConversationID = "conv-updated"
	if err := handle.Update(info); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	readInfo, _ = Read(wt)
	if readInfo.ConversationID != "conv-updated" {
		t.Fatalf("expected updated conversation conv-updated, got %s", readInfo.ConversationID)
	}

	// Release
	if err := handle.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	readInfo, _ = Read(wt)
	if readInfo.Status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED after release, got %s", readInfo.Status)
	}
}

func TestAcquire_BusyAlivePID(t *testing.T) {
	wt := t.TempDir()
	info1 := LockInfo{
		JobID: "job-1",
		PID:   2001,
	}

	// First job acquires and releases flock, but leaves lock file with alive PID
	handle1, err := Acquire(wt, info1, func(int) bool { return true })
	if err != nil {
		t.Fatalf("Acquire job-1 failed: %v", err)
	}
	// Simulate process kept running after flock was closed/released or in background
	_ = handle1.flockFile.Close()
	handle1.released = true // keep status RUNNING in file

	// Second job tries to acquire while PID 2001 is reported alive
	info2 := LockInfo{
		JobID: "job-2",
		PID:   2002,
	}
	_, err = Acquire(wt, info2, func(pid int) bool { return pid == 2001 })
	if !errors.Is(err, ErrRepoBusy) {
		t.Fatalf("expected ErrRepoBusy when PID 2001 is alive, got: %v", err)
	}
}

func TestAcquire_CrashedPID_AdoptsAndPreservesConversation(t *testing.T) {
	wt := t.TempDir()
	info1 := LockInfo{
		JobID:          "job-1",
		PID:            3001,
		ConversationID: "conv-preserved-123",
		StoryID:        "STORY-RECOVER",
		Prompt:         "resume prompt",
	}

	handle1, err := Acquire(wt, info1, func(int) bool { return true })
	if err != nil {
		t.Fatalf("Acquire job-1 failed: %v", err)
	}
	_ = handle1.flockFile.Close()
	handle1.released = true // simulates crash without calling Release()

	// Second job arrives after crash (PID 3001 is dead)
	info2 := LockInfo{
		JobID:   "job-1-resume",
		PID:     3002,
		StoryID: "STORY-RECOVER",
	}
	handle2, err := Acquire(wt, info2, func(pid int) bool { return false }) // PID is dead
	if err != nil {
		t.Fatalf("Acquire should succeed when prior PID is dead, got: %v", err)
	}
	defer handle2.Release()

	if handle2.Info().ConversationID != "conv-preserved-123" {
		t.Fatalf("expected preserved conversation ID 'conv-preserved-123', got: %q", handle2.Info().ConversationID)
	}
}

func TestAcquire_UnrelatedStoryOrCompleted_DoesNotAdoptConversation(t *testing.T) {
	// Case 1: Generic prompt without story ID -> MUST NOT adopt
	t.Run("StoryLessPrompt", func(t *testing.T) {
		wt := t.TempDir()
		info1 := LockInfo{
			JobID:          "job-1",
			PID:            3001,
			ConversationID: "conv-leak-prevention",
			StoryID:        "STORY-FIRST",
			Status:         "RUNNING",
		}
		h1, err := Acquire(wt, info1, func(int) bool { return true })
		if err != nil {
			t.Fatalf("Acquire job-1 failed: %v", err)
		}
		_ = h1.flockFile.Close()
		h1.released = true // simulate crash

		infoGeneric := LockInfo{
			JobID:   "job-generic",
			PID:     3002,
			StoryID: "",
		}
		handleGen, err := Acquire(wt, infoGeneric, func(int) bool { return false })
		if err != nil {
			t.Fatalf("Acquire generic failed: %v", err)
		}
		defer handleGen.Release()
		if handleGen.Info().ConversationID != "" {
			t.Fatalf("expected empty ConversationID for story-less prompt, got: %q", handleGen.Info().ConversationID)
		}
	})

	// Case 2: Different story ID -> MUST NOT adopt
	t.Run("DifferentStory", func(t *testing.T) {
		wt := t.TempDir()
		info1 := LockInfo{
			JobID:          "job-1",
			PID:            3001,
			ConversationID: "conv-leak-prevention",
			StoryID:        "STORY-FIRST",
			Status:         "RUNNING",
		}
		h1, err := Acquire(wt, info1, func(int) bool { return true })
		if err != nil {
			t.Fatalf("Acquire job-1 failed: %v", err)
		}
		_ = h1.flockFile.Close()
		h1.released = true

		infoDiff := LockInfo{
			JobID:   "job-diff",
			PID:     3003,
			StoryID: "STORY-SECOND",
		}
		handleDiff, err := Acquire(wt, infoDiff, func(int) bool { return false })
		if err != nil {
			t.Fatalf("Acquire different story failed: %v", err)
		}
		defer handleDiff.Release()
		if handleDiff.Info().ConversationID != "" {
			t.Fatalf("expected empty ConversationID for different story, got: %q", handleDiff.Info().ConversationID)
		}
	})

	// Case 3: Prior job was COMPLETED -> MUST NOT adopt even for same story
	t.Run("CompletedPriorJob", func(t *testing.T) {
		wt := t.TempDir()
		infoCompleted := LockInfo{
			JobID:          "job-done",
			PID:            3004,
			ConversationID: "conv-completed",
			StoryID:        "STORY-DONE",
			Status:         "COMPLETED",
		}
		hDone, err := Acquire(wt, infoCompleted, func(int) bool { return false })
		if err != nil {
			t.Fatalf("Acquire completed failed: %v", err)
		}
		_ = hDone.flockFile.Close()
		hDone.released = true

		infoNewSameStory := LockInfo{
			JobID:   "job-new",
			PID:     3005,
			StoryID: "STORY-DONE",
		}
		handleNew, err := Acquire(wt, infoNewSameStory, func(int) bool { return false })
		if err != nil {
			t.Fatalf("Acquire new failed: %v", err)
		}
		defer handleNew.Release()
		if handleNew.Info().ConversationID != "" {
			t.Fatalf("expected empty ConversationID from COMPLETED job, got: %q", handleNew.Info().ConversationID)
		}
	})
}

func TestAcquire_ConcurrentFlockContention(t *testing.T) {
	wt := t.TempDir()
	info1 := LockInfo{
		JobID: "job-concurrent-1",
		PID:   os.Getpid(),
	}

	handle1, err := Acquire(wt, info1, func(int) bool { return true })
	if err != nil {
		t.Fatalf("Acquire handle1 failed: %v", err)
	}
	defer handle1.Release()

	// Concurrent acquire while handle1 holds OS flock
	info2 := LockInfo{
		JobID: "job-concurrent-2",
		PID:   os.Getpid() + 1,
	}
	_, err = Acquire(wt, info2, func(int) bool { return true })
	if !errors.Is(err, ErrRepoBusy) {
		t.Fatalf("expected ErrRepoBusy under OS flock conflict, got: %v", err)
	}
}

func TestForceRelease(t *testing.T) {
	wt := t.TempDir()
	info := LockInfo{
		JobID: "job-force-rel",
		PID:   1234,
	}
	handle, err := Acquire(wt, info, func(int) bool { return true })
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// While OS lock is actively held, ForceRelease must fail closed with ErrRepoBusy
	if err := ForceRelease(wt); !errors.Is(err, ErrRepoBusy) {
		_ = handle.Release()
		t.Fatalf("expected ForceRelease to return ErrRepoBusy while OS lock held, got: %v", err)
	}

	_ = handle.Release()

	if err := ForceRelease(wt); err != nil {
		t.Fatalf("ForceRelease failed after Release: %v", err)
	}

	_, readErr := Read(wt)
	if readErr == nil {
		t.Fatalf("expected Read to fail after ForceRelease")
	}

	// Verify .iazio/repo.lock.flock remains on disk to prevent Unix inode replacement bypass
	if _, statErr := os.Stat(filepath.Join(wt, ".iazio", "repo.lock.flock")); statErr != nil {
		t.Fatalf("expected .iazio/repo.lock.flock to remain on disk after ForceRelease, got: %v", statErr)
	}
}

func TestHelperProcessHoldRepoLock(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_REPO_LOCK") != "1" {
		return
	}
	wt := os.Getenv("GO_HELPER_REPO_LOCK_WT")
	h, err := Acquire(wt, LockInfo{
		JobID: "helper-job",
		PID:   os.Getpid(),
	}, func(int) bool { return true })
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "helper Acquire failed: %v\n", err)
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(os.Stdout, "LOCKED")
	buf := make([]byte, 1)
	_, _ = os.Stdin.Read(buf)
	_ = h.Release()
	os.Exit(0)
}

func TestAcquire_CrossProcessLockContention(t *testing.T) {
	wt := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessHoldRepoLock$")
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_REPO_LOCK=1",
		"GO_HELPER_REPO_LOCK_WT="+wt,
	)
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start helper: %v", err)
	}
	defer func() {
		_ = stdinPipe.Close()
		_ = cmd.Wait()
	}()

	readyBuf := make([]byte, 64)
	n, rErr := stdoutPipe.Read(readyBuf)
	if rErr != nil || !strings.Contains(string(readyBuf[:n]), "LOCKED") {
		t.Fatalf("helper did not signal LOCKED: err=%v out=%q", rErr, string(readyBuf[:n]))
	}

	// Attempt Acquire from parent process with isAlive=false to prove OS-level LockFileEx/flock enforcement
	_, err = Acquire(wt, LockInfo{
		JobID: "parent-contender",
		PID:   os.Getpid(),
	}, func(int) bool { return false })
	if !errors.Is(err, ErrRepoBusy) {
		t.Fatalf("expected ErrRepoBusy from cross-process OS lock contention, got: %v", err)
	}

	// ForceRelease must also refuse while child process holds the OS lock
	if fErr := ForceRelease(wt); !errors.Is(fErr, ErrRepoBusy) {
		t.Fatalf("expected ForceRelease to return ErrRepoBusy while child holds OS lock, got: %v", fErr)
	}

	// Release child process and verify parent can now acquire
	_ = stdinPipe.Close()
	if wErr := cmd.Wait(); wErr != nil {
		t.Fatalf("helper wait failed: %v", wErr)
	}

	parentHandle, err := Acquire(wt, LockInfo{
		JobID: "parent-after-child-release",
		PID:   os.Getpid(),
	}, func(int) bool { return true })
	if err != nil {
		t.Fatalf("expected Acquire to succeed after child released OS lock, got: %v", err)
	}
	_ = parentHandle.Release()
}

func TestTryLockFileAndUnlockFile_DirectOSExclusivity(t *testing.T) {
	wt := t.TempDir()
	lockFile := filepath.Join(wt, "repo.lock.flock")

	f1, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open f1: %v", err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open f2: %v", err)
	}
	defer f2.Close()

	if err := tryLockFile(f1); err != nil {
		t.Fatalf("tryLockFile(f1) failed: %v", err)
	}

	// Second file handle must fail to acquire exclusive lock immediately
	if err := tryLockFile(f2); err == nil {
		_ = unlockFile(f2)
		t.Fatal("expected tryLockFile(f2) to fail while f1 holds exclusive lock")
	}

	// After unlocking f1, f2 must succeed
	if err := unlockFile(f1); err != nil {
		t.Fatalf("unlockFile(f1) failed: %v", err)
	}
	if err := tryLockFile(f2); err != nil {
		t.Fatalf("tryLockFile(f2) should succeed after f1 unlock: %v", err)
	}
	if err := unlockFile(f2); err != nil {
		t.Fatalf("unlockFile(f2) failed: %v", err)
	}
}

func TestAcquire_OSLockConflictWithoutPIDLiveness(t *testing.T) {
	wt := t.TempDir()
	info1 := LockInfo{
		JobID: "job-os-lock-1",
		PID:   os.Getpid(),
	}

	// Even when isAlive returns false, an active OS lock must block concurrent Acquire
	handle1, err := Acquire(wt, info1, func(int) bool { return false })
	if err != nil {
		t.Fatalf("Acquire handle1 failed: %v", err)
	}

	info2 := LockInfo{
		JobID: "job-os-lock-2",
		PID:   os.Getpid() + 1,
	}
	_, err = Acquire(wt, info2, func(int) bool { return false })
	if !errors.Is(err, ErrRepoBusy) || !strings.Contains(err.Error(), "lock conflict on") {
		_ = handle1.Release()
		t.Fatalf("expected OS lock conflict ErrRepoBusy even with isAlive=false, got: %v", err)
	}

	if err := handle1.Release(); err != nil {
		t.Fatalf("Release handle1 failed: %v", err)
	}

	handle2, err := Acquire(wt, info2, func(int) bool { return false })
	if err != nil {
		t.Fatalf("Acquire handle2 after Release failed: %v", err)
	}
	_ = handle2.Release()
}

func TestWriteLockFileAtomic_ConcurrentReadNeverSeesPartialJSON(t *testing.T) {
	wt := t.TempDir()
	initial := LockInfo{
		JobID:          "job-atomic-init",
		PID:            os.Getpid(),
		Engine:         "agy",
		ConversationID: "conv-atomic-0",
		StoryID:        "STORY-IAZIO-0188",
		Prompt:         strings.Repeat("atomic lock payload ", 32),
	}

	handle, err := Acquire(wt, initial, func(int) bool { return true })
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	const writers = 4
	const iterations = 40
	const readers = 6

	done := make(chan struct{})
	var readerWG sync.WaitGroup
	errCh := make(chan error, readers+writers)

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-done:
					return
				default:
					info, rErr := Read(wt)
					if rErr != nil {
						errCh <- fmt.Errorf("concurrent Read observed error/partial JSON: %w", rErr)
						return
					}
					if info.JobID == "" || info.StoryID != "STORY-IAZIO-0188" {
						errCh <- fmt.Errorf("concurrent Read observed unexpected LockInfo: %+v", info)
						return
					}
				}
			}
		}()
	}

	var writerWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(workerID int) {
			defer writerWG.Done()
			for i := 0; i < iterations; i++ {
				upd := initial
				upd.JobID = fmt.Sprintf("job-atomic-%d-%d", workerID, i)
				upd.ConversationID = fmt.Sprintf("conv-%d-%d", workerID, i)
				if uErr := handle.Update(upd); uErr != nil {
					errCh <- fmt.Errorf("handle.Update failed: %w", uErr)
					return
				}
			}
		}(w)
	}

	writerWG.Wait()
	if err := handle.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	close(done)
	readerWG.Wait()
	close(errCh)

	for e := range errCh {
		t.Fatal(e)
	}

	finalInfo, err := Read(wt)
	if err != nil {
		t.Fatalf("final Read failed: %v", err)
	}
	if finalInfo.Status != "COMPLETED" {
		t.Fatalf("expected COMPLETED status after Release, got %q", finalInfo.Status)
	}
	if err := handle.Update(initial); err == nil {
		t.Fatal("expected Update after Release to return error")
	}

	// Verify no leftover .repo.lock.tmp.* files remain in .iazio
	entries, err := os.ReadDir(filepath.Join(wt, ".iazio"))
	if err != nil {
		t.Fatalf("ReadDir .iazio failed: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".repo.lock.tmp.") {
			t.Fatalf("leftover temp lock file found: %s", entry.Name())
		}
	}
}

func TestIsWorktreeDirty_And_EdgeCases(t *testing.T) {
	// 1. Empty worktree paths
	if _, err := Acquire("", LockInfo{JobID: "j"}, nil); err == nil {
		t.Fatal("expected error for empty worktree in Acquire")
	}
	if err := ForceRelease(""); err != nil {
		t.Fatalf("expected nil for empty worktree in ForceRelease, got %v", err)
	}
	if IsWorktreeDirty("") {
		t.Fatal("expected false for empty worktree in IsWorktreeDirty")
	}
	if IsWorktreeDirty(t.TempDir()) {
		t.Fatal("expected false for non-git dir in IsWorktreeDirty")
	}

	// 2. Git repo dirty vs clean check
	repoDir := t.TempDir()
	if err := exec.Command("git", "init", repoDir).Run(); err != nil {
		t.Skip("git not available")
	}
	_ = exec.Command("git", "-C", repoDir, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.name", "Test").Run()

	if IsWorktreeDirty(repoDir) {
		t.Fatal("expected empty git repo to be clean")
	}

	f := filepath.Join(repoDir, "dirty_file.txt")
	if err := os.WriteFile(f, []byte("dirty\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !IsWorktreeDirty(repoDir) {
		t.Fatal("expected repo with untracked file to be dirty")
	}

	_ = exec.Command("git", "-C", repoDir, "add", "dirty_file.txt").Run()
	_ = exec.Command("git", "-C", repoDir, "commit", "-m", "initial").Run()
	if IsWorktreeDirty(repoDir) {
		t.Fatal("expected committed git repo to be clean")
	}

	// 3. Update with zero LockedAt and empty Status preserves/defaults fields
	wt := t.TempDir()
	h, err := Acquire(wt, LockInfo{JobID: "j-edge", PID: os.Getpid()}, nil)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	h.info.Status = ""
	if err := h.Update(LockInfo{JobID: "j-edge-updated", LockedAt: time.Time{}, Status: ""}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if h.Info().Status != "RUNNING" || h.Info().LockedAt.IsZero() {
		t.Fatalf("expected Update to default Status=RUNNING and preserve LockedAt, got %+v", h.Info())
	}
	// Double release is a no-op
	if err := h.Release(); err != nil {
		t.Fatalf("first Release failed: %v", err)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("second Release failed: %v", err)
	}

	// 4. ensureGitInfoExclude on directory .git and gitfile .git
	wtGitDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(wtGitDir, ".git", "info"), 0o755)
	_ = os.WriteFile(filepath.Join(wtGitDir, ".git", "info", "exclude"), []byte("# comment"), 0o644)
	ensureGitInfoExclude(wtGitDir)
	ensureGitInfoExclude(wtGitDir) // idempotent second call
	exclBytes, err := os.ReadFile(filepath.Join(wtGitDir, ".git", "info", "exclude"))
	if err != nil || !strings.Contains(string(exclBytes), ".iazio/") || !strings.Contains(string(exclBytes), ".iazio-repolock") {
		t.Fatalf("expected .iazio/ and .iazio-repolock in exclude, got %q (err=%v)", string(exclBytes), err)
	}

	wtSub := t.TempDir()
	actualGitDir := filepath.Join(wtSub, "real-git-dir")
	_ = os.MkdirAll(actualGitDir, 0o755)
	subWorktree := filepath.Join(wtSub, "sub")
	_ = os.MkdirAll(subWorktree, 0o755)
	_ = os.WriteFile(filepath.Join(subWorktree, ".git"), []byte("gitdir: ../real-git-dir\n"), 0o644)
	ensureGitInfoExclude(subWorktree)
	subExclBytes, err := os.ReadFile(filepath.Join(actualGitDir, "info", "exclude"))
	if err != nil || !strings.Contains(string(subExclBytes), ".iazio/") {
		t.Fatalf("expected .iazio/ in submodule gitdir exclude, got %q (err=%v)", string(subExclBytes), err)
	}
}

func TestIsWorktreeCleanForNewJob_And_UnpushedCommits(t *testing.T) {
	// 1. Non-git or empty directory is considered clean
	if clean, _ := IsWorktreeCleanForNewJob(""); !clean {
		t.Fatal("expected empty path to be clean")
	}
	tmpEmpty := t.TempDir()
	if clean, _ := IsWorktreeCleanForNewJob(tmpEmpty); !clean {
		t.Fatal("expected empty non-git dir to be clean")
	}

	// 2. Active .session_story_id makes it not clean
	_ = os.WriteFile(filepath.Join(tmpEmpty, ".session_story_id"), []byte("STORY-GOV-5232\n"), 0o644)
	if clean, reason := IsWorktreeCleanForNewJob(tmpEmpty); clean || !strings.Contains(reason, "STORY-GOV-5232") {
		t.Fatalf("expected active .session_story_id to block new job, got clean=%v reason=%s", clean, reason)
	}
	_ = os.Remove(filepath.Join(tmpEmpty, ".session_story_id"))

	// 3. Real git repository tests
	repoDir := t.TempDir()
	if err := exec.Command("git", "init", repoDir).Run(); err != nil {
		t.Skip("git not available")
	}
	_ = exec.Command("git", "-C", repoDir, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.name", "Test").Run()

	// Initial commit on main
	_ = exec.Command("git", "-C", repoDir, "checkout", "-b", "main").Run()
	f := filepath.Join(repoDir, "file.txt")
	_ = os.WriteFile(f, []byte("initial\n"), 0o644)
	_ = exec.Command("git", "-C", repoDir, "add", "file.txt").Run()
	_ = exec.Command("git", "-C", repoDir, "commit", "-m", "initial commit").Run()

	// 3a. Fails closed when no remote is configured (finding F3)
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); clean || !strings.Contains(reason, "no git remotes configured") {
		t.Fatalf("expected repo with no remotes to fail closed, got clean=%v reason=%s", clean, reason)
	}

	// Configure remote
	remoteDir := t.TempDir()
	_ = exec.Command("git", "init", "--bare", remoteDir).Run()
	_ = exec.Command("git", "-C", repoDir, "remote", "add", "origin", remoteDir).Run()
	_ = exec.Command("git", "-C", repoDir, "push", "-u", "origin", "main").Run()

	// Clean repository with remote tracking is clean
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); !clean {
		t.Fatalf("expected committed main repo to be clean, got reason=%s", reason)
	}

	// 4. Dirty file blocks
	dirtyFile := filepath.Join(repoDir, "dirty.txt")
	_ = os.WriteFile(dirtyFile, []byte("uncommitted\n"), 0o644)
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); clean || !strings.Contains(reason, "uncommitted") {
		t.Fatalf("expected dirty repo to block, got clean=%v reason=%s", clean, reason)
	}
	_ = os.Remove(dirtyFile)

	// 5. Feature branch blocks
	_ = exec.Command("git", "-C", repoDir, "checkout", "-b", "feat/my-feature").Run()
	if onFeat, branch := IsOnFeatureBranch(repoDir); !onFeat || branch != "feat/my-feature" {
		t.Fatalf("expected IsOnFeatureBranch=true on feat/my-feature, got %v (%s)", onFeat, branch)
	}
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); clean || !strings.Contains(reason, "feat/my-feature") {
		t.Fatalf("expected feature branch to block new job, got clean=%v reason=%s", clean, reason)
	}

	// Switch back to main
	_ = exec.Command("git", "-C", repoDir, "checkout", "main").Run()
	if onFeat, _ := IsOnFeatureBranch(repoDir); onFeat {
		t.Fatal("expected IsOnFeatureBranch=false on main")
	}

	// 6. Upstream ahead blocks
	ahead, err := HasUnpushedCommits(repoDir)
	if err != nil || ahead {
		t.Fatalf("expected 0 unpushed commits after initial push, got ahead=%v err=%v", ahead, err)
	}
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); !clean {
		t.Fatalf("expected clean worktree after push, got reason=%s", reason)
	}

	// Add local unpushed commit
	_ = os.WriteFile(f, []byte("updated\n"), 0o644)
	_ = exec.Command("git", "-C", repoDir, "commit", "-am", "local commit ahead").Run()

	ahead, err = HasUnpushedCommits(repoDir)
	if err != nil || !ahead {
		t.Fatalf("expected HasUnpushedCommits=true when ahead of origin/main, got ahead=%v err=%v", ahead, err)
	}
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); clean || !strings.Contains(reason, "unpushed") {
		t.Fatalf("expected unpushed commit to block new job, got clean=%v reason=%s", clean, reason)
	}

	// 7. Verify isStoryDone YAML frontmatter parsing precision (finding F7) and ADR-002 .autopilot/ase-state.json
	storyDir := filepath.Join(repoDir, "docs", "stories")
	_ = os.MkdirAll(storyDir, 0o755)
	// Story 2 is DONE, and its markdown body mentions STORY-TEST-1
	story2File := filepath.Join(storyDir, "2026-11-09-test-2.story.md")
	_ = os.WriteFile(story2File, []byte("---\nid: STORY-TEST-2\nstatus: DONE\n---\n# Story 2\nMentions STORY-TEST-1 in body\n"), 0o644)
	if isStoryDone(repoDir, "STORY-TEST-1") {
		t.Fatal("expected isStoryDone to return false for STORY-TEST-1 when only mentioned in body of STORY-TEST-2")
	}
	if !isStoryDone(repoDir, "STORY-TEST-2") {
		t.Fatal("expected isStoryDone to return true for STORY-TEST-2")
	}
	// Substring prefix STORY-TEST-20 vs STORY-TEST-2 must NOT false-match
	story20File := filepath.Join(storyDir, "2026-11-09-test-20.story.md")
	_ = os.WriteFile(story20File, []byte("---\nid: STORY-TEST-20\nstatus: DONE\n---\n"), 0o644)
	if isStoryDone(repoDir, "STORY-TEST-200") {
		t.Fatal("expected isStoryDone to return false for substring prefix mismatch")
	}
	// ADR-002: .autopilot/ase-state.json marks story DONE in OSS leaf worktrees without docs/stories/
	ossDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(ossDir, ".autopilot"), 0o755)
	_ = os.WriteFile(filepath.Join(ossDir, ".autopilot", "ase-state.json"), []byte(`{"story_id":"STORY-GOV-5258","status":"DONE"}`), 0o644)
	if !isStoryDone(ossDir, "STORY-GOV-5258") {
		t.Fatal("expected isStoryDone to return true from .autopilot/ase-state.json")
	}
	if isStoryDone(ossDir, "STORY-GOV-525") {
		t.Fatal("expected isStoryDone to return false for mismatched story_id in .autopilot/ase-state.json")
	}

	// 8. Post-ASE sync failure quarantine marker (Rule B90 §2 / finding F1)
	quarantineDir := filepath.Join(repoDir, ".iazio")
	_ = os.MkdirAll(quarantineDir, 0o755)
	quarantineFile := filepath.Join(quarantineDir, "post-ase-sync-failed")
	_ = os.WriteFile(quarantineFile, []byte("sync failed\n"), 0o644)
	if clean, reason := IsWorktreeCleanForNewJob(repoDir); clean || !strings.Contains(reason, "quarantined") {
		t.Fatalf("expected quarantined worktree to be rejected, got clean=%v reason=%s", clean, reason)
	}
	_ = os.Remove(quarantineFile)

	// 9. Fail-closed on corrupted git repo / git failure (finding F3)
	badGitDir := t.TempDir()
	_ = exec.Command("git", "-C", badGitDir, "init", "-b", "main").Run()
	_ = os.WriteFile(filepath.Join(badGitDir, ".git", "HEAD"), []byte("corrupted head ref\n"), 0o644)
	if onFeat, _ := IsOnFeatureBranch(badGitDir); !onFeat {
		t.Fatal("expected IsOnFeatureBranch to fail closed (return true) on git error")
	}
	if clean, _ := IsWorktreeCleanForNewJob(badGitDir); clean {
		t.Fatal("expected IsWorktreeCleanForNewJob to fail closed on git error")
	}
}

