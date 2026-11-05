package repolock

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	_ = handle.Release()

	if err := ForceRelease(wt); err != nil {
		t.Fatalf("ForceRelease failed: %v", err)
	}

	_, readErr := Read(wt)
	if readErr == nil {
		t.Fatalf("expected Read to fail after ForceRelease")
	}
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
