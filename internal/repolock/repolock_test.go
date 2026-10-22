package repolock

import (
	"errors"
	"os"
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
