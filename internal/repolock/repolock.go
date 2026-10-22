package repolock

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	// ErrRepoBusy indicates another live job is actively executing on the worktree.
	ErrRepoBusy = errors.New("repository worktree busy with running job")
	// ErrLockAcquisitionFailed indicates an OS-level flock conflict.
	ErrLockAcquisitionFailed = errors.New("failed to acquire repository file lock")
)

// LockInfo describes the active or last recorded job holding a repository worktree.
type LockInfo struct {
	JobID          string    `json:"job_id"`
	ScheduleID     string    `json:"schedule_id,omitempty"`
	PID            int       `json:"pid"`
	HarnessPID     int       `json:"harness_pid,omitempty"`
	Engine         string    `json:"engine,omitempty"`
	ConversationID string    `json:"conversation_id,omitempty"`
	StoryID        string    `json:"story_id,omitempty"`
	Prompt         string    `json:"prompt,omitempty"`
	Status         string    `json:"status"` // RUNNING, RESUMING, COMPLETED, FAILED
	LockedAt       time.Time `json:"locked_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// LockHandle holds an exclusive OS-level flock on a repository worktree.
type LockHandle struct {
	worktree  string
	flockFile *os.File
	info      LockInfo
	released  bool
}

// Info returns the current lock information.
func (h *LockHandle) Info() LockInfo {
	return h.info
}

// Worktree returns the locked repository path.
func (h *LockHandle) Worktree() string {
	return h.worktree
}

// Update updates the in-memory and on-disk lock state while holding the OS flock.
func (h *LockHandle) Update(info LockInfo) error {
	if h.released {
		return errors.New("lock already released")
	}
	info.UpdatedAt = time.Now().UTC()
	if info.LockedAt.IsZero() {
		info.LockedAt = h.info.LockedAt
	}
	if info.Status == "" {
		info.Status = h.info.Status
	}
	if info.Status == "" {
		info.Status = "RUNNING"
	}
	h.info = info

	lockPath := filepath.Join(h.worktree, ".iazio", "repo.lock")
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(lockPath, data, 0o644)
}

// Release writes status COMPLETED if still running and releases the OS flock.
func (h *LockHandle) Release() error {
	if h.released {
		return nil
	}
	h.released = true

	if h.info.Status == "RUNNING" || h.info.Status == "RESUMING" {
		h.info.Status = "COMPLETED"
		h.info.UpdatedAt = time.Now().UTC()
		lockPath := filepath.Join(h.worktree, ".iazio", "repo.lock")
		if data, err := json.MarshalIndent(h.info, "", "  "); err == nil {
			_ = os.WriteFile(lockPath, data, 0o644)
		}
	}

	if h.flockFile != nil {
		_ = unlockFile(h.flockFile)
		_ = h.flockFile.Close()
	}
	return nil
}

// Acquire attempts to acquire an exclusive lock on the repository worktree.
// If another process is actively running (determined by OS flock or alive PID), it returns ErrRepoBusy.
// If a previous execution crashed (PID dead), it allows the caller to adopt/resume the lock.
func Acquire(worktree string, info LockInfo, isAlive func(int) bool) (*LockHandle, error) {
	if worktree == "" {
		return nil, errors.New("empty worktree path")
	}
	iazioDir := filepath.Join(worktree, ".iazio")
	if err := os.MkdirAll(iazioDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create .iazio directory: %w", err)
	}

	flockPath := filepath.Join(iazioDir, "repo.lock.flock")
	f, err := os.OpenFile(flockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open flock file: %w", err)
	}

	if err := tryLockFile(f); err != nil {
		_ = f.Close()
		existing, _ := Read(worktree)
		return nil, fmt.Errorf("%w: lock conflict on %s (current holder job: %q, pid: %d)",
			ErrRepoBusy, worktree, existing.JobID, existing.PID)
	}

	// Read existing metadata to detect running processes or crashes
	existing, readErr := Read(worktree)
	if readErr == nil && existing.JobID != "" && existing.JobID != info.JobID {
		checkPID := existing.PID
		if existing.HarnessPID > 0 {
			checkPID = existing.HarnessPID
		}
		if checkPID > 0 && isAlive != nil && isAlive(checkPID) {
			_ = unlockFile(f)
			_ = f.Close()
			return nil, fmt.Errorf("%w: job %s (pid %d) actively running on %s",
				ErrRepoBusy, existing.JobID, checkPID, worktree)
		}
	}

	// Preserve prior conversation ID / story ID if caller didn't provide them
	if info.ConversationID == "" && existing.ConversationID != "" {
		if info.StoryID == "" || info.StoryID == existing.StoryID {
			info.ConversationID = existing.ConversationID
		}
	}
	if info.StoryID == "" && existing.StoryID != "" {
		info.StoryID = existing.StoryID
	}
	if info.LockedAt.IsZero() {
		info.LockedAt = time.Now().UTC()
	}
	info.UpdatedAt = time.Now().UTC()
	if info.Status == "" {
		info.Status = "RUNNING"
	}

	handle := &LockHandle{
		worktree:  worktree,
		flockFile: f,
		info:      info,
	}

	lockPath := filepath.Join(iazioDir, "repo.lock")
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		_ = handle.Release()
		return nil, err
	}
	if err := os.WriteFile(lockPath, data, 0o644); err != nil {
		_ = handle.Release()
		return nil, err
	}

	return handle, nil
}

// Read reads the last recorded LockInfo from .iazio/repo.lock without acquiring the OS flock.
func Read(worktree string) (LockInfo, error) {
	lockPath := filepath.Join(worktree, ".iazio", "repo.lock")
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return LockInfo{}, err
	}
	var info LockInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return LockInfo{}, err
	}
	return info, nil
}
