package repolock

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
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
	ToolPID        int       `json:"tool_pid,omitempty"`
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
	mu        sync.Mutex
	worktree  string
	flockFile *os.File
	info      LockInfo
	released  bool
}

// Info returns the current lock information.
func (h *LockHandle) Info() LockInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.info
}

// Worktree returns the locked repository path.
func (h *LockHandle) Worktree() string {
	return h.worktree
}

// writeLockFileAtomic marshals info and atomically replaces lockPath via a temporary file + rename
// so concurrent unlocked Read(worktree) callers never observe a 0-byte or partially written JSON file.
func writeLockFileAtomic(lockPath string, info LockInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(lockPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, ".repo.lock.tmp.*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Chmod(0o644); err != nil && runtime.GOOS != "windows" {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, lockPath); err != nil {
		if runtime.GOOS == "windows" {
			_ = os.Remove(lockPath)
			if err2 := os.Rename(tmpPath, lockPath); err2 == nil {
				return nil
			}
		}
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// Update updates the in-memory and on-disk lock state while holding the OS flock.
func (h *LockHandle) Update(info LockInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()
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
	return writeLockFileAtomic(lockPath, info)
}

// Release writes status COMPLETED if still running and releases the OS flock.
func (h *LockHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	h.released = true

	if h.info.Status == "RUNNING" || h.info.Status == "RESUMING" {
		h.info.Status = "COMPLETED"
		h.info.UpdatedAt = time.Now().UTC()
		lockPath := filepath.Join(h.worktree, ".iazio", "repo.lock")
		_ = writeLockFileAtomic(lockPath, h.info)
	}

	if h.flockFile != nil {
		_ = unlockFile(h.flockFile)
		_ = h.flockFile.Close()
	}
	return nil
}

func ensureGitInfoExclude(worktree string) {
	gitPath := filepath.Join(worktree, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return
	}
	gitDir := gitPath
	if !fi.IsDir() {
		if b, rErr := os.ReadFile(gitPath); rErr == nil {
			content := strings.TrimSpace(string(b))
			if strings.HasPrefix(content, "gitdir:") {
				target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(target) {
					target = filepath.Join(worktree, target)
				}
				gitDir = target
			}
		}
	}
	infoDir := filepath.Join(gitDir, "info")
	if err := os.MkdirAll(infoDir, 0o755); err != nil {
		return
	}
	excludePath := filepath.Join(infoDir, "exclude")
	existing, _ := os.ReadFile(excludePath)
	text := string(existing)
	var toAdd []string
	for _, entry := range []string{".iazio/", ".iazio-repolock"} {
		if !strings.Contains(text, entry) {
			toAdd = append(toAdd, entry)
		}
	}
	if len(toAdd) == 0 {
		return
	}
	if len(text) > 0 && !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	text += strings.Join(toAdd, "\n") + "\n"
	_ = os.WriteFile(excludePath, []byte(text), 0o644)
}

// Acquire attempts to acquire an exclusive lock on the repository worktree.
// If another process is actively running (determined by OS flock or alive PID), it returns ErrRepoBusy.
// If a previous execution crashed (PID dead), it allows the caller to adopt/resume the lock.
func Acquire(worktree string, info LockInfo, isAlive func(int) bool) (*LockHandle, error) {
	if worktree == "" {
		return nil, errors.New("empty worktree path")
	}
	ensureGitInfoExclude(worktree)
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
	existingStatus := strings.ToUpper(strings.TrimSpace(existing.Status))
	if readErr == nil && existing.JobID != "" && existing.JobID != info.JobID &&
		existingStatus != "COMPLETED" && existingStatus != "FAILED" && existingStatus != "IDLE" {
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

	// Only adopt prior conversation ID if explicitly continuing an unfinished run
	// for the EXACT same non-empty story ID. Never adopt for generic/story-less prompts
	// or from completed runs (prevents cross-prompt session leak).
	if info.ConversationID == "" && existing.ConversationID != "" {
		if info.StoryID != "" && info.StoryID == existing.StoryID && existing.Status != "COMPLETED" {
			info.ConversationID = existing.ConversationID
		}
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
	if err := writeLockFileAtomic(lockPath, info); err != nil {
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

// ForceRelease removes the repository lock file and flock, marking it idle.
func ForceRelease(worktree string) error {
	if worktree == "" {
		return nil
	}
	_ = os.Remove(filepath.Join(worktree, ".iazio", "repo.lock"))
	_ = os.Remove(filepath.Join(worktree, ".iazio", "repo.lock.flock"))
	return nil
}

// IsWorktreeDirty checks if a repository worktree has uncommitted modifications or untracked files.
func IsWorktreeDirty(worktree string) bool {
	if worktree == "" {
		return false
	}
	cmd := procutil.Command("git", "-C", worktree, "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	for _, l := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(l)
		if len(trimmed) > 3 {
			return true
		}
	}
	return false
}
