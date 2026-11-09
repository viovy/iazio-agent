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

func resolveWorktreeGitDir(worktree string) string {
	gitPath := filepath.Join(worktree, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return ""
	}
	if fi.IsDir() {
		return gitPath
	}
	b, rErr := os.ReadFile(gitPath)
	if rErr != nil {
		return gitPath
	}
	content := strings.TrimSpace(string(b))
	if !strings.HasPrefix(content, "gitdir:") {
		return gitPath
	}
	target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
	if !filepath.IsAbs(target) {
		target = filepath.Join(worktree, target)
	}
	return target
}

func ensureGitInfoExclude(worktree string) {
	gitDir := resolveWorktreeGitDir(worktree)
	if gitDir == "" {
		return
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

// IsExpiredMaintenanceLock returns true when LockInfo belongs to a bounded internal maintenance task
// (such as "autopilot-git-update" or "autopilot-git-update-agy") that has exceeded its maximum TTL,
// preventing self-PID (iazio-agent daemon) locks from wedging worktrees if an update times out or aborts.
func IsExpiredMaintenanceLock(info LockInfo) bool {
	jobID := strings.TrimSpace(info.JobID)
	var maxAge time.Duration
	switch jobID {
	case "autopilot-git-update":
		maxAge = 5 * time.Minute
	case "autopilot-git-update-agy":
		maxAge = 12 * time.Minute
	default:
		return false
	}
	ref := info.UpdatedAt
	if ref.IsZero() {
		ref = info.LockedAt
	}
	if ref.IsZero() {
		return true
	}
	return time.Since(ref) > maxAge
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
		existingStatus != "COMPLETED" && existingStatus != "FAILED" && existingStatus != "IDLE" &&
		!IsExpiredMaintenanceLock(existing) {
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

// IsWorktreeDirty checks if a repository worktree has uncommitted modifications or untracked files,
// ignoring internal iazio runtime files (.iazio/, .iazio-repolock, .session_story_id, .autopilot/).
func IsWorktreeDirty(worktree string) bool {
	if worktree == "" {
		return false
	}
	ensureGitInfoExclude(worktree)
	cmd := procutil.Command("git", "-C", worktree, "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	for _, l := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(l)
		if len(trimmed) < 3 {
			continue
		}
		pathPart := strings.TrimSpace(trimmed[2:])
		pathPart = strings.TrimPrefix(filepath.ToSlash(pathPart), "./")
		if pathPart == ".iazio" || strings.HasPrefix(pathPart, ".iazio/") ||
			pathPart == ".iazio-repolock" || pathPart == ".session_story_id" ||
			pathPart == ".autopilot" || strings.HasPrefix(pathPart, ".autopilot/") {
			continue
		}
		return true
	}
	return false
}

// HasUnpushedCommits checks if a repository worktree has commits ahead of its upstream or origin tracking branch.
func HasUnpushedCommits(worktree string) bool {
	if worktree == "" {
		return false
	}
	// 1. Try checking against upstream @{u}
	cmdUpstream := procutil.Command("git", "-C", worktree, "rev-list", "--count", "@{u}..HEAD")
	if out, err := cmdUpstream.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil && cnt > 0 {
			return true
		}
	}
	// 2. Try checking against origin/main..HEAD
	cmdOriginMain := procutil.Command("git", "-C", worktree, "rev-list", "--count", "origin/main..HEAD")
	if out, err := cmdOriginMain.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil && cnt > 0 {
			return true
		}
	}
	// 3. Try checking against origin/master..HEAD
	cmdOriginMaster := procutil.Command("git", "-C", worktree, "rev-list", "--count", "origin/master..HEAD")
	if out, err := cmdOriginMaster.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil && cnt > 0 {
			return true
		}
	}
	return false
}

// IsOnFeatureBranch checks if the repository worktree is currently checked out to a feature branch (not main/master/HEAD).
func IsOnFeatureBranch(worktree string) (bool, string) {
	if worktree == "" {
		return false, ""
	}
	cmd := procutil.Command("git", "-C", worktree, "rev-parse", "--abbrev-ref", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return false, ""
	}
	branch := strings.TrimSpace(string(out))
	if branch == "" || branch == "main" || branch == "master" || branch == "HEAD" {
		return false, branch
	}
	return true, branch
}

func isStoryDone(worktree, storyID string) bool {
	if storyID == "" {
		return false
	}
	globs := []string{
		filepath.Join(worktree, "docs", "stories", "*.story.md"),
		filepath.Join(worktree, "docs", "stories", "*.md"),
		filepath.Join(worktree, "..", "docs", "stories", "*.story.md"),
	}
	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			continue
		}
		for _, m := range matches {
			if b, err := os.ReadFile(m); err == nil {
				text := string(b)
				if strings.Contains(text, storyID) && (strings.Contains(text, "status: DONE") || strings.Contains(text, "status: \"DONE\"") || strings.Contains(text, "status: 'DONE'")) {
					return true
				}
			}
		}
	}
	return false
}

// IsWorktreeCleanForNewJob verifies that a worktree is fully clean and safe for a new autonomous job to claim:
// - no active unfinished .session_story_id from a different story
// - no uncommitted/untracked files (IsWorktreeDirty)
// - no unpushed commits (HasUnpushedCommits)
// - not checked out to an unmerged feature branch
func IsWorktreeCleanForNewJob(worktree string, allowedStoryID ...string) (bool, string) {
	if worktree == "" {
		return true, ""
	}
	fi, err := os.Stat(worktree)
	if err != nil || !fi.IsDir() {
		return true, ""
	}
	var allowed string
	if len(allowedStoryID) > 0 {
		allowed = strings.TrimSpace(allowedStoryID[0])
	}
	// Check .session_story_id
	if b, err := os.ReadFile(filepath.Join(worktree, ".session_story_id")); err == nil {
		storyID := strings.TrimSpace(string(b))
		if storyID != "" && (allowed == "" || !strings.EqualFold(storyID, allowed)) {
			if !isStoryDone(worktree, storyID) {
				return false, fmt.Sprintf("active .session_story_id found (%s)", storyID)
			}
		}
	}
	// Check dirty files
	if IsWorktreeDirty(worktree) {
		return false, "uncommitted or untracked changes in worktree"
	}
	// Check unpushed commits
	if HasUnpushedCommits(worktree) {
		return false, "unpushed commits present ahead of remote origin"
	}
	// Check feature branch
	if onFeat, branch := IsOnFeatureBranch(worktree); onFeat {
		return false, fmt.Sprintf("worktree on feature branch %q (must be on clean main/master)", branch)
	}
	return true, ""
}
