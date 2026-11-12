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

	"github.com/viovy/iazio-core/pkg/procutil"
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

var lockFileIOMu sync.RWMutex

// writeLockFileAtomic marshals info and atomically replaces lockPath via a temporary file + rename
// so concurrent unlocked Read(worktree) callers never observe a 0-byte or partially written JSON file.
func writeLockFileAtomic(lockPath string, info LockInfo) error {
	lockFileIOMu.Lock()
	defer lockFileIOMu.Unlock()

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
	var renameErr error
	for attempt := 0; attempt < 25; attempt++ {
		renameErr = os.Rename(tmpPath, lockPath)
		if renameErr == nil {
			return nil
		}
		if runtime.GOOS != "windows" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	_ = os.Remove(tmpPath)
	return renameErr
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
	lockFileIOMu.RLock()
	defer lockFileIOMu.RUnlock()

	lockPath := filepath.Join(worktree, ".iazio", "repo.lock")
	var data []byte
	var err error
	for attempt := 0; attempt < 25; attempt++ {
		data, err = os.ReadFile(lockPath)
		if err == nil {
			break
		}
		if runtime.GOOS != "windows" || os.IsNotExist(err) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return LockInfo{}, err
	}
	var info LockInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return LockInfo{}, err
	}
	return info, nil
}

// ForceRelease removes the repository lock metadata file when no live process holds the OS lock.
// It never unlinks an actively locked repo.lock.flock inode, preventing Unix inode-replacement lock bypass
// and Windows sharing-violation corruption.
func ForceRelease(worktree string) error {
	if worktree == "" {
		return nil
	}
	flockPath := filepath.Join(worktree, ".iazio", "repo.lock.flock")
	if f, err := os.OpenFile(flockPath, os.O_CREATE|os.O_RDWR, 0o600); err == nil {
		if lockErr := tryLockFile(f); lockErr != nil {
			_ = f.Close()
			return fmt.Errorf("%w: cannot force-release active OS lock on %s", ErrRepoBusy, worktree)
		}
		_ = unlockFile(f)
		_ = f.Close()
	}
	_ = os.Remove(filepath.Join(worktree, ".iazio", "repo.lock"))
	return nil
}

// IsWorktreeDirty checks if a repository worktree has uncommitted modifications or untracked files,
// ignoring internal iazio runtime files (.iazio/, .iazio-repolock, .session_story_id, .autopilot/).
// Fails closed by returning true if git status fails.
func IsWorktreeDirty(worktree string) bool {
	if worktree == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(worktree, ".git")); err != nil {
		return false
	}
	ensureGitInfoExclude(worktree)
	cmd := procutil.Command("git", "-C", worktree, "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return true
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
// Returns (true, nil) if ahead, (false, nil) if clean and up to date, or (false, err) if probe fails.
func HasUnpushedCommits(worktree string) (bool, error) {
	if worktree == "" {
		return false, nil
	}
	cmdRemote := procutil.Command("git", "-C", worktree, "remote")
	remoteOut, remoteErr := cmdRemote.Output()
	if remoteErr != nil {
		return false, fmt.Errorf("git remote check failed in %s: %w", worktree, remoteErr)
	}
	if strings.TrimSpace(string(remoteOut)) == "" {
		if os.Getenv("IAZIO_TEST_SKIP_REMOTE") == "1" {
			return false, nil
		}
		return false, fmt.Errorf("no git remotes configured in %s", worktree)
	}

	// 1. Try checking against upstream @{u}
	cmdUpstream := procutil.Command("git", "-C", worktree, "rev-list", "--count", "@{u}..HEAD")
	if out, err := cmdUpstream.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil {
			return cnt > 0, nil
		}
	}
	// 2. Try checking against origin/main..HEAD
	cmdOriginMain := procutil.Command("git", "-C", worktree, "rev-list", "--count", "origin/main..HEAD")
	if out, err := cmdOriginMain.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil {
			return cnt > 0, nil
		}
	}
	// 3. Try checking against origin/master..HEAD
	cmdOriginMaster := procutil.Command("git", "-C", worktree, "rev-list", "--count", "origin/master..HEAD")
	if out, err := cmdOriginMaster.Output(); err == nil {
		cnt := 0
		if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &cnt); scanErr == nil {
			return cnt > 0, nil
		}
	}
	return false, fmt.Errorf("no upstream tracking ref or remote origin branch found in %s", worktree)
}

// IsOnFeatureBranch checks if the repository worktree is currently checked out to a feature branch (not main/master/HEAD).
func IsOnFeatureBranch(worktree string) (bool, string) {
	if worktree == "" {
		return false, ""
	}
	if _, err := os.Stat(filepath.Join(worktree, ".git")); err != nil {
		return false, ""
	}
	cmd := procutil.Command("git", "-C", worktree, "rev-parse", "--abbrev-ref", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return true, "git_error"
	}
	branch := strings.TrimSpace(string(out))
	if branch == "" || branch == "main" || branch == "master" || branch == "HEAD" {
		return false, branch
	}
	return true, branch
}

func parseStoryFrontmatter(filePath string) (id, status string) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return "", ""
	}
	lines := strings.Split(string(b), "\n")
	inFrontmatter := false
	sawOpening := false
	for idx, rawLine := range lines {
		trimmed := strings.TrimSpace(rawLine)
		if idx == 0 && trimmed == "---" {
			inFrontmatter = true
			sawOpening = true
			continue
		}
		if inFrontmatter && trimmed == "---" {
			break
		}
		if inFrontmatter {
			if strings.HasPrefix(trimmed, "id:") && id == "" {
				id = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "id:")), `"'`)
			} else if strings.HasPrefix(trimmed, "status:") && status == "" {
				status = strings.ToUpper(strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "status:")), `"'`))
			}
		}
	}
	if !sawOpening {
		return "", ""
	}
	return id, status
}

func matchesStoryIDBoundary(filename, storyID string) bool {
	lowerFile := strings.ToLower(strings.TrimSuffix(filename, ".story.md"))
	lowerFile = strings.TrimSuffix(lowerFile, ".md")
	for _, cand := range []string{strings.ToLower(storyID), strings.TrimPrefix(strings.ToLower(storyID), "story-")} {
		if cand == "" {
			continue
		}
		idx := strings.Index(lowerFile, cand)
		for idx >= 0 {
			beforeOK := idx == 0 || !isAlphaNumByte(lowerFile[idx-1])
			end := idx + len(cand)
			afterOK := end == len(lowerFile) || !isAlphaNumByte(lowerFile[end])
			if beforeOK && afterOK {
				return true
			}
			next := strings.Index(lowerFile[idx+1:], cand)
			if next < 0 {
				break
			}
			idx += 1 + next
		}
	}
	return false
}

func isAlphaNumByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// IsStoryDone checks whether the given storyID is marked DONE or COMPLETED in .autopilot state or docs/stories/*.story.md.
func IsStoryDone(worktree, storyID string) bool {
	return isStoryDone(worktree, storyID)
}

func isStoryDone(worktree, storyID string) bool {
	storyID = strings.TrimSpace(storyID)
	if storyID == "" {
		return false
	}
	// Check opaque autopilot state first (ADR-002)
	for _, stateRel := range []string{
		filepath.Join(worktree, ".autopilot", "ase-state.json"),
		filepath.Join(worktree, ".autopilot", "state.json"),
	} {
		if raw, err := os.ReadFile(stateRel); err == nil {
			var st struct {
				StoryID string `json:"story_id"`
				Status  string `json:"status"`
				Phase   string `json:"phase"`
			}
			if json.Unmarshal(raw, &st) == nil && strings.EqualFold(strings.TrimSpace(st.StoryID), storyID) {
				s := strings.ToUpper(strings.TrimSpace(st.Status))
				p := strings.ToUpper(strings.TrimSpace(st.Phase))
				if s == "DONE" || s == "COMPLETED" || p == "DONE" || p == "COMPLETED" {
					return true
				}
			}
		}
	}
	globs := []string{
		filepath.Join(worktree, "docs", "stories", "*.story.md"),
		filepath.Join(worktree, "docs", "stories", "*.md"),
		filepath.Join(worktree, "..", "docs", "stories", "*.story.md"),
		filepath.Join(worktree, "..", "docs", "stories", "*.md"),
	}
	if out, err := procutil.Command("git", "-C", worktree, "rev-parse", "--show-superproject-working-tree").Output(); err == nil {
		if sp := strings.TrimSpace(string(out)); sp != "" {
			globs = append(globs,
				filepath.Join(sp, "docs", "stories", "*.story.md"),
				filepath.Join(sp, "docs", "stories", "*.md"),
			)
		}
	}
	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			continue
		}
		for _, m := range matches {
			fmID, fmStatus := parseStoryFrontmatter(m)
			if strings.EqualFold(fmID, storyID) || (fmID == "" && matchesStoryIDBoundary(filepath.Base(m), storyID)) {
				if fmStatus == "DONE" || fmStatus == "COMPLETED" {
					return true
				}
			}
		}
	}
	return false
}

// IsWorktreeCleanForNewJob verifies that a worktree is fully clean and safe for a new autonomous job to claim:
// - no active unfinished .session_story_id from a different story
// - no post-ase-sync-failed quarantine marker (.iazio/post-ase-sync-failed)
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
	// Check for post-ase-sync-failed quarantine marker (Rule B90 §2)
	if _, err := os.Stat(filepath.Join(worktree, ".iazio", "post-ase-sync-failed")); err == nil {
		return false, "quarantined: post-ASE tool update or codebase synchronization failed"
	}
	// Check if this is a git worktree
	if _, err := os.Stat(filepath.Join(worktree, ".git")); err != nil {
		return true, ""
	}
	// Check dirty files
	if IsWorktreeDirty(worktree) {
		return false, "uncommitted or untracked changes in worktree"
	}
	// Check unpushed commits unconditionally (fails closed)
	ahead, unpushedErr := HasUnpushedCommits(worktree)
	if unpushedErr != nil {
		return false, fmt.Sprintf("unpushed commit check failed: %v", unpushedErr)
	}
	if ahead {
		return false, "unpushed commits present ahead of remote origin"
	}
	// Check feature branch
	if onFeat, branch := IsOnFeatureBranch(worktree); onFeat {
		return false, fmt.Sprintf("worktree on feature branch %q (must be on clean main/master)", branch)
	}
	return true, ""
}
