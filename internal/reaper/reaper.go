// Package reaper provides process group isolation, deterministic child process tree termination,
// and orphan process / lock reaping for workspace hygiene before job resume.
package reaper

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ReapResult summarizes orphan processes terminated and session locks released.
type ReapResult struct {
	OrphansReaped int      `json:"orphans_reaped"`
	ReapedPIDs    []int    `json:"reaped_pids,omitempty"`
	LocksReleased int      `json:"locks_released"`
	ReleasedPaths []string `json:"released_paths,omitempty"`
}

// isSessionLockHeld checks if an active OS process holds an open file handle on lockPath.
// On Linux, scans /proc/*/fd directly. On macOS/Unix, uses lsof or fuser.
// Fails closed (returns true) if inspection is unavailable and the lock file exists.
func isSessionLockHeld(lockPath string) bool {
	cleanPath, err := filepath.Abs(lockPath)
	if err != nil {
		cleanPath = filepath.Clean(lockPath)
	}

	if runtime.GOOS == "windows" {
		if _, err := os.Stat(cleanPath); os.IsNotExist(err) {
			return false
		}
		f, err := os.OpenFile(cleanPath, os.O_RDWR, 0)
		if err != nil {
			return true
		}
		_ = f.Close()
		return false
	}

	// 1. Linux /proc/*/fd scanning
	procEntries, err := os.ReadDir("/proc")
	if err == nil {
		for _, entry := range procEntries {
			if !entry.IsDir() {
				continue
			}
			pidStr := entry.Name()
			if pidStr == "self" || pidStr == "thread-self" {
				continue
			}
			fdDir := filepath.Join("/proc", pidStr, "fd")
			fds, err := os.ReadDir(fdDir)
			if err != nil {
				continue
			}
			for _, fd := range fds {
				target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
				if err == nil && filepath.Clean(target) == cleanPath {
					return true
				}
			}
		}
		// On Linux /proc is authoritative
		return false
	}

	// 2. lsof detection for macOS / Darwin / Unix
	if lsofBin, err := exec.LookPath("lsof"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, _ := exec.CommandContext(ctx, lsofBin, cleanPath, "-t").Output()
		if len(strings.TrimSpace(string(out))) > 0 {
			return true
		}
		return false
	}

	// 3. Fallback: if fuser exists
	if fuserBin, err := exec.LookPath("fuser"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, _ := exec.CommandContext(ctx, fuserBin, cleanPath).Output()
		if len(strings.TrimSpace(string(out))) > 0 {
			return true
		}
		return false
	}

	// Fail closed if inspection tooling is unavailable and the file exists
	if _, err := os.Stat(cleanPath); err == nil {
		return true
	}
	return false
}

// ReapWorkspaceOrphansAndLocks scans the host for any lingering orphan processes
// associated with workspacePath or conversationID, terminates them cleanly via their
// process group, and clears any exclusive session file locks.
func ReapWorkspaceOrphansAndLocks(ctx context.Context, workspacePath, conversationID string) (*ReapResult, error) {
	res := &ReapResult{
		ReapedPIDs:    make([]int, 0),
		ReleasedPaths: make([]string, 0),
	}

	workspacePath = strings.TrimSpace(workspacePath)
	conversationID = strings.TrimSpace(conversationID)

	if workspacePath == "" && conversationID == "" {
		return res, nil
	}

	var reapErrors []string

	// 1. Find and terminate orphaned processes
	pids, err := findOrphanPIDs(workspacePath, conversationID)
	if err != nil {
		reapErrors = append(reapErrors, fmt.Sprintf("findOrphanPIDs error: %v", err))
	}
	for _, pid := range pids {
		if pid <= 1 || pid == os.Getpid() || pid == os.Getppid() {
			continue
		}
		if termErr := TerminateProcessGroup(pid, 3*time.Second); termErr == nil {
			res.OrphansReaped++
			res.ReapedPIDs = append(res.ReapedPIDs, pid)
		} else {
			reapErrors = append(reapErrors, fmt.Sprintf("terminate pid %d: %v", pid, termErr))
		}
	}

	// 2. Release local conversation database and session locks
	if conversationID != "" {
		released, lockErr := releaseConversationSessionLocks(conversationID)
		if lockErr != nil {
			reapErrors = append(reapErrors, fmt.Sprintf("releaseSessionLocks: %v", lockErr))
		}
		if len(released) > 0 {
			res.LocksReleased += len(released)
			res.ReleasedPaths = append(res.ReleasedPaths, released...)
		}
	}

	if len(reapErrors) > 0 {
		return res, fmt.Errorf("reap errors: %s", strings.Join(reapErrors, "; "))
	}
	return res, nil
}

// releaseConversationSessionLocks identifies and clears stale .lock, LevelDB LOCK,
// and SQLite -wal/-shm lock files for a specific conversation ID across standard brain dirs,
// verifying no active process owns the file handle before unlinking.
func releaseConversationSessionLocks(conversationID string) ([]string, error) {
	var released []string
	var lockErrors []string
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return released, nil
	}

	searchDirs := []string{
		filepath.Join(home, ".gemini", "antigravity-cli", "conversations"),
		filepath.Join(home, ".gemini", "antigravity-cli", "brain", conversationID),
		filepath.Join(home, ".gemini", "brain", conversationID),
		filepath.Join(home, ".iazio", "conversations"),
	}

	lockFileCandidates := []string{
		filepath.Join(home, ".gemini", "antigravity-cli", "conversations", conversationID+".db.lock"),
		filepath.Join(home, ".gemini", "antigravity-cli", "conversations", conversationID+".lock"),
		filepath.Join(home, ".gemini", "antigravity-cli", "brain", conversationID, ".lock"),
		filepath.Join(home, ".gemini", "antigravity-cli", "brain", conversationID, "LOCK"),
		filepath.Join(home, ".gemini", "brain", conversationID, ".lock"),
		filepath.Join(home, ".gemini", "brain", conversationID, "LOCK"),
	}

	for _, p := range lockFileCandidates {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			if isSessionLockHeld(p) {
				lockErrors = append(lockErrors, fmt.Sprintf("conversation lock %s is held by an active process", p))
				continue
			}
			if err := os.Remove(p); err == nil {
				released = append(released, p)
			}
		}
	}

	for _, dir := range searchDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, conversationID) && (strings.HasSuffix(name, ".lock") || strings.EqualFold(name, "LOCK")) {
				p := filepath.Join(dir, name)
				if isSessionLockHeld(p) {
					lockErrors = append(lockErrors, fmt.Sprintf("conversation lock %s is held by an active process", p))
					continue
				}
				if err := os.Remove(p); err == nil {
					released = append(released, p)
				}
			}
		}
	}

	if len(lockErrors) > 0 {
		return released, fmt.Errorf("%s", strings.Join(lockErrors, "; "))
	}
	return released, nil
}
