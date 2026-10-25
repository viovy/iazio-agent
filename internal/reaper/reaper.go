// Package reaper provides process group isolation, deterministic child process tree termination,
// and orphan process / lock reaping for workspace hygiene before job resume.
package reaper

import (
	"context"
	"os"
	"path/filepath"
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

	// 1. Find and terminate orphaned processes
	pids, err := findOrphanPIDs(workspacePath, conversationID)
	if err == nil && len(pids) > 0 {
		for _, pid := range pids {
			if pid <= 1 || pid == os.Getpid() || pid == os.Getppid() {
				continue
			}
			if err := TerminateProcessGroup(pid, 3*time.Second); err == nil {
				res.OrphansReaped++
				res.ReapedPIDs = append(res.ReapedPIDs, pid)
			}
		}
	}

	// 2. Release local conversation database and session locks
	if conversationID != "" {
		released, err := releaseConversationSessionLocks(conversationID)
		if err == nil && len(released) > 0 {
			res.LocksReleased += len(released)
			res.ReleasedPaths = append(res.ReleasedPaths, released...)
		}
	}

	return res, nil
}

// releaseConversationSessionLocks identifies and clears stale .lock, LevelDB LOCK,
// and SQLite -wal/-shm lock files for a specific conversation ID across standard brain dirs.
func releaseConversationSessionLocks(conversationID string) ([]string, error) {
	var released []string
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
				if err := os.Remove(p); err == nil {
					released = append(released, p)
				}
			}
		}
	}

	return released, nil
}
