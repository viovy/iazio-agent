// Package workspace provides pre-resume workspace and submodule forensic reconciliation,
// lock cleanup, interrupted git operation recovery, and in-flight work preservation.
package workspace

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// WorkspaceManifest captures non-destructive snapshot telemetry for a workspace.
type WorkspaceManifest struct {
	Branch              string   `json:"branch"`
	HeadSHA             string   `json:"head_sha"`
	CleanLocks          bool     `json:"clean_locks"`
	ModifiedFiles       []string `json:"modified_files,omitempty"`
	UntrackedFiles      []string `json:"untracked_files,omitempty"`
	ModifiedSubmodules  []string `json:"modified_submodules,omitempty"`
	PreservedFilesCount int      `json:"preserved_files_count"`
	InterruptedGitOp    string   `json:"interrupted_git_op,omitempty"`
}

// WorkspaceSanitizeResult records the results of the pre-resume sanitization and reconciliation pass.
type WorkspaceSanitizeResult struct {
	StaleLocksRemoved       int               `json:"stale_locks_removed"`
	RemovedLockPaths        []string          `json:"removed_lock_paths,omitempty"`
	InterruptedGitOperation string            `json:"interrupted_git_operation,omitempty"`
	InterruptedSubmodules   []string          `json:"interrupted_submodules,omitempty"`
	Manifest                WorkspaceManifest `json:"manifest"`
}

// SanitizeWorkspace reconciles locks and git state across workspacePath and its submodules
// without running any destructive commands (never reset --hard or clean -f).
func SanitizeWorkspace(ctx context.Context, workspacePath string) (*WorkspaceSanitizeResult, error) {
	res := &WorkspaceSanitizeResult{
		RemovedLockPaths:      make([]string, 0),
		InterruptedSubmodules: make([]string, 0),
		Manifest: WorkspaceManifest{
			ModifiedFiles:      make([]string, 0),
			UntrackedFiles:     make([]string, 0),
			ModifiedSubmodules: make([]string, 0),
			CleanLocks:         true,
		},
	}

	workspacePath = strings.TrimSpace(workspacePath)
	if workspacePath == "" {
		return res, nil
	}

	gitDir := filepath.Join(workspacePath, ".git")
	fi, err := os.Stat(gitDir)
	if err != nil {
		// Not a git repository
		return res, nil
	}

	// 1. Stale Git Lock Cleanup
	// Verify no active git process exists in workspace before removing locks
	if !hasRunningGitProcess(workspacePath) {
		removedLocks := cleanGitLocks(workspacePath, gitDir, fi.IsDir())
		res.StaleLocksRemoved = len(removedLocks)
		res.RemovedLockPaths = removedLocks
	} else {
		res.Manifest.CleanLocks = false
	}

	// 2. Interrupted Git Operation Recovery
	res.InterruptedGitOperation, res.InterruptedSubmodules = detectInterruptedGitOps(workspacePath, gitDir, fi.IsDir())
	res.Manifest.InterruptedGitOp = res.InterruptedGitOperation

	// 3. In-Flight Work Preservation Manifest
	manifest, err := computeManifest(ctx, workspacePath)
	if err == nil {
		manifest.CleanLocks = res.Manifest.CleanLocks
		manifest.InterruptedGitOp = res.InterruptedGitOperation
		res.Manifest = manifest
	}

	return res, nil
}

func hasRunningGitProcess(workspacePath string) bool {
	// Look for any running git process referencing workspacePath
	procEntries, err := os.ReadDir("/proc")
	if err == nil {
		cleanWorkspace := filepath.Clean(workspacePath)
		for _, entry := range procEntries {
			if !entry.IsDir() {
				continue
			}
			cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
			if err != nil || len(cmdlineBytes) == 0 {
				continue
			}
			cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
			if strings.HasPrefix(cmdline, "git ") || strings.Contains(cmdline, "/git ") {
				if strings.Contains(cmdline, cleanWorkspace) {
					return true
				}
				if cwdLink, err := os.Readlink(filepath.Join("/proc", entry.Name(), "cwd")); err == nil {
					if filepath.Clean(cwdLink) == cleanWorkspace {
						return true
					}
				}
			}
		}
		return false
	}

	// Fallback using ps
	out, err := exec.Command("ps", "-eo", "command").Output()
	if err == nil {
		lines := strings.Split(string(out), "\n")
		for _, line := range lines {
			if strings.Contains(line, "git") && strings.Contains(line, workspacePath) {
				return true
			}
		}
	}
	return false
}

func cleanGitLocks(workspacePath, gitDir string, isDir bool) []string {
	var removed []string

	targetGitDir := gitDir
	if !isDir {
		// Submodule or worktree where .git is a file containing "gitdir: <path>"
		if data, err := os.ReadFile(gitDir); err == nil {
			content := strings.TrimSpace(string(data))
			if strings.HasPrefix(content, "gitdir:") {
				targetGitDir = strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(targetGitDir) {
					targetGitDir = filepath.Join(workspacePath, targetGitDir)
				}
			}
		}
	}

	// Root lock files
	rootLocks := []string{
		filepath.Join(targetGitDir, "index.lock"),
		filepath.Join(targetGitDir, "HEAD.lock"),
		filepath.Join(targetGitDir, "config.lock"),
	}
	for _, l := range rootLocks {
		if _, err := os.Stat(l); err == nil {
			if err := os.Remove(l); err == nil {
				removed = append(removed, l)
			}
		}
	}

	// Modules lock files
	modulesDir := filepath.Join(targetGitDir, "modules")
	if _, err := os.Stat(modulesDir); err == nil {
		_ = filepath.Walk(modulesDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(info.Name(), ".lock") {
				if err := os.Remove(path); err == nil {
					removed = append(removed, path)
				}
			}
			return nil
		})
	}

	return removed
}

func detectInterruptedGitOps(workspacePath, gitDir string, isDir bool) (string, []string) {
	targetGitDir := gitDir
	if !isDir {
		if data, err := os.ReadFile(gitDir); err == nil {
			content := strings.TrimSpace(string(data))
			if strings.HasPrefix(content, "gitdir:") {
				targetGitDir = strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(targetGitDir) {
					targetGitDir = filepath.Join(workspacePath, targetGitDir)
				}
			}
		}
	}

	interruptedOp := ""
	checkPaths := map[string]string{
		"rebase-merge":    filepath.Join(targetGitDir, "rebase-merge"),
		"rebase-apply":    filepath.Join(targetGitDir, "rebase-apply"),
		"MERGE_HEAD":      filepath.Join(targetGitDir, "MERGE_HEAD"),
		"CHERRY_PICK_HEAD": filepath.Join(targetGitDir, "CHERRY_PICK_HEAD"),
		"BISECT_LOG":      filepath.Join(targetGitDir, "BISECT_LOG"),
	}

	for op, p := range checkPaths {
		if _, err := os.Stat(p); err == nil {
			interruptedOp = op
			break
		}
	}

	var interruptedSubmodules []string
	modulesDir := filepath.Join(targetGitDir, "modules")
	if _, err := os.Stat(modulesDir); err == nil {
		_ = filepath.Walk(modulesDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil {
				return nil
			}
			if info.IsDir() && (info.Name() == "rebase-merge" || info.Name() == "rebase-apply") {
				rel, _ := filepath.Rel(modulesDir, filepath.Dir(path))
				interruptedSubmodules = append(interruptedSubmodules, fmt.Sprintf("%s (%s)", rel, info.Name()))
			} else if !info.IsDir() && (info.Name() == "MERGE_HEAD" || info.Name() == "CHERRY_PICK_HEAD") {
				rel, _ := filepath.Rel(modulesDir, filepath.Dir(path))
				interruptedSubmodules = append(interruptedSubmodules, fmt.Sprintf("%s (%s)", rel, info.Name()))
			}
			return nil
		})
	}

	return interruptedOp, interruptedSubmodules
}

func computeManifest(ctx context.Context, workspacePath string) (WorkspaceManifest, error) {
	manifest := WorkspaceManifest{
		CleanLocks:          true,
		ModifiedFiles:       make([]string, 0),
		UntrackedFiles:      make([]string, 0),
		ModifiedSubmodules:  make([]string, 0),
		PreservedFilesCount: 0,
	}

	// Branch
	cmdBranch := exec.CommandContext(ctx, "git", "-C", workspacePath, "branch", "--show-current")
	if out, err := cmdBranch.Output(); err == nil {
		manifest.Branch = strings.TrimSpace(string(out))
	}

	// HEAD SHA
	cmdHead := exec.CommandContext(ctx, "git", "-C", workspacePath, "rev-parse", "HEAD")
	if out, err := cmdHead.Output(); err == nil {
		manifest.HeadSHA = strings.TrimSpace(string(out))
	}

	// Status porcelain
	cmdStatus := exec.CommandContext(ctx, "git", "-C", workspacePath, "status", "--porcelain")
	if out, err := cmdStatus.Output(); err == nil {
		lines := strings.Split(string(out), "\n")
		for _, l := range lines {
			trimmed := strings.TrimRight(l, "\r\n")
			if len(trimmed) < 4 {
				continue
			}
			statusCode := trimmed[:2]
			filePath := strings.TrimSpace(trimmed[3:])
			if statusCode == "??" {
				manifest.UntrackedFiles = append(manifest.UntrackedFiles, filePath)
			} else {
				manifest.ModifiedFiles = append(manifest.ModifiedFiles, filePath)
			}
		}
	}

	// Submodule status
	cmdSub := exec.CommandContext(ctx, "git", "-C", workspacePath, "submodule", "status")
	if out, err := cmdSub.Output(); err == nil {
		lines := strings.Split(string(out), "\n")
		for _, l := range lines {
			trimmed := strings.TrimSpace(l)
			if trimmed == "" {
				continue
			}
			// Leading '+' or 'U' indicates changed or unmerged submodule
			if strings.HasPrefix(trimmed, "+") || strings.HasPrefix(trimmed, "U") {
				fields := strings.Fields(trimmed)
				if len(fields) >= 2 {
					manifest.ModifiedSubmodules = append(manifest.ModifiedSubmodules, fields[1])
				}
			}
		}
	}

	manifest.PreservedFilesCount = len(manifest.ModifiedFiles) + len(manifest.UntrackedFiles)
	return manifest, nil
}
