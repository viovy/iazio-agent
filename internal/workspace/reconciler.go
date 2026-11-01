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
	"runtime"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
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
	gitProcRunning := hasRunningGitProcess(workspacePath)
	if !gitProcRunning {
		removedLocks := cleanGitLocks(workspacePath, gitDir, fi.IsDir())
		res.StaleLocksRemoved = len(removedLocks)
		res.RemovedLockPaths = removedLocks
	}

	// Verify no remaining lock files exist across root and submodules
	remainingLocks := hasRemainingGitLocks(workspacePath, gitDir, fi.IsDir())
	res.Manifest.CleanLocks = !gitProcRunning && !remainingLocks

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
	cleanWorkspace := filepath.Clean(workspacePath)

	// 1. Linux /proc scanning
	procEntries, err := os.ReadDir("/proc")
	if err == nil {
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
					cleanCwd := filepath.Clean(cwdLink)
					if cleanCwd == cleanWorkspace || strings.HasPrefix(cleanCwd, cleanWorkspace+string(filepath.Separator)) {
						return true
					}
				}
			}
		}
		return false
	}

	if runtime.GOOS == "windows" {
		return false
	}

	// 2. lsof detection for macOS/Darwin (and BSD/Solaris)
	if lsofBin, err := exec.LookPath("lsof"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		out, _ := procutil.CommandContext(ctx, lsofBin, "-a", "-c", "git", "+D", cleanWorkspace, "-t").Output()
		cancel()
		if len(strings.TrimSpace(string(out))) > 0 {
			return true
		}
	}

	// 3. Fallback using ps
	out, err := procutil.Command("ps", "-eo", "command").Output()
	if err == nil {
		lines := strings.Split(string(out), "\n")
		for _, line := range lines {
			if strings.Contains(line, "git") && strings.Contains(line, cleanWorkspace) {
				return true
			}
		}
	}
	return false
}

func isLockFileHeld(lockPath string) bool {
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
		out, _ := procutil.CommandContext(ctx, lsofBin, cleanPath, "-t").Output()
		if len(strings.TrimSpace(string(out))) > 0 {
			return true
		}
		return false
	}

	// 3. Fallback: if fuser exists
	if fuserBin, err := exec.LookPath("fuser"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, _ := procutil.CommandContext(ctx, fuserBin, cleanPath).Output()
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

func resolveGitDir(workspacePath, gitDir string, isDir bool) string {
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
	return targetGitDir
}

func hasRemainingGitLocks(workspacePath, gitDir string, isDir bool) bool {
	targetGitDir := resolveGitDir(workspacePath, gitDir, isDir)

	rootLocks := []string{
		filepath.Join(targetGitDir, "index.lock"),
		filepath.Join(targetGitDir, "HEAD.lock"),
		filepath.Join(targetGitDir, "config.lock"),
	}
	for _, l := range rootLocks {
		if _, err := os.Stat(l); err == nil {
			return true
		}
	}

	modulesDir := filepath.Join(targetGitDir, "modules")
	var foundLock bool
	if _, err := os.Stat(modulesDir); err == nil {
		_ = filepath.Walk(modulesDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || foundLock {
				return nil
			}
			if !info.IsDir() && strings.HasSuffix(info.Name(), ".lock") {
				foundLock = true
			}
			return nil
		})
	}
	return foundLock
}

func cleanGitLocks(workspacePath, gitDir string, isDir bool) []string {
	var removed []string
	targetGitDir := resolveGitDir(workspacePath, gitDir, isDir)

	// Root lock files
	rootLocks := []string{
		filepath.Join(targetGitDir, "index.lock"),
		filepath.Join(targetGitDir, "HEAD.lock"),
		filepath.Join(targetGitDir, "config.lock"),
	}
	for _, l := range rootLocks {
		if _, err := os.Stat(l); err == nil {
			if !isLockFileHeld(l) {
				if err := os.Remove(l); err == nil {
					removed = append(removed, l)
				}
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
				if !isLockFileHeld(path) {
					if err := os.Remove(path); err == nil {
						removed = append(removed, path)
					}
				}
			}
			return nil
		})
	}

	return removed
}

func detectInterruptedGitOps(workspacePath, gitDir string, isDir bool) (string, []string) {
	targetGitDir := resolveGitDir(workspacePath, gitDir, isDir)

	interruptedOp := ""
	checkPaths := map[string]string{
		"rebase-merge":     filepath.Join(targetGitDir, "rebase-merge"),
		"rebase-apply":     filepath.Join(targetGitDir, "rebase-apply"),
		"MERGE_HEAD":       filepath.Join(targetGitDir, "MERGE_HEAD"),
		"CHERRY_PICK_HEAD": filepath.Join(targetGitDir, "CHERRY_PICK_HEAD"),
		"BISECT_LOG":       filepath.Join(targetGitDir, "BISECT_LOG"),
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
	cmdBranch := procutil.CommandContext(ctx, "git", "-C", workspacePath, "branch", "--show-current")
	if out, err := cmdBranch.Output(); err == nil {
		manifest.Branch = strings.TrimSpace(string(out))
	}

	// HEAD SHA
	cmdHead := procutil.CommandContext(ctx, "git", "-C", workspacePath, "rev-parse", "HEAD")
	if out, err := cmdHead.Output(); err == nil {
		manifest.HeadSHA = strings.TrimSpace(string(out))
	}

	// Status porcelain
	cmdStatus := procutil.CommandContext(ctx, "git", "-C", workspacePath, "status", "--porcelain")
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
	cmdSub := procutil.CommandContext(ctx, "git", "-C", workspacePath, "submodule", "status")
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

// ResetAndSanitizeFreshWorkspace cleans stale locks and recovers dirty state to
// .autopilot/recovery/<timestamp>-<job_id>.patch before resetting root and submodules to clean origin/main (or optional targetBranch).
func ResetAndSanitizeFreshWorkspace(ctx context.Context, workspacePath, jobID string, targetBranch ...string) (*WorkspaceSanitizeResult, error) {
	res, err := SanitizeWorkspace(ctx, workspacePath)
	if err != nil {
		return res, err
	}
	if workspacePath == "" {
		return res, nil
	}

	var requestedBranch string
	if len(targetBranch) > 0 {
		requestedBranch = strings.TrimSpace(targetBranch[0])
	}

	// 1. Check if git status --porcelain is dirty
	cmdStatus := procutil.CommandContext(ctx, "git", "-C", workspacePath, "status", "--porcelain")
	statusOut, _ := cmdStatus.Output()
	if len(bytes.TrimSpace(statusOut)) > 0 {
		// Archive dirty changes to .autopilot/recovery/<timestamp>-<job_id>.patch
		recoveryDir := filepath.Join(workspacePath, ".autopilot", "recovery")
		_ = os.MkdirAll(recoveryDir, 0755)
		patchFileName := fmt.Sprintf("%d-%s.patch", time.Now().UTC().Unix(), jobID)
		patchPath := filepath.Join(recoveryDir, patchFileName)

		// Stage untracked files with intent-to-add so git diff HEAD captures all untracked files into the patch
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "add", "-N", ".").Run()

		cmdDiff := procutil.CommandContext(ctx, "git", "-C", workspacePath, "diff", "HEAD")
		if diffOut, err := cmdDiff.Output(); err == nil && len(diffOut) > 0 {
			_ = os.WriteFile(patchPath, diffOut, 0644)
		} else {
			_ = os.WriteFile(patchPath, statusOut, 0644)
		}

		// Hard reset and clean root
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "reset", "--hard", "HEAD").Run()
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "clean", "-fd").Run()

		// Fetch all remotes and reset to clean origin/main if available (unless a specific target branch is requested)
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "fetch", "--all", "--prune").Run()
		if requestedBranch == "" || requestedBranch == "main" {
			if errOrigin := procutil.CommandContext(ctx, "git", "-C", workspacePath, "rev-parse", "--verify", "origin/main").Run(); errOrigin == nil {
				_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "reset", "--hard", "origin/main").Run()
			}
		}

		// Hard reset and clean recursive submodules
		submoduleCmd := `rm -f .git/index.lock || true; git reset --hard; git clean -fd`
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "submodule", "foreach", "--recursive", submoduleCmd).Run()
	}

	if requestedBranch != "" {
		_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "fetch", "origin", requestedBranch).Run()
		if err := procutil.CommandContext(ctx, "git", "-C", workspacePath, "checkout", requestedBranch).Run(); err != nil {
			_ = procutil.CommandContext(ctx, "git", "-C", workspacePath, "checkout", "-B", requestedBranch, "origin/"+requestedBranch).Run()
		}
	}

	// Re-clean any newly discovered stale locks
	gitDir := filepath.Join(workspacePath, ".git")
	if fi, err := os.Stat(gitDir); err == nil {
		isDir := fi.IsDir()
		removed := cleanGitLocks(workspacePath, gitDir, isDir)
		res.StaleLocksRemoved += len(removed)
		res.RemovedLockPaths = append(res.RemovedLockPaths, removed...)
	}

	res.Manifest, _ = computeManifest(ctx, workspacePath)
	return res, nil
}
