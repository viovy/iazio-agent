package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/procutil"
	"github.com/viovy/iazio-agent/internal/repolock"
)

const (
	// DefaultGitUpdateInterval is the default periodic repository update interval (30m).
	DefaultGitUpdateInterval = 30 * time.Minute
	// MinGitUpdateInterval is the minimum permitted periodic repository update interval (15m).
	MinGitUpdateInterval = 15 * time.Minute
	// MaxGitUpdateInterval is the maximum permitted periodic repository update interval (1h).
	MaxGitUpdateInterval = 1 * time.Hour
	// DefaultAgyRemediationCooldown is the circuit-breaker cooldown (60m) per worktree dirty state fingerprint.
	DefaultAgyRemediationCooldown = 60 * time.Minute
	// AgyGitUpdateRemediationPrompt is the canonical prompt passed to iazio-harness (agy) during Stage-2 escalation.
	AgyGitUpdateRemediationPrompt = `execute shell prompt "autopilot git-update"`
)

// ShellGitUpdateFunc executes "autopilot git-update" directly via shell in repoPath and returns combined stdout/stderr and error.
type ShellGitUpdateFunc func(ctx context.Context, repoPath string) (string, error)

// AgyEscalationFunc launches iazio-harness with target agent "agy" in repoPath executing prompt with diagnosticOutput context.
type AgyEscalationFunc func(ctx context.Context, repoPath, prompt, diagnosticOutput string) (string, error)

// ResumeRepoQueueFunc resumes a worktree queue on the control plane when it was halted on uncommitted changes.
type ResumeRepoQueueFunc func(ctx context.Context, hostID, repoPath string) error

// WorktreeGitUpdateResult captures the outcome of a 2-stage worktree git-update and remediation pass.
type WorktreeGitUpdateResult struct {
	RepoPath              string       `json:"repo_path"`
	Skipped               bool         `json:"skipped,omitempty"`
	SkipReason            string       `json:"skip_reason,omitempty"`
	SanitizedArtifacts    []string     `json:"sanitized_artifacts,omitempty"`
	Stage1Executed        bool         `json:"stage1_executed"`
	Stage1Success         bool         `json:"stage1_success"`
	Stage1Output          string       `json:"stage1_output,omitempty"`
	Stage2Escalated       bool         `json:"stage2_escalated"`
	Stage2CooldownSkipped bool         `json:"stage2_cooldown_skipped,omitempty"`
	Stage2Success         bool         `json:"stage2_success"`
	Stage2Output          string       `json:"stage2_output,omitempty"`
	DirtyFingerprint      string       `json:"dirty_fingerprint,omitempty"`
	QueueResumed          bool         `json:"queue_resumed"`
	FinalState            RepoGitState `json:"final_state"`
}

// ClampGitUpdateInterval clamps a periodic git-update interval into the permitted [15m, 1h] window (defaulting <=0 to 30m).
func ClampGitUpdateInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultGitUpdateInterval
	}
	if d < MinGitUpdateInterval {
		return MinGitUpdateInterval
	}
	if d > MaxGitUpdateInterval {
		return MaxGitUpdateInterval
	}
	return d
}

// ParseGitUpdateInterval parses a duration string and clamps it into [15m, 1h] (defaulting empty/invalid to 30m).
func ParseGitUpdateInterval(raw string) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return DefaultGitUpdateInterval
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d <= 0 {
		return DefaultGitUpdateInterval
	}
	return ClampGitUpdateInterval(d)
}

// SubRepoGitState details the parsed git posture of a nested submodule or child repository.
type SubRepoGitState struct {
	SubRepoPath         string `json:"sub_repo_path"`
	RelPath             string `json:"rel_path"`
	Branch              string `json:"branch"`
	HeadSHA             string `json:"head_sha"`
	IsClean             bool   `json:"is_clean"`
	DirtyFilesCount     int    `json:"dirty_files_count"`
	UntrackedFilesCount int    `json:"untracked_files_count"`
	Ahead               int    `json:"ahead"`
	Behind              int    `json:"behind"`
	HasActiveGitLock    bool   `json:"has_active_git_lock"`
	Error               string `json:"error,omitempty"`
}

// RepoGitState details the parsed git posture of a repository on the host.
type RepoGitState struct {
	RepoPath            string                     `json:"repo_path"`
	Alias               string                     `json:"alias,omitempty"`
	Branch              string                     `json:"branch"`
	HeadSHA             string                     `json:"head_sha"`
	IsClean             bool                       `json:"is_clean"`
	RootClean           bool                       `json:"root_clean"`
	SubmodulesClean     bool                       `json:"submodules_clean"`
	DirtySubmodules     []string                   `json:"dirty_submodules,omitempty"`
	DirtyFilesCount     int                        `json:"dirty_files_count"`
	UntrackedFilesCount int                        `json:"untracked_files_count"`
	Ahead               int                        `json:"ahead"`
	Behind              int                        `json:"behind"`
	HasActiveGitLock    bool                       `json:"has_active_git_lock"`
	SubRepos            map[string]SubRepoGitState `json:"sub_repos,omitempty"`
	LastCheckedAt       time.Time                  `json:"last_checked_at"`
	Error               string                     `json:"error,omitempty"`
}

// FleetStateSnapshot represents the aggregated git state of all host repos.
type FleetStateSnapshot struct {
	HostID         string                  `json:"host_id"`
	HarnessActive  bool                    `json:"harness_active"`
	FleetGitClean  bool                    `json:"fleet_git_clean"`
	DirtyRepoPaths []string                `json:"dirty_repo_paths,omitempty"`
	Repos          map[string]RepoGitState `json:"repos"`
	CheckedAt      time.Time               `json:"checked_at"`
}

type singleDirGitResult struct {
	Branch              string
	HeadSHA             string
	Ahead               int
	Behind              int
	DirtyFilesCount     int
	UntrackedFilesCount int
	HasActiveGitLock    bool
	IsClean             bool
	Error               string
}

// DiscoverSubRepositories discovers nested git submodules and child repositories within repoPath.
func DiscoverSubRepositories(repoPath string) []string {
	seen := make(map[string]bool)
	var subRepos []string

	// 1. Parse .gitmodules if present
	gitmodulesPath := filepath.Join(repoPath, ".gitmodules")
	if data, err := os.ReadFile(gitmodulesPath); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "path =") || strings.HasPrefix(trimmed, "path=") {
				parts := strings.SplitN(trimmed, "=", 2)
				if len(parts) == 2 {
					p := strings.TrimSpace(parts[1])
					p = strings.Trim(p, "\"'")
					if p != "" && !seen[p] {
						fullPath := filepath.Join(repoPath, p)
						if fi, statErr := os.Stat(fullPath); statErr == nil && fi.IsDir() {
							seen[p] = true
							subRepos = append(subRepos, p)
						}
					}
				}
			}
		}
	}

	// 2. Discover child directories with .git (up to 2 levels deep)
	entries, err := os.ReadDir(repoPath)
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" ||
				name == "build" || name == "target" || name == "tmp" || name == "dist" ||
				name == "bin" || name == "obj" || name == ".dart_tool" {
				continue
			}

			subPath := filepath.Join(repoPath, name)
			gitTarget := filepath.Join(subPath, ".git")
			if _, statErr := os.Stat(gitTarget); statErr == nil {
				if !seen[name] {
					seen[name] = true
					subRepos = append(subRepos, name)
				}
				continue // Do not recurse into another git repo
			}

			// Check 1 level deeper
			subEntries, subErr := os.ReadDir(subPath)
			if subErr == nil {
				for _, subEntry := range subEntries {
					if !subEntry.IsDir() {
						continue
					}
					subName := subEntry.Name()
					if strings.HasPrefix(subName, ".") || subName == "node_modules" || subName == "vendor" ||
						subName == "build" || subName == "target" || subName == "tmp" || subName == "dist" {
						continue
					}
					deepPath := filepath.Join(subPath, subName)
					deepGit := filepath.Join(deepPath, ".git")
					rel, relErr := filepath.Rel(repoPath, deepPath)
					if relErr == nil && !seen[rel] {
						if _, deepStatErr := os.Stat(deepGit); deepStatErr == nil {
							seen[rel] = true
							subRepos = append(subRepos, rel)
						}
					}
				}
			}
		}
	}

	sort.Strings(subRepos)
	return subRepos
}

// IsEphemeralArtifactPath returns true for ephemeral session, closeout, coverage, or IDE/daemon cache
// files (.session_story_id, TestResults/Diagnostics/pr-closeout-proof-*.json, cov_auth, .DS_Store, etc.)
// that should be sanitized when the worktree is idle.
func IsEphemeralArtifactPath(relPath string) bool {
	p := filepath.ToSlash(strings.TrimSpace(relPath))
	p = strings.TrimPrefix(p, "./")
	if p == "" {
		return false
	}
	base := filepath.Base(p)
	if base == ".DS_Store" || base == "Thumbs.db" || base == ".session_story_id" || base == "cov_auth" || strings.HasPrefix(base, "cov_") {
		return true
	}
	if strings.HasPrefix(base, "pr-closeout-proof-") && strings.HasSuffix(base, ".json") {
		return true
	}
	if strings.Contains(p, "/.gradle/") || strings.HasPrefix(p, ".gradle/") {
		return true
	}
	if strings.Contains(p, "/.dart_tool/") || strings.HasPrefix(p, ".dart_tool/") {
		return true
	}
	if strings.Contains(p, "/build/tmp/") || strings.HasPrefix(p, "build/tmp/") {
		return true
	}
	return false
}

func isEphemeralCachePath(relPath string) bool {
	return IsEphemeralArtifactPath(relPath)
}

func isStoryMarkedDoneOnDisk(repoPath, storyID string) bool {
	storyID = strings.TrimSpace(storyID)
	if storyID == "" {
		return false
	}
	lowerID := strings.ToLower(strings.TrimPrefix(storyID, "STORY-"))
	storiesDir := filepath.Join(repoPath, "docs", "stories")
	if matches, err := filepath.Glob(filepath.Join(storiesDir, "*"+lowerID+"*.story.md")); err == nil {
		for _, m := range matches {
			if b, rErr := os.ReadFile(m); rErr == nil {
				content := string(b)
				if strings.Contains(content, "id: "+storyID) || strings.Contains(content, "id: \""+storyID+"\"") {
					if strings.Contains(content, "status: DONE") || strings.Contains(content, "status: \"DONE\"") {
						return true
					}
					if strings.Contains(content, "status: IN_PROGRESS") || strings.Contains(content, "status: \"IN_PROGRESS\"") {
						return false
					}
				}
			}
		}
	}
	return false
}

func isStoryExplicitlyInProgressOnDisk(repoPath, storyID string) bool {
	storyID = strings.TrimSpace(storyID)
	if storyID == "" {
		return false
	}
	lowerID := strings.ToLower(strings.TrimPrefix(storyID, "STORY-"))
	storiesDir := filepath.Join(repoPath, "docs", "stories")
	if matches, err := filepath.Glob(filepath.Join(storiesDir, "*"+lowerID+"*.story.md")); err == nil {
		for _, m := range matches {
			if b, rErr := os.ReadFile(m); rErr == nil {
				content := string(b)
				if strings.Contains(content, "id: "+storyID) || strings.Contains(content, "id: \""+storyID+"\"") {
					if strings.Contains(content, "status: IN_PROGRESS") || strings.Contains(content, "status: \"IN_PROGRESS\"") {
						return true
					}
				}
			}
		}
	}
	return false
}

// IsWorktreeIdleForGitUpdate returns true when repolock is idle (no active iazio-harness / agy process running)
// and there is no active IN_PROGRESS ASE story in .autopilot/ase-state.json (or the referenced story is already DONE).
func IsWorktreeIdleForGitUpdate(repoPath string, sup *Sup, isPIDAlive func(int) bool) bool {
	cleanPath := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanPath == "" {
		return false
	}
	if sup != nil && sup.Lock(cleanPath) != "IDLE" {
		return false
	}
	if isPIDAlive == nil {
		isPIDAlive = isProcessAlive
	}
	if lockInfo, err := repolock.Read(cleanPath); err == nil {
		lockStatus := strings.ToUpper(strings.TrimSpace(lockInfo.Status))
		if lockStatus != "COMPLETED" && lockStatus != "FAILED" && lockStatus != "IDLE" && !repolock.IsExpiredMaintenanceLock(lockInfo) {
			if (lockInfo.PID > 0 && isPIDAlive(lockInfo.PID)) ||
				(lockInfo.HarnessPID > 0 && isPIDAlive(lockInfo.HarnessPID)) ||
				(lockInfo.ToolPID > 0 && isPIDAlive(lockInfo.ToolPID)) {
				return false
			}
		}
	}
	if _, err := os.Stat(filepath.Join(cleanPath, ".autopilot", "lock")); err == nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(cleanPath, ".ase", "active.lock")); err == nil {
		return false
	}

	aseStateDone := false
	aseStatePath := filepath.Join(cleanPath, ".autopilot", "ase-state.json")
	if b, err := os.ReadFile(aseStatePath); err == nil {
		var as struct {
			StoryID string `json:"story_id"`
			Status  string `json:"status"`
			Phase   string `json:"phase"`
		}
		if json.Unmarshal(b, &as) == nil {
			status := strings.ToUpper(strings.TrimSpace(as.Status))
			phase := strings.ToLower(strings.TrimSpace(as.Phase))
			if status == "DONE" || status == "COMPLETED" || phase == "completed" {
				aseStateDone = true
			} else if status == "IN_PROGRESS" || status == "RUNNING" {
				if as.StoryID != "" && isStoryMarkedDoneOnDisk(cleanPath, as.StoryID) {
					aseStateDone = true
				} else {
					return false
				}
			}
		}
	}

	statePath := filepath.Join(cleanPath, ".autopilot", "state.json")
	if b, err := os.ReadFile(statePath); err == nil {
		var s struct {
			State   string `json:"state"`
			StoryID string `json:"story_id"`
		}
		if json.Unmarshal(b, &s) == nil {
			st := strings.ToUpper(strings.TrimSpace(s.State))
			if (st == "IN_PROGRESS" || st == "RUNNING") && !aseStateDone {
				if s.StoryID == "" || !isStoryMarkedDoneOnDisk(cleanPath, s.StoryID) {
					return false
				}
			}
		}
	}

	sessionPath := filepath.Join(cleanPath, ".session_story_id")
	if b, err := os.ReadFile(sessionPath); err == nil {
		storyID := strings.TrimSpace(string(b))
		if storyID != "" && !aseStateDone && !isStoryMarkedDoneOnDisk(cleanPath, storyID) {
			if isStoryExplicitlyInProgressOnDisk(cleanPath, storyID) {
				return false
			}
			if _, rErr := os.Stat(filepath.Join(cleanPath, "tmp", "ase-resume-"+storyID+".json")); rErr == nil {
				return false
			}
			if brOut, brErr := procutil.Command("git", "-C", cleanPath, "symbolic-ref", "--short", "-q", "HEAD").Output(); brErr == nil {
				br := strings.TrimSpace(string(brOut))
				if br != "" && br != "main" && br != "master" && br != "HEAD" {
					return false
				}
			}
		}
	}

	return true
}

func cleanOrRestoreSingleArtifact(ctx context.Context, dir, relPath string) bool {
	relSlash := filepath.ToSlash(strings.TrimSpace(relPath))
	if relSlash == "" {
		return false
	}
	fullPath := filepath.Join(dir, filepath.FromSlash(relSlash))
	tracked := procutil.CommandContext(ctx, "git", "-C", dir, "ls-files", "--error-unmatch", relSlash).Run() == nil
	if tracked {
		out, err := procutil.CommandContext(ctx, "git", "-C", dir, "status", "--porcelain", "--", relSlash).Output()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			if procutil.CommandContext(ctx, "git", "-C", dir, "checkout", "HEAD", "--", relSlash).Run() == nil ||
				procutil.CommandContext(ctx, "git", "-C", dir, "checkout", "--", relSlash).Run() == nil {
				return true
			}
		}
		return false
	}
	if _, err := os.Stat(fullPath); err == nil {
		if os.RemoveAll(fullPath) == nil {
			return true
		}
	}
	return false
}

func sanitizeSingleDirEphemeralArtifacts(ctx context.Context, dir string) []string {
	var cleaned []string
	seen := make(map[string]bool)
	record := func(rel string) {
		if !seen[rel] {
			seen[rel] = true
			cleaned = append(cleaned, rel)
		}
	}

	for _, rel := range []string{".session_story_id", "cov_auth"} {
		if cleanOrRestoreSingleArtifact(ctx, dir, rel) {
			record(rel)
		}
	}

	if matches, err := filepath.Glob(filepath.Join(dir, "TestResults", "Diagnostics", "pr-closeout-proof-*.json")); err == nil {
		for _, m := range matches {
			if rel, rErr := filepath.Rel(dir, m); rErr == nil {
				relSlash := filepath.ToSlash(rel)
				if cleanOrRestoreSingleArtifact(ctx, dir, relSlash) {
					record(relSlash)
				}
			}
		}
	}

	if matches, err := filepath.Glob(filepath.Join(dir, "cov_*")); err == nil {
		for _, m := range matches {
			if rel, rErr := filepath.Rel(dir, m); rErr == nil {
				relSlash := filepath.ToSlash(rel)
				if cleanOrRestoreSingleArtifact(ctx, dir, relSlash) {
					record(relSlash)
				}
			}
		}
	}

	out, err := procutil.CommandContext(ctx, "git", "-C", dir, "status", "--porcelain=v2", "--ignore-submodules=all").Output()
	if err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			l := strings.TrimSpace(line)
			if l == "" || strings.HasPrefix(l, "# ") {
				continue
			}
			p := extractPorcelainV2Path(l)
			if IsEphemeralArtifactPath(p) {
				if cleanOrRestoreSingleArtifact(ctx, dir, p) {
					record(filepath.ToSlash(p))
				}
			}
		}
	}

	return cleaned
}

// SanitizeIdleEphemeralArtifacts cleans or restores ephemeral session/closeout/coverage artifacts
// (.session_story_id, TestResults/Diagnostics/pr-closeout-proof-*.json, cov_auth) across root and submodules
// when repolock is idle and no IN_PROGRESS ASE story is active.
func SanitizeIdleEphemeralArtifacts(ctx context.Context, repoPath string, sup *Sup, isPIDAlive func(int) bool) ([]string, bool) {
	if !IsWorktreeIdleForGitUpdate(repoPath, sup, isPIDAlive) {
		return nil, false
	}
	var allCleaned []string
	allCleaned = append(allCleaned, sanitizeSingleDirEphemeralArtifacts(ctx, repoPath)...)
	for _, subRel := range DiscoverSubRepositories(repoPath) {
		subFull := filepath.Join(repoPath, subRel)
		for _, c := range sanitizeSingleDirEphemeralArtifacts(ctx, subFull) {
			allCleaned = append(allCleaned, filepath.ToSlash(filepath.Join(subRel, c)))
		}
	}
	return allCleaned, true
}

func extractPorcelainV2Path(line string) string {
	if strings.HasPrefix(line, "? ") || strings.HasPrefix(line, "! ") {
		return strings.TrimSpace(line[2:])
	}
	// Format "1 <XY> <sub> <mH> <mI> <mW> <hH> <hI> <path>"
	if strings.HasPrefix(line, "1 ") {
		fields := strings.Fields(line)
		if len(fields) >= 9 {
			return strings.Join(fields[8:], " ")
		}
	}
	return ""
}

func inspectSingleGitDirectoryWithIdle(ctx context.Context, dir string, canSanitizeEphemeral bool) singleDirGitResult {
	return inspectSingleGitDirectoryWithOptions(ctx, dir, canSanitizeEphemeral, true)
}

func inspectSingleGitDirectoryWithOptions(ctx context.Context, dir string, canSanitizeEphemeral bool, computeAheadBehind bool) singleDirGitResult {
	res := singleDirGitResult{IsClean: true}

	// 1. Sentinel checks for active git operations or locks
	gitDir := filepath.Join(dir, ".git")
	fi, statErr := os.Stat(gitDir)
	if os.IsNotExist(statErr) {
		// Uninitialized directory without .git metadata is not a dirty git repository.
		return res
	}
	if statErr == nil && !fi.IsDir() {
		// Submodule gitfile pointing to actual gitdir
		if b, rErr := os.ReadFile(gitDir); rErr == nil {
			content := strings.TrimSpace(string(b))
			if strings.HasPrefix(content, "gitdir:") {
				target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(target) {
					target = filepath.Join(dir, target)
				}
				gitDir = target
			}
		}
	}

	lockSentinels := []string{
		filepath.Join(gitDir, "index.lock"),
		filepath.Join(gitDir, "HEAD.lock"),
		filepath.Join(gitDir, "shallow.lock"),
		filepath.Join(gitDir, "MERGE_HEAD"),
		filepath.Join(gitDir, "rebase-merge"),
		filepath.Join(gitDir, "rebase-apply"),
		filepath.Join(gitDir, "CHERRY_PICK_HEAD"),
		filepath.Join(gitDir, "REVERT_HEAD"),
		filepath.Join(gitDir, "BISECT_LOG"),
	}
	for _, sentinel := range lockSentinels {
		if fi, err := os.Stat(sentinel); err == nil {
			base := filepath.Base(sentinel)
			if strings.HasSuffix(base, ".lock") && time.Since(fi.ModTime()) > 5*time.Minute {
				// Lock file is older than 5 minutes; attempt to auto-prune abandoned stale lock.
				// If another active process holds an open handle (especially on Windows), Remove will fail.
				if rmErr := os.Remove(sentinel); rmErr == nil {
					continue
				}
			}
			res.HasActiveGitLock = true
			res.IsClean = false
			res.Error = fmt.Sprintf("active git operation or lockfile present: %s", base)
			return res
		}
	}

	// 2. Non-locking git status --porcelain=v2 --branch with per-repo timeout.
	// For superprojects with submodules (e.g. meta-repo with 52 submodules), bare status
	// traverses all submodule working trees and times out over WSL/DrvFs NTFS.
	// Since DiscoverSubRepositories inspects each submodule individually in step 2,
	// root status uses --ignore-submodules=all and an extended 30s timeout.
	statusTimeout := 15 * time.Second
	gitArgs := []string{"-C", dir, "status", "--porcelain=v2", "--branch"}
	if _, statErr := os.Stat(filepath.Join(dir, ".gitmodules")); statErr == nil {
		gitArgs = append(gitArgs, "--ignore-submodules=all")
		statusTimeout = 30 * time.Second
	}
	repoCtx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	cmd := procutil.CommandContext(repoCtx, "git", gitArgs...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		res.IsClean = false
		res.Error = fmt.Sprintf("git status failed: %v (%s)", err, strings.TrimSpace(string(out)))
		return res
	}

	sawBranchAB := false
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		if l == "" {
			continue
		}
		if strings.HasPrefix(l, "# branch.oid ") {
			res.HeadSHA = strings.TrimPrefix(l, "# branch.oid ")
		} else if strings.HasPrefix(l, "# branch.head ") {
			res.Branch = strings.TrimPrefix(l, "# branch.head ")
		} else if strings.HasPrefix(l, "# branch.ab ") {
			sawBranchAB = true
			parts := strings.Fields(strings.TrimPrefix(l, "# branch.ab "))
			if len(parts) >= 2 {
				if strings.HasPrefix(parts[0], "+") {
					res.Ahead, _ = strconv.Atoi(parts[0][1:])
				}
				if strings.HasPrefix(parts[1], "-") {
					res.Behind, _ = strconv.Atoi(parts[1][1:])
				}
			}
		} else if strings.HasPrefix(l, "? ") {
			p := extractPorcelainV2Path(l)
			pSlash := strings.TrimPrefix(filepath.ToSlash(strings.TrimSpace(p)), "./")
			if pSlash == ".iazio" || strings.HasPrefix(pSlash, ".iazio/") || pSlash == ".autopilot" || strings.HasPrefix(pSlash, ".autopilot/") {
				continue
			}
			if canSanitizeEphemeral && isEphemeralCachePath(p) {
				_ = os.RemoveAll(filepath.Join(dir, p))
				continue
			}
			res.UntrackedFilesCount++
		} else if !strings.HasPrefix(l, "# ") {
			p := extractPorcelainV2Path(l)
			if canSanitizeEphemeral && isEphemeralCachePath(p) {
				_ = procutil.CommandContext(ctx, "git", "-C", dir, "checkout", "--", p).Run()
				continue
			}
			res.DirtyFilesCount++
		}
	}

	if computeAheadBehind && !sawBranchAB && (res.Branch == "main" || res.Branch == "master") {
		remoteRef := "refs/remotes/origin/" + res.Branch
		if procutil.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--verify", "--quiet", remoteRef).Run() == nil {
			if abOut, abErr := procutil.CommandContext(ctx, "git", "-C", dir, "rev-list", "--left-right", "--count", "HEAD..."+remoteRef).Output(); abErr == nil {
				parts := strings.Fields(strings.TrimSpace(string(abOut)))
				if len(parts) >= 2 {
					res.Ahead, _ = strconv.Atoi(parts[0])
					res.Behind, _ = strconv.Atoi(parts[1])
				}
			}
		}
	}

	res.IsClean = (res.DirtyFilesCount == 0 && res.UntrackedFilesCount == 0 && !res.HasActiveGitLock)
	return res
}

func inspectSingleGitDirectory(ctx context.Context, dir string) singleDirGitResult {
	return inspectSingleGitDirectoryWithIdle(ctx, dir, IsWorktreeIdleForGitUpdate(dir, nil, isProcessAlive))
}

// InspectRepoGitState inspects a repository and recursively inspects all nested submodules and sub-repositories.
func InspectRepoGitState(ctx context.Context, repoPath, alias string) RepoGitState {
	st := RepoGitState{
		RepoPath:        repoPath,
		Alias:           alias,
		LastCheckedAt:   time.Now().UTC(),
		IsClean:         true,
		RootClean:       true,
		SubmodulesClean: true,
		SubRepos:        make(map[string]SubRepoGitState),
	}

	idle := IsWorktreeIdleForGitUpdate(repoPath, nil, isProcessAlive)
	if idle {
		_, _ = SanitizeIdleEphemeralArtifacts(ctx, repoPath, nil, isProcessAlive)
	}

	// 1. Inspect root repository
	rootRes := inspectSingleGitDirectoryWithIdle(ctx, repoPath, idle)
	st.Branch = rootRes.Branch
	st.HeadSHA = rootRes.HeadSHA
	st.Ahead = rootRes.Ahead
	st.Behind = rootRes.Behind
	st.DirtyFilesCount = rootRes.DirtyFilesCount
	st.UntrackedFilesCount = rootRes.UntrackedFilesCount
	st.HasActiveGitLock = rootRes.HasActiveGitLock
	st.RootClean = rootRes.IsClean
	if rootRes.Error != "" {
		st.Error = rootRes.Error
	}

	// 2. Discover and inspect nested sub-repositories / submodules concurrently
	subRepoRelPaths := DiscoverSubRepositories(repoPath)
	if len(subRepoRelPaths) > 0 {
		results := make([]SubRepoGitState, len(subRepoRelPaths))
		sem := make(chan struct{}, 8)
		var wg sync.WaitGroup
		for i, relPath := range subRepoRelPaths {
			wg.Add(1)
			go func(idx int, rel string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				subFullPath := filepath.Join(repoPath, rel)
				subRes := inspectSingleGitDirectoryWithOptions(ctx, subFullPath, idle, false)
				results[idx] = SubRepoGitState{
					SubRepoPath:         subFullPath,
					RelPath:             rel,
					Branch:              subRes.Branch,
					HeadSHA:             subRes.HeadSHA,
					IsClean:             subRes.IsClean,
					DirtyFilesCount:     subRes.DirtyFilesCount,
					UntrackedFilesCount: subRes.UntrackedFilesCount,
					Ahead:               subRes.Ahead,
					Behind:              subRes.Behind,
					HasActiveGitLock:    subRes.HasActiveGitLock,
					Error:               subRes.Error,
				}
			}(i, relPath)
		}
		wg.Wait()

		for _, subState := range results {
			st.SubRepos[subState.RelPath] = subState
			if !subState.IsClean {
				st.SubmodulesClean = false
				st.DirtySubmodules = append(st.DirtySubmodules, subState.RelPath)
				if subState.HasActiveGitLock {
					st.HasActiveGitLock = true
				}
			}
			st.DirtyFilesCount += subState.DirtyFilesCount
			st.UntrackedFilesCount += subState.UntrackedFilesCount
		}
	}
	sort.Strings(st.DirtySubmodules)

	// Workspace is clean iff root is clean AND 100% of nested sub-repositories are clean
	st.IsClean = st.RootClean && st.SubmodulesClean
	if !st.IsClean && st.Error == "" {
		if !st.SubmodulesClean {
			st.Error = fmt.Sprintf("nested submodules dirty: %s", strings.Join(st.DirtySubmodules, ", "))
		} else {
			st.Error = "root repository dirty or has active git lock"
		}
	}

	return st
}

// InspectWorktreeGitState inspects the git state of a worktree path with a background context.
func InspectWorktreeGitState(repoPath string) RepoGitState {
	return InspectRepoGitState(context.Background(), repoPath, "")
}

// ComputeWorktreeDirtyFingerprint computes a deterministic fingerprint combining git_head_sha,
// git_tree_sha, and the current dirty status porcelain across the worktree and submodules.
func ComputeWorktreeDirtyFingerprint(ctx context.Context, repoPath string) (headSHA, treeSHA, fingerprint string) {
	if out, err := procutil.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "HEAD").Output(); err == nil {
		headSHA = strings.TrimSpace(string(out))
	}
	if out, err := procutil.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "HEAD^{tree}").Output(); err == nil {
		treeSHA = strings.TrimSpace(string(out))
	}
	var statusBuf strings.Builder
	if out, err := procutil.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain=v2", "--branch").Output(); err == nil {
		statusBuf.WriteString(strings.TrimSpace(string(out)))
	}
	sum := sha256.Sum256([]byte(headSHA + ":" + treeSHA + ":" + statusBuf.String()))
	fingerprint = fmt.Sprintf("%s:%s:%x", headSHA, treeSHA, sum[:8])
	return headSHA, treeSHA, fingerprint
}

// DefaultShellGitUpdate executes "autopilot git-update" directly in repoPath.
func DefaultShellGitUpdate(ctx context.Context, repoPath string) (string, error) {
	autopilotBin := "autopilot"
	if p, err := exec.LookPath("autopilot"); err == nil && p != "" {
		autopilotBin = p
	} else if home, err := os.UserHomeDir(); err == nil && home != "" {
		cand := filepath.Join(home, ".local", "bin", "autopilot")
		if fi, sErr := os.Stat(cand); sErr == nil && !fi.IsDir() {
			autopilotBin = cand
		}
	}
	upCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := procutil.CommandContext(upCtx, autopilotBin, "git-update")
	cmd.Dir = repoPath
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// DefaultAgyEscalation launches iazio-harness with target agent "agy" in repoPath executing prompt.
func DefaultAgyEscalation(ctx context.Context, repoPath, prompt, diagnosticOutput string) (string, error) {
	harnessBin := "iazio-harness"
	if p, err := exec.LookPath("iazio-harness"); err == nil && p != "" {
		harnessBin = p
	} else if home, err := os.UserHomeDir(); err == nil && home != "" {
		cand := filepath.Join(home, ".local", "bin", "iazio-harness")
		if fi, sErr := os.Stat(cand); sErr == nil && !fi.IsDir() {
			harnessBin = cand
		}
	}
	fullPrompt := prompt
	if strings.TrimSpace(diagnosticOutput) != "" {
		fullPrompt = fmt.Sprintf("%s\n\nDiagnostic failure summary from shell autopilot git-update:\n%s", prompt, strings.TrimSpace(diagnosticOutput))
	}
	escCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := procutil.CommandContext(escCtx, harnessBin, "prompt", "run", "--agent", "agy", "--cwd", repoPath, "--prompt", fullPrompt, "--json")
	cmd.Dir = repoPath
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

type agyCooldownRecord struct {
	Fingerprint string
	HeadSHA     string
	TreeSHA     string
	AttemptedAt time.Time
}

// RepoConfigItem identifies a monitored repo path and optional alias.
type RepoConfigItem struct {
	Path  string
	Alias string
}

// RepoStateMonitor periodically inspects host repositories and executes 2-stage git-update remediation when idle.
type RepoStateMonitor struct {
	mu                  sync.RWMutex
	inspectMu           sync.Mutex // serializes InspectFleet across callers (F8)
	bgWg                sync.WaitGroup
	sup                 *Sup
	hostID              string
	getRepos            func() ([]RepoConfigItem, error)
	reportStatuses      func(ctx context.Context, hostID string, statuses []controlplane.RepoRuntimeStatus) error
	postNotif           func(ctx context.Context, rec controlplane.NotificationRecord) error
	onFleetClean        func(snap FleetStateSnapshot)
	lastSnapshot        FleetStateSnapshot
	interval            time.Duration
	gitUpdateInterval   time.Duration
	shellGitUpdate      ShellGitUpdateFunc
	agyEscalation       AgyEscalationFunc
	resumeRepoQueue     ResumeRepoQueueFunc
	remediateBeforeHalt bool
	agyCooldownDur      time.Duration
	agyCooldowns        map[string]agyCooldownRecord
	isPIDAlive          func(int) bool
	nowFn               func() time.Time
}

// NewRepoStateMonitor creates a monitor.
func NewRepoStateMonitor(
	sup *Sup,
	hostID string,
	interval time.Duration,
	getRepos func() ([]RepoConfigItem, error),
	reportStatuses func(ctx context.Context, hostID string, statuses []controlplane.RepoRuntimeStatus) error,
	postNotif func(ctx context.Context, rec controlplane.NotificationRecord) error,
	onFleetClean func(snap FleetStateSnapshot),
) *RepoStateMonitor {
	if interval < 1*time.Minute || interval > 5*time.Minute {
		fmt.Fprintf(os.Stderr, "[repomonitor] configured interval %v outside permitted range [1m..5m]; defaulting to 2m\n", interval)
		interval = 2 * time.Minute
	}
	return &RepoStateMonitor{
		sup:               sup,
		hostID:            hostID,
		interval:          interval,
		gitUpdateInterval: DefaultGitUpdateInterval,
		getRepos:          getRepos,
		reportStatuses:    reportStatuses,
		postNotif:         postNotif,
		onFleetClean:      onFleetClean,
		agyCooldownDur:    DefaultAgyRemediationCooldown,
		agyCooldowns:      make(map[string]agyCooldownRecord),
		isPIDAlive:        isProcessAlive,
		nowFn:             time.Now,
		lastSnapshot: FleetStateSnapshot{
			HostID: hostID,
			Repos:  make(map[string]RepoGitState),
		},
	}
}

// SetGitUpdateInterval sets and clamps the periodic git-update interval into [15m, 1h].
func (m *RepoStateMonitor) SetGitUpdateInterval(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gitUpdateInterval = ClampGitUpdateInterval(d)
}

// GitUpdateInterval returns the configured periodic git-update interval.
func (m *RepoStateMonitor) GitUpdateInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.gitUpdateInterval <= 0 {
		return DefaultGitUpdateInterval
	}
	return m.gitUpdateInterval
}

// ConfigureGitUpdate wires the periodic git-update interval, Stage-1 shell git-update runner,
// Stage-2 agy escalation runner, and control-plane queue resume callback.
func (m *RepoStateMonitor) ConfigureGitUpdate(
	gitUpdateInterval time.Duration,
	shellFn ShellGitUpdateFunc,
	agyFn AgyEscalationFunc,
	resumeFn ResumeRepoQueueFunc,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gitUpdateInterval = ClampGitUpdateInterval(gitUpdateInterval)
	if shellFn == nil {
		shellFn = DefaultShellGitUpdate
	}
	if agyFn == nil {
		agyFn = DefaultAgyEscalation
	}
	m.shellGitUpdate = shellFn
	m.agyEscalation = agyFn
	m.resumeRepoQueue = resumeFn
	m.remediateBeforeHalt = true
}

// SetAgyCooldownDuration overrides the Stage-2 agy escalation cooldown window (default 60m).
func (m *RepoStateMonitor) SetAgyCooldownDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.agyCooldownDur = d
	}
}

// SetNowFunc overrides the wall-clock provider (used in unit tests).
func (m *RepoStateMonitor) SetNowFunc(fn func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fn != nil {
		m.nowFn = fn
	}
}

// SetIsPIDAlive overrides the process liveness check (used in unit tests).
func (m *RepoStateMonitor) SetIsPIDAlive(fn func(int) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fn != nil {
		m.isPIDAlive = fn
	}
}

func isCleanOnMainSynced(st RepoGitState) bool {
	if !st.IsClean || !st.RootClean || !st.SubmodulesClean || st.HasActiveGitLock {
		return false
	}
	if st.Branch != "main" && st.Branch != "master" {
		return false
	}
	if st.Ahead != 0 || st.Behind != 0 {
		return false
	}
	for _, sub := range st.SubRepos {
		if !sub.IsClean || sub.HasActiveGitLock {
			return false
		}
	}
	return true
}

func toRepoRuntimeStatus(st RepoGitState) controlplane.RepoRuntimeStatus {
	return controlplane.RepoRuntimeStatus{
		RepoPath:            st.RepoPath,
		Alias:               st.Alias,
		IsClean:             st.IsClean,
		Branch:              st.Branch,
		HeadSHA:             st.HeadSHA,
		DirtyFilesCount:     st.DirtyFilesCount,
		UntrackedFilesCount: st.UntrackedFilesCount,
		Ahead:               st.Ahead,
		Behind:              st.Behind,
		HasActiveGitLock:    st.HasActiveGitLock,
		SubmodulesClean:     st.SubmodulesClean,
		DirtySubmodules:     st.DirtySubmodules,
		LastCheckedAt:       st.LastCheckedAt,
	}
}

// UpdateAndRemediateWorktree executes Step 0 (ephemeral artifact cleanup), Step 1 (shell "autopilot git-update"
// under repolock), and Step 2 (fallback escalation to iazio-harness agy with 60-minute dirty fingerprint cooldown)
// for a single worktree when no job/IN_PROGRESS ASE story is running on it.
func (m *RepoStateMonitor) UpdateAndRemediateWorktree(ctx context.Context, item RepoConfigItem) WorktreeGitUpdateResult {
	cleanPath := filepath.Clean(strings.TrimSpace(item.Path))
	res := WorktreeGitUpdateResult{
		RepoPath: cleanPath,
	}

	m.mu.RLock()
	sup := m.sup
	hostID := m.hostID
	shellFn := m.shellGitUpdate
	agyFn := m.agyEscalation
	resumeFn := m.resumeRepoQueue
	reportFn := m.reportStatuses
	cooldownDur := m.agyCooldownDur
	isAlive := m.isPIDAlive
	nowFn := m.nowFn
	m.mu.RUnlock()

	if shellFn == nil {
		shellFn = DefaultShellGitUpdate
	}
	if agyFn == nil {
		agyFn = DefaultAgyEscalation
	}
	if cooldownDur <= 0 {
		cooldownDur = DefaultAgyRemediationCooldown
	}
	if isAlive == nil {
		isAlive = isProcessAlive
	}
	if nowFn == nil {
		nowFn = time.Now
	}

	if ctx.Err() != nil {
		res.Skipped = true
		res.SkipReason = "context_canceled"
		return res
	}
	// Guard: skip if a harness job or IN_PROGRESS ASE story is active on this worktree.
	if !IsWorktreeIdleForGitUpdate(cleanPath, sup, isAlive) {
		res.Skipped = true
		res.SkipReason = "active_execution_or_in_progress_story"
		res.FinalState = InspectRepoGitState(ctx, cleanPath, item.Alias)
		return res
	}

	// Step 0: Sanitize idle ephemeral artifacts (.session_story_id, pr-closeout-proof-*.json, cov_auth)
	sanitized, _ := SanitizeIdleEphemeralArtifacts(ctx, cleanPath, sup, isAlive)
	res.SanitizedArtifacts = sanitized

	// Acquire worktree repolock before running Stage 1 / Stage 2 git-update
	lockHandle, lockErr := repolock.Acquire(cleanPath, repolock.LockInfo{
		JobID:  "autopilot-git-update",
		PID:    os.Getpid(),
		Status: "RUNNING",
	}, isAlive)
	if lockErr != nil {
		res.Skipped = true
		res.SkipReason = "repolock_busy"
		res.FinalState = InspectRepoGitState(ctx, cleanPath, item.Alias)
		return res
	}
	releaseLock := func() {
		if lockHandle != nil {
			_ = lockHandle.Release()
			_ = repolock.ForceRelease(cleanPath)
			lockHandle = nil
		}
	}
	defer releaseLock()

	// Step 1: Execute "autopilot git-update" directly via shell in the worktree root
	res.Stage1Executed = true
	step1Out, step1Err := shellFn(ctx, cleanPath)
	res.Stage1Output = step1Out

	// Release lock temporarily to inspect/sanitize without self-blocking IsWorktreeIdleForGitUpdate
	releaseLock()
	_, _ = SanitizeIdleEphemeralArtifacts(ctx, cleanPath, sup, isAlive)
	st := InspectRepoGitState(ctx, cleanPath, item.Alias)
	res.FinalState = st

	if step1Err == nil && isCleanOnMainSynced(st) {
		res.Stage1Success = true
		m.mu.Lock()
		delete(m.agyCooldowns, cleanPath)
		m.mu.Unlock()

		if reportFn != nil {
			_ = reportFn(ctx, hostID, []controlplane.RepoRuntimeStatus{toRepoRuntimeStatus(st)})
		}
		if resumeFn != nil {
			if err := resumeFn(ctx, hostID, cleanPath); err == nil {
				res.QueueResumed = true
			}
		}
		return res
	}

	// Step 2: Automatic escalation to iazio-harness (agy) remediation if no IN_PROGRESS ASE story is active
	if !IsWorktreeIdleForGitUpdate(cleanPath, sup, isAlive) {
		return res
	}

	headSHA, treeSHA, fp := ComputeWorktreeDirtyFingerprint(ctx, cleanPath)
	res.DirtyFingerprint = fp
	now := nowFn().UTC()

	m.mu.Lock()
	prevRec, hasPrev := m.agyCooldowns[cleanPath]
	if hasPrev && prevRec.Fingerprint == fp && now.Sub(prevRec.AttemptedAt) < cooldownDur {
		m.mu.Unlock()
		res.Stage2CooldownSkipped = true
		return res
	}
	m.agyCooldowns[cleanPath] = agyCooldownRecord{
		Fingerprint: fp,
		HeadSHA:     headSHA,
		TreeSHA:     treeSHA,
		AttemptedAt: now,
	}
	m.mu.Unlock()

	diagSummary := step1Out
	if diagSummary == "" && step1Err != nil {
		diagSummary = step1Err.Error()
	} else if step1Err != nil {
		diagSummary = fmt.Sprintf("%s (err: %v)", step1Out, step1Err)
	}
	if diagSummary == "" && st.Error != "" {
		diagSummary = st.Error
	}

	lockHandle2, lockErr2 := repolock.Acquire(cleanPath, repolock.LockInfo{
		JobID:  "autopilot-git-update-agy",
		PID:    os.Getpid(),
		Engine: "agy",
		Prompt: AgyGitUpdateRemediationPrompt,
		Status: "RUNNING",
	}, isAlive)
	if lockErr2 != nil {
		return res
	}
	res.Stage2Escalated = true
	step2Out, step2Err := agyFn(ctx, cleanPath, AgyGitUpdateRemediationPrompt, diagSummary)
	_ = lockHandle2.Release()
	_ = repolock.ForceRelease(cleanPath)
	res.Stage2Output = step2Out

	_, _ = SanitizeIdleEphemeralArtifacts(ctx, cleanPath, sup, isAlive)
	stAfter := InspectRepoGitState(ctx, cleanPath, item.Alias)
	res.FinalState = stAfter

	if step2Err == nil && isCleanOnMainSynced(stAfter) {
		res.Stage2Success = true
		m.mu.Lock()
		delete(m.agyCooldowns, cleanPath)
		m.mu.Unlock()

		if reportFn != nil {
			_ = reportFn(ctx, hostID, []controlplane.RepoRuntimeStatus{toRepoRuntimeStatus(stAfter)})
		}
		if resumeFn != nil {
			if err := resumeFn(ctx, hostID, cleanPath); err == nil {
				res.QueueResumed = true
			}
		}
	}

	return res
}

// RunGitUpdateCycle executes UpdateAndRemediateWorktree across all registered repos when no harness is active.
func (m *RepoStateMonitor) RunGitUpdateCycle(ctx context.Context) []WorktreeGitUpdateResult {
	if m.sup != nil && m.sup.HasRunningHarness() {
		return nil
	}
	if m.getRepos == nil {
		return nil
	}
	repos, err := m.getRepos()
	if err != nil || len(repos) == 0 {
		return nil
	}
	results := make([]WorktreeGitUpdateResult, 0, len(repos))
	for _, it := range repos {
		results = append(results, m.UpdateAndRemediateWorktree(ctx, it))
	}
	return results
}

// LastSnapshot returns the most recently captured fleet state snapshot.
func (m *RepoStateMonitor) LastSnapshot() FleetStateSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSnapshot
}

// InspectFleet performs one full inspection pass across registered repos.
func (m *RepoStateMonitor) InspectFleet(ctx context.Context) FleetStateSnapshot {
	m.inspectMu.Lock()
	defer m.inspectMu.Unlock()

	m.mu.RLock()
	lastSnap := m.lastSnapshot
	hostID := m.hostID
	remediateOnDirty := m.remediateBeforeHalt
	m.mu.RUnlock()

	now := time.Now().UTC()
	snap := FleetStateSnapshot{
		HostID:    hostID,
		CheckedAt: now,
		Repos:     make(map[string]RepoGitState),
	}

	// Execution Guard: if harness is active, skip inspection
	if m.sup != nil && m.sup.HasRunningHarness() {
		snap.HarnessActive = true
		snap.FleetGitClean = false
		if len(lastSnap.Repos) > 0 {
			snap.Repos = lastSnap.Repos
		}
		m.mu.Lock()
		m.lastSnapshot = snap
		m.mu.Unlock()
		return snap
	}

	var repos []RepoConfigItem
	if m.getRepos != nil {
		var err error
		repos, err = m.getRepos()
		if err != nil {
			// Fail-closed when repo inventory is unavailable
			snap.FleetGitClean = false
			snap.DirtyRepoPaths = []string{"repo_inventory_unavailable"}
			m.mu.Lock()
			m.lastSnapshot = snap
			m.mu.Unlock()
			return snap
		}
	}

	if len(repos) == 0 {
		snap.FleetGitClean = true
		m.mu.Lock()
		m.lastSnapshot = snap
		m.mu.Unlock()
		return snap
	}

	// Concurrent bounded inspection (up to 4 workers)
	type result struct {
		path  string
		state RepoGitState
	}
	resCh := make(chan result, len(repos))
	sem := make(chan struct{}, 4)

	var wg sync.WaitGroup
	for _, item := range repos {
		wg.Add(1)
		go func(it RepoConfigItem) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			st := InspectRepoGitState(ctx, it.Path, it.Alias)
			if remediateOnDirty && (!st.IsClean || st.Behind > 0) && IsWorktreeIdleForGitUpdate(it.Path, m.sup, m.isPIDAlive) {
				upRes := m.UpdateAndRemediateWorktree(ctx, it)
				st = upRes.FinalState
			}
			resCh <- result{path: it.Path, state: st}
		}(item)
	}

	wg.Wait()
	close(resCh)

	allClean := true
	var dirtyList []string
	var statuses []controlplane.RepoRuntimeStatus

	for r := range resCh {
		snap.Repos[r.path] = r.state
		if !r.state.IsClean {
			allClean = false
			dirtyList = append(dirtyList, r.path)
		}
		// Transition check
		if prev, ok := lastSnap.Repos[r.path]; ok {
			if prev.IsClean != r.state.IsClean && m.postNotif != nil {
				category := "warning"
				statusStr := "Dirty"
				if r.state.IsClean {
					category = "info"
					statusStr = "Clean"
				}
				if err := m.postNotif(ctx, controlplane.NotificationRecord{
					Title:    fmt.Sprintf("Repo Transitioned to %s", statusStr),
					Message:  fmt.Sprintf("Repository %s (%s) is now %s.", r.path, r.state.Branch, statusStr),
					Category: category,
					Source:   "repo-monitor",
					HostID:   m.hostID,
					Metadata: map[string]any{
						"event":       "repo.state_transition",
						"repo":        r.path,
						"branch":      r.state.Branch,
						"is_clean":    r.state.IsClean,
						"dirty_files": r.state.DirtyFilesCount,
					},
				}); err != nil {
					fmt.Fprintf(os.Stderr, "[repomonitor] postNotif error: %v\n", err)
				}
			}
		}

		statuses = append(statuses, toRepoRuntimeStatus(r.state))
	}

	sort.Strings(dirtyList)
	snap.FleetGitClean = allClean
	snap.DirtyRepoPaths = dirtyList

	m.mu.Lock()
	m.lastSnapshot = snap
	m.mu.Unlock()

	// Report to backend
	if m.reportStatuses != nil && len(statuses) > 0 {
		if rErr := m.reportStatuses(ctx, m.hostID, statuses); rErr != nil {
			fmt.Fprintf(os.Stderr, "[repomonitor] reportStatuses error: %v\n", rErr)
		}
	}

	// Level-triggered: evaluate onFleetClean on every inspection pass when no harness is active
	if !snap.HarnessActive && m.onFleetClean != nil {
		m.onFleetClean(snap)
	}

	return snap
}

// Start begins periodic background inspection and periodic configurable git-update loop.
func (m *RepoStateMonitor) Start(ctx context.Context) {
	m.bgWg.Add(2)
	go func() {
		defer m.bgWg.Done()
		// Run initial check after 3 seconds
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			if ctx.Err() == nil {
				m.InspectFleet(ctx)
			}
		}

		for {
			// Apply ±15s jitter to avoid synchronized fleet requests
			jitter := time.Duration(rand.Intn(30)-15) * time.Second
			waitDur := m.interval + jitter
			if waitDur < 45*time.Second {
				waitDur = 45 * time.Second
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(waitDur):
				if ctx.Err() == nil {
					m.InspectFleet(ctx)
				}
			}
		}
	}()

	go func() {
		defer m.bgWg.Done()
		for {
			gitInterval := m.GitUpdateInterval()
			select {
			case <-ctx.Done():
				return
			case <-time.After(gitInterval):
				if ctx.Err() == nil {
					m.RunGitUpdateCycle(ctx)
				}
			}
		}
	}()
}

// Wait blocks until background goroutines started by Start have exited.
func (m *RepoStateMonitor) Wait() {
	m.bgWg.Wait()
}
