package supervisor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/agyhooks"
	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-core/pkg/procutil"
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

func storyCandidateDirsForRepo(repoPath, sid string) []string {
	candidateDirs := []string{
		filepath.Join(repoPath, "docs", "stories"),
		filepath.Join(filepath.Dir(repoPath), "docs", "stories"),
	}
	parts := strings.Split(sid, "-")
	if len(parts) >= 3 {
		cat := strings.ToLower(parts[1])
		switch cat {
		case "vigo":
			candidateDirs = append(candidateDirs,
				filepath.Join(repoPath, "vigo", "docs", "stories"),
				filepath.Join(repoPath, "vigo-api", "docs", "stories"),
			)
		case "iazio":
			candidateDirs = append(candidateDirs,
				filepath.Join(repoPath, "iazio-infra", "docs", "stories"),
				filepath.Join(repoPath, "iazio", "docs", "stories"),
			)
		case "gov", "auto", "ap":
			candidateDirs = append(candidateDirs,
				filepath.Join(repoPath, "autopilot-umbrella", "docs", "stories"),
				filepath.Join(repoPath, "iazio-infra", "docs", "stories"),
			)
		case "eye":
			candidateDirs = append(candidateDirs,
				filepath.Join(repoPath, "eye-habit", "docs", "stories"),
			)
		}
	}
	return candidateDirs
}

func isStoryMarkedDoneOnDisk(repoPath, storyID string) bool {
	sid := strings.ToUpper(strings.TrimSpace(storyID))
	if repoPath == "" || !strings.HasPrefix(sid, "STORY-") {
		return false
	}

	if b, err := os.ReadFile(filepath.Join(repoPath, ".autopilot", "ase-state.json")); err == nil {
		var aseSt struct {
			StoryID string `json:"story_id"`
			Status  string `json:"status"`
			Phase   string `json:"phase"`
		}
		if json.Unmarshal(b, &aseSt) == nil && strings.EqualFold(strings.TrimSpace(aseSt.StoryID), sid) {
			if strings.EqualFold(aseSt.Status, "DONE") || strings.EqualFold(aseSt.Phase, "completed") {
				return true
			}
		}
	}

	lowerID := strings.ToLower(strings.TrimPrefix(sid, "STORY-"))
	fullLower := strings.ToLower(sid)
	for _, storiesDir := range storyCandidateDirsForRepo(repoPath, sid) {
		entries, err := os.ReadDir(storiesDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(e.Name(), ".story.md") {
					continue
				}
				nameLower := strings.ToLower(e.Name())
				if !strings.Contains(nameLower, lowerID) && !strings.Contains(nameLower, fullLower) {
					continue
				}
				if b, rErr := os.ReadFile(filepath.Join(storiesDir, e.Name())); rErr == nil {
					content := string(b)
					if strings.Contains(strings.ToUpper(content), sid) {
						if isStoryMarkdownIncompleteInRepo(repoPath, sid, content) {
							return false
						}
						if strings.Contains(content, "status: DONE") || strings.Contains(content, "status: \"DONE\"") || strings.Contains(content, "status: 'DONE'") {
							return true
						}
						if strings.Contains(content, "status: IN_PROGRESS") || strings.Contains(content, "status: \"IN_PROGRESS\"") {
							return false
						}
					}
				}
			}
		}
		if indexBytes, idxErr := os.ReadFile(filepath.Join(storiesDir, "index.yml")); idxErr == nil {
			lines := strings.Split(string(indexBytes), "\n")
			for i, l := range lines {
				if strings.Contains(strings.ToUpper(l), sid) {
					end := i + 8
					if end > len(lines) {
						end = len(lines)
					}
					for j := i; j < end; j++ {
						trimmed := strings.TrimSpace(lines[j])
						if j > i && strings.HasPrefix(trimmed, "- id:") {
							break
						}
						if strings.HasPrefix(strings.ToLower(trimmed), "status:") {
							val := strings.Trim(strings.TrimSpace(trimmed[len("status:"):]), "\"'")
							if strings.EqualFold(val, "DONE") {
								if hasUnresolvedStorySIRInRepo(repoPath, sid) {
									return false
								}
								return true
							}
							break
						}
					}
				}
			}
		}
	}
	return false
}

func extractStoryMarkdownSection(content, headingSubstr string) string {
	lines := strings.Split(content, "\n")
	inSection := false
	var out []string
	target := strings.ToLower(strings.TrimSpace(headingSubstr))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			if inSection {
				break
			}
			if strings.Contains(strings.ToLower(trimmed), target) {
				inSection = true
			}
			continue
		}
		if inSection {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func hasUnresolvedStorySIRInRepo(repoPath, storyID string) bool {
	sid := strings.ToLower(strings.TrimSpace(storyID))
	if repoPath == "" || sid == "" {
		return false
	}
	shortID := strings.TrimPrefix(sid, "story-")
	patterns := []string{
		filepath.Join(repoPath, "tmp", "reports", fmt.Sprintf("intervention-%s-sir-*.json", sid)),
		filepath.Join(repoPath, "tmp", "reports", fmt.Sprintf("intervention-story-%s-sir-*.json", shortID)),
	}
	for _, pat := range patterns {
		matches, err := filepath.Glob(pat)
		if err != nil {
			continue
		}
		for _, m := range matches {
			b, rErr := os.ReadFile(m)
			if rErr != nil {
				continue
			}
			s := strings.ToUpper(string(b))
			if strings.Contains(s, `"STATUS": "OPEN"`) || strings.Contains(s, `"STATUS":"OPEN"`) {
				return true
			}
		}
	}
	return false
}

func isStoryMarkdownIncompleteInRepo(repoPath, storyID, content string) bool {
	if dodSection := extractStoryMarkdownSection(content, "Definition of Done"); dodSection != "" {
		if strings.Contains(dodSection, "- [ ]") {
			return true
		}
	}
	if revSection := extractStoryMarkdownSection(content, "Reviews"); revSection != "" {
		if strings.Contains(strings.ToLower(revSection), "_pending_") {
			return true
		}
	}
	return hasUnresolvedStorySIRInRepo(repoPath, storyID)
}

func isStoryExplicitlyInProgressOnDisk(repoPath, storyID string) bool {
	sid := strings.ToUpper(strings.TrimSpace(storyID))
	if repoPath == "" || !strings.HasPrefix(sid, "STORY-") {
		return false
	}
	lowerID := strings.ToLower(strings.TrimPrefix(sid, "STORY-"))
	fullLower := strings.ToLower(sid)
	for _, storiesDir := range storyCandidateDirsForRepo(repoPath, sid) {
		entries, err := os.ReadDir(storiesDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".story.md") {
				continue
			}
			nameLower := strings.ToLower(e.Name())
			if !strings.Contains(nameLower, lowerID) && !strings.Contains(nameLower, fullLower) {
				continue
			}
			if b, rErr := os.ReadFile(filepath.Join(storiesDir, e.Name())); rErr == nil {
				content := string(b)
				if strings.Contains(strings.ToUpper(content), sid) {
					if strings.Contains(content, "status: IN_PROGRESS") || strings.Contains(content, "status: \"IN_PROGRESS\"") ||
						strings.Contains(content, "status: 'IN_PROGRESS'") ||
						strings.Contains(content, "status: READY") || strings.Contains(content, "status: \"READY\"") ||
						isStoryMarkdownIncompleteInRepo(repoPath, sid, content) {
						return true
					}
				}
			}
		}
	}
	return false
}

var (
	branchExplicitStoryRe = regexp.MustCompile(`(?i)\b(STORY-[A-Z0-9]+-[0-9]+)\b`)
	branchSlugStoryRe     = regexp.MustCompile(`(?i)^(?:chore|feat|fix|refactor|docs|test|ci|perf|build)/(?:story-)?([a-z]{2,10})-([0-9]{2,6})(?:[-/]|$)`)
)

// InferStoryIDFromBranch extracts a canonical STORY-<CAT>-<NUM> identifier from a story branch name
// (e.g. "feat/gov-5209-pointer-pr-rebase-rollback-and-fleet-healing" -> "STORY-GOV-5209") or submodule status entry.
func InferStoryIDFromBranch(branch string) string {
	b := strings.TrimSpace(branch)
	if b == "" || b == "main" || b == "master" || b == "HEAD" || b == "detached" {
		return ""
	}
	if idx := strings.Index(b, "(on "); idx >= 0 {
		b = strings.TrimSuffix(strings.TrimSpace(b[idx+4:]), ")")
	}
	if m := branchExplicitStoryRe.FindStringSubmatch(b); len(m) == 2 {
		return strings.ToUpper(m[1])
	}
	if m := branchSlugStoryRe.FindStringSubmatch(b); len(m) == 3 {
		return fmt.Sprintf("STORY-%s-%s", strings.ToUpper(m[1]), m[2])
	}
	return ""
}

// IsWorktreeIdleForGitUpdate returns true when repolock is idle (no active iazio-harness / agy process running)
// and there is no active IN_PROGRESS ASE story in .autopilot/ase-state.json, git HEAD story branch, or .session_story_id.
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

	// 1. Check in-flight branch directly from git HEAD or submodules:
	// Any non-main, non-maintenance feature/fix/chore branch has not yet landed to main and is NEVER idle.
	if brOut, brErr := procutil.Command("git", "-C", cleanPath, "symbolic-ref", "--short", "-q", "HEAD").Output(); brErr == nil {
		br := strings.TrimSpace(string(brOut))
		if br != "" && br != "main" && br != "master" && br != "HEAD" && !isAutomatedMaintenanceBranchInRepo(br) {
			return false
		}
	}
	if hasAnyNonMainSubmoduleInRepo(cleanPath) || hasActiveNonMainSubmoduleStoryInRepo(cleanPath) {
		return false
	}

	// 2. Check active ASE state from .autopilot/ase-state.json
	aseStatePath := filepath.Join(cleanPath, ".autopilot", "ase-state.json")
	if b, err := os.ReadFile(aseStatePath); err == nil {
		var as struct {
			StoryID string `json:"story_id"`
			Status  string `json:"status"`
			Phase   string `json:"phase"`
		}
		if json.Unmarshal(b, &as) == nil {
			status := strings.ToUpper(strings.TrimSpace(as.Status))
			if status == "IN_PROGRESS" || status == "RUNNING" {
				if as.StoryID == "" || !isStoryMarkedDoneOnDisk(cleanPath, as.StoryID) {
					return false
				}
			}
		}
	}

	// 3. Check active state from .autopilot/state.json
	statePath := filepath.Join(cleanPath, ".autopilot", "state.json")
	if b, err := os.ReadFile(statePath); err == nil {
		var s struct {
			State   string `json:"state"`
			StoryID string `json:"story_id"`
		}
		if json.Unmarshal(b, &s) == nil {
			st := strings.ToUpper(strings.TrimSpace(s.State))
			if st == "IN_PROGRESS" || st == "RUNNING" {
				if s.StoryID == "" || !isStoryMarkedDoneOnDisk(cleanPath, s.StoryID) {
					return false
				}
			}
		}
	}

	// 4. Check active session story from .session_story_id
	sessionPath := filepath.Join(cleanPath, ".session_story_id")
	if b, err := os.ReadFile(sessionPath); err == nil {
		storyID := strings.TrimSpace(string(b))
		if storyID != "" && !isStoryMarkedDoneOnDisk(cleanPath, storyID) {
			if isStoryExplicitlyInProgressOnDisk(cleanPath, storyID) || hasUnresolvedStorySIRInRepo(cleanPath, storyID) {
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
			if hasAnyNonMainSubmoduleInRepo(cleanPath) {
				return false
			}
		}
	}

	// 5. Check active in-flight Antigravity brain transcript ASE turn for this repository
	if active, _, _ := InspectActiveBrainASETurnForRepo(cleanPath, time.Now()); active {
		return false
	}

	return true
}

func readWorktreeHeadBranchFast(dir string) (string, bool) {
	gitPath := filepath.Join(dir, ".git")
	fi, err := os.Stat(gitPath)
	if err != nil {
		return "", false
	}
	gitDir := gitPath
	if !fi.IsDir() {
		b, rErr := os.ReadFile(gitPath)
		if rErr != nil {
			return "", false
		}
		content := strings.TrimSpace(string(b))
		if strings.HasPrefix(content, "gitdir:") {
			target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
			if !filepath.IsAbs(target) {
				target = filepath.Join(dir, target)
			}
			gitDir = target
		}
	}
	headBytes, hErr := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if hErr != nil {
		return "", false
	}
	headStr := strings.TrimSpace(string(headBytes))
	if strings.HasPrefix(headStr, "ref: refs/heads/") {
		return strings.TrimPrefix(headStr, "ref: refs/heads/"), true
	}
	return "detached", true
}

func hasAnyNonMainSubmoduleInRepo(repoPath string) bool {
	for _, sub := range DiscoverSubRepositories(repoPath) {
		subDir := filepath.Join(repoPath, sub)
		if br, ok := readWorktreeHeadBranchFast(subDir); ok && br != "main" && br != "master" && br != "detached" {
			return true
		}
	}
	return false
}

func hasActiveNonMainSubmoduleStoryInRepo(repoPath string) bool {
	for _, sub := range DiscoverSubRepositories(repoPath) {
		subDir := filepath.Join(repoPath, sub)
		if br, ok := readWorktreeHeadBranchFast(subDir); ok && br != "main" && br != "master" && br != "detached" {
			if inferred := InferStoryIDFromBranch(br); inferred != "" && !isStoryMarkedDoneOnDisk(repoPath, inferred) {
				return true
			}
		}
	}
	return false
}

func isAutomatedMaintenanceBranchInRepo(branch string) bool {
	b := strings.ToLower(strings.TrimSpace(branch))
	return strings.HasPrefix(b, "chore/governance-pointers-") ||
		strings.HasPrefix(b, "chore/submodule-pointers-") ||
		strings.HasPrefix(b, "chore/auto-sync-")
}

func readRebaseHeadBranchInRepo(gitDir string) (branch string, sentinelPath string, modTime time.Time) {
	for _, name := range []string{"rebase-merge", "rebase-apply"} {
		cand := filepath.Join(gitDir, name)
		fi, err := os.Stat(cand)
		if err != nil {
			continue
		}
		latest := fi.ModTime()
		for _, f := range []string{"head-name", "done", "git-rebase-todo", "stopped-sha", "orig-head"} {
			if sfi, sErr := os.Stat(filepath.Join(cand, f)); sErr == nil && sfi.ModTime().After(latest) {
				latest = sfi.ModTime()
			}
		}
		if b, rErr := os.ReadFile(filepath.Join(cand, "head-name")); rErr == nil {
			raw := strings.TrimSpace(string(b))
			raw = strings.TrimPrefix(raw, "refs/heads/")
			if raw != "" {
				return raw, cand, latest
			}
		}
	}
	return "", "", time.Time{}
}

func healStrandedMaintenanceBranchBeforeGitUpdate(ctx context.Context, cleanPath string) {
	NormalizeGitAlternatesPaths(cleanPath)
	gitDir := filepath.Join(cleanPath, ".git")
	if rebBr, rebSentinel, _ := readRebaseHeadBranchInRepo(gitDir); rebBr != "" && isAutomatedMaintenanceBranchInRepo(rebBr) {
		if procutil.CommandContext(ctx, "git", "-C", cleanPath, "rebase", "--abort").Run() != nil {
			_ = os.RemoveAll(rebSentinel)
			_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "reset", "--hard", "HEAD").Run()
		}
		_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "checkout", "-f", "main").Run()
		fetchCmd := procutil.CommandContext(ctx, "git", "-C", cleanPath, "fetch", "origin", "main")
		fetchCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if fetchCmd.Run() == nil {
			_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "reset", "--hard", "origin/main").Run()
		}
		_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "branch", "-D", rebBr).Run()
		if sirs, gErr := filepath.Glob(filepath.Join(cleanPath, "tmp", "reports", "intervention-git-update-sir-*.json")); gErr == nil {
			for _, sir := range sirs {
				_ = os.Remove(sir)
			}
		}
		return
	}

	brOut, brErr := procutil.CommandContext(ctx, "git", "-C", cleanPath, "symbolic-ref", "--short", "-q", "HEAD").Output()
	if brErr != nil {
		return
	}
	br := strings.TrimSpace(string(brOut))
	if !isAutomatedMaintenanceBranchInRepo(br) {
		return
	}
	_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "checkout", "-f", "main").Run()
	fetchCmd := procutil.CommandContext(ctx, "git", "-C", cleanPath, "fetch", "origin", "main")
	fetchCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if fetchCmd.Run() == nil {
		_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "reset", "--hard", "origin/main").Run()
	}
	_ = procutil.CommandContext(ctx, "git", "-C", cleanPath, "branch", "-D", br).Run()
	if sirs, gErr := filepath.Glob(filepath.Join(cleanPath, "tmp", "reports", "intervention-git-update-sir-*.json")); gErr == nil {
		for _, sir := range sirs {
			_ = os.Remove(sir)
		}
	}
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

	syncFailedMarker := filepath.Join(dir, ".iazio", "post-ase-sync-failed")
	if b, rErr := os.ReadFile(syncFailedMarker); rErr == nil {
		lower := strings.ToLower(strings.TrimSpace(string(b)))
		if lower == "" || strings.Contains(lower, strings.ToLower(repolock.ErrJobDeferredText)) || strings.Contains(lower, "queued/deferred") {
			if os.Remove(syncFailedMarker) == nil {
				record(".iazio/post-ase-sync-failed")
			}
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

var supervisorTranscriptBoundStoryRe = regexp.MustCompile(`(?i)(?:--story(?:-id)?[=\s\\"']+|story\s+begin\s+|/autonomous-story-execution[^\n]{0,120}?|(?:^|[^a-z0-9_])story_id[\\"\s:=]+|session_story_id[^\n]{0,80}?)(STORY-[A-Z0-9]+-[0-9]+)`)

func canonicalizeSupervisorTranscriptPath(s string) string {
	lower := strings.ToLower(s)
	norm := strings.ReplaceAll(lower, `\\`, `/`)
	if !strings.Contains(norm, `:\`) {
		norm = strings.ReplaceAll(norm, `\r\n`, "\n")
		norm = strings.ReplaceAll(norm, `\n`, "\n")
		norm = strings.ReplaceAll(norm, `\t`, " ")
	}
	norm = strings.ReplaceAll(norm, `\"`, `"`)
	norm = strings.ReplaceAll(norm, `\`, `/`)
	for strings.Contains(norm, "//") {
		norm = strings.ReplaceAll(norm, "//", "/")
	}
	norm = strings.ReplaceAll(norm, "/private/var/folders/", "/var/folders/")
	norm = strings.ReplaceAll(norm, "/private/tmp/", "/tmp/")
	for i := 0; i < 26; i++ {
		ch := string(rune('a' + i))
		wslPrefix := "/mnt/" + ch + "/"
		winPrefix := ch + ":/"
		if strings.Contains(norm, wslPrefix) {
			norm = strings.ReplaceAll(norm, wslPrefix, winPrefix)
		}
	}
	return norm
}

func stripSupervisorTrailingWorktreeNumber(cleanPath string) string {
	idx := strings.LastIndexByte(cleanPath, '-')
	slashIdx := strings.LastIndexByte(cleanPath, '/')
	if idx <= slashIdx+1 || idx >= len(cleanPath)-1 {
		return cleanPath
	}
	for i := idx + 1; i < len(cleanPath); i++ {
		if cleanPath[i] < '0' || cleanPath[i] > '9' {
			return cleanPath
		}
	}
	return cleanPath[:idx]
}

func isSupervisorTranscriptBoundaryChar(ch byte) bool {
	return ch == '/' || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
		ch == '"' || ch == '\'' || ch == ':' || ch == ';' ||
		ch == ',' || ch == ')' || ch == ']' || ch == '}' || ch == '>'
}

func countSupervisorWorktreeFamilyOccurrences(searchAll, cleanRepo string) (selfCount int, maxOtherSiblingCount int) {
	familyPrefix := stripSupervisorTrailingWorktreeNumber(cleanRepo)
	counts := make(map[string]int)
	search := searchAll
	for {
		idx := strings.Index(search, familyPrefix)
		if idx < 0 {
			break
		}
		afterPrefix := idx + len(familyPrefix)
		validBefore := true
		if idx > 0 && !strings.HasPrefix(familyPrefix, "/") {
			prev := search[idx-1]
			if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' || prev == '-' {
				validBefore = false
			}
		}
		advance := afterPrefix
		if validBefore {
			candPath := familyPrefix
			afterCand := afterPrefix
			if afterPrefix < len(search) && search[afterPrefix] == '-' {
				j := afterPrefix + 1
				for j < len(search) && search[j] >= '0' && search[j] <= '9' {
					j++
				}
				if j > afterPrefix+1 {
					candPath = search[idx:j]
					afterCand = j
					advance = j
				}
			}
			if afterCand >= len(search) {
				counts[candPath]++
			} else {
				rest := search[afterCand:]
				nextCh := search[afterCand]
				if nextCh != '-' && !strings.HasPrefix(rest, "/.git/objects") && !strings.HasPrefix(rest, "/.git/modules") {
					if isSupervisorTranscriptBoundaryChar(nextCh) {
						counts[candPath]++
					}
				}
			}
		}
		if advance <= 0 || advance > len(search) {
			break
		}
		search = search[advance:]
	}
	selfCount = counts[cleanRepo]
	for k, v := range counts {
		if k != cleanRepo && v > maxOtherSiblingCount {
			maxOtherSiblingCount = v
		}
	}
	return selfCount, maxOtherSiblingCount
}

// IsStoryOwnedBySiblingWorktree returns true when storyID is not explicitly active in repoPath
// and a sibling worktree in the same parent directory explicitly owns storyID.
func IsStoryOwnedBySiblingWorktree(repoPath, storyID string) bool {
	sid := strings.ToUpper(strings.TrimSpace(storyID))
	cleanRepo := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanRepo == "" || cleanRepo == "." || !strings.HasPrefix(sid, "STORY-") {
		return false
	}
	if b, err := os.ReadFile(filepath.Join(cleanRepo, ".session_story_id")); err == nil &&
		strings.EqualFold(strings.TrimSpace(string(b)), sid) {
		return false
	}
	if br, ok := readWorktreeHeadBranchFast(cleanRepo); ok && strings.EqualFold(InferStoryIDFromBranch(br), sid) {
		return false
	}
	if isStoryExplicitlyInProgressOnDisk(cleanRepo, sid) {
		return false
	}

	parentDir := filepath.Dir(cleanRepo)
	familyBase := stripSupervisorTrailingWorktreeNumber(strings.ToLower(filepath.Base(cleanRepo)))
	entries, err := os.ReadDir(parentDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		candNameLower := strings.ToLower(e.Name())
		if stripSupervisorTrailingWorktreeNumber(candNameLower) != familyBase {
			continue
		}
		sibPath := filepath.Clean(filepath.Join(parentDir, e.Name()))
		if sibPath == cleanRepo {
			continue
		}
		if b, sErr := os.ReadFile(filepath.Join(sibPath, ".session_story_id")); sErr == nil &&
			strings.EqualFold(strings.TrimSpace(string(b)), sid) {
			return true
		}
		if lockInfo, lErr := repolock.Read(sibPath); lErr == nil && strings.EqualFold(strings.TrimSpace(lockInfo.StoryID), sid) {
			return true
		}
		if br, ok := readWorktreeHeadBranchFast(sibPath); ok && strings.EqualFold(InferStoryIDFromBranch(br), sid) {
			return true
		}
		if isStoryExplicitlyInProgressOnDisk(sibPath, sid) {
			return true
		}
	}
	return false
}

func hasExplicitSupervisorIdleStateFile(repoPath string) bool {
	if strings.TrimSpace(repoPath) == "" {
		return false
	}
	if b, err := os.ReadFile(filepath.Join(repoPath, ".autopilot", "ase-state.json")); err == nil {
		var as struct {
			Status string `json:"status"`
			Phase  string `json:"phase"`
		}
		if json.Unmarshal(b, &as) == nil {
			if strings.EqualFold(strings.TrimSpace(as.Status), "DONE") ||
				strings.EqualFold(strings.TrimSpace(as.Status), "COMPLETED") ||
				strings.EqualFold(strings.TrimSpace(as.Phase), "completed") {
				return true
			}
		}
	}
	if b, err := os.ReadFile(filepath.Join(repoPath, ".autopilot", "state.json")); err == nil {
		var s struct {
			State   string `json:"state"`
			StoryID string `json:"story_id"`
		}
		if json.Unmarshal(b, &s) == nil {
			if strings.EqualFold(strings.TrimSpace(s.State), "IDLE") && strings.TrimSpace(s.StoryID) == "" {
				return true
			}
		}
	}
	return false
}

func canBindSupervisorTranscriptStoryToRepo(repoPath, storyID string) bool {
	sid := strings.ToUpper(strings.TrimSpace(storyID))
	if !strings.HasPrefix(sid, "STORY-") {
		return false
	}
	// Automated submodule pointer maintenance synthetic IDs are never ASE stories.
	if strings.HasPrefix(sid, "STORY-GOV-POINTERS-") || sid == "STORY-GOV-POINTERS" {
		return false
	}
	if isStoryMarkedDoneOnDisk(repoPath, sid) || IsStoryOwnedBySiblingWorktree(repoPath, sid) {
		return false
	}
	hasSession := false
	if b, err := os.ReadFile(filepath.Join(repoPath, ".session_story_id")); err == nil {
		hasSession = strings.HasPrefix(strings.ToUpper(strings.TrimSpace(string(b))), "STORY-")
	}
	if strings.TrimSpace(repoPath) != "" && hasExplicitSupervisorIdleStateFile(repoPath) && !hasSession {
		if br, ok := readWorktreeHeadBranchFast(repoPath); !ok || br == "main" || br == "master" {
			return isStoryExplicitlyInProgressOnDisk(repoPath, sid)
		}
	}
	return true
}

// TranscriptReferencesRepoPath returns true if transcript text references repoPath across Windows/WSL path formats
// with strict path boundary verification (rejecting sibling suffixes like "-04", .git/objects alternate references,
// and incidental prose mentions when a numbered sibling worktree has dominant occurrences in the transcript).
func TranscriptReferencesRepoPath(text, repoPath string) bool {
	cleanRepo := strings.TrimRight(canonicalizeSupervisorTranscriptPath(strings.TrimSpace(repoPath)), "/")
	if cleanRepo == "" || cleanRepo == "." {
		return false
	}
	searchAll := canonicalizeSupervisorTranscriptPath(text)
	if cwdMarker := "command working directory: "; strings.Contains(searchAll, cwdMarker) {
		searchCWD := searchAll
		for {
			idx := strings.Index(searchCWD, cwdMarker)
			if idx < 0 {
				break
			}
			rest := searchCWD[idx+len(cwdMarker):]
			if nl := strings.IndexAny(rest, "\r\n\"'"); nl >= 0 {
				rest = rest[:nl]
			}
			if cutIdx := strings.Index(rest, "/napp data directory:"); cutIdx >= 0 {
				rest = rest[:cutIdx]
			}
			declaredCWD := strings.TrimRight(strings.TrimSpace(rest), "/")
			if strings.HasPrefix(declaredCWD, "/") || (len(declaredCWD) >= 3 && declaredCWD[1] == ':' && declaredCWD[2] == '/') {
				return declaredCWD == cleanRepo
			}
			searchCWD = searchCWD[idx+len(cwdMarker):]
		}
	}

	// Also check for explicit CLI/harness worktree argument markers
	for _, marker := range []string{"--worktree-path ", "--worktree-path=", "--worktree_path ", "--worktree_path="} {
		if strings.Contains(searchAll, marker) {
			searchMarker := searchAll
			for {
				idx := strings.Index(searchMarker, marker)
				if idx < 0 {
					break
				}
				rest := searchMarker[idx+len(marker):]
				rest = strings.TrimLeft(rest, `"'`)
				if endIdx := strings.IndexAny(rest, " \t\r\n\"'"); endIdx >= 0 {
					rest = rest[:endIdx]
				}
				declaredWT := strings.TrimRight(strings.TrimSpace(rest), "/")
				if declaredWT != "" && (strings.HasPrefix(declaredWT, "/") || (len(declaredWT) >= 3 && declaredWT[1] == ':' && declaredWT[2] == '/')) {
					familyPrefix := stripSupervisorTrailingWorktreeNumber(cleanRepo)
					if strings.HasPrefix(declaredWT, familyPrefix) {
						return declaredWT == cleanRepo
					}
				}
				searchMarker = searchMarker[idx+len(marker):]
			}
		}
	}

	selfCount, maxOtherSiblingCount := countSupervisorWorktreeFamilyOccurrences(searchAll, cleanRepo)
	if selfCount == 0 || maxOtherSiblingCount >= selfCount {
		return false
	}
	return true
}

func isTempSupervisorHomeDir(p string) bool {
	slash := strings.ToLower(filepath.ToSlash(strings.TrimSpace(p)))
	if slash == "" {
		return false
	}
	return strings.Contains(slash, "/tmp/") ||
		strings.Contains(slash, "/var/folders/") ||
		strings.Contains(slash, "/appdata/local/temp/") ||
		strings.Contains(slash, "teste2e_") ||
		strings.Contains(slash, "go-build")
}

func supervisorBrainRootDirs() []string {
	if custom := strings.TrimSpace(os.Getenv("IAZIO_BRAIN_DIR")); custom != "" {
		dirs := []string{filepath.Clean(custom)}
		if filepath.Base(filepath.Clean(custom)) != "brain" {
			sub := filepath.Join(filepath.Clean(custom), "brain")
			if fi, err := os.Stat(sub); err == nil && fi.IsDir() {
				dirs = append([]string{sub}, dirs...)
			}
		}
		return dirs
	}
	seen := make(map[string]bool)
	var dirs []string
	addDir := func(d string) {
		clean := filepath.Clean(strings.TrimSpace(d))
		if clean == "" || seen[clean] {
			return
		}
		if fi, err := os.Stat(clean); err == nil && fi.IsDir() {
			seen[clean] = true
			dirs = append(dirs, clean)
		}
	}
	home, _ := os.UserHomeDir()
	if home != "" {
		addDir(filepath.Join(home, ".gemini", "antigravity-cli", "brain"))
		if isTempSupervisorHomeDir(home) {
			return dirs
		}
	}
	if up := strings.TrimSpace(os.Getenv("USERPROFILE")); up != "" {
		addDir(filepath.Join(up, ".gemini", "antigravity-cli", "brain"))
		if isTempSupervisorHomeDir(up) {
			return dirs
		}
	}
	if runtime.GOOS == "linux" {
		if matches, err := filepath.Glob("/mnt/c/Users/*/.gemini/antigravity-cli/brain"); err == nil {
			for _, m := range matches {
				addDir(m)
			}
		}
	}
	return dirs
}

func isSupervisorAutomatedMaintenanceRepoLock(repoPath string) bool {
	clean := filepath.Clean(strings.TrimSpace(repoPath))
	if clean == "" || clean == "." {
		return false
	}
	info, err := repolock.Read(clean)
	if err != nil {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(info.Status))
	if status == "COMPLETED" || status == "FAILED" || status == "IDLE" || repolock.IsExpiredMaintenanceLock(info) {
		return false
	}
	alive := (info.PID > 0 && isProcessAlive(info.PID)) ||
		(info.HarnessPID > 0 && isProcessAlive(info.HarnessPID)) ||
		(info.ToolPID > 0 && isProcessAlive(info.ToolPID))
	if !alive {
		return false
	}
	jid := strings.ToLower(strings.TrimSpace(info.JobID))
	prompt := strings.ToLower(strings.TrimSpace(info.Prompt))
	return jid == "autopilot-git-update" ||
		jid == "autopilot-git-update-agy" ||
		strings.HasPrefix(jid, "git-update-") ||
		strings.Contains(prompt, "autopilot git-update")
}

func stripSupervisorTranscriptToolResponseLines(s string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		lowerLine := strings.ToLower(line)
		if strings.Contains(lowerLine, `"type":"tool_response"`) || strings.Contains(lowerLine, `"type": "tool_response"`) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func isSupervisorAutomatedMaintenanceTranscript(s string) bool {
	lower := strings.ToLower(stripSupervisorTranscriptToolResponseLines(s))
	return strings.Contains(lower, `execute shell prompt "autopilot git-update"`) ||
		strings.Contains(lower, `execute shell prompt \"autopilot git-update\"`) ||
		strings.Contains(lower, "diagnostic failure summary from shell autopilot git-update") ||
		strings.Contains(lower, "iazio-agy-log-git-update-agy-")
}

func extractSupervisorContextSummaryUserRequests(s string) string {
	const openTag = "<context_summary>"
	const closeTag = "</context_summary>"
	const reqHeader = "# user requests"
	lowerAll := strings.ToLower(s)
	preserveOriginal := len(lowerAll) == len(s)
	var collected []string
	offset := 0
	for offset < len(lowerAll) {
		relStart := strings.Index(lowerAll[offset:], openTag)
		if relStart < 0 {
			break
		}
		blockStart := offset + relStart + len(openTag)
		blockLower := lowerAll[blockStart:]
		advance := len(lowerAll)
		if relEnd := strings.Index(blockLower, closeTag); relEnd >= 0 {
			blockLower = blockLower[:relEnd]
			advance = blockStart + relEnd + len(closeTag)
		}
		if reqIdx := strings.Index(blockLower, reqHeader); reqIdx >= 0 {
			reqSecLower := blockLower[reqIdx:]
			cut := len(reqSecLower)
			for _, sep := range []string{"\n# ", `\n# `, "\n</context_summary>", `\n</context_summary>`} {
				if idx := strings.Index(reqSecLower[len(reqHeader):], sep); idx >= 0 {
					cand := len(reqHeader) + idx
					if cand < cut {
						cut = cand
					}
				}
			}
			if preserveOriginal && blockStart+reqIdx+cut <= len(s) {
				collected = append(collected, s[blockStart+reqIdx:blockStart+reqIdx+cut])
			} else {
				collected = append(collected, reqSecLower[:cut])
			}
		}
		offset = advance
	}
	return strings.Join(collected, "\n")
}

func stripSupervisorSystemTagRange(s, openTag, closeTag string) string {
	for {
		lower := strings.ToLower(s)
		start := strings.Index(lower, openTag)
		if start < 0 {
			break
		}
		rest := lower[start+len(openTag):]
		endRel := strings.Index(rest, closeTag)
		if endRel >= 0 {
			end := start + len(openTag) + endRel + len(closeTag)
			s = s[:start] + " " + s[end:]
			continue
		}
		reqIdx := strings.Index(rest, "<user_request>")
		nlIdx := strings.IndexByte(rest, '\n')
		cut := len(s)
		if reqIdx >= 0 && (nlIdx < 0 || reqIdx < nlIdx) {
			cut = start + len(openTag) + reqIdx
		} else if nlIdx >= 0 {
			cut = start + len(openTag) + nlIdx
		}
		s = s[:start] + " " + s[cut:]
	}
	return s
}

func stripSupervisorTranscriptBoilerplateAndToolOutputs(s string) string {
	if s == "" {
		return ""
	}
	out := stripSupervisorTranscriptToolResponseLines(s)
	preservedRequests := extractSupervisorContextSummaryUserRequests(out)
	for _, pair := range [][2]string{
		{"<user_rules>", "</user_rules>"},
		{"<rule[", "</rule["},
		{"<skills>", "</skills>"},
		{"<subagents>", "</subagents>"},
		{"<slash_commands>", "</slash_commands>"},
		{"<mcp_servers>", "</mcp_servers>"},
		{"<context_summary>", "</context_summary>"},
	} {
		out = stripSupervisorSystemTagRange(out, pair[0], pair[1])
	}
	if preservedRequests != "" {
		out += "\n" + preservedRequests
	}
	return out
}

func containsSupervisorASEMarker(s string) bool {
	if isSupervisorAutomatedMaintenanceTranscript(s) {
		return false
	}
	cleaned := stripSupervisorTranscriptBoilerplateAndToolOutputs(s)
	lower := strings.ToLower(cleaned)
	return strings.Contains(lower, "/autonomous-story-execution") ||
		strings.Contains(lower, "skills/autonomous-story-execution") ||
		strings.Contains(lower, "autopilot-story") ||
		strings.Contains(lower, "autopilot-git-flow") ||
		strings.Contains(lower, "autopilot-review-scheme") ||
		strings.Contains(lower, "autopilot review-scheme") ||
		strings.Contains(lower, "autopilot conformance") ||
		strings.Contains(lower, "autopilot story begin") ||
		strings.Contains(lower, "autopilot ase") ||
		strings.Contains(lower, "git-flow ase-start") ||
		strings.Contains(lower, "git-flow ase-ship") ||
		strings.Contains(lower, ".session_story_id")
}

var supervisorBgTaskStartRe = regexp.MustCompile(`Tool is running as a background task with task id:\s*([a-zA-Z0-9_-]+/task-[0-9]+)`)

func hasSupervisorPendingBackgroundTask(tail string) bool {
	matches := supervisorBgTaskStartRe.FindAllStringSubmatchIndex(tail, -1)
	for _, m := range matches {
		if len(m) < 4 {
			continue
		}
		taskID := tail[m[2]:m[3]]
		rest := tail[m[1]:]
		if !strings.Contains(rest, taskID+`" finished with result:`) &&
			!strings.Contains(rest, taskID+`\" finished with result:`) {
			return true
		}
	}
	return false
}

func isSupervisorTranscriptTurnInFlight(tail []byte, modTime, now time.Time) bool {
	if modTime.IsZero() {
		return false
	}
	age := now.Sub(modTime)
	if age > 15*time.Minute {
		return false
	}
	tailStr := string(tail)
	hasBg := hasSupervisorPendingBackgroundTask(tailStr)
	if !hasBg && age > 3*time.Minute {
		return false
	}
	lines := strings.Split(tailStr, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
			continue
		}
		var step struct {
			Type            string          `json:"type"`
			Status          string          `json:"status"`
			ToolCalls       json.RawMessage `json:"tool_calls"`
			TruncatedFields []string        `json:"truncated_fields"`
		}
		if err := json.Unmarshal([]byte(line), &step); err != nil {
			continue
		}
		trimmedTools := strings.TrimSpace(string(step.ToolCalls))
		hasTools := trimmedTools != "" && trimmedTools != "null" && trimmedTools != "[]"
		if !hasTools {
			for _, tf := range step.TruncatedFields {
				if strings.EqualFold(strings.TrimSpace(tf), "tool_calls") {
					hasTools = true
					break
				}
			}
		}
		if strings.EqualFold(step.Type, "PLANNER_RESPONSE") && strings.EqualFold(step.Status, "DONE") && !hasTools {
			return hasBg
		}
		return true
	}
	return false
}

// isSupervisorConversationClaimedByOtherWorktree returns true if candID is explicitly bound
// or belongs to a different worktree (via .iazio/repo.lock, watchdog/harness active jobs,
// or the Antigravity conversation DB workspace URI).
func isSupervisorConversationClaimedByOtherWorktree(repoPath, candID string) bool {
	cleanRepo := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanRepo == "" || cleanRepo == "." {
		return false
	}
	cleanRepoSlash := strings.ToLower(filepath.ToSlash(cleanRepo))

	// 1. Check if an active watchdog or harness job binds candID to another worktree
	home, _ := os.UserHomeDir()
	if home != "" {
		for _, stateFile := range []string{
			filepath.Join(home, ".iazio", "watchdog-active-jobs.json"),
			filepath.Join(home, ".iazio", "harness-active-jobs.json"),
		} {
			if data, err := os.ReadFile(stateFile); err == nil {
				var jobs []struct {
					WorktreePath   string `json:"worktree_path"`
					ConversationID string `json:"conversation_id"`
					Status         string `json:"status"`
				}
				if json.Unmarshal(data, &jobs) == nil {
					for _, j := range jobs {
						if strings.EqualFold(strings.TrimSpace(j.ConversationID), candID) {
							wtSlash := strings.ToLower(filepath.ToSlash(filepath.Clean(j.WorktreePath)))
							if wtSlash != "" && wtSlash != cleanRepoSlash {
								return true
							}
						}
					}
				}
			}
		}
	}

	// 2. Check sibling worktrees in the same parent directory for .iazio/repo.lock holding candID
	parentDir := filepath.Dir(cleanRepo)
	if entries, err := os.ReadDir(parentDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			sibPath := filepath.Clean(filepath.Join(parentDir, e.Name()))
			if strings.EqualFold(filepath.ToSlash(sibPath), cleanRepoSlash) {
				continue
			}
			if lockInfo, lErr := repolock.Read(sibPath); lErr == nil {
				if strings.EqualFold(strings.TrimSpace(lockInfo.ConversationID), candID) {
					return true
				}
			}
		}
	}

	// 3. Check conversation db in ~/.gemini/antigravity-cli/conversations/<candID>.db for declared workspace URI
	if home != "" {
		dbPath := filepath.Join(home, ".gemini", "antigravity-cli", "conversations", candID+".db")
		if raw, err := os.ReadFile(dbPath); err == nil {
			if isSupervisorDBWorkspaceClaimedBySiblingWorktree(raw, cleanRepoSlash) {
				return true
			}
		}
	}

	return false
}

func stripSupervisorWorktreeNumber(cleanPath string) string {
	idx := strings.LastIndexByte(cleanPath, '-')
	slashIdx := strings.LastIndexByte(cleanPath, '/')
	if idx <= slashIdx+1 || idx >= len(cleanPath)-1 {
		return cleanPath
	}
	for i := idx + 1; i < len(cleanPath); i++ {
		if cleanPath[i] < '0' || cleanPath[i] > '9' {
			return cleanPath
		}
	}
	return cleanPath[:idx]
}

func splitSupervisorSlashDirBase(p string) (string, string) {
	p = strings.TrimRight(p, "/")
	idx := strings.LastIndexByte(p, '/')
	if idx < 0 {
		return "", p
	}
	if idx == 0 {
		return "/", p[1:]
	}
	return p[:idx], p[idx+1:]
}

func isSupervisorDBWorkspaceClaimedBySiblingWorktree(raw []byte, cleanRepoSlash string) bool {
	if len(raw) == 0 || strings.TrimSpace(cleanRepoSlash) == "" {
		return false
	}
	cleanRepoSlash = strings.TrimRight(strings.ToLower(filepath.ToSlash(cleanRepoSlash)), "/")
	parentSlash, baseName := splitSupervisorSlashDirBase(cleanRepoSlash)
	familyBase := stripSupervisorWorktreeNumber(baseName)
	if parentSlash == "" || familyBase == "" {
		return false
	}

	selfRootCount := 0
	siblingCounts := make(map[string]int)
	maxSiblingRootCount := 0

	prefix := []byte("file://")
	idx := 0
	for idx < len(raw) {
		pos := bytes.Index(raw[idx:], prefix)
		if pos < 0 {
			break
		}
		start := idx + pos + len(prefix)
		end := start
		for end < len(raw) {
			b := raw[end]
			if b < 32 || b > 126 ||
				b == '"' || b == '\'' || b == '\r' || b == '\n' ||
				b == '}' || b == ']' || b == ')' || b == ' ' || b == '\t' ||
				b == '<' || b == '>' || b == '#' || b == '?' ||
				b == ',' || b == ';' || b == '`' {
				break
			}
			end++
		}
		if end > start {
			token := strings.TrimSpace(string(raw[start:end]))
			declaredSlash := strings.TrimRight(strings.ToLower(filepath.ToSlash(filepath.Clean(token))), "/")
			if strings.HasPrefix(declaredSlash, "/") {
				dirSlash, decBase := splitSupervisorSlashDirBase(declaredSlash)
				if dirSlash == parentSlash && stripSupervisorWorktreeNumber(decBase) == familyBase {
					if declaredSlash == cleanRepoSlash {
						selfRootCount++
					} else {
						siblingCounts[declaredSlash]++
						if siblingCounts[declaredSlash] > maxSiblingRootCount {
							maxSiblingRootCount = siblingCounts[declaredSlash]
						}
					}
				}
			}
		}
		idx = start
	}

	return maxSiblingRootCount > 0 && (selfRootCount == 0 || maxSiblingRootCount > selfRootCount*2)
}

func readSupervisorMiddleUserInputs(f *os.File, size, headLimit, tailLimit int64) string {
	const maxScanFileSize = 32 * 1024 * 1024
	const maxCollectedBytes = 256 * 1024
	if f == nil || size <= headLimit+tailLimit || size > maxScanFileSize {
		return ""
	}
	startOffset := headLimit - 4096
	if startOffset < 0 {
		startOffset = 0
	}
	endOffset := size - tailLimit + 4096
	if endOffset > size {
		endOffset = size
	}
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return ""
	}
	reader := bufio.NewReaderSize(f, 64*1024)
	offset := startOffset
	if startOffset > 0 {
		firstFrag, rErr := reader.ReadString('\n')
		offset += int64(len(firstFrag))
		if rErr != nil {
			return ""
		}
	}
	var b strings.Builder
	for offset < endOffset {
		line, rErr := reader.ReadString('\n')
		offset += int64(len(line))
		if len(line) > 0 {
			lower := strings.ToLower(line)
			if strings.Contains(lower, `"type":"user_input"`) || strings.Contains(lower, `"type": "user_input"`) {
				if b.Len()+len(line) > maxCollectedBytes {
					remaining := maxCollectedBytes - b.Len()
					if remaining > 0 {
						b.WriteString(line[:remaining])
						b.WriteByte('\n')
					}
					break
				}
				b.WriteString(line)
			}
		}
		if rErr != nil {
			break
		}
	}
	return b.String()
}

// InspectActiveBrainASETurnForRepo scans Antigravity brain directories for an in-flight /autonomous-story-execution
// turn targeting repoPath.
func InspectActiveBrainASETurnForRepo(repoPath string, now time.Time) (active bool, convID string, storyID string) {
	if strings.TrimSpace(repoPath) == "" {
		return false, "", ""
	}
	if isSupervisorAutomatedMaintenanceRepoLock(repoPath) {
		return false, "", ""
	}
	if now.IsZero() {
		now = time.Now()
	}
	var newestMod time.Time
	for _, brainRoot := range supervisorBrainRootDirs() {
		entries, err := os.ReadDir(brainRoot)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") || len(e.Name()) < 8 {
				continue
			}
			candID := e.Name()
			if isSupervisorConversationClaimedByOtherWorktree(repoPath, candID) {
				continue
			}
			for _, rel := range []string{
				filepath.Join(".system_generated", "logs", "transcript.jsonl"),
				filepath.Join(".system_generated", "logs", "transcript_full.jsonl"),
			} {
				full := filepath.Join(brainRoot, candID, rel)
				fi, stErr := os.Stat(full)
				if stErr != nil || fi.IsDir() {
					continue
				}
				modTime := fi.ModTime()
				if modTime.IsZero() || now.Sub(modTime) > 15*time.Minute {
					continue
				}
				f, oErr := os.Open(full)
				if oErr != nil {
					continue
				}
				size := fi.Size()
				const headLimit = 96 * 1024
				const tailLimit = 128 * 1024
				var head, tail []byte
				var midUserInputs string
				if size <= headLimit+tailLimit {
					all := make([]byte, size)
					n, _ := f.Read(all)
					head = all[:n]
					tail = all[:n]
				} else {
					head = make([]byte, headLimit)
					hn, _ := f.ReadAt(head, 0)
					head = head[:hn]
					tail = make([]byte, tailLimit)
					tn, _ := f.ReadAt(tail, size-tailLimit)
					tail = tail[:tn]
					midUserInputs = readSupervisorMiddleUserInputs(f, size, headLimit, tailLimit)
				}
				_ = f.Close()

				headStr := string(head)
				tailStr := string(tail)
				combined := headStr + "\n" + tailStr
				if midUserInputs != "" {
					combined = headStr + "\n" + midUserInputs + "\n" + tailStr
				}
				if isSupervisorAutomatedMaintenanceTranscript(headStr) ||
					isSupervisorAutomatedMaintenanceTranscript(midUserInputs) ||
					isSupervisorAutomatedMaintenanceTranscript(tailStr) {
					continue
				}
				if !TranscriptReferencesRepoPath(combined, repoPath) {
					continue
				}
				if !containsSupervisorASEMarker(headStr) &&
					!containsSupervisorASEMarker(midUserInputs) &&
					!containsSupervisorASEMarker(tailStr) {
					continue
				}
				if !isSupervisorTranscriptTurnInFlight(tail, modTime, now) {
					continue
				}
				var candStory string
				combinedHead := headStr
				if midUserInputs != "" {
					combinedHead += "\n" + midUserInputs
				}
				cleanedHead := stripSupervisorTranscriptBoilerplateAndToolOutputs(combinedHead)
				cleanedTail := stripSupervisorTranscriptBoilerplateAndToolOutputs(tailStr)
				allMatches := append(
					supervisorTranscriptBoundStoryRe.FindAllStringSubmatch(cleanedHead, -1),
					supervisorTranscriptBoundStoryRe.FindAllStringSubmatch(cleanedTail, -1)...,
				)
				for i := len(allMatches) - 1; i >= 0; i-- {
					sid := strings.ToUpper(allMatches[i][1])
					if canBindSupervisorTranscriptStoryToRepo(repoPath, sid) {
						candStory = sid
						break
					}
				}
				if modTime.After(newestMod) {
					active = true
					convID = candID
					storyID = candStory
					newestMod = modTime
				}
			}
		}
	}
	return active, convID, storyID
}

// NormalizeGitAlternatesPaths rewrites cross-OS WSL (/mnt/<drive>/...) or non-portable absolute paths
// in .git/objects/info/alternates and .git/modules/*/objects/info/alternates to portable relative paths.
func NormalizeGitAlternatesPaths(repoPath string) int {
	cleanPath := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanPath == "" {
		return 0
	}
	gitDir := filepath.Join(cleanPath, ".git")
	fi, err := os.Stat(gitDir)
	if err != nil {
		return 0
	}
	if !fi.IsDir() {
		if b, rErr := os.ReadFile(gitDir); rErr == nil {
			content := strings.TrimSpace(string(b))
			if strings.HasPrefix(content, "gitdir:") {
				target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(target) {
					target = filepath.Join(cleanPath, target)
				}
				gitDir = filepath.Clean(target)
			}
		}
	}
	var altFiles []string
	rootAlt := filepath.Join(gitDir, "objects", "info", "alternates")
	if _, err := os.Stat(rootAlt); err == nil {
		altFiles = append(altFiles, rootAlt)
	}
	for _, pattern := range []string{
		filepath.Join(gitDir, "modules", "*", "objects", "info", "alternates"),
		filepath.Join(gitDir, "modules", "*", "*", "objects", "info", "alternates"),
	} {
		if matches, err := filepath.Glob(pattern); err == nil {
			altFiles = append(altFiles, matches...)
		}
	}
	count := 0
	for _, f := range altFiles {
		if normalizeSingleGitAlternatesFile(cleanPath, f) {
			count++
		}
	}
	return count
}

func normalizeSingleGitAlternatesFile(repoPath, altFile string) bool {
	raw, err := os.ReadFile(altFile)
	if err != nil {
		return false
	}
	objectsDir := filepath.Clean(filepath.Dir(filepath.Dir(altFile)))
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	changed := false
	outLines := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			outLines = append(outLines, trimmed)
			continue
		}
		if !strings.HasPrefix(trimmed, "/") && !strings.HasPrefix(trimmed, `\`) && !(len(trimmed) >= 2 && trimmed[1] == ':') {
			cand := filepath.Clean(filepath.Join(objectsDir, filepath.FromSlash(trimmed)))
			if fi, stErr := os.Stat(cand); stErr == nil && fi.IsDir() {
				normRel := filepath.ToSlash(trimmed)
				if normRel != trimmed {
					changed = true
				}
				outLines = append(outLines, normRel)
				continue
			}
		}
		slash := strings.ReplaceAll(trimmed, `\`, `/`)
		var candidates []string
		candidates = append(candidates, filepath.FromSlash(slash))
		if strings.HasPrefix(strings.ToLower(slash), "/mnt/") && len(slash) > 6 && slash[6] == '/' {
			drive := strings.ToUpper(slash[5:6])
			rest := slash[7:]
			if runtime.GOOS == "windows" {
				candidates = append([]string{drive + `:\` + strings.ReplaceAll(rest, "/", `\`)}, candidates...)
			} else {
				candidates = append(candidates, drive+`:/`+rest)
			}
		} else if len(slash) >= 3 && slash[1] == ':' && slash[2] == '/' {
			drive := strings.ToLower(slash[0:1])
			rest := slash[3:]
			candidates = append([]string{"/mnt/" + drive + "/" + rest}, candidates...)
		}
		if repoPath != "" {
			parentDir := filepath.Dir(filepath.Clean(repoPath))
			if idx := strings.Index(slash, "/.git/"); idx > 0 {
				prefix := slash[:idx]
				gitSuffix := slash[idx+1:]
				siblingName := filepath.Base(filepath.FromSlash(prefix))
				if siblingName != "" && siblingName != "." && siblingName != "/" {
					candidates = append([]string{filepath.Join(parentDir, siblingName, filepath.FromSlash(gitSuffix))}, candidates...)
				}
			}
		}
		resolved := ""
		for _, cand := range candidates {
			cleanCand := filepath.Clean(cand)
			if fi, stErr := os.Stat(cleanCand); stErr == nil && fi.IsDir() {
				resolved = cleanCand
				break
			}
		}
		if resolved != "" {
			if rel, relErr := filepath.Rel(objectsDir, resolved); relErr == nil && rel != "" && !strings.HasPrefix(rel, "/") {
				relSlash := filepath.ToSlash(rel)
				if relSlash != trimmed {
					changed = true
				}
				outLines = append(outLines, relSlash)
				continue
			}
		}
		outLines = append(outLines, trimmed)
	}
	if !changed {
		return false
	}
	newContent := strings.TrimRight(strings.Join(outLines, "\n"), "\n") + "\n"
	return os.WriteFile(altFile, []byte(newContent), 0o644) == nil
}

func inspectSingleGitDirectoryWithOptions(ctx context.Context, dir string, canSanitizeEphemeral bool, computeAheadBehind bool) singleDirGitResult {
	res := singleDirGitResult{IsClean: true}
	NormalizeGitAlternatesPaths(dir)

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
			if canSanitizeEphemeral && (base == "rebase-merge" || base == "rebase-apply") {
				_, hasStoppedSHA := os.Stat(filepath.Join(sentinel, "stopped-sha"))
				_, hasIndexLock := os.Stat(filepath.Join(gitDir, "index.lock"))
				conflictHalted := hasStoppedSHA == nil && os.IsNotExist(hasIndexLock)
				if rebBr, rebSentinel, rebMod := readRebaseHeadBranchInRepo(gitDir); rebBr != "" && isAutomatedMaintenanceBranchInRepo(rebBr) && (time.Since(rebMod) > 2*time.Minute || conflictHalted) {
					healStrandedMaintenanceBranchBeforeGitUpdate(ctx, dir)
					if _, stErr := os.Stat(rebSentinel); os.IsNotExist(stErr) {
						continue
					}
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

func computeDirtyOnlyStatusHash(ctx context.Context, repoPath string) string {
	out, err := procutil.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain=v2").Output()
	if err != nil {
		return ""
	}
	var lines []string
	for _, l := range strings.Split(string(out), "\n") {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "# ") {
			continue
		}
		lines = append(lines, t)
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return fmt.Sprintf("%x", sum[:8])
}

func maintenanceCommandEnv() []string {
	base := AugmentedEnv(os.Environ())
	env := make([]string, 0, len(base)+3)
	for _, kv := range base {
		if strings.HasPrefix(kv, "IAZIO_MANAGED_BY=") ||
			strings.HasPrefix(kv, "IAZIO_HARNESS_JOB_ID=") ||
			strings.HasPrefix(kv, "GIT_TERMINAL_PROMPT=") ||
			strings.HasPrefix(kv, "IAZIO_AGENT_MANAGED=") ||
			strings.HasPrefix(kv, "IAZIO_MAINTENANCE_RUN=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"GIT_TERMINAL_PROMPT=0",
		"IAZIO_AGENT_MANAGED=1",
		"IAZIO_MAINTENANCE_RUN=1",
	)
}

// DefaultShellGitUpdate executes "autopilot git-update" directly in repoPath.
func DefaultShellGitUpdate(ctx context.Context, repoPath string) (string, error) {
	autopilotBin := ResolveBinary("autopilot")
	upCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	cmd := procutil.CommandContext(upCtx, autopilotBin, "git-update")
	cmd.Dir = repoPath
	cmd.Env = maintenanceCommandEnv()
	setSupervisorProcGroup(cmd)
	cmd.Cancel = func() error {
		return killSupervisorProcGroup(cmd)
	}
	cmd.WaitDelay = 3 * time.Second
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// DefaultAgyEscalation launches iazio-harness with target agent "agy" in repoPath executing prompt.
func DefaultAgyEscalation(ctx context.Context, repoPath, prompt, diagnosticOutput string) (string, error) {
	_, _ = agyhooks.SanitizeUserGeminiHooks(agyhooks.ResolveAgentHome(os.Getenv, agyhooks.IsTestProcess()))
	harnessBin := ResolveBinary("iazio-harness")
	fullPrompt := prompt
	if strings.TrimSpace(diagnosticOutput) != "" {
		fullPrompt = fmt.Sprintf("%s\n\nDiagnostic failure summary from shell autopilot git-update:\n%s", prompt, strings.TrimSpace(diagnosticOutput))
	}
	escCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	payloadBytes, _ := json.Marshal(map[string]any{
		"prompt_id":     fmt.Sprintf("git-update-agy-%d", time.Now().UnixNano()),
		"prompt":        fullPrompt,
		"target_agent":  "agy",
		"repo":          repoPath,
		"worktree_path": repoPath,
	})
	cmd := procutil.CommandContext(escCtx, harnessBin, "prompt", "run", "--json")
	cmd.Dir = repoPath
	cmd.Env = maintenanceCommandEnv()
	setSupervisorProcGroup(cmd)
	cmd.Cancel = func() error {
		return killSupervisorProcGroup(cmd)
	}
	cmd.WaitDelay = 3 * time.Second
	cmd.Stdin = bytes.NewReader(payloadBytes)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

type agyCooldownRecord struct {
	Fingerprint     string
	DirtyStatusHash string
	HeadSHA         string
	TreeSHA         string
	AttemptedAt     time.Time
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

	healStrandedMaintenanceBranchBeforeGitUpdate(ctx, cleanPath)

	// Step 1: Execute "autopilot git-update" directly via shell in the worktree root
	res.Stage1Executed = true
	step1Out, step1Err := shellFn(ctx, cleanPath)
	res.Stage1Output = step1Out

	// Release lock temporarily to inspect/sanitize without self-blocking IsWorktreeIdleForGitUpdate
	releaseLock()
	healStrandedMaintenanceBranchBeforeGitUpdate(ctx, cleanPath)
	_, _ = SanitizeIdleEphemeralArtifacts(ctx, cleanPath, sup, isAlive)
	st := InspectRepoGitState(ctx, cleanPath, item.Alias)
	res.FinalState = st

	if isCleanOnMainSynced(st) {
		_ = os.Remove(filepath.Join(cleanPath, ".iazio", "post-ase-sync-failed"))
		res.Stage1Success = (step1Err == nil)
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
	dirtyStatusHash := computeDirtyOnlyStatusHash(ctx, cleanPath)
	res.DirtyFingerprint = fp
	now := nowFn().UTC()

	m.mu.Lock()
	prevRec, hasPrev := m.agyCooldowns[cleanPath]
	sameDirtyState := hasPrev && (prevRec.Fingerprint == fp || (dirtyStatusHash != "" && prevRec.DirtyStatusHash == dirtyStatusHash))
	if sameDirtyState && now.Sub(prevRec.AttemptedAt) < cooldownDur {
		m.mu.Unlock()
		res.Stage2CooldownSkipped = true
		return res
	}
	m.agyCooldowns[cleanPath] = agyCooldownRecord{
		Fingerprint:     fp,
		DirtyStatusHash: dirtyStatusHash,
		HeadSHA:         headSHA,
		TreeSHA:         treeSHA,
		AttemptedAt:     now,
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
		_ = os.Remove(filepath.Join(cleanPath, ".iazio", "post-ase-sync-failed"))
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

	_, _ = agyhooks.SanitizeUserGeminiHooks(agyhooks.ResolveAgentHome(os.Getenv, agyhooks.IsTestProcess()))

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
