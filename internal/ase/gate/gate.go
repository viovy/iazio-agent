// Package gate provides the "Don't Stop Till Done" ASE completion verification gate.
package gate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GateResult captures the evaluation of all ASE completion gates.
type GateResult struct {
	Passed          bool     `json:"passed"`
	Reason          string   `json:"reason,omitempty"`
	UnmetGates      []string `json:"unmet_gates,omitempty"`
	GitClean        bool     `json:"git_clean"`
	BranchClean     bool     `json:"branch_clean"`
	SubmodulesClean bool     `json:"submodules_clean"`
	StoryDone       bool     `json:"story_done"`
	TestsPassed     bool     `json:"tests_passed"`
	ReleaseVerified bool     `json:"release_verified"`
}

// GateOptions configures the post-exit completion check.
type GateOptions struct {
	WorktreePath           string
	StoryID                string
	RequireGitClean        bool
	RequireSubmodulesClean bool
	RequireTestsPassed     bool
	RequireReleaseVerified bool
	GitRunner              func(args ...string) (string, error)
}

// DefaultGitRunner executes a git command in the target directory.
func DefaultGitRunner(dir string) func(args ...string) (string, error) {
	return func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
}

// Verify evaluates all ASE completion preconditions.
func Verify(opts GateOptions) GateResult {
	res := GateResult{
		Passed:          true,
		GitClean:        true,
		BranchClean:     true,
		SubmodulesClean: true,
		StoryDone:       true,
		TestsPassed:     true,
		ReleaseVerified: true,
		UnmetGates:      []string{},
	}

	if opts.WorktreePath == "" {
		if opts.StoryID != "" {
			res.Passed = false
			res.Reason = "empty_worktree_path"
			res.UnmetGates = append(res.UnmetGates, "worktree_path")
		}
		return res
	}

	git := opts.GitRunner
	if git == nil {
		git = DefaultGitRunner(opts.WorktreePath)
	}

	// 1. Workspace & Git Integrity Check: Clean working tree
	porc, err := git("status", "--porcelain")
	if err == nil {
		lines := strings.Split(porc, "\n")
		var dirtyCount int
		for _, l := range lines {
			trimmed := strings.TrimSpace(l)
			if trimmed == "" {
				continue
			}
			// Ignore known transient noise
			if strings.Contains(trimmed, "coverage.out") || strings.Contains(trimmed, ".scannerwork") {
				continue
			}
			dirtyCount++
		}
		if dirtyCount > 0 {
			res.GitClean = false
			res.UnmetGates = append(res.UnmetGates, "clean_working_tree")
		}
	}

	// 2. Branch cleanliness: detached HEAD check
	branch, err := git("rev-parse", "--abbrev-ref", "HEAD")
	if err == nil {
		branch = strings.TrimSpace(branch)
		if branch == "HEAD" {
			res.BranchClean = false
			res.UnmetGates = append(res.UnmetGates, "branch_not_detached")
		}
	}

	// 3. Submodule gitlinks cleanliness
	subStatus, err := git("submodule", "status")
	if err == nil && subStatus != "" {
		lines := strings.Split(subStatus, "\n")
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if len(l) > 0 && (l[0] == '+' || l[0] == 'U') {
				res.SubmodulesClean = false
				res.UnmetGates = append(res.UnmetGates, "submodule_gitlinks_synchronized")
				break
			}
		}
	}

	// 4. Story Done & Definition of Done verification
	if opts.StoryID != "" {
		storyDone := isStoryCompleted(opts.WorktreePath, opts.StoryID, git)
		if !storyDone {
			res.StoryDone = false
			res.UnmetGates = append(res.UnmetGates, "story_definition_of_done")
		}
	}

	// 5. Verification / test targets check
	if opts.RequireTestsPassed {
		stampFile := filepath.Join(opts.WorktreePath, "tmp", "reports", "validate-local-ok.stamp")
		if _, err := os.Stat(stampFile); err != nil {
			res.TestsPassed = false
			res.UnmetGates = append(res.UnmetGates, "validation_tests_passed")
		}
	}

	// 6. Release & live verification check
	if opts.RequireReleaseVerified {
		relStamp := filepath.Join(opts.WorktreePath, "tmp", "reports", "verify-live-release-ok.stamp")
		if _, err := os.Stat(relStamp); err != nil {
			res.ReleaseVerified = false
			res.UnmetGates = append(res.UnmetGates, "release_verified")
		}
	}

	if len(res.UnmetGates) > 0 {
		res.Passed = false
		res.Reason = "incomplete_ase_flow"
	}

	return res
}

func isStoryCompleted(worktreePath, storyID string, git func(args ...string) (string, error)) bool {
	// First check git log on origin/main or HEAD for story commit
	if git != nil {
		out, err := git("log", "-n", "10", "--grep="+storyID, "--format=%H")
		if err == nil && strings.TrimSpace(out) != "" {
			return true
		}
	}

	// Next inspect story files in docs/stories/
	storiesDir := filepath.Join(worktreePath, "docs", "stories")
	entries, err := os.ReadDir(storiesDir)
	if err == nil {
		hasStoryFile := false
		cleanID := strings.TrimPrefix(strings.ToLower(storyID), "story-")
		for _, e := range entries {
			lowerName := strings.ToLower(e.Name())
			if !e.IsDir() && strings.HasSuffix(lowerName, ".story.md") && (strings.Contains(lowerName, strings.ToLower(storyID)) || strings.Contains(lowerName, cleanID)) {
				hasStoryFile = true
				content, err := os.ReadFile(filepath.Join(storiesDir, e.Name()))
				if err == nil {
					text := string(content)
					if strings.Contains(text, "status: DONE") {
						return true
					}
				}
			}
		}
		if hasStoryFile {
			return false
		}
	}

	return true
}
