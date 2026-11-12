// Package gate provides the "Don't Stop Till Done" ASE completion verification gate.
package gate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/viovy/iazio-core/pkg/procutil"
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
	DocsHubPath            string
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
		cmd := procutil.Command("git", args...)
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
	if err != nil {
		res.GitClean = false
		res.UnmetGates = append(res.UnmetGates, "clean_working_tree")
	} else {
		lines := strings.Split(porc, "\n")
		var dirtyCount int
		for _, l := range lines {
			trimmed := strings.TrimSpace(l)
			if trimmed == "" {
				continue
			}
			// Ignore known transient noise by exact path/basename matching on the extracted porcelain file path
			if isTransientPorcelainNoise(trimmed) {
				continue
			}
			dirtyCount++
		}
		if dirtyCount > 0 {
			res.GitClean = false
			res.UnmetGates = append(res.UnmetGates, "clean_working_tree")
		}
	}

	// 2. Branch cleanliness: must be on main/master (not detached HEAD or unmerged feature branch)
	branch, err := git("rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		res.BranchClean = false
		res.UnmetGates = append(res.UnmetGates, "branch_not_detached")
	} else {
		branch = strings.TrimSpace(branch)
		if branch != "main" && branch != "master" {
			res.BranchClean = false
			res.UnmetGates = append(res.UnmetGates, "branch_not_detached")
		}
	}

	// 3. Submodule gitlinks cleanliness
	subStatus, err := git("submodule", "status")
	if err != nil {
		res.SubmodulesClean = false
		res.UnmetGates = append(res.UnmetGates, "submodule_gitlinks_synchronized")
	} else if subStatus != "" {
		lines := strings.Split(subStatus, "\n")
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if len(l) > 0 && (l[0] == '+' || l[0] == 'U' || l[0] == '-') {
				res.SubmodulesClean = false
				res.UnmetGates = append(res.UnmetGates, "submodule_gitlinks_synchronized")
				break
			}
		}
	}

	// 4. Story Done & Definition of Done verification
	if opts.StoryID != "" {
		storyDone := isStoryCompleted(opts.WorktreePath, opts.DocsHubPath, opts.StoryID)
		if !storyDone {
			res.StoryDone = false
			res.UnmetGates = append(res.UnmetGates, "story_definition_of_done")
		}
	}

	// 5. Verification / test targets check
	if opts.RequireTestsPassed {
		stampFile := filepath.Join(opts.WorktreePath, "tmp", "reports", "validate-local-ok.stamp")
		if !isStampValidForHEAD(stampFile, git) {
			res.TestsPassed = false
			res.UnmetGates = append(res.UnmetGates, "validation_tests_passed")
		}
	}

	// 6. Release & live verification check
	if opts.RequireReleaseVerified {
		relStamp := filepath.Join(opts.WorktreePath, "tmp", "reports", "verify-live-release-ok.stamp")
		if !isStampValidForHEAD(relStamp, git) {
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

func extractPorcelainPath(trimmedLine string) (string, bool) {
	if strings.Contains(trimmedLine, " -> ") {
		return "", false
	}
	idx := strings.IndexAny(trimmedLine, " \t")
	if idx < 0 {
		return "", false
	}
	p := strings.TrimSpace(trimmedLine[idx+1:])
	p = strings.Trim(p, `"`)
	p = filepath.ToSlash(p)
	p = strings.TrimPrefix(p, "./")
	if p == "" {
		return "", false
	}
	return p, true
}

func isTransientPorcelainNoise(trimmedLine string) bool {
	p, ok := extractPorcelainPath(trimmedLine)
	if !ok {
		return false
	}
	if !strings.HasSuffix(p, "/") && (p == "coverage.out" || filepath.Base(p) == "coverage.out") {
		return true
	}
	cleanP := strings.TrimSuffix(p, "/")
	if cleanP == "tmp" || cleanP == "tmp/reports" || strings.HasPrefix(cleanP, "tmp/reports/") {
		return true
	}
	for _, seg := range strings.Split(cleanP, "/") {
		if seg == ".scannerwork" {
			return true
		}
	}
	return false
}

func isStampValidForHEAD(stampPath string, git func(args ...string) (string, error)) bool {
	data, err := os.ReadFile(stampPath)
	if err != nil {
		return false
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return false
	}
	stampSHA := extractStampSHA(content)
	if stampSHA == "" {
		// Fail closed: stamp files without an explicit commit SHA cannot be verified against HEAD
		return false
	}
	if git == nil {
		return false
	}
	headOut, err := git("rev-parse", "HEAD")
	if err != nil {
		return false
	}
	headSHA := strings.ToLower(strings.TrimSpace(headOut))
	if len(headSHA) < 7 {
		return false
	}
	return headSHA == stampSHA || strings.HasPrefix(headSHA, stampSHA) || strings.HasPrefix(stampSHA, headSHA)
}

func extractStampSHA(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, sep := range []string{"=", ":"} {
			if k, v, ok := strings.Cut(line, sep); ok {
				key := strings.ToLower(strings.Trim(strings.TrimSpace(k), `"'`))
				if key == "sha" || key == "head" || key == "head_sha" || key == "commit" || key == "commit_sha" {
					cand := strings.ToLower(strings.Trim(strings.TrimSpace(v), `"',;`))
					if isHexCommitSHA(cand) {
						return cand
					}
				}
			}
		}
		for _, field := range strings.Fields(line) {
			cand := strings.ToLower(strings.Trim(field, `"',;[]{}`))
			if isHexCommitSHA(cand) {
				return cand
			}
		}
	}
	return ""
}

func isHexCommitSHA(s string) bool {
	if len(s) < 7 || len(s) > 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func candidateStoryDirs(worktreePath, docsHubPath string) []string {
	var dirs []string
	seen := make(map[string]bool)
	addDir := func(d string) {
		d = strings.TrimSpace(d)
		if d == "" {
			return
		}
		clean := filepath.Clean(d)
		if !seen[clean] {
			seen[clean] = true
			dirs = append(dirs, clean)
		}
	}
	if strings.TrimSpace(worktreePath) != "" {
		addDir(filepath.Join(worktreePath, "docs", "stories"))
	}
	if strings.TrimSpace(docsHubPath) != "" {
		addDir(filepath.Join(docsHubPath, "docs", "stories"))
		addDir(filepath.Join(docsHubPath, "stories"))
		addDir(docsHubPath)
	}
	return dirs
}

func isStoryCompleted(worktreePath, docsHubPath, storyID string) bool {
	storyID = strings.TrimSpace(storyID)
	if storyID == "" {
		return false
	}

	// ADR-002: Check .autopilot/ase-state.json / .autopilot/state.json first
	for _, rootDir := range []string{worktreePath, docsHubPath} {
		if strings.TrimSpace(rootDir) == "" {
			continue
		}
		if matched, done := checkAutopilotStateStoryDone(rootDir, storyID); matched {
			return done
		}
	}

	lowerStoryID := strings.ToLower(storyID)
	cleanID := strings.TrimPrefix(lowerStoryID, "story-")

	for _, storiesDir := range candidateStoryDirs(worktreePath, docsHubPath) {
		entries, err := os.ReadDir(storiesDir)
		if err != nil {
			continue
		}
		hasStoryFile := false
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			lowerName := strings.ToLower(e.Name())
			if !strings.HasSuffix(lowerName, ".story.md") {
				continue
			}
			if matchesStoryIDBoundary(lowerName, lowerStoryID, cleanID) {
				content, err := os.ReadFile(filepath.Join(storiesDir, e.Name()))
				if err != nil {
					hasStoryFile = true
					continue
				}
				matchedID, statusDone := hasFrontmatterStatusDoneForStory(string(content), storyID)
				if !matchedID {
					continue
				}
				hasStoryFile = true
				if statusDone {
					return true
				}
			}
		}
		if hasStoryFile {
			// Story file exists and is NOT marked DONE -> story is still incomplete
			return false
		}
	}

	return false
}

// checkAutopilotStateStoryDone inspects .autopilot/ase-state.json and .autopilot/state.json (ADR-002).
// It returns (matchedStoryID, isDone).
func checkAutopilotStateStoryDone(rootDir, storyID string) (bool, bool) {
	for _, rel := range []string{
		filepath.Join(".autopilot", "ase-state.json"),
		filepath.Join(".autopilot", "state.json"),
	} {
		data, err := os.ReadFile(filepath.Join(rootDir, rel))
		if err != nil {
			continue
		}
		var state struct {
			StoryID   string `json:"story_id"`
			Status    string `json:"status"`
			State     string `json:"state"`
			Phase     string `json:"phase"`
			Completed bool   `json:"completed"`
		}
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(state.StoryID), storyID) {
			continue
		}
		st := strings.ToUpper(strings.TrimSpace(state.Status))
		if st == "" {
			st = strings.ToUpper(strings.TrimSpace(state.State))
		}
		if st == "" {
			st = strings.ToUpper(strings.TrimSpace(state.Phase))
		}
		return true, state.Completed || st == "DONE" || st == "COMPLETED"
	}
	return false, false
}

func matchesStoryIDBoundary(lowerFilename, lowerStoryID, cleanID string) bool {
	for _, token := range []string{lowerStoryID, cleanID} {
		if token == "" {
			continue
		}
		idx := strings.Index(lowerFilename, token)
		for idx >= 0 {
			beforeOK := idx == 0 || !isAlphaNum(lowerFilename[idx-1])
			afterIdx := idx + len(token)
			afterOK := afterIdx == len(lowerFilename) || !isAlphaNum(lowerFilename[afterIdx])
			if beforeOK && afterOK {
				return true
			}
			next := strings.Index(lowerFilename[idx+1:], token)
			if next < 0 {
				break
			}
			idx += 1 + next
		}
	}
	return false
}

func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// hasFrontmatterStatusDone extracts the YAML frontmatter block strictly between the opening and closing "---"
// delimiters and returns true only if `status: DONE` (unquoted or single/double quoted) appears inside frontmatter.
func hasFrontmatterStatusDone(text string) bool {
	_, done := hasFrontmatterStatusDoneForStory(text, "")
	return done
}

func hasFrontmatterStatusDoneForStory(text, expectedStoryID string) (bool, bool) {
	trimmed := strings.TrimLeft(text, "\ufeff \t\r\n")
	lines := strings.Split(trimmed, "\n")
	if len(lines) < 3 || strings.TrimSpace(lines[0]) != "---" {
		return false, false
	}
	closingIdx := -1
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			closingIdx = i
			break
		}
	}
	if closingIdx < 1 {
		return false, false
	}
	statusDone := false
	frontmatterID := ""
	for i := 1; i < closingIdx; i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k := strings.TrimSpace(key)
		if idx := strings.Index(val, "#"); idx >= 0 {
			val = val[:idx]
		}
		val = strings.TrimSpace(val)
		val = strings.Trim(val, `"'`)
		switch k {
		case "id", "story_id":
			frontmatterID = val
		case "status":
			statusDone = (val == "DONE")
		}
	}
	if expectedStoryID != "" && frontmatterID != "" && !strings.EqualFold(frontmatterID, expectedStoryID) {
		return false, false
	}
	return true, statusDone
}
