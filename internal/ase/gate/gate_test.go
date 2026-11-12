package gate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerify_AllClean(t *testing.T) {
	tmpDir := t.TempDir()
	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	if err := os.MkdirAll(storiesDir, 0755); err != nil {
		t.Fatal(err)
	}
	storyFile := filepath.Join(storiesDir, "2026-10-25-gov-3684-test.story.md")
	if err := os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-3684\nstatus: DONE\n---\n"), 0644); err != nil {
		t.Fatal(err)
	}

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return " abcdef0 submod (heads/main)", nil
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if !res.Passed {
		t.Fatalf("expected gate to pass, got failed: reason=%s, unmet=%v", res.Reason, res.UnmetGates)
	}
	if !res.GitClean || !res.BranchClean || !res.SubmodulesClean || !res.StoryDone {
		t.Errorf("expected all sub-gates true, got %+v", res)
	}
}

func TestVerify_DirtyTree(t *testing.T) {
	tmpDir := t.TempDir()

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return " M file.go\n?? untracked.txt", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		case "log":
			return "commit12345", nil
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if res.Passed {
		t.Fatalf("expected gate to fail on dirty tree, got passed")
	}
	if res.Reason != "incomplete_ase_flow" {
		t.Errorf("expected reason incomplete_ase_flow, got %s", res.Reason)
	}
	if res.GitClean {
		t.Errorf("expected GitClean=false")
	}
}

func TestVerify_DetachedHead(t *testing.T) {
	tmpDir := t.TempDir()

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "HEAD", nil // detached HEAD!
		case "submodule":
			return "", nil
		case "log":
			return "commit12345", nil
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if res.Passed {
		t.Fatalf("expected gate to fail on detached HEAD, got passed")
	}
	if res.BranchClean {
		t.Errorf("expected BranchClean=false")
	}
}

func TestVerify_SubmoduleGitlinkLag(t *testing.T) {
	tmpDir := t.TempDir()

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "+abcdef0 submod (heads/main)", nil // + indicates uncommitted gitlink change!
		case "log":
			return "commit12345", nil
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if res.Passed {
		t.Fatalf("expected gate to fail on submodule lag, got passed")
	}
	if res.SubmodulesClean {
		t.Errorf("expected SubmodulesClean=false")
	}
}

func TestVerify_StoryNotDone(t *testing.T) {
	tmpDir := t.TempDir()
	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	_ = os.MkdirAll(storiesDir, 0755)

	storyFile := filepath.Join(storiesDir, "2026-10-25-gov-3684-test.story.md")
	_ = os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-3684\nstatus: IN_PROGRESS\n---\n"), 0644)

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		case "log":
			return "", fmt.Errorf("no commit found")
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if res.Passed {
		t.Fatalf("expected gate to fail when story not DONE, got passed")
	}
	if res.StoryDone {
		t.Errorf("expected StoryDone=false")
	}
}

func TestVerify_StoryNotDone_EvenWithGitCommit(t *testing.T) {
	tmpDir := t.TempDir()
	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	_ = os.MkdirAll(storiesDir, 0755)

	storyFile := filepath.Join(storiesDir, "2026-10-25-gov-3684-test.story.md")
	_ = os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-3684\nstatus: IN_PROGRESS\n---\n"), 0644)

	// Even if git log returns commits on HEAD matching the story ID,
	// the story file is still IN_PROGRESS and must NOT be considered complete!
	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		case "log":
			return "commit-initial-bind-3684", nil
		default:
			return "", nil
		}
	}

	opts := GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-3684",
		GitRunner:    mockGit,
	}

	res := Verify(opts)
	if res.Passed {
		t.Fatalf("expected gate to fail when story file is IN_PROGRESS despite git commits on branch")
	}
	if res.StoryDone {
		t.Errorf("expected StoryDone=false when story markdown status is IN_PROGRESS")
	}
}

func TestDefaultGitRunner(t *testing.T) {
	tmpDir := t.TempDir()
	runner := DefaultGitRunner(tmpDir)
	out, err := runner("version")
	if err != nil || len(out) == 0 {
		t.Fatalf("DefaultGitRunner failed: %v out=%s", err, out)
	}
}

func TestVerify_GitCommandsFailClosed(t *testing.T) {
	tmpDir := t.TempDir()

	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", fmt.Errorf("git status failed")
		case "rev-parse":
			return "", fmt.Errorf("git rev-parse failed")
		case "submodule":
			return "", fmt.Errorf("git submodule status failed")
		default:
			return "", nil
		}
	}

	res := Verify(GateOptions{
		WorktreePath: tmpDir,
		GitRunner:    mockGit,
	})
	if res.Passed {
		t.Fatal("expected Verify to fail closed when git commands return errors")
	}
	if res.GitClean || res.BranchClean || res.SubmodulesClean {
		t.Fatalf("expected GitClean, BranchClean, and SubmodulesClean to be false, got %+v", res)
	}
	if len(res.UnmetGates) != 3 {
		t.Fatalf("expected 3 unmet gates (clean_working_tree, branch_not_detached, submodule_gitlinks_synchronized), got %v", res.UnmetGates)
	}
}

func TestVerify_FrontmatterOnlyStatusDone(t *testing.T) {
	tmpDir := t.TempDir()
	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	if err := os.MkdirAll(storiesDir, 0755); err != nil {
		t.Fatal(err)
	}

	storyFile := filepath.Join(storiesDir, "2026-11-07-gov-5176-test.story.md")
	// Body mentions `status: DONE` (e.g. closeout template), while YAML frontmatter is IN PROGRESS.
	bodyWithFalsePositive := "---\nid: STORY-GOV-5176\nstatus: IN PROGRESS\n---\n\n# Story\nUpdate `status: DONE` in closeout.\nstatus: \"DONE\"\n"
	if err := os.WriteFile(storyFile, []byte(bodyWithFalsePositive), 0644); err != nil {
		t.Fatal(err)
	}

	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}

	res := Verify(GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-5176",
		GitRunner:    mockGit,
	})
	if res.Passed || res.StoryDone {
		t.Fatalf("expected StoryDone=false when status: DONE is only in markdown body, got %+v", res)
	}

	// Now update frontmatter to status: DONE
	doneFrontmatter := "---\nid: STORY-GOV-5176\nstatus: \"DONE\" # completed\n---\n\n# Story\n"
	if err := os.WriteFile(storyFile, []byte(doneFrontmatter), 0644); err != nil {
		t.Fatal(err)
	}
	res2 := Verify(GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-5176",
		GitRunner:    mockGit,
	})
	if !res2.Passed || !res2.StoryDone {
		t.Fatalf("expected StoryDone=true when frontmatter status is DONE, got %+v", res2)
	}
}

func TestVerify_UninitializedSubmoduleAndMissingStoriesFailsClosed(t *testing.T) {
	tmpDir := t.TempDir()
	logCalled := false

	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "-abcdef0 uninitialized-submod", nil
		case "log":
			logCalled = true
			return "deadbeef", nil
		default:
			return "", nil
		}
	}

	res := Verify(GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-5176",
		GitRunner:    mockGit,
	})
	if res.SubmodulesClean {
		t.Fatalf("expected SubmodulesClean=false for '-' uninitialized submodule, got %+v", res)
	}
	if res.StoryDone {
		t.Fatalf("expected StoryDone=false when docs/stories does not exist and DocsHubPath is empty, got %+v", res)
	}
	if logCalled {
		t.Fatalf("expected git log fallback NOT to be called in isStoryCompleted")
	}
}

func TestVerify_DocsHubStoryResolutionAndStrictFrontmatter(t *testing.T) {
	worktreeDir := t.TempDir() // submodule worktree without docs/stories
	docsHubDir := t.TempDir()
	hubStoriesDir := filepath.Join(docsHubDir, "docs", "stories")
	if err := os.MkdirAll(hubStoriesDir, 0755); err != nil {
		t.Fatal(err)
	}
	storyFile := filepath.Join(hubStoriesDir, "2026-11-07-gov-6001-bypass.story.md")

	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}

	// 1. Unclosed frontmatter delimiter must fail closed even if status: DONE appears
	if err := os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-6001\nstatus: DONE\n# missing closing delimiter\n"), 0644); err != nil {
		t.Fatal(err)
	}
	res := Verify(GateOptions{
		WorktreePath: worktreeDir,
		DocsHubPath:  docsHubDir,
		StoryID:      "STORY-GOV-6001",
		GitRunner:    mockGit,
	})
	if res.StoryDone || res.Passed {
		t.Fatalf("expected unclosed YAML frontmatter to fail closed, got %+v", res)
	}

	// 2. Story in DocsHubPath with status: READY must fail closed
	if err := os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-6001\nstatus: READY\n---\nstatus: DONE\n"), 0644); err != nil {
		t.Fatal(err)
	}
	res = Verify(GateOptions{
		WorktreePath: worktreeDir,
		DocsHubPath:  docsHubDir,
		StoryID:      "STORY-GOV-6001",
		GitRunner:    mockGit,
	})
	if res.StoryDone || res.Passed {
		t.Fatalf("expected READY story in DocsHubPath to fail closed, got %+v", res)
	}

	// 3. Story in DocsHubPath with status: DONE between --- delimiters passes
	if err := os.WriteFile(storyFile, []byte("---\nid: STORY-GOV-6001\nstatus: DONE\n---\n"), 0644); err != nil {
		t.Fatal(err)
	}
	res = Verify(GateOptions{
		WorktreePath: worktreeDir,
		DocsHubPath:  docsHubDir,
		StoryID:      "STORY-GOV-6001",
		GitRunner:    mockGit,
	})
	if !res.StoryDone || !res.Passed {
		t.Fatalf("expected DONE story in DocsHubPath to pass, got %+v", res)
	}
}

func TestVerify_PorcelainExactPathMatching(t *testing.T) {
	tmpDir := t.TempDir()

	// Legitimate transient paths: exact coverage.out or .scannerwork segment
	cleanGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "?? coverage.out\n?? internal/gate/coverage.out\n?? .scannerwork/\n?? sub/.scannerwork/report.txt", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}
	resClean := Verify(GateOptions{
		WorktreePath: tmpDir,
		GitRunner:    cleanGit,
	})
	if !resClean.Passed || !resClean.GitClean {
		t.Fatalf("expected exact coverage.out and .scannerwork paths to be ignored, got %+v", resClean)
	}

	// Bypass attempts using substring matches
	bypassCases := []string{
		"?? hack_coverage.out.go",
		"?? my_coverage.out",
		"?? coverage.out/backdoor.go",
		"?? coverage.out/",
		"?? .scannerwork_bypass.go",
		"?? fake.scannerwork",
		"R  coverage.out -> evil.go",
	}
	for _, line := range bypassCases {
		dirtyGit := func(args ...string) (string, error) {
			switch args[0] {
			case "status":
				return line, nil
			case "rev-parse":
				return "main", nil
			case "submodule":
				return "", nil
			default:
				return "", nil
			}
		}
		res := Verify(GateOptions{
			WorktreePath: tmpDir,
			GitRunner:    dirtyGit,
		})
		if res.Passed || res.GitClean {
			t.Fatalf("expected porcelain line %q to mark worktree dirty, got %+v", line, res)
		}
	}
}

func TestVerify_StampHEADBoundVerification(t *testing.T) {
	tmpDir := t.TempDir()
	reportsDir := filepath.Join(tmpDir, "tmp", "reports")
	if err := os.MkdirAll(reportsDir, 0755); err != nil {
		t.Fatal(err)
	}
	testStamp := filepath.Join(reportsDir, "validate-local-ok.stamp")
	relStamp := filepath.Join(reportsDir, "verify-live-release-ok.stamp")

	currentHEAD := "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4"
	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			if len(args) > 1 && args[1] == "--abbrev-ref" {
				return "main", nil
			}
			return currentHEAD, nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}

	// 1. Empty stamp files fail closed
	_ = os.WriteFile(testStamp, []byte("   \n"), 0644)
	_ = os.WriteFile(relStamp, []byte(""), 0644)
	resEmpty := Verify(GateOptions{
		WorktreePath:           tmpDir,
		RequireTestsPassed:     true,
		RequireReleaseVerified: true,
		GitRunner:              mockGit,
	})
	if resEmpty.Passed || resEmpty.TestsPassed || resEmpty.ReleaseVerified {
		t.Fatalf("expected empty stamp files to fail closed, got %+v", resEmpty)
	}

	// 2. Stale/mismatched SHA in stamp fails closed
	_ = os.WriteFile(testStamp, []byte("deadbeef1234567890"), 0644)
	_ = os.WriteFile(relStamp, []byte("head_sha=0123456789abcdef"), 0644)
	resStale := Verify(GateOptions{
		WorktreePath:           tmpDir,
		RequireTestsPassed:     true,
		RequireReleaseVerified: true,
		GitRunner:              mockGit,
	})
	if resStale.Passed || resStale.TestsPassed || resStale.ReleaseVerified {
		t.Fatalf("expected stale SHA in stamp files to fail closed, got %+v", resStale)
	}

	// 3. Matching HEAD SHA (full or short prefix / key-value) passes
	_ = os.WriteFile(testStamp, []byte(currentHEAD+"\n"), 0644)
	_ = os.WriteFile(relStamp, []byte("head_sha="+currentHEAD[:12]+"\n"), 0644)
	resMatch := Verify(GateOptions{
		WorktreePath:           tmpDir,
		RequireTestsPassed:     true,
		RequireReleaseVerified: true,
		GitRunner:              mockGit,
	})
	if !resMatch.Passed || !resMatch.TestsPassed || !resMatch.ReleaseVerified {
		t.Fatalf("expected matching HEAD SHA in stamp files to pass, got %+v", resMatch)
	}

	// 4. Non-empty stamp file without a commit SHA fails closed (P1-IAZIO-05)
	_ = os.WriteFile(testStamp, []byte("status=ok\n"), 0644)
	_ = os.WriteFile(relStamp, []byte("verified\n"), 0644)
	resNoSHA := Verify(GateOptions{
		WorktreePath:           tmpDir,
		RequireTestsPassed:     true,
		RequireReleaseVerified: true,
		GitRunner:              mockGit,
	})
	if resNoSHA.Passed || resNoSHA.TestsPassed || resNoSHA.ReleaseVerified {
		t.Fatalf("expected stamp files without commit SHA to fail closed, got %+v", resNoSHA)
	}
}

func TestVerify_UnmergedFeatureBranchFailsClosed(t *testing.T) {
	tmpDir := t.TempDir()
	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "feat/gov-5258-unmerged", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}

	res := Verify(GateOptions{
		WorktreePath: tmpDir,
		GitRunner:    mockGit,
	})
	if res.Passed || res.BranchClean {
		t.Fatalf("expected unmerged feature branch to fail BranchClean, got %+v", res)
	}
}

func TestVerify_ADR002AutopilotStateAndSubstringBoundary(t *testing.T) {
	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}

	// 1. Substring prefix STORY-GOV-12 must NOT false-match 2026-10-25-gov-123-other.story.md
	tmpDir := t.TempDir()
	storiesDir := filepath.Join(tmpDir, "docs", "stories")
	if err := os.MkdirAll(storiesDir, 0755); err != nil {
		t.Fatal(err)
	}
	otherStory := filepath.Join(storiesDir, "2026-10-25-gov-123-other.story.md")
	if err := os.WriteFile(otherStory, []byte("---\nid: STORY-GOV-123\nstatus: DONE\n---\n"), 0644); err != nil {
		t.Fatal(err)
	}

	resSubstr := Verify(GateOptions{
		WorktreePath: tmpDir,
		StoryID:      "STORY-GOV-12",
		GitRunner:    mockGit,
	})
	if resSubstr.Passed || resSubstr.StoryDone {
		t.Fatalf("expected STORY-GOV-12 not to false-match STORY-GOV-123, got %+v", resSubstr)
	}

	// 2. ADR-002 .autopilot/ase-state.json in OSS leaf worktree without docs/stories/
	ossDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ossDir, ".autopilot"), 0755); err != nil {
		t.Fatal(err)
	}
	stateFile := filepath.Join(ossDir, ".autopilot", "ase-state.json")
	if err := os.WriteFile(stateFile, []byte(`{"story_id":"STORY-GOV-5258","status":"IN_PROGRESS"}`), 0644); err != nil {
		t.Fatal(err)
	}
	resInProg := Verify(GateOptions{
		WorktreePath: ossDir,
		StoryID:      "STORY-GOV-5258",
		GitRunner:    mockGit,
	})
	if resInProg.Passed || resInProg.StoryDone {
		t.Fatalf("expected IN_PROGRESS .autopilot/ase-state.json to fail closed, got %+v", resInProg)
	}

	if err := os.WriteFile(stateFile, []byte(`{"story_id":"STORY-GOV-5258","status":"DONE"}`), 0644); err != nil {
		t.Fatal(err)
	}
	resDone := Verify(GateOptions{
		WorktreePath: ossDir,
		StoryID:      "STORY-GOV-5258",
		GitRunner:    mockGit,
	})
	if !resDone.Passed || !resDone.StoryDone {
		t.Fatalf("expected DONE .autopilot/ase-state.json to pass StoryDone, got %+v", resDone)
	}
}
