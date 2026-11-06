package gate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVerify_AllClean(t *testing.T) {
	tmpDir := t.TempDir()

	mockGit := func(args ...string) (string, error) {
		cmd := args[0]
		switch cmd {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return " abcdef0 submod (heads/main)", nil
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

func TestVerify_UninitializedSubmoduleAndGitLogNoLimit(t *testing.T) {
	tmpDir := t.TempDir()
	var logArgs []string

	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "-abcdef0 uninitialized-submod", nil
		case "log":
			logArgs = append([]string(nil), args...)
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
	for _, a := range logArgs {
		if a == "-n" || a == "10" {
			t.Fatalf("expected git log without -n 10 limit, got args %v", logArgs)
		}
	}
}
