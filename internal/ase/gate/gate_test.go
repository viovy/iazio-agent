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
