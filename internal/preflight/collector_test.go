package preflight

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestFreeSpaceSmoke(t *testing.T) {
	bytes, err := FreeSpace(".")
	if err != nil {
		t.Fatalf("FreeSpace failed: %v", err)
	}
	if bytes == 0 {
		t.Fatalf("expected non-zero free bytes, got 0")
	}
}

func TestCollectorMocked(t *testing.T) {
	col := &Collector{
		FreeSpace: func(string) (uint64, error) {
			return 15 << 30, nil // 15 GiB
		},
		Git: func(_ context.Context, dir string, args ...string) (string, error) {
			cmd := strings.Join(args, " ")
			switch {
			case strings.Contains(cmd, "rev-parse --is-inside-work-tree"):
				return "true", nil
			case strings.Contains(cmd, "status --porcelain"):
				if dir == "/hub" {
					return "", nil
				}
				return " M file.go", nil
			case strings.Contains(cmd, "symbolic-ref -q HEAD"):
				return "refs/heads/feature", nil
			case strings.Contains(cmd, "branch --show-current"):
				return "feature", nil
			case strings.Contains(cmd, "symbolic-ref --short refs/remotes/origin/HEAD"):
				return "origin/main", nil
			case strings.Contains(cmd, "rev-parse --verify --quiet @{upstream}"):
				return "commit-sha", nil
			case strings.Contains(cmd, "ls-remote"):
				return "head-sha", nil
			default:
				return "", nil
			}
		},
	}

	rep, err := col.Collect(context.Background(), "/work", "/hub", KindOrdinary)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if rep.Kind != KindOrdinary {
		t.Fatalf("expected KindOrdinary, got %s", rep.Kind)
	}
	if rep.FreeBytes != 15<<30 {
		t.Fatalf("expected 15 GiB, got %d", rep.FreeBytes)
	}
	if !rep.GitWorkTree || !rep.DocsHubOK {
		t.Fatalf("expected GitWorkTree and DocsHubOK, got %+v", rep)
	}
	if rep.WorkPorcelain != " M file.go" {
		t.Fatalf("unexpected WorkPorcelain: %q", rep.WorkPorcelain)
	}
	if rep.HubPorcelain != "" {
		t.Fatalf("unexpected HubPorcelain: %q", rep.HubPorcelain)
	}
	if !rep.HeadAttached {
		t.Fatalf("expected HeadAttached")
	}
	if rep.Branch != "feature" || rep.DefaultBranch != "main" {
		t.Fatalf("unexpected branches: %s / %s", rep.Branch, rep.DefaultBranch)
	}
	if !rep.HasUpstream || !rep.GitAuthOK {
		t.Fatalf("expected HasUpstream and GitAuthOK")
	}

	dec := Decide(rep)
	if dec.Reason != ReasonDirty || !dec.PauseQueue {
		t.Fatalf("expected ReasonDirty pause, got %+v", dec)
	}
}

func TestCollectorWithTempWorktree(t *testing.T) {
	dir := t.TempDir()
	// Non-git directory
	col := DefaultCollector()
	rep, err := col.Collect(context.Background(), dir, "", KindOrdinary)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if rep.GitWorkTree {
		t.Fatalf("expected non-git worktree to have GitWorkTree false")
	}
	if rep.FreeBytes == 0 {
		t.Fatalf("expected positive FreeBytes")
	}
	// Explicitly isolate disk headroom check from host disk variance
	rep.FreeBytes = 20 << 30
	dec := Decide(rep)
	// Missing docs hub or not a work tree halts with ReasonNoDocsHub
	if dec.Reason != ReasonNoDocsHub {
		t.Fatalf("expected ReasonNoDocsHub, got %s", dec.Reason)
	}
	_ = os.Chdir(dir)
}

func TestCollectorEmptyDocsHub(t *testing.T) {
	col := &Collector{
		FreeSpace: func(string) (uint64, error) {
			return 15 << 30, nil
		},
		Git: func(_ context.Context, dir string, args ...string) (string, error) {
			cmd := strings.Join(args, " ")
			switch {
			case strings.Contains(cmd, "rev-parse --is-inside-work-tree"):
				return "true", nil
			case strings.Contains(cmd, "status --porcelain"):
				return "", nil
			case strings.Contains(cmd, "symbolic-ref -q HEAD"):
				return "refs/heads/main", nil
			case strings.Contains(cmd, "branch --show-current"):
				return "main", nil
			case strings.Contains(cmd, "rev-parse --verify --quiet @{upstream}"):
				return "commit-sha", nil
			case strings.Contains(cmd, "ls-remote"):
				return "head-sha", nil
			default:
				return "", nil
			}
		},
	}

	// Ordinary job without docs hub: DocsHubOK is true, Decides cleanly
	repOrd, err := col.Collect(context.Background(), "/work", "", KindOrdinary)
	if err != nil {
		t.Fatalf("Collect ordinary failed: %v", err)
	}
	if !repOrd.DocsHubOK {
		t.Fatalf("expected DocsHubOK true for ordinary job with empty docsHub, got %+v", repOrd)
	}
	decOrd := Decide(repOrd)
	if decOrd.Reason != "" {
		t.Fatalf("expected clean decision for ordinary job, got %+v", decOrd)
	}

	// Refinement job without docs hub: DocsHubOK is false, Decides ReasonNoDocsHub
	repRef, err := col.Collect(context.Background(), "/work", "", KindRefinement)
	if err != nil {
		t.Fatalf("Collect refinement failed: %v", err)
	}
	if repRef.DocsHubOK {
		t.Fatalf("expected DocsHubOK false for refinement job with empty docsHub, got %+v", repRef)
	}
	decRef := Decide(repRef)
	if decRef.Reason != ReasonNoDocsHub {
		t.Fatalf("expected ReasonNoDocsHub for refinement job without docs hub, got %+v", decRef)
	}
}
