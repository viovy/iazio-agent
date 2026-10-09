package preflight

import (
	"context"
	"os/exec"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/sessionstore"
)

// GitRunner runs a git command in a working directory and returns trimmed output and error.
type GitRunner func(ctx context.Context, dir string, args ...string) (string, error)

// DiskChecker returns available bytes on the filesystem containing path.
type DiskChecker func(path string) (uint64, error)

// Collector gathers the preflight Report from the filesystem and git.
type Collector struct {
	FreeSpace DiskChecker
	Git       GitRunner
}

// DefaultCollector returns a collector using the real filesystem and git CLI.
func DefaultCollector() *Collector {
	return &Collector{
		FreeSpace: FreeSpace,
		Git: func(ctx context.Context, dir string, args ...string) (string, error) {
			cmdArgs := append([]string{"-C", dir}, args...)
			cmd := exec.CommandContext(ctx, "git", cmdArgs...)
			out, err := cmd.CombinedOutput()
			return strings.TrimSpace(string(out)), err
		},
	}
}

// Collect gathers the preflight Report for a given worktree and docs-hub.
func (c *Collector) Collect(ctx context.Context, worktree, docsHub, kind string) (Report, error) {
	r := Report{
		Kind: kind,
	}

	if c.FreeSpace != nil && worktree != "" {
		if bytes, err := c.FreeSpace(worktree); err == nil {
			r.FreeBytes = bytes
		}
	}

	if worktree != "" && c.Git != nil {
		out, err := c.Git(ctx, worktree, "rev-parse", "--is-inside-work-tree")
		if err == nil && out == "true" {
			r.GitWorkTree = true
		}
	}

	if docsHub != "" && c.Git != nil {
		out, err := c.Git(ctx, docsHub, "rev-parse", "--is-inside-work-tree")
		if err == nil && out == "true" {
			r.DocsHubOK = true
		}
	} else if docsHub == "" && kind != KindRefinement {
		r.DocsHubOK = true
	}

	if r.GitWorkTree && c.Git != nil {
		if out, err := c.Git(ctx, worktree, "status", "--porcelain"); err == nil {
			r.WorkPorcelain = out
			if strings.TrimSpace(out) != "" {
				dCtx := sessionstore.DefaultResolver().DetectDirtyWorktreeContext(worktree)
				r.DirtyStoryID = dCtx.StoryID
				r.DirtyReviewFile = dCtx.ReviewFile
				r.DetectedConversationID = dCtx.ConversationID
				r.DetectedVerdict = dCtx.Verdict
			}
		}

		_, errRef := c.Git(ctx, worktree, "symbolic-ref", "-q", "HEAD")
		r.HeadAttached = (errRef == nil)

		branch, _ := c.Git(ctx, worktree, "branch", "--show-current")
		if branch == "" {
			branch, _ = c.Git(ctx, worktree, "symbolic-ref", "--short", "-q", "HEAD")
		}
		r.Branch = branch

		defBranch := "main"
		if originHead, err := c.Git(ctx, worktree, "symbolic-ref", "--short", "refs/remotes/origin/HEAD"); err == nil && originHead != "" {
			defBranch = strings.TrimPrefix(originHead, "origin/")
		}
		r.DefaultBranch = defBranch

		_, errUpstream := c.Git(ctx, worktree, "rev-parse", "--verify", "--quiet", "@{upstream}")
		r.HasUpstream = (errUpstream == nil)

		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, errAuth := c.Git(probeCtx, worktree, "ls-remote", "--exit-code", "origin", "HEAD")
		r.GitAuthOK = (errAuth == nil)
	}

	if r.DocsHubOK && docsHub != "" && c.Git != nil {
		if out, err := c.Git(ctx, docsHub, "status", "--porcelain"); err == nil {
			r.HubPorcelain = out
		}
	}

	return r, nil
}

// Collect runs preflight collection using the DefaultCollector.
func Collect(ctx context.Context, worktree, docsHub, kind string) (Report, error) {
	return DefaultCollector().Collect(ctx, worktree, docsHub, kind)
}
