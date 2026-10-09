// Package preflight decides whether a harness child may start.
package preflight

import (
	"os"
	"strconv"
	"strings"
)

// DefaultMinFreeBytes is the default free-space floor on the worktree mount (5 GiB).
const DefaultMinFreeBytes uint64 = 5 << 30

// MinFreeBytes is the fallback constant kept for backward compatibility (10 GiB).
const MinFreeBytes uint64 = 10 << 30

// RequiredMinFreeBytes returns the active threshold in bytes, configurable via IAZIO_PREFLIGHT_MIN_FREE_BYTES.
func RequiredMinFreeBytes() uint64 {
	if v := os.Getenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES"); v != "" {
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMinFreeBytes
}

const (
	// KindOrdinary is a normal scheduled slice.
	KindOrdinary = "ordinary"
	// KindRefinement checks docs-hub porcelain only.
	KindRefinement = "story_refinement"
	// KindIntervention may start on a dirty or detached checkout.
	KindIntervention = "intervention"
	// KindLoop follows the ordinary rules.
	KindLoop = "loop"
	// KindResume follows the ordinary rules.
	KindResume = "resume"

	// ReasonDirty means porcelain is not empty. The queue is paused.
	ReasonDirty = "HALTED_DIRTY"
	// ReasonDetached means HEAD is detached. The queue is paused.
	ReasonDetached = "HALTED_DETACHED"
	// ReasonUntracked means the branch is not the default and has no upstream. The queue is paused.
	ReasonUntracked = "HALTED_UNTRACKED"
	// ReasonDisk means free space is under the required floor (default 5 GiB). The queue is not paused.
	ReasonDisk = "HALTED_DISK"
	// ReasonNoDocsHub means the docs hub is not a git work tree. The queue is not paused.
	ReasonNoDocsHub = "HALTED_NO_DOCS_HUB"
	// ReasonGitAuth means the git remote probe failed. The queue is not paused and healing stays put.
	ReasonGitAuth = "HALTED_GIT_AUTH"
)

// Report is the checkout snapshot taken before spawn.
type Report struct {
	Kind          string
	FreeBytes     uint64
	WorkPorcelain string
	HubPorcelain  string
	HeadAttached  bool
	Branch        string
	DefaultBranch string
	HasUpstream   bool
	DocsHubOK     bool
	GitAuthOK     bool
	GitWorkTree   bool
}

// Decision is the spawn gate result.
// IncrementHealing is never set: disk, auth, and dirty stops do not heal.
type Decision struct {
	Reason           string
	PauseQueue       bool
	IncrementHealing bool
}

// Decide applies the pre-launch rules.
// Disk, a missing docs hub, and a failed git probe do not pause the queue.
// story_refinement checks porcelain only on the docs hub.
// intervention may start when porcelain is dirty, HEAD is detached, or the branch has no upstream.
func Decide(r Report) Decision {
	if r.FreeBytes < RequiredMinFreeBytes() {
		return Decision{Reason: ReasonDisk}
	}
	if !r.DocsHubOK || !r.GitWorkTree {
		return Decision{Reason: ReasonNoDocsHub}
	}
	if !r.GitAuthOK {
		return Decision{Reason: ReasonGitAuth}
	}
	if r.Kind == KindIntervention {
		return Decision{}
	}
	if r.Kind == KindResume {
		// Resumption is explicitly intended to recover an in-flight prompt execution
		// that left uncommitted work, modified submodules, or local review reports in the worktree.
		// It permits dirty porcelain and untracked branches, skipping ReasonDirty and ReasonUntracked.
		if !r.HeadAttached {
			return Decision{Reason: ReasonDetached, PauseQueue: true}
		}
		return Decision{}
	}
	hubDirty := strings.TrimSpace(r.HubPorcelain) != ""
	workDirty := strings.TrimSpace(r.WorkPorcelain) != ""
	if r.Kind == KindRefinement {
		if hubDirty {
			return Decision{Reason: ReasonDirty, PauseQueue: true}
		}
		return Decision{}
	}
	if hubDirty || workDirty {
		return Decision{Reason: ReasonDirty, PauseQueue: true}
	}
	if !r.HeadAttached {
		return Decision{Reason: ReasonDetached, PauseQueue: true}
	}
	if r.Branch != r.DefaultBranch && !r.HasUpstream {
		return Decision{Reason: ReasonUntracked, PauseQueue: true}
	}
	return Decision{}
}
