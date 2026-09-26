// Package preflight decides whether a harness child may start.
package preflight

// MinFreeBytes is the free-space floor on the worktree mount.
const MinFreeBytes = 10 << 30

const (
	KindOrdinary     = "ordinary"
	KindRefinement   = "story_refinement"
	KindIntervention = "intervention"

	ReasonDirty     = "HALTED_DIRTY"
	ReasonDetached  = "HALTED_DETACHED"
	ReasonUntracked = "HALTED_UNTRACKED"
	ReasonDisk      = "HALTED_DISK"
	ReasonNoDocsHub = "HALTED_NO_DOCS_HUB"
	ReasonGitAuth   = "HALTED_GIT_AUTH"
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
type Decision struct {
	Reason     string
	PauseQueue bool
}

// Decide applies the pre-launch rules. Disk, missing docs hub, and git auth
// do not pause the queue and do not consume a healing attempt.
func Decide(r Report) Decision {
	if r.FreeBytes < MinFreeBytes {
		return Decision{Reason: ReasonDisk}
	}
	if !r.DocsHubOK {
		return Decision{Reason: ReasonNoDocsHub}
	}
	if !r.GitAuthOK {
		return Decision{Reason: ReasonGitAuth}
	}
	if r.Kind == KindIntervention {
		if !r.GitWorkTree {
			return Decision{Reason: ReasonNoDocsHub}
		}
		return Decision{}
	}
	hubDirty := r.HubPorcelain != ""
	workDirty := r.WorkPorcelain != ""
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
