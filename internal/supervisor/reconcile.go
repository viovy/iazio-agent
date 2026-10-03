package supervisor

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Proc is one row from an injected process table.
type Proc struct {
	PID  int
	Name string
	Cwd  string
}

// Lease is a job the control plane still considers checked out on this host.
type Lease struct {
	JobID    string
	Worktree string
}

// LeaseOutcome is the disposition of one stranded lease.
type LeaseOutcome struct {
	JobID   string
	State   string
	Reason  string
	Healing int
}

// ReconcileResult is the startup reconciliation report.
type ReconcileResult struct {
	Terminated        []int
	Pruned            []string
	IndexLocksRemoved []string
	Locks             map[string]string
	Leases            []LeaseOutcome
	PauseQueue        bool
	HaltCode          string
	Healing           int
}

// Reconcile terminates orphan harnesses on registered checkouts, prunes only idle checkouts,
// marks stranded leases failed with host_restart, and pauses when porcelain is dirty.
// Healing is not incremented.
func Reconcile(procs []Proc, leases []Lease, porcelain map[string]string, signal func(pid int, sig string) error, prune func(string) error, unlock func(string) error) ReconcileResult {
	order := make([]string, 0, len(leases))
	registered := map[string]struct{}{}
	for _, lease := range leases {
		path := filepath.Clean(lease.Worktree)
		if _, ok := registered[path]; ok {
			continue
		}
		registered[path] = struct{}{}
		order = append(order, path)
	}
	still := make([]Proc, 0, len(procs))
	terminated := make([]int, 0)
	for _, proc := range procs {
		cwd := filepath.Clean(proc.Cwd)
		_, onRegistered := registered[cwd]
		if orphanName(proc.Name) && onRegistered {
			if signal == nil || signal(proc.PID, "TERM") == nil {
				terminated = append(terminated, proc.PID)
				continue
			}
		}
		still = append(still, proc)
	}
	held := map[string]bool{}
	for _, proc := range still {
		held[filepath.Clean(proc.Cwd)] = true
	}
	pruned := make([]string, 0)
	unlocked := make([]string, 0)
	for _, path := range order {
		if held[path] {
			continue
		}
		if prune != nil && prune(path) == nil {
			pruned = append(pruned, path)
		}
		if unlock != nil && unlock(path) == nil {
			unlocked = append(unlocked, path)
		}
	}
	locks := make(map[string]string, len(order))
	dirty := false
	for _, path := range order {
		locks[path] = LockIdle
		if strings.TrimSpace(porcelain[path]) != "" {
			dirty = true
		}
	}
	outcomes := make([]LeaseOutcome, 0)
	for _, lease := range leases {
		if harnessLive(still, lease.Worktree) {
			continue
		}
		outcomes = append(outcomes, LeaseOutcome{
			JobID:   lease.JobID,
			State:   LockFailed,
			Reason:  ReasonHostRestart,
			Healing: 0,
		})
	}
	res := ReconcileResult{
		Terminated:        terminated,
		Pruned:            pruned,
		IndexLocksRemoved: unlocked,
		Locks:             locks,
		Leases:            outcomes,
		Healing:           0,
	}
	if dirty {
		res.PauseQueue = true
		res.HaltCode = "HALTED_DIRTY"
	}
	return res
}

// DiscardCheckout cleans an IDLE checkout.
// A docs-hub discard acquires the hub lock first. If that lock fails, neither git command runs.
func DiscardCheckout(idle bool, docsHub bool, lock func(time.Duration) error, git func(args ...string) error) error {
	if !idle {
		return errors.New("discard requires IDLE")
	}
	if docsHub {
		if lock == nil {
			return errors.New("docs hub locker is not configured")
		}
		if err := lock(HubLockTimeout); err != nil {
			return err
		}
	}
	if git == nil {
		return errors.New("git runner is not configured")
	}
	if err := git("reset", "--hard"); err != nil {
		return err
	}
	return git("clean", "-fd")
}

func harnessLive(procs []Proc, worktree string) bool {
	want := filepath.Clean(worktree)
	for _, proc := range procs {
		if filepath.Clean(proc.Cwd) == want && baseName(proc.Name) == "iazio-harness" {
			return true
		}
	}
	return false
}

func orphanName(name string) bool {
	switch baseName(name) {
	case "iazio-harness", "agy", "agent", "opencode":
		return true
	default:
		return false
	}
}

func baseName(name string) string {
	name = filepath.Base(name)
	ext := filepath.Ext(name)
	if strings.ToLower(ext) == ".exe" {
		return name[:len(name)-len(ext)]
	}
	return name
}

var (
	processOnce sync.Once
	processID   string
)

// FormatHostID joins a hostname and a uuid.
func FormatHostID(hostname, uuid string) string {
	return hostname + "-" + uuid
}

// ProcessHostID returns a hostname-uuid generated once for this process.
func ProcessHostID(hostname, uuid string) string {
	processOnce.Do(func() {
		processID = FormatHostID(hostname, uuid)
	})
	return processID
}

// RepoState represents one repository status to reconcile.
type RepoState struct {
	WorktreePath   string
	Queue          string
	Reason         string
	DiscardPending bool
}

// ReconcileHostCheckouts checks registered repos on each tick:
// 1. If DiscardPending is set and the lock is idle, it discards changes and calls resume.
// 2. If the queue is PAUSED with HALTED_DIRTY and the worktree is idle and clean, it calls resume.
// Errors are reported to stderr, informative events to stdout.
func ReconcileHostCheckouts(
	repos []RepoState,
	isLockIdle func(worktree string) bool,
	discard func(worktree string) error,
	isClean func(worktree string) (bool, error),
	resume func(worktree string) error,
	stdout io.Writer,
	stderr io.Writer,
) {
	if isLockIdle == nil || resume == nil {
		return
	}
	for _, repo := range repos {
		worktree := repo.WorktreePath
		if worktree == "" {
			continue
		}
		if repo.DiscardPending {
			if isLockIdle(worktree) {
				if stdout != nil {
					fmt.Fprintf(stdout, "discarding checkout for %s as requested by control plane\n", worktree)
				}
				if discard != nil {
					if err := discard(worktree); err != nil {
						if stderr != nil {
							fmt.Fprintf(stderr, "discard checkout error for %s: %v\n", worktree, err)
						}
						continue
					}
				}
				if err := resume(worktree); err != nil {
					if stderr != nil {
						fmt.Fprintf(stderr, "resume repo after discard error for %s: %v\n", worktree, err)
					}
				}
			}
			continue
		}
		if repo.Queue == "PAUSED" && repo.Reason == "HALTED_DIRTY" {
			if isLockIdle(worktree) {
				if isClean != nil {
					clean, err := isClean(worktree)
					if err != nil {
						if stderr != nil {
							fmt.Fprintf(stderr, "check clean status error for %s: %v\n", worktree, err)
						}
						continue
					}
					if clean {
						if stdout != nil {
							fmt.Fprintf(stdout, "worktree %s is clean; resuming paused queue\n", worktree)
						}
						if err := resume(worktree); err != nil {
							if stderr != nil {
								fmt.Fprintf(stderr, "resume repo error for %s: %v\n", worktree, err)
							}
						}
					}
				}
			}
		}
	}
}
