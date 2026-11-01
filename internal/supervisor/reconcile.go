package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
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

func cleanPath(p string) string {
	if p == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Clean(p))
}

// Reconcile terminates orphan harnesses on registered checkouts, prunes only idle checkouts,
// marks stranded leases failed with host_restart, and pauses when porcelain is dirty.
// Healing is not incremented.
func Reconcile(procs []Proc, leases []Lease, porcelain map[string]string, signal func(pid int, sig string) error, prune func(string) error, unlock func(string) error) ReconcileResult {
	order := make([]string, 0, len(leases))
	registered := map[string]struct{}{}
	for _, lease := range leases {
		path := cleanPath(lease.Worktree)
		if _, ok := registered[path]; ok {
			continue
		}
		registered[path] = struct{}{}
		order = append(order, path)
	}
	still := make([]Proc, 0, len(procs))
	terminated := make([]int, 0)
	for _, proc := range procs {
		cwd := cleanPath(proc.Cwd)
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
		held[cleanPath(proc.Cwd)] = true
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
	cleanPorcelain := make(map[string]string, len(porcelain))
	for k, v := range porcelain {
		cleanPorcelain[cleanPath(k)] = v
	}
	locks := make(map[string]string, len(order))
	dirty := false
	for _, path := range order {
		locks[path] = LockIdle
		if strings.TrimSpace(cleanPorcelain[path]) != "" {
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
	want := cleanPath(worktree)
	for _, proc := range procs {
		if cleanPath(proc.Cwd) == want && baseName(proc.Name) == "iazio-harness" {
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
	Lock           string
	RunningJobID   string
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
	diskChecker ...func(worktree string) (bool, error),
) {
	if isLockIdle == nil || resume == nil {
		return
	}
	var checkDisk func(string) (bool, error)
	if len(diskChecker) > 0 {
		checkDisk = diskChecker[0]
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
						continue
					}

					// Worktree is dirty: check if this is an auto-healable superseded branch
					healable, sirID, sirBranch, _ := DetectAutoHealableSIR(worktree)
					if healable {
						gitCmd := func(args ...string) error {
							cmd := procutil.Command("git", append([]string{"-C", worktree}, args...)...)
							return cmd.Run()
						}
						if err := AutoHealSupersededWorktree(true, sirBranch, gitCmd); err == nil {
							_ = ArchiveSIR(worktree, sirID)
							if stdout != nil {
								fmt.Fprintf(stdout, "[reconcile] auto-healed superseded worktree %s (SIR %s, branch %s); resuming paused queue\n", worktree, sirID, sirBranch)
							}
							if err := resume(worktree); err != nil && stderr != nil {
								fmt.Fprintf(stderr, "resume repo after auto-heal error for %s: %v\n", worktree, err)
							}
						} else if stderr != nil {
							fmt.Fprintf(stderr, "auto-heal error for %s: %v\n", worktree, err)
						}
					}
				}
			}
			continue
		}
		if repo.Reason == "HALTED_DISK" {
			if isLockIdle(worktree) && checkDisk != nil {
				sufficient, err := checkDisk(worktree)
				if err != nil {
					if stderr != nil {
						fmt.Fprintf(stderr, "check disk status error for %s: %v\n", worktree, err)
					}
					continue
				}
				if sufficient {
					if stdout != nil {
						fmt.Fprintf(stdout, "worktree %s has sufficient disk space; resuming halted queue\n", worktree)
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

// ReconcileStrandedCheckouts checks whether the control plane believes a job or lock is active on this host,
// but the local supervisor has no active process running for that checkout.
// If stranded, it triggers automatic self-remediation (abandoning the stranded job and clearing the repo lock).
func ReconcileStrandedCheckouts(
	ctx context.Context,
	hostID string,
	repos []RepoState,
	isLockActive func(worktree string) bool,
	abandonJob func(ctx context.Context, jobID, reason string) error,
	remediateRepo func(ctx context.Context, hostID, worktree string) error,
	stdout io.Writer,
	stderr io.Writer,
) {
	for _, repo := range repos {
		worktree := repo.WorktreePath
		if worktree == "" {
			continue
		}
		isStranded := repo.Lock == "RUNNING_HARNESS" &&
			(isLockActive != nil && !isLockActive(worktree))
		if !isStranded {
			continue
		}
		if stderr != nil {
			fmt.Fprintf(stderr, "[reconcile] worktree %s has stranded job %q (lock: %s) on host %s; initiating self-remediation\n",
				worktree, repo.RunningJobID, repo.Lock, hostID)
		}
		if repo.RunningJobID != "" && abandonJob != nil {
			if err := abandonJob(ctx, repo.RunningJobID, "process_disappeared_on_host"); err != nil && stderr != nil {
				fmt.Fprintf(stderr, "[reconcile] failed to abandon stranded job %s: %v\n", repo.RunningJobID, err)
			}
		}
		if remediateRepo != nil {
			if err := remediateRepo(ctx, hostID, worktree); err != nil && stderr != nil {
				fmt.Fprintf(stderr, "[reconcile] failed to remediate repo %s on host %s: %v\n", worktree, hostID, err)
			}
		}
	}
}

// SIRPayload is the minimal representation of an intervention report in tmp/reports/.
type SIRPayload struct {
	ID           string `json:"id"`
	StoryID      string `json:"story_id"`
	Category     string `json:"category"`
	AutoHealable bool   `json:"auto_healable"`
	WorktreePath string `json:"worktree_path"`
}

// DetectAutoHealableSIR checks tmp/reports for any unhandled auto-healable intervention request.
// Returns healable, sirID, currentBranch, and any error.
func DetectAutoHealableSIR(worktree string, gitBranchReader ...func(string) (string, error)) (bool, string, string, error) {
	reportsDir := filepath.Join(worktree, "tmp", "reports")
	entries, err := os.ReadDir(reportsDir)
	if err != nil {
		return false, "", "", nil
	}
	var targetSIR *SIRPayload
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "intervention-") && strings.HasSuffix(name, ".json") {
			data, err := os.ReadFile(filepath.Join(reportsDir, name))
			if err != nil {
				continue
			}
			var sir SIRPayload
			if err := json.Unmarshal(data, &sir); err == nil {
				if sir.AutoHealable && sir.Category == "SUPERSEDED_BRANCH" {
					targetSIR = &sir
					break
				}
			}
		}
	}
	if targetSIR == nil {
		return false, "", "", nil
	}

	branch := ""
	if len(gitBranchReader) > 0 && gitBranchReader[0] != nil {
		branch, _ = gitBranchReader[0](worktree)
	} else {
		cmd := procutil.Command("git", "-C", worktree, "rev-parse", "--abbrev-ref", "HEAD")
		if out, err := cmd.CombinedOutput(); err == nil {
			branch = strings.TrimSpace(string(out))
		}
	}
	if branch == "" || branch == "main" || branch == "master" {
		return false, targetSIR.ID, branch, nil
	}
	return true, targetSIR.ID, branch, nil
}

// ArchiveSIR marks an intervention report as healed by renaming it.
func ArchiveSIR(worktree, sirID string) error {
	reportsDir := filepath.Join(worktree, "tmp", "reports")
	entries, err := os.ReadDir(reportsDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "intervention-") && strings.HasSuffix(name, ".json") {
			fullPath := filepath.Join(reportsDir, name)
			data, err := os.ReadFile(fullPath)
			if err != nil {
				continue
			}
			var sir SIRPayload
			if err := json.Unmarshal(data, &sir); err == nil && sir.ID == sirID {
				return os.Rename(fullPath, fullPath+".healed")
			}
		}
	}
	return nil
}

// AutoHealSupersededWorktree returns a stranded duplicate branch worktree back to clean origin/main.
func AutoHealSupersededWorktree(idle bool, branch string, git func(args ...string) error) error {
	if !idle {
		return errors.New("auto-heal requires IDLE")
	}
	if git == nil {
		return errors.New("git runner is not configured")
	}
	if branch == "" || branch == "main" || branch == "master" {
		return nil
	}
	if err := git("reset", "--hard"); err != nil {
		return fmt.Errorf("reset hard: %w", err)
	}
	if err := git("clean", "-fd"); err != nil {
		return fmt.Errorf("clean fd: %w", err)
	}
	if err := git("checkout", "main"); err != nil {
		if errMaster := git("checkout", "master"); errMaster != nil {
			return fmt.Errorf("checkout main/master: %w", err)
		}
	}
	_ = git("pull", "--ff-only")
	_ = git("branch", "-D", branch)
	return nil
}
