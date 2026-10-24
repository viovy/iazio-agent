package supervisor

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
)

// RepoGitState details the parsed git posture of a repository on the host.
type RepoGitState struct {
	RepoPath            string    `json:"repo_path"`
	Alias               string    `json:"alias,omitempty"`
	Branch              string    `json:"branch"`
	HeadSHA             string    `json:"head_sha"`
	IsClean             bool      `json:"is_clean"`
	DirtyFilesCount     int       `json:"dirty_files_count"`
	UntrackedFilesCount int       `json:"untracked_files_count"`
	Ahead               int       `json:"ahead"`
	Behind              int       `json:"behind"`
	HasActiveGitLock    bool      `json:"has_active_git_lock"`
	LastCheckedAt       time.Time `json:"last_checked_at"`
	Error               string    `json:"error,omitempty"`
}

// FleetStateSnapshot represents the aggregated git state of all host repos.
type FleetStateSnapshot struct {
	HostID         string                  `json:"host_id"`
	HarnessActive  bool                    `json:"harness_active"`
	FleetGitClean  bool                    `json:"fleet_git_clean"`
	DirtyRepoPaths []string                `json:"dirty_repo_paths,omitempty"`
	Repos          map[string]RepoGitState `json:"repos"`
	CheckedAt      time.Time               `json:"checked_at"`
}

// InspectRepoGitState inspects a single repository via non-locking read-only operations.
func InspectRepoGitState(ctx context.Context, repoPath, alias string) RepoGitState {
	st := RepoGitState{
		RepoPath:      repoPath,
		Alias:         alias,
		LastCheckedAt: time.Now().UTC(),
		IsClean:       true,
	}

	// 1. Sentinel checks for active git operations or locks
	gitDir := filepath.Join(repoPath, ".git")
	if fi, err := os.Stat(gitDir); err == nil && !fi.IsDir() {
		// Submodule gitfile pointing to actual gitdir
		if b, rErr := os.ReadFile(gitDir); rErr == nil {
			content := strings.TrimSpace(string(b))
			if strings.HasPrefix(content, "gitdir:") {
				target := strings.TrimSpace(strings.TrimPrefix(content, "gitdir:"))
				if !filepath.IsAbs(target) {
					target = filepath.Join(repoPath, target)
				}
				gitDir = target
			}
		}
	}

	lockSentinels := []string{
		filepath.Join(gitDir, "index.lock"),
		filepath.Join(gitDir, "HEAD.lock"),
		filepath.Join(gitDir, "shallow.lock"),
		filepath.Join(gitDir, "MERGE_HEAD"),
		filepath.Join(gitDir, "rebase-merge"),
		filepath.Join(gitDir, "rebase-apply"),
		filepath.Join(gitDir, "CHERRY_PICK_HEAD"),
		filepath.Join(gitDir, "REVERT_HEAD"),
		filepath.Join(gitDir, "BISECT_LOG"),
	}
	for _, sentinel := range lockSentinels {
		if _, err := os.Stat(sentinel); err == nil {
			st.HasActiveGitLock = true
			st.IsClean = false
			st.Error = fmt.Sprintf("active git operation or lockfile present: %s", filepath.Base(sentinel))
			return st
		}
	}

	// 2. Non-locking git status --porcelain=v2 --branch with per-repo 15s timeout
	repoCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(repoCtx, "git", "-C", repoPath, "status", "--porcelain=v2", "--branch")
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		st.IsClean = false
		st.Error = fmt.Sprintf("git status failed: %v (%s)", err, strings.TrimSpace(string(out)))
		return st
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		if l == "" {
			continue
		}
		if strings.HasPrefix(l, "# branch.oid ") {
			st.HeadSHA = strings.TrimPrefix(l, "# branch.oid ")
		} else if strings.HasPrefix(l, "# branch.head ") {
			st.Branch = strings.TrimPrefix(l, "# branch.head ")
		} else if strings.HasPrefix(l, "# branch.ab ") {
			parts := strings.Fields(strings.TrimPrefix(l, "# branch.ab "))
			if len(parts) >= 2 {
				if strings.HasPrefix(parts[0], "+") {
					st.Ahead, _ = strconv.Atoi(parts[0][1:])
				}
				if strings.HasPrefix(parts[1], "-") {
					st.Behind, _ = strconv.Atoi(parts[1][1:])
				}
			}
		} else if strings.HasPrefix(l, "? ") {
			st.UntrackedFilesCount++
		} else if !strings.HasPrefix(l, "# ") {
			st.DirtyFilesCount++
		}
	}

	st.IsClean = (st.DirtyFilesCount == 0 && st.UntrackedFilesCount == 0 && !st.HasActiveGitLock)
	return st
}

// RepoConfigItem identifies a monitored repo path and optional alias.
type RepoConfigItem struct {
	Path  string
	Alias string
}

// RepoStateMonitor periodically inspects host repositories when no harness is executing.
type RepoStateMonitor struct {
	mu             sync.RWMutex
	sup            *Sup
	hostID         string
	getRepos       func() ([]RepoConfigItem, error)
	reportStatuses func(ctx context.Context, hostID string, statuses []controlplane.RepoRuntimeStatus) error
	postNotif      func(ctx context.Context, rec controlplane.NotificationRecord) error
	onFleetClean   func(snap FleetStateSnapshot)
	lastSnapshot   FleetStateSnapshot
	interval       time.Duration
}

// NewRepoStateMonitor creates a monitor.
func NewRepoStateMonitor(
	sup *Sup,
	hostID string,
	interval time.Duration,
	getRepos func() ([]RepoConfigItem, error),
	reportStatuses func(ctx context.Context, hostID string, statuses []controlplane.RepoRuntimeStatus) error,
	postNotif func(ctx context.Context, rec controlplane.NotificationRecord) error,
	onFleetClean func(snap FleetStateSnapshot),
) *RepoStateMonitor {
	if interval < 1*time.Minute || interval > 5*time.Minute {
		fmt.Fprintf(os.Stderr, "[repomonitor] configured interval %v outside permitted range [1m..5m]; defaulting to 2m\n", interval)
		interval = 2 * time.Minute
	}
	return &RepoStateMonitor{
		sup:            sup,
		hostID:         hostID,
		interval:       interval,
		getRepos:       getRepos,
		reportStatuses: reportStatuses,
		postNotif:      postNotif,
		onFleetClean:   onFleetClean,
		lastSnapshot: FleetStateSnapshot{
			HostID: hostID,
			Repos:  make(map[string]RepoGitState),
		},
	}
}

// LastSnapshot returns the most recently captured fleet state snapshot.
func (m *RepoStateMonitor) LastSnapshot() FleetStateSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSnapshot
}

// InspectFleet performs one full inspection pass across registered repos.
func (m *RepoStateMonitor) InspectFleet(ctx context.Context) FleetStateSnapshot {
	m.mu.RLock()
	lastSnap := m.lastSnapshot
	hostID := m.hostID
	m.mu.RUnlock()

	now := time.Now().UTC()
	snap := FleetStateSnapshot{
		HostID:    hostID,
		CheckedAt: now,
		Repos:     make(map[string]RepoGitState),
	}

	// Execution Guard: if harness is active, skip inspection
	if m.sup != nil && m.sup.HasRunningHarness() {
		snap.HarnessActive = true
		snap.FleetGitClean = false
		if len(lastSnap.Repos) > 0 {
			snap.Repos = lastSnap.Repos
		}
		m.mu.Lock()
		m.lastSnapshot = snap
		m.mu.Unlock()
		return snap
	}

	var repos []RepoConfigItem
	if m.getRepos != nil {
		var err error
		repos, err = m.getRepos()
		if err != nil {
			// Fail-closed when repo inventory is unavailable
			snap.FleetGitClean = false
			snap.DirtyRepoPaths = []string{"repo_inventory_unavailable"}
			m.mu.Lock()
			m.lastSnapshot = snap
			m.mu.Unlock()
			return snap
		}
	}

	if len(repos) == 0 {
		snap.FleetGitClean = true
		m.mu.Lock()
		m.lastSnapshot = snap
		m.mu.Unlock()
		return snap
	}

	// Concurrent bounded inspection (up to 4 workers)
	type result struct {
		path  string
		state RepoGitState
	}
	resCh := make(chan result, len(repos))
	sem := make(chan struct{}, 4)

	var wg sync.WaitGroup
	for _, item := range repos {
		wg.Add(1)
		go func(it RepoConfigItem) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			st := InspectRepoGitState(ctx, it.Path, it.Alias)
			resCh <- result{path: it.Path, state: st}
		}(item)
	}

	wg.Wait()
	close(resCh)

	allClean := true
	var dirtyList []string
	var statuses []controlplane.RepoRuntimeStatus

	for r := range resCh {
		snap.Repos[r.path] = r.state
		if !r.state.IsClean {
			allClean = false
			dirtyList = append(dirtyList, r.path)
		}
		// Transition check
		if prev, ok := lastSnap.Repos[r.path]; ok {
			if prev.IsClean != r.state.IsClean && m.postNotif != nil {
				category := "warning"
				statusStr := "Dirty"
				if r.state.IsClean {
					category = "info"
					statusStr = "Clean"
				}
				if err := m.postNotif(ctx, controlplane.NotificationRecord{
					Title:    fmt.Sprintf("Repo Transitioned to %s", statusStr),
					Message:  fmt.Sprintf("Repository %s (%s) is now %s.", r.path, r.state.Branch, statusStr),
					Category: category,
					Source:   "repo-monitor",
					HostID:   m.hostID,
					Metadata: map[string]any{
						"event":       "repo.state_transition",
						"repo":        r.path,
						"branch":      r.state.Branch,
						"is_clean":    r.state.IsClean,
						"dirty_files": r.state.DirtyFilesCount,
					},
				}); err != nil {
					fmt.Fprintf(os.Stderr, "[repomonitor] postNotif error: %v\n", err)
				}
			}
		}

		statuses = append(statuses, controlplane.RepoRuntimeStatus{
			RepoPath:            r.state.RepoPath,
			Alias:               r.state.Alias,
			IsClean:             r.state.IsClean,
			Branch:              r.state.Branch,
			HeadSHA:             r.state.HeadSHA,
			DirtyFilesCount:     r.state.DirtyFilesCount,
			UntrackedFilesCount: r.state.UntrackedFilesCount,
			Ahead:               r.state.Ahead,
			Behind:              r.state.Behind,
			HasActiveGitLock:    r.state.HasActiveGitLock,
			LastCheckedAt:       r.state.LastCheckedAt,
		})
	}

	snap.FleetGitClean = allClean
	snap.DirtyRepoPaths = dirtyList

	m.mu.Lock()
	wasClean := m.lastSnapshot.FleetGitClean
	m.lastSnapshot = snap
	m.mu.Unlock()

	// Report to backend
	if m.reportStatuses != nil && len(statuses) > 0 {
		if rErr := m.reportStatuses(ctx, m.hostID, statuses); rErr != nil {
			fmt.Fprintf(os.Stderr, "[repomonitor] reportStatuses error: %v\n", rErr)
		}
	}

	// Trigger onFleetClean only on transition to clean
	if allClean && !wasClean && m.onFleetClean != nil {
		m.onFleetClean(snap)
	}

	return snap
}

// Start begins periodic background inspection with jitter.
func (m *RepoStateMonitor) Start(ctx context.Context) {
	go func() {
		// Run initial check after 3 seconds
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			m.InspectFleet(ctx)
		}

		for {
			// Apply ±15s jitter to avoid synchronized fleet requests
			jitter := time.Duration(rand.Intn(30)-15) * time.Second
			waitDur := m.interval + jitter
			if waitDur < 45*time.Second {
				waitDur = 45 * time.Second
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(waitDur):
				m.InspectFleet(ctx)
			}
		}
	}()
}
