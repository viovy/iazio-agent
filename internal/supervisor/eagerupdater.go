package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
)

// UpdateResult reflects the outcome of an updater execution.
type UpdateResult struct {
	UpdatedTools []string `json:"updated_tools"`
	AgentUpdated bool     `json:"agent_updated"`
}

var (
	// ErrUpdateInProgress is returned when an update is already executing and a forced or targeted request is queued (G4).
	ErrUpdateInProgress = errors.New("tool update already in progress")
)

// EagerToolUpdater handles safe atomic binary updates guarded by harness idle and fleet git clean states.
type EagerToolUpdater struct {
	mu              sync.Mutex
	updateMutex     sync.Mutex // host-level execution and update mutex
	sup             *Sup
	monitor         *RepoStateMonitor
	hostID          string
	doUpdate        func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error)
	postNotif       func(ctx context.Context, rec controlplane.NotificationRecord) error
	CheckDrift      func(ctx context.Context, targetTools []string) (bool, error)
	lastDeferredKey string
	updating        bool
	pendingTools    []string
	pendingForce    bool
	hasPending      bool
}

// NewEagerToolUpdater constructs an EagerToolUpdater.
func NewEagerToolUpdater(
	sup *Sup,
	monitor *RepoStateMonitor,
	hostID string,
	doUpdate func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error),
	postNotif func(ctx context.Context, rec controlplane.NotificationRecord) error,
) *EagerToolUpdater {
	return &EagerToolUpdater{
		sup:       sup,
		monitor:   monitor,
		hostID:    hostID,
		doUpdate:  doUpdate,
		postNotif: postNotif,
	}
}

// IsUpdating reports whether a tool update is currently in-flight.
func (u *EagerToolUpdater) IsUpdating() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.updating
}

// EvaluateAndTrigger checks preconditions and performs an eager update if eligible.
// If a concurrent update is running, forced or targeted requests are queued and executed
// sequentially inside the update lock, and any agent binary upgrade triggers a restart (E3).
func (u *EagerToolUpdater) EvaluateAndTrigger(ctx context.Context, targetTools []string, force bool) (agentUpdated bool, deferred bool, err error) {
	res, def, err := u.EvaluateAndTriggerDetailed(ctx, targetTools, force)
	return res.AgentUpdated, def, err
}

// EvaluateAndTriggerDetailed performs the eager update returning the full UpdateResult (F-3).
func (u *EagerToolUpdater) EvaluateAndTriggerDetailed(ctx context.Context, targetTools []string, force bool) (res UpdateResult, deferred bool, err error) {
	if !u.updateMutex.TryLock() {
		// Coalesce or queue: if caller requested forced or targeted tools, record pending request (G4, E3)
		if force || len(targetTools) > 0 {
			u.mu.Lock()
			u.pendingForce = u.pendingForce || force
			for _, t := range targetTools {
				found := false
				for _, pt := range u.pendingTools {
					if pt == t {
						found = true
						break
					}
				}
				if !found {
					u.pendingTools = append(u.pendingTools, t)
				}
			}
			u.hasPending = true
			u.mu.Unlock()
			return UpdateResult{}, true, ErrUpdateInProgress
		}
		return UpdateResult{}, true, nil
	}
	defer u.updateMutex.Unlock()

	var aggregateRes UpdateResult
	var lastErr error

	curTools := targetTools
	curForce := force

	for {
		passRes, passDeferred, passErr := u.runSinglePass(ctx, curTools, curForce)
		if passRes.AgentUpdated {
			aggregateRes.AgentUpdated = true
		}
		if len(passRes.UpdatedTools) > 0 {
			for _, ut := range passRes.UpdatedTools {
				found := false
				for _, existing := range aggregateRes.UpdatedTools {
					if existing == ut {
						found = true
						break
					}
				}
				if !found {
					aggregateRes.UpdatedTools = append(aggregateRes.UpdatedTools, ut)
				}
			}
		}
		if passErr != nil {
			lastErr = passErr
		}

		u.mu.Lock()
		if !u.hasPending {
			u.mu.Unlock()
			if passDeferred && !curForce {
				return aggregateRes, true, lastErr
			}
			break
		}
		curTools = u.pendingTools
		curForce = u.pendingForce
		u.hasPending = false
		u.pendingForce = false
		u.pendingTools = nil
		u.mu.Unlock()
	}

	return aggregateRes, false, lastErr
}

func (u *EagerToolUpdater) runSinglePass(ctx context.Context, targetTools []string, force bool) (res UpdateResult, deferred bool, err error) {
	u.mu.Lock()
	if u.updating {
		u.mu.Unlock()
		return UpdateResult{}, true, nil
	}
	u.mu.Unlock()

	// 1. Guard check: Is harness executing? Protects all updates including forced (F-3)
	if u.sup != nil && u.sup.HasRunningHarness() {
		reason := "BlockedByActiveHarness"
		u.emitDeferredOnce(ctx, targetTools, reason, "Tool update deferred: harness execution active on host")
		return UpdateResult{}, true, nil
	}

	// 2. Fleet Git Clean check
	snap := u.monitor.LastSnapshot()
	if snap.HarnessActive {
		// If supervisor reports no active harness, snapshot is stale; refresh inspection (M1)
		if u.sup != nil && !u.sup.HasRunningHarness() {
			snap = u.monitor.InspectFleet(ctx)
		}
		if snap.HarnessActive {
			reason := "BlockedByActiveHarness"
			u.emitDeferredOnce(ctx, targetTools, reason, "Tool update deferred: harness execution active on host")
			return UpdateResult{}, true, nil
		}
	}
	if !snap.FleetGitClean && !force {
		// If dirty repo paths is empty, run a fresh inspection instead of deferring on stale guard (R3)
		if len(snap.DirtyRepoPaths) == 0 {
			snap = u.monitor.InspectFleet(ctx)
		}
		if !snap.FleetGitClean {
			pathsCopy := append([]string(nil), snap.DirtyRepoPaths...)
			sort.Strings(pathsCopy)
			reason := fmt.Sprintf("BlockedByDirtyRepos: %v", pathsCopy)
			u.emitDeferredOnce(ctx, targetTools, reason, fmt.Sprintf("Tool update deferred: registered repos are dirty (%s)", strings.Join(pathsCopy, ", ")))
			return UpdateResult{}, true, nil
		}
	}

	// 3. Acquire exclusive host execution lock from supervisor (always, even on force)
	var releaseHostLock func()
	if u.sup != nil {
		var ok bool
		releaseHostLock, ok = u.sup.TryAcquireToolUpdate()
		if !ok {
			reason := "BlockedByActiveHarness"
			u.emitDeferredOnce(ctx, targetTools, reason, "Tool update deferred: harness execution active on host")
			return UpdateResult{}, true, nil
		}
		if releaseHostLock != nil {
			defer releaseHostLock()
		}
	}

	u.mu.Lock()
	u.updating = true
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		u.updating = false
		u.mu.Unlock()
	}()

	// Re-verify inside lock if not forced
	if !force {
		snap2 := u.monitor.InspectFleet(ctx)
		if !snap2.FleetGitClean {
			pathsCopy := append([]string(nil), snap2.DirtyRepoPaths...)
			sort.Strings(pathsCopy)
			reason := fmt.Sprintf("BlockedByDirtyRepos: %v", pathsCopy)
			u.emitDeferredOnce(ctx, targetTools, reason, fmt.Sprintf("Tool update deferred: registered repos are dirty (%s)", strings.Join(pathsCopy, ", ")))
			return UpdateResult{}, true, nil
		}
	}

	// Reset deferred key on any pass that passed the guards (L6)
	u.mu.Lock()
	u.lastDeferredKey = ""
	u.mu.Unlock()

	// Check drift before emitting update_started to eliminate noise on unforced no-op ticks (E4)
	if !force && u.CheckDrift != nil {
		hasDrift, driftErr := u.CheckDrift(ctx, targetTools)
		if driftErr == nil && !hasDrift {
			return UpdateResult{}, false, nil
		}
	}

	// Emit update_started prior to executing update operations (G5)
	if u.postNotif != nil {
		targetsDesc := "all outdated tools"
		if len(targetTools) > 0 {
			targetsDesc = strings.Join(targetTools, ", ")
		}
		if err := u.postNotif(ctx, controlplane.NotificationRecord{
			Title:    "Tool Update Started",
			Message:  fmt.Sprintf("Eager tool update started on host %s (target: %s)", u.hostID, targetsDesc),
			Category: "info",
			Source:   "tool-updater",
			HostID:   u.hostID,
			Metadata: map[string]any{
				"event":        "agent.tool.update_started",
				"target_tools": targetTools,
				"force":        force,
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "[eagerupdater] postNotif error (update_started): %v\n", err)
		}
	}

	start := time.Now()
	res, updateErr := u.doUpdate(ctx, targetTools, force)
	durationMs := time.Since(start).Milliseconds()

	if updateErr != nil {
		if u.postNotif != nil {
			if err := u.postNotif(ctx, controlplane.NotificationRecord{
				Title:    "Tool Update Failed",
				Message:  fmt.Sprintf("Tool update failed on %s: %v", u.hostID, updateErr),
				Category: "error",
				Source:   "tool-updater",
				HostID:   u.hostID,
				Metadata: map[string]any{
					"event":       "agent.tool.update_failed",
					"error":       updateErr.Error(),
					"duration_ms": durationMs,
				},
			}); err != nil {
				fmt.Fprintf(os.Stderr, "[eagerupdater] postNotif error (update_failed): %v\n", err)
			}
		}
		return UpdateResult{}, false, updateErr
	}

	if u.postNotif != nil {
		if len(res.UpdatedTools) > 0 {
			if err := u.postNotif(ctx, controlplane.NotificationRecord{
				Title:    "Tool Update Succeeded",
				Message:  fmt.Sprintf("Successfully updated tools on %s: %s (took %dms)", u.hostID, strings.Join(res.UpdatedTools, ", "), durationMs),
				Category: "success",
				Source:   "tool-updater",
				HostID:   u.hostID,
				Metadata: map[string]any{
					"event":         "agent.tool.update_succeeded",
					"updated_tools": res.UpdatedTools,
					"duration_ms":   durationMs,
					"agent_updated": res.AgentUpdated,
				},
			}); err != nil {
				fmt.Fprintf(os.Stderr, "[eagerupdater] postNotif error (update_succeeded): %v\n", err)
			}
		} else if !force {
			// Terminal noop notification to pair with started (E4)
			_ = u.postNotif(ctx, controlplane.NotificationRecord{
				Title:    "Tools Up To Date",
				Message:  fmt.Sprintf("Tool update check completed on %s: all tools already up to date", u.hostID),
				Category: "info",
				Source:   "tool-updater",
				HostID:   u.hostID,
				Metadata: map[string]any{
					"event":       "agent.tool.update_noop",
					"duration_ms": durationMs,
				},
			})
		}
	}

	return res, false, nil
}

func (u *EagerToolUpdater) emitDeferredOnce(ctx context.Context, targetTools []string, reason, msg string) {
	u.mu.Lock()
	key := fmt.Sprintf("%s:%s", reason, strings.Join(targetTools, ","))
	if u.lastDeferredKey == key {
		u.mu.Unlock()
		return
	}
	u.lastDeferredKey = key
	u.mu.Unlock()

	if u.postNotif != nil {
		if err := u.postNotif(ctx, controlplane.NotificationRecord{
			Title:    "Tool Update Deferred",
			Message:  msg,
			Category: "info",
			Source:   "tool-updater",
			HostID:   u.hostID,
			Metadata: map[string]any{
				"event":        "agent.tool.update_deferred",
				"reason":       reason,
				"target_tools": targetTools,
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "[eagerupdater] postNotif error (update_deferred): %v\n", err)
		}
	}
}
