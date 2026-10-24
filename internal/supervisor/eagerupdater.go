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
	FailedTools  []string `json:"failed_tools,omitempty"`
	Errors       []error  `json:"errors,omitempty"`
	AgentUpdated bool     `json:"agent_updated"`
}

var (
	// ErrUpdateInProgress is returned when an update is already executing and a forced or targeted request is queued (G4).
	ErrUpdateInProgress = errors.New("tool update already in progress")
	// ErrQueueFull is returned when pending update requests exceed maximum queue capacity (N5-R).
	ErrQueueFull = errors.New("tool update pending queue full")
)

// PendingToolRequest models a discrete queued tool update request (R4, R5, N2, N5).
type PendingToolRequest struct {
	Tool          string
	Force         bool
	TargetVersion string
	BypassDirty   bool
}

// TriggerOptions configures an eager update pass (F-3, F1, R4, R5, N2, N3).
type TriggerOptions struct {
	TargetTools   []string
	Force         bool
	TargetVersion string
	BypassDirty   bool
}

const maxPendingRequests = 32

// EagerToolUpdater handles safe atomic binary updates guarded by harness idle and fleet git clean states.
type EagerToolUpdater struct {
	mu                    sync.Mutex
	updateMutex           sync.Mutex // host-level execution and update mutex
	sup                   *Sup
	monitor               *RepoStateMonitor
	hostID                string
	doUpdate              func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error)
	doUpdateDetailed      func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error)
	postNotif             func(ctx context.Context, rec controlplane.NotificationRecord) error
	CheckDrift            func(ctx context.Context, targetTools []string) (bool, error)
	ListSuiteTools        func(ctx context.Context) ([]string, error)
	lastDeferredKey       string
	lastPartialFailureKey string
	updating              bool
	pendingRequests       []PendingToolRequest // discrete requests preserved per tool (R4, R5, N5)
	hasPending            bool

	// Per-tool failure backoff tracking to prevent retry storms on un-installable tools (F5, R1, R7, N1, N1-R)
	failedAttempts   map[string]int
	failedLastTime   map[string]time.Time
	suiteFailCount   int
	suiteLastFailure time.Time
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
		sup:             sup,
		monitor:         monitor,
		hostID:          hostID,
		doUpdate:        doUpdate,
		postNotif:       postNotif,
		pendingRequests: make([]PendingToolRequest, 0),
		failedAttempts:  make(map[string]int),
		failedLastTime:  make(map[string]time.Time),
	}
}

// SetDetailedUpdateFunc configures an update callback that supports explicit TargetVersion (F1).
func (u *EagerToolUpdater) SetDetailedUpdateFunc(fn func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.doUpdateDetailed = fn
}

// IsToolInBackoff reports whether the specified tool is currently within its backoff window (N1).
func (u *EagerToolUpdater) IsToolInBackoff(tool string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	toolKey := strings.ToLower(strings.TrimSpace(tool))
	if toolKey == "" {
		return false
	}
	count := u.failedAttempts[toolKey]
	if count <= 0 {
		return false
	}
	backoff := calculateBackoff(count)
	return time.Since(u.failedLastTime[toolKey]) < backoff
}

// FilterActiveTools returns only those tools from the input list that are not currently backed off (N1-R).
func (u *EagerToolUpdater) FilterActiveTools(tools []string) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	now := time.Now()
	var active []string
	for _, t := range tools {
		toolKey := strings.ToLower(strings.TrimSpace(t))
		if toolKey == "" {
			continue
		}
		count := u.failedAttempts[toolKey]
		if count > 0 {
			backoff := calculateBackoff(count)
			if now.Sub(u.failedLastTime[toolKey]) < backoff {
				continue // tool is currently backing off
			}
		}
		active = append(active, t)
	}
	return active
}

func (u *EagerToolUpdater) shouldBackoff(targetTools []string, force bool) bool {
	if force {
		return false
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	now := time.Now()
	// Single-tool targeted check
	if len(targetTools) == 1 {
		tool := strings.ToLower(strings.TrimSpace(targetTools[0]))
		count := u.failedAttempts[tool]
		if count > 0 {
			backoff := calculateBackoff(count)
			if now.Sub(u.failedLastTime[tool]) < backoff {
				return true
			}
		}
		return false
	}

	// Targeted multi-tool check: if all requested tools are currently backing off, back off the whole pass (N1)
	if len(targetTools) > 1 {
		allBackedOff := true
		for _, t := range targetTools {
			tool := strings.ToLower(strings.TrimSpace(t))
			count := u.failedAttempts[tool]
			if count == 0 || now.Sub(u.failedLastTime[tool]) >= calculateBackoff(count) {
				allBackedOff = false
				break
			}
		}
		if allBackedOff {
			return true
		}
		return false
	}

	// Full suite / unconstrained check: only backs off if a suite-wide fatal error occurred (N1-R)
	if len(targetTools) == 0 {
		if u.suiteFailCount > 0 {
			backoff := calculateBackoff(u.suiteFailCount)
			if now.Sub(u.suiteLastFailure) < backoff {
				return true
			}
		}
	}
	return false
}

func (u *EagerToolUpdater) appendPendingRequestLocked(req PendingToolRequest) (added bool, full bool) {
	normTool := strings.ToLower(strings.TrimSpace(req.Tool))
	req.Tool = normTool
	for _, p := range u.pendingRequests {
		if strings.ToLower(strings.TrimSpace(p.Tool)) == normTool &&
			p.Force == req.Force &&
			p.TargetVersion == req.TargetVersion &&
			p.BypassDirty == req.BypassDirty {
			return false, false // deduplicate identical request (N5)
		}
	}
	if len(u.pendingRequests) >= maxPendingRequests {
		return false, true // cap queue limit (N5-R)
	}
	u.pendingRequests = append(u.pendingRequests, req)
	return true, false
}

func (u *EagerToolUpdater) prependPendingRequestsLocked(reqs []PendingToolRequest) {
	var toAdd []PendingToolRequest
	for _, req := range reqs {
		normTool := strings.ToLower(strings.TrimSpace(req.Tool))
		req.Tool = normTool
		dup := false
		for _, p := range u.pendingRequests {
			if strings.ToLower(strings.TrimSpace(p.Tool)) == normTool &&
				p.Force == req.Force &&
				p.TargetVersion == req.TargetVersion &&
				p.BypassDirty == req.BypassDirty {
				dup = true
				break
			}
		}
		if !dup {
			toAdd = append(toAdd, req)
		}
	}
	if len(toAdd) == 0 {
		return
	}
	combined := append(toAdd, u.pendingRequests...)
	if len(combined) > maxPendingRequests {
		fmt.Fprintf(os.Stderr, "[eagerupdater] WARN: pending requests queue truncated from %d to %d (N5-R)\n", len(combined), maxPendingRequests)
		combined = combined[:maxPendingRequests]
	}
	u.pendingRequests = combined
}

func calculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	// 30s * 2^(attempts-1), capped at 1 hour (R7)
	b := 30 * time.Second * (1 << (attempts - 1))
	if b > 1*time.Hour || b <= 0 {
		b = 1 * time.Hour
	}
	return b
}

func (u *EagerToolUpdater) recordToolFailure(tool string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	toolKey := strings.ToLower(strings.TrimSpace(tool))
	if toolKey == "" {
		return
	}
	if u.failedAttempts == nil {
		u.failedAttempts = make(map[string]int)
		u.failedLastTime = make(map[string]time.Time)
	}
	u.failedAttempts[toolKey]++
	u.failedLastTime[toolKey] = time.Now()
}

func (u *EagerToolUpdater) recordToolSuccess(tool string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	toolKey := strings.ToLower(strings.TrimSpace(tool))
	if toolKey == "" {
		return
	}
	delete(u.failedAttempts, toolKey)
	delete(u.failedLastTime, toolKey)
}

func (u *EagerToolUpdater) recordSuiteFailure() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.suiteFailCount++
	u.suiteLastFailure = time.Now()
}

func (u *EagerToolUpdater) recordSuiteSuccess() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.suiteFailCount = 0
	u.suiteLastFailure = time.Time{}
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

// EvaluateAndTriggerDetailed performs the eager update returning the full UpdateResult (F-3, F1, R4, R5).
func (u *EagerToolUpdater) EvaluateAndTriggerDetailed(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (res UpdateResult, deferred bool, err error) {
	var requestedVersion string
	if len(targetVersion) > 0 {
		requestedVersion = targetVersion[0]
	}
	return u.EvaluateAndTriggerWithOpts(ctx, TriggerOptions{
		TargetTools:   targetTools,
		Force:         force,
		TargetVersion: requestedVersion,
		BypassDirty:   false,
	})
}

// EvaluateAndTriggerWithOpts performs the eager update using TriggerOptions (N2, N3, N5).
func (u *EagerToolUpdater) EvaluateAndTriggerWithOpts(ctx context.Context, opts TriggerOptions) (res UpdateResult, deferred bool, err error) {
	if !u.updateMutex.TryLock() {
		// Coalesce or queue: record individual tool requests without merging (R4, R5, N5, R3)
		if opts.Force || len(opts.TargetTools) > 0 || opts.BypassDirty || opts.TargetVersion != "" {
			u.mu.Lock()
			var candidates []PendingToolRequest
			if len(opts.TargetTools) > 0 {
				for _, t := range opts.TargetTools {
					candidates = append(candidates, PendingToolRequest{
						Tool:          strings.ToLower(strings.TrimSpace(t)),
						Force:         opts.Force,
						TargetVersion: opts.TargetVersion,
						BypassDirty:   opts.BypassDirty,
					})
				}
			} else {
				candidates = append(candidates, PendingToolRequest{
					Tool:          "",
					Force:         opts.Force,
					TargetVersion: opts.TargetVersion,
					BypassDirty:   opts.BypassDirty,
				})
			}

			var toAppend []PendingToolRequest
			for _, c := range candidates {
				dup := false
				for _, p := range u.pendingRequests {
					if p.Tool == c.Tool && p.Force == c.Force && p.TargetVersion == c.TargetVersion && p.BypassDirty == c.BypassDirty {
						dup = true
						break
					}
				}
				if !dup {
					toAppend = append(toAppend, c)
				}
			}

			// Atomic all-or-nothing capacity check (R3)
			if len(u.pendingRequests)+len(toAppend) > maxPendingRequests {
				u.mu.Unlock()
				return UpdateResult{}, false, ErrQueueFull
			}

			u.pendingRequests = append(u.pendingRequests, toAppend...)
			u.hasPending = len(u.pendingRequests) > 0
			u.mu.Unlock()
			return UpdateResult{}, true, ErrUpdateInProgress
		}
		return UpdateResult{}, true, nil
	}
	defer u.updateMutex.Unlock()

	var aggregateRes UpdateResult
	var lastErr error

	curTools := opts.TargetTools
	curForce := opts.Force
	curVersion := opts.TargetVersion
	curBypassDirty := opts.BypassDirty

	for {
		passRes, passDeferred, passErr := u.runSinglePass(ctx, curTools, curForce, curBypassDirty, curVersion)
		if passRes.AgentUpdated {
			aggregateRes.AgentUpdated = true
		}
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
		for _, ft := range passRes.FailedTools {
			found := false
			for _, existing := range aggregateRes.FailedTools {
				if existing == ft {
					found = true
					break
				}
			}
			if !found {
				aggregateRes.FailedTools = append(aggregateRes.FailedTools, ft)
			}
		}
		for _, e := range passRes.Errors {
			aggregateRes.Errors = append(aggregateRes.Errors, e)
		}
		if passErr != nil {
			lastErr = passErr
		}

		if passDeferred && (curForce || curVersion != "" || curBypassDirty) {
			// R4, N5: Re-queue deferred forced/targeted/dispatched requests so they are not dropped when harness is active
			u.mu.Lock()
			var reqs []PendingToolRequest
			if len(curTools) > 0 {
				for _, t := range curTools {
					reqs = append(reqs, PendingToolRequest{
						Tool:          t,
						Force:         curForce,
						TargetVersion: curVersion,
						BypassDirty:   curBypassDirty,
					})
				}
			} else {
				reqs = append(reqs, PendingToolRequest{
					Tool:          "",
					Force:         curForce,
					TargetVersion: curVersion,
					BypassDirty:   curBypassDirty,
				})
			}
			u.prependPendingRequestsLocked(reqs)
			u.hasPending = len(u.pendingRequests) > 0
			u.mu.Unlock()
			return aggregateRes, true, lastErr
		}

		u.mu.Lock()
		if len(u.pendingRequests) == 0 {
			u.hasPending = false
			u.mu.Unlock()
			if passDeferred && !curForce && !curBypassDirty {
				return aggregateRes, true, lastErr
			}
			break
		}
		// R5: Drain pending requests one by one without merging
		nextReq := u.pendingRequests[0]
		u.pendingRequests = u.pendingRequests[1:]
		u.hasPending = len(u.pendingRequests) > 0
		u.mu.Unlock()

		if nextReq.Tool != "" {
			curTools = []string{nextReq.Tool}
		} else {
			curTools = nil
		}
		curForce = nextReq.Force
		curVersion = nextReq.TargetVersion
		curBypassDirty = nextReq.BypassDirty
	}

	return aggregateRes, false, lastErr
}

func (u *EagerToolUpdater) runSinglePass(ctx context.Context, targetTools []string, force, bypassDirty bool, targetVersion ...string) (res UpdateResult, deferred bool, err error) {
	u.mu.Lock()
	if u.updating {
		u.mu.Unlock()
		return UpdateResult{}, true, nil
	}
	u.mu.Unlock()

	// Guard against failure retry storm via backoff (F5, N1, B-1).
	// Operator-dispatched updates (bypassDirty) do not skip execution on prior background backoff (B-1).
	if !force && !bypassDirty && u.shouldBackoff(targetTools, false) {
		return UpdateResult{}, false, nil
	}

	// Filter out backed-off tools on unforced background runs so healthy tools can update (N1-R, R2):
	effectiveTargets := targetTools
	if !force && !bypassDirty {
		if len(targetTools) > 0 {
			filtered := u.FilterActiveTools(targetTools)
			if len(filtered) == 0 {
				return UpdateResult{}, false, nil
			}
			effectiveTargets = filtered
		} else if u.ListSuiteTools != nil {
			allSuite, err := u.ListSuiteTools(ctx)
			if err == nil && len(allSuite) > 0 {
				filtered := u.FilterActiveTools(allSuite)
				if len(filtered) == 0 {
					return UpdateResult{}, false, nil
				}
				if len(filtered) < len(allSuite) {
					effectiveTargets = filtered
				}
			}
		}
	}

	// 1. Guard check: Is harness executing? Protects all updates including forced (F-3)
	if u.sup != nil && u.sup.HasRunningHarness() {
		reason := "BlockedByActiveHarness"
		u.emitDeferredOnce(ctx, targetTools, reason, "Tool update deferred: harness execution active on host")
		return UpdateResult{}, true, nil
	}

	// 2. Fleet Git Clean check (bypassed if force or bypassDirty)
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
	if !snap.FleetGitClean && !force && !bypassDirty {
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

	// Re-verify inside lock if not forced and not bypassing dirty guard
	if !force && !bypassDirty {
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
		if driftErr != nil {
			// Fail open on drift inspection error (F3): log and proceed with update
			fmt.Fprintf(os.Stderr, "[eagerupdater] CheckDrift error, proceeding with update (fail-open): %v\n", driftErr)
		} else if !hasDrift {
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
	var updateErr error
	var tv string
	if len(targetVersion) > 0 {
		tv = targetVersion[0]
	}
	if u.doUpdateDetailed != nil {
		res, updateErr = u.doUpdateDetailed(ctx, effectiveTargets, force, tv)
	} else if u.doUpdate != nil {
		res, updateErr = u.doUpdate(ctx, effectiveTargets, force)
	}
	durationMs := time.Since(start).Milliseconds()

	// Deduplicate failures to prevent double backoff growth (C-1)
	failedSet := make(map[string]bool)
	for _, ft := range res.FailedTools {
		if s := strings.ToLower(strings.TrimSpace(ft)); s != "" {
			failedSet[s] = true
		}
	}
	if updateErr != nil && len(effectiveTargets) == 1 {
		if s := strings.ToLower(strings.TrimSpace(effectiveTargets[0])); s != "" {
			failedSet[s] = true
		}
	}
	for ft := range failedSet {
		u.recordToolFailure(ft)
	}

	for _, ut := range res.UpdatedTools {
		u.recordToolSuccess(ut)
	}

	// Only record suite failure on fatal errors for whole-suite passes, never for individual FailedTools or targeted passes (N1-R)
	if updateErr != nil && len(targetTools) == 0 {
		u.recordSuiteFailure()
	} else if len(targetTools) == 0 && len(res.FailedTools) == 0 {
		u.recordSuiteSuccess()
	}

	if updateErr != nil {
		if u.postNotif != nil {
			if err := u.postNotif(ctx, controlplane.NotificationRecord{
				Title:    "Tool Update Failed",
				Message:  fmt.Sprintf("Tool update failed on %s: %v", u.hostID, updateErr),
				Category: "error",
				Source:   "tool-updater",
				HostID:   u.hostID,
				Metadata: map[string]any{
					"event":        "agent.tool.update_failed",
					"error":        updateErr.Error(),
					"duration_ms":  durationMs,
					"failed_tools": res.FailedTools,
				},
			}); err != nil {
				fmt.Fprintf(os.Stderr, "[eagerupdater] postNotif error (update_failed): %v\n", err)
			}
		}
		return res, false, updateErr
	}

	if u.postNotif != nil {
		if len(res.FailedTools) > 0 {
			u.emitPartialFailureOnce(ctx, res.FailedTools, durationMs) // N1: deduped notification
		} else {
			u.mu.Lock()
			u.lastPartialFailureKey = "" // reset dedupe on clean run
			u.mu.Unlock()
		}
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
		} else if !force && len(res.FailedTools) == 0 {
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

func (u *EagerToolUpdater) emitPartialFailureOnce(ctx context.Context, failedTools []string, durationMs int64) {
	if len(failedTools) == 0 || u.postNotif == nil {
		return
	}
	sortedFailed := append([]string(nil), failedTools...)
	sort.Strings(sortedFailed)
	key := strings.Join(sortedFailed, ",")

	u.mu.Lock()
	if u.lastPartialFailureKey == key {
		u.mu.Unlock()
		return
	}
	u.lastPartialFailureKey = key
	u.mu.Unlock()

	_ = u.postNotif(ctx, controlplane.NotificationRecord{
		Title:    "Tool Update Partial Failure",
		Message:  fmt.Sprintf("Some tools failed to update on %s: %s", u.hostID, strings.Join(sortedFailed, ", ")),
		Category: "warning",
		Source:   "tool-updater",
		HostID:   u.hostID,
		Metadata: map[string]any{
			"event":        "agent.tool.update_partial_failure",
			"failed_tools": sortedFailed,
			"duration_ms":  durationMs,
		},
	})
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
