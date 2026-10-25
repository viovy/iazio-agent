package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
)

func initTestGitRepo(t *testing.T, dir string) {
	t.Helper()
	runCmd := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v (%s)", args, err, string(out))
		}
	}

	runCmd("init", "-b", "main")
	testFile := filepath.Join(dir, "README.md")
	if err := os.WriteFile(testFile, []byte("# Test Repo\n"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	runCmd("add", "README.md")
	runCmd("commit", "-m", "initial commit")
}

func TestInspectRepoGitState_CleanAndDirty(t *testing.T) {
	tempDir := t.TempDir()
	initTestGitRepo(t, tempDir)

	ctx := context.Background()

	// 1. Initial state should be clean
	st := InspectRepoGitState(ctx, tempDir, "test-repo")
	if !st.IsClean {
		t.Fatalf("expected clean repo, got dirty (dirty=%d, untracked=%d, err=%s)",
			st.DirtyFilesCount, st.UntrackedFilesCount, st.Error)
	}
	if st.Branch != "main" {
		t.Errorf("expected branch main, got %s", st.Branch)
	}
	if st.HeadSHA == "" {
		t.Errorf("expected non-empty head SHA")
	}

	// 2. Add untracked file
	untrackedFile := filepath.Join(tempDir, "untracked.txt")
	if err := os.WriteFile(untrackedFile, []byte("hello"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	st2 := InspectRepoGitState(ctx, tempDir, "test-repo")
	if st2.IsClean {
		t.Fatalf("expected dirty repo after adding untracked file")
	}
	if st2.UntrackedFilesCount != 1 {
		t.Errorf("expected 1 untracked file, got %d", st2.UntrackedFilesCount)
	}

	// 3. Clean up untracked file
	_ = os.Remove(untrackedFile)
	st3 := InspectRepoGitState(ctx, tempDir, "test-repo")
	if !st3.IsClean {
		t.Fatalf("expected clean repo after removing untracked file")
	}

	// 4. Create active git lockfile
	lockPath := filepath.Join(tempDir, ".git", "index.lock")
	if err := os.WriteFile(lockPath, []byte(""), 0644); err != nil {
		t.Fatalf("write lockfile: %v", err)
	}
	st4 := InspectRepoGitState(ctx, tempDir, "test-repo")
	if st4.IsClean || !st4.HasActiveGitLock {
		t.Fatalf("expected HasActiveGitLock=true and IsClean=false when lockfile exists")
	}
	_ = os.Remove(lockPath)
}

func TestRepoStateMonitor_ExecutionGuardAndFleetClean(t *testing.T) {
	tempDir := t.TempDir()
	initTestGitRepo(t, tempDir)

	sup := New(time.Now)

	var reportedStatuses []controlplane.RepoRuntimeStatus
	var notifications []controlplane.NotificationRecord
	var cleanTriggered bool

	monitor := NewRepoStateMonitor(
		sup,
		"test-host",
		2*time.Minute,
		func() ([]RepoConfigItem, error) {
			return []RepoConfigItem{{Path: tempDir, Alias: "test-repo"}}, nil
		},
		func(ctx context.Context, hostID string, statuses []controlplane.RepoRuntimeStatus) error {
			reportedStatuses = statuses
			return nil
		},
		func(ctx context.Context, rec controlplane.NotificationRecord) error {
			notifications = append(notifications, rec)
			return nil
		},
		func(snap FleetStateSnapshot) {
			cleanTriggered = true
		},
	)

	ctx := context.Background()

	// 1. When harness is active: must skip check and report HarnessActive
	if err := sup.TrySpawn(tempDir); err != nil {
		t.Fatalf("setup TrySpawn: %v", err)
	}
	snapActive := monitor.InspectFleet(ctx)
	if !snapActive.HarnessActive {
		t.Fatalf("expected HarnessActive=true when harness lock is held")
	}
	if snapActive.FleetGitClean {
		t.Fatalf("expected FleetGitClean=false when harness is active")
	}

	// 2. Unlock harness: now should inspect and report FleetGitClean=true
	sup.MarkIdle(tempDir)
	cleanTriggered = false
	snapClean := monitor.InspectFleet(ctx)
	if snapClean.HarnessActive {
		t.Fatalf("expected HarnessActive=false")
	}
	if !snapClean.FleetGitClean {
		t.Fatalf("expected FleetGitClean=true")
	}
	if !cleanTriggered {
		t.Fatalf("expected onFleetClean callback triggered")
	}
	if len(reportedStatuses) != 1 || !reportedStatuses[0].IsClean {
		t.Fatalf("expected 1 clean reported status")
	}
}

func TestEagerToolUpdater_PreconditionsAndDeduplicatedNotifications(t *testing.T) {
	tempDir := t.TempDir()
	initTestGitRepo(t, tempDir)

	sup := New(time.Now)

	var notifications []controlplane.NotificationRecord
	postNotif := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		notifications = append(notifications, rec)
		return nil
	}

	monitor := NewRepoStateMonitor(
		sup,
		"test-host",
		2*time.Minute,
		func() ([]RepoConfigItem, error) {
			return []RepoConfigItem{{Path: tempDir, Alias: "test-repo"}}, nil
		},
		nil,
		postNotif,
		nil,
	)

	var updateRunCount int
	doUpdate := func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		updateRunCount++
		return UpdateResult{
			UpdatedTools: []string{"autopilot-plugins-internal"},
			AgentUpdated: false,
		}, nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", doUpdate, postNotif)
	ctx := context.Background()

	// 1. Blocked by Active Harness
	if err := sup.TrySpawn(tempDir); err != nil {
		t.Fatalf("setup TrySpawn: %v", err)
	}
	_, deferred, err := updater.EvaluateAndTrigger(ctx, []string{"autopilot"}, false)
	if err != nil || !deferred {
		t.Fatalf("expected deferred=true when harness active, err=%v", err)
	}
	if len(notifications) != 1 || (notifications[0].Metadata["event"] != "agent.tool.update_deferred" && notifications[0].Metadata["event"] != "agent.eager_update.deferred") {
		t.Fatalf("expected 1 update_deferred notification, got %v", notifications)
	}

	// Second check while still blocked: must be deduplicated
	_, deferred2, _ := updater.EvaluateAndTrigger(ctx, []string{"autopilot"}, false)
	if !deferred2 || len(notifications) != 1 {
		t.Fatalf("expected deduplication to prevent second identical notification, count=%d", len(notifications))
	}

	sup.MarkIdle(tempDir)

	// 2. Blocked by Dirty Repo
	dirtyFile := filepath.Join(tempDir, "dirty.txt")
	_ = os.WriteFile(dirtyFile, []byte("dirty content"), 0644)
	monitor.InspectFleet(ctx) // Update snapshot to dirty

	_, deferredDirty, _ := updater.EvaluateAndTrigger(ctx, []string{"autopilot"}, false)
	if !deferredDirty {
		t.Fatalf("expected deferred=true when repo is dirty")
	}
	if len(notifications) != 2 || (notifications[1].Metadata["event"] != "agent.tool.update_deferred" && notifications[1].Metadata["event"] != "agent.eager_update.deferred") {
		t.Fatalf("expected new notification for dirty repo deferral")
	}

	// 3. Clean repo & idle harness: update runs successfully
	_ = os.Remove(dirtyFile)
	monitor.InspectFleet(ctx) // Update snapshot to clean

	agentUpdated, deferredClean, err := updater.EvaluateAndTrigger(ctx, []string{"autopilot"}, false)
	if err != nil || deferredClean || agentUpdated {
		t.Fatalf("expected successful update, got deferred=%v, err=%v", deferredClean, err)
	}
	if updateRunCount != 1 {
		t.Fatalf("expected doUpdate called once, got %d", updateRunCount)
	}

	// Check that started and succeeded notifications were emitted
	var foundStarted, foundSucceeded bool
	for _, n := range notifications {
		if n.Metadata["event"] == "agent.tool.update_started" || n.Metadata["event"] == "agent.eager_update.started" {
			foundStarted = true
		}
		if n.Metadata["event"] == "agent.tool.update_succeeded" || n.Metadata["event"] == "agent.eager_update.succeeded" {
			foundSucceeded = true
		}
	}
	if !foundStarted || !foundSucceeded {
		t.Fatalf("expected update_started and update_succeeded notifications, got: %v", notifications)
	}
}

func TestRepoStateMonitor_InventoryFailureFailClosed(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(
		sup,
		"test-host",
		2*time.Minute,
		func() ([]RepoConfigItem, error) {
			return nil, errors.New("simulated control-plane 503 outage")
		},
		nil,
		nil,
		nil,
	)

	snap := monitor.InspectFleet(context.Background())
	if snap.FleetGitClean {
		t.Fatalf("expected FleetGitClean=false on inventory lookup failure (fail-closed)")
	}
	if len(snap.DirtyRepoPaths) == 0 || snap.DirtyRepoPaths[0] != "repo_inventory_unavailable" {
		t.Fatalf("expected DirtyRepoPaths to include repo_inventory_unavailable, got %v", snap.DirtyRepoPaths)
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		t.Fatalf("doUpdate must NOT be called when inventory failed")
		return UpdateResult{}, nil
	}, nil)

	_, deferred, _ := updater.EvaluateAndTrigger(context.Background(), nil, false)
	if !deferred {
		t.Fatalf("expected update to be deferred when repo inventory is unavailable")
	}
}

func TestSupervisor_ToolUpdateHostExecutionLock(t *testing.T) {
	sup := New(time.Now)

	// Acquire tool update host lock
	release, ok := sup.TryAcquireToolUpdate()
	if !ok || release == nil {
		t.Fatalf("expected TryAcquireToolUpdate to succeed")
	}
	if !sup.IsToolUpdating() {
		t.Fatalf("expected IsToolUpdating=true")
	}

	// While tool update is in progress, harness Spawn / TrySpawn must be refused
	err := sup.TrySpawn("/tmp/test-repo")
	if err == nil || !strings.Contains(err.Error(), "tool update in progress") {
		t.Fatalf("expected TrySpawn to be blocked while tool update in progress, got %v", err)
	}

	// Release lock
	release()
	if sup.IsToolUpdating() {
		t.Fatalf("expected IsToolUpdating=false after release")
	}

	// Now TrySpawn succeeds
	if err := sup.TrySpawn("/tmp/test-repo"); err != nil {
		t.Fatalf("expected TrySpawn to succeed after release, got %v", err)
	}
}

func TestEagerToolUpdater_ForceTakesHostExecutionLock(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)

	var hostLockedDuringUpdate bool
	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		hostLockedDuringUpdate = sup.IsToolUpdating()
		// While update is running, TrySpawn must be blocked even when update was forced
		err := sup.TrySpawn("/tmp/test-worktree")
		if err == nil || !strings.Contains(err.Error(), "tool update in progress") {
			t.Errorf("expected TrySpawn to fail with 'tool update in progress', got: %v", err)
		}
		return UpdateResult{UpdatedTools: []string{"autopilot"}}, nil
	}, nil)

	// Trigger forced update
	_, deferred, err := updater.EvaluateAndTrigger(context.Background(), []string{"autopilot"}, true)
	if err != nil || deferred {
		t.Fatalf("expected forced update to execute, got deferred=%v, err=%v", deferred, err)
	}
	if !hostLockedDuringUpdate {
		t.Fatalf("expected sup.IsToolUpdating()=true during forced update swap")
	}
	if sup.IsToolUpdating() {
		t.Fatalf("expected sup.IsToolUpdating()=false after update finished")
	}
}

func TestEagerToolUpdater_OverlappingCallsQueuePendingForce(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	// Set initial fleet state clean
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var executionCount int
	var executedForces []bool
	var mu sync.Mutex

	inProgressCh := make(chan struct{})
	continueCh := make(chan struct{})

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		mu.Lock()
		executionCount++
		executedForces = append(executedForces, force)
		count := executionCount
		mu.Unlock()

		if count == 1 {
			close(inProgressCh)
			<-continueCh
			return UpdateResult{UpdatedTools: []string{"tool-a"}}, nil
		}
		return UpdateResult{UpdatedTools: []string{"tool-b"}, AgentUpdated: true}, nil
	}, nil)

	// Call 1: unforced
	var call1AgentUpdated bool
	call1Done := make(chan struct{})
	go func() {
		updated, _, _ := updater.EvaluateAndTrigger(context.Background(), nil, false)
		call1AgentUpdated = updated
		close(call1Done)
	}()

	// Wait until call 1 is actively inside doUpdate
	<-inProgressCh

	// Call 2: concurrent forced call must return ErrUpdateInProgress and queue pending execution (G4, E3)
	_, deferred, err := updater.EvaluateAndTrigger(context.Background(), []string{"tool-b"}, true)
	if !deferred || err != ErrUpdateInProgress {
		t.Fatalf("expected call 2 to return deferred=true with ErrUpdateInProgress, got: deferred=%v, err=%v", deferred, err)
	}

	// Release call 1
	close(continueCh)
	// Channel synchronization guarantees loop finishes both passes before call1Done closes (E3)
	<-call1Done

	if !call1AgentUpdated {
		t.Fatalf("expected queued agent update in pass 2 to return agentUpdated=true to caller (E3)")
	}

	mu.Lock()
	defer mu.Unlock()
	if executionCount != 2 {
		t.Fatalf("expected exactly 2 executions (initial + queued pending force), got %d", executionCount)
	}
	if len(executedForces) != 2 || executedForces[0] != false || executedForces[1] != true {
		t.Fatalf("expected executions [false, true], got %v", executedForces)
	}
}

func TestEagerToolUpdater_NoUpdateStartedWhenNoDrift(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var emittedEvents []string
	var mu sync.Mutex
	notifFn := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		mu.Lock()
		defer mu.Unlock()
		if evt, ok := rec.Metadata["event"].(string); ok {
			emittedEvents = append(emittedEvents, evt)
		}
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		return UpdateResult{}, nil
	}, notifFn)

	// Configure CheckDrift to report no drift (all tools up-to-date)
	updater.CheckDrift = func(ctx context.Context, targetTools []string) (bool, error) {
		return false, nil
	}

	// Unforced check should suppress noise (no update_started emitted) (E4)
	agentUpdated, deferred, err := updater.EvaluateAndTrigger(context.Background(), nil, false)
	if err != nil || deferred || agentUpdated {
		t.Fatalf("expected clean up-to-date return, got updated=%v deferred=%v err=%v", agentUpdated, deferred, err)
	}

	mu.Lock()
	eventsCount := len(emittedEvents)
	mu.Unlock()

	if eventsCount != 0 {
		t.Fatalf("expected 0 notifications when CheckDrift returns false, got %v (E4)", emittedEvents)
	}
}

func TestEagerToolUpdater_PendingForceRunsAfterUnforcedPassDefersOnDirtyFleet(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	// Fleet is dirty
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: false, DirtyRepoPaths: []string{"/work/dirty"}}

	var forcedExecuted bool
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		mu.Lock()
		if force {
			forcedExecuted = true
		}
		mu.Unlock()
		return UpdateResult{UpdatedTools: []string{"tool-forced"}}, nil
	}, nil)

	// Pre-queue a pending forced update by taking the mutex temporarily
	updater.updateMutex.Lock()
	_, def, err := updater.EvaluateAndTrigger(context.Background(), []string{"tool-forced"}, true)
	if !def || err != ErrUpdateInProgress {
		t.Fatalf("expected queued pending force, got def=%v err=%v", def, err)
	}
	updater.updateMutex.Unlock()

	// An unforced call now runs; pass 1 will defer because fleet is dirty, but MUST drain pending force (H1)
	_, deferred, err := updater.EvaluateAndTrigger(context.Background(), nil, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if deferred {
		t.Fatalf("expected deferred=false because queued force was executed")
	}

	mu.Lock()
	wasForced := forcedExecuted
	mu.Unlock()

	if !wasForced {
		t.Fatalf("expected queued forced update to run even when initial pass deferred on dirty fleet (H1)")
	}
}

func TestEagerToolUpdater_AgentUpdatedPreservedWhenPass2Defers(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var pass int
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		mu.Lock()
		pass++
		currentPass := pass
		mu.Unlock()

		if currentPass == 1 {
			// Inside pass 1, simulate repos becoming dirty so pass 2 defers
			monitor.mu.Lock()
			monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: false, DirtyRepoPaths: []string{"/work/dirty"}}
			monitor.mu.Unlock()
			return UpdateResult{AgentUpdated: true, UpdatedTools: []string{"iazio-agent"}}, nil
		}
		return UpdateResult{}, nil
	}, nil)

	// Pre-queue pass 2 targeted but UNFORCED so it defers on the dirty repos (N2)
	updater.updateMutex.Lock()
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), []string{"tool-queued"}, false)
	updater.updateMutex.Unlock()

	agentUpdated, deferred, _ := updater.EvaluateAndTrigger(context.Background(), nil, false)
	if !agentUpdated {
		t.Fatalf("expected agentUpdated=true to be sticky and preserved for caller restart even when pass 2 defers (H2, N2)")
	}
	if !deferred {
		t.Fatalf("expected deferred=true because pass 2 deferred on dirty repos (N2)")
	}
}

func TestEagerToolUpdater_StaleHarnessSnapshotRefreshes(t *testing.T) {
	sup := New(time.Now)
	// No running harness in supervisor (idle)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	// Stale snapshot claims harness is active
	monitor.lastSnapshot = FleetStateSnapshot{HarnessActive: true, FleetGitClean: false}

	var updatedCalled bool
	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		updatedCalled = true
		return UpdateResult{UpdatedTools: []string{"tool-a"}}, nil
	}, nil)

	_, deferred, err := updater.EvaluateAndTrigger(context.Background(), nil, false)
	if err != nil || deferred {
		t.Fatalf("expected stale snapshot to be refreshed and proceed, got deferred=%v err=%v (N6)", deferred, err)
	}
	if !updatedCalled {
		t.Fatalf("expected doUpdate to be called after refreshing stale harness snapshot (N6)")
	}
}

func TestEagerToolUpdater_DedupeKeyResetOnCleanPass(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	// Start dirty
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: false, DirtyRepoPaths: []string{"/work/dirty"}}

	var deferredNotifs int
	var mu sync.Mutex
	notifFn := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		mu.Lock()
		defer mu.Unlock()
		if evt, ok := rec.Metadata["event"].(string); ok && (evt == "agent.tool.update_deferred" || evt == "agent.eager_update.deferred") {
			deferredNotifs++
		}
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		return UpdateResult{}, nil
	}, notifFn)
	updater.CheckDrift = func(ctx context.Context, targetTools []string) (bool, error) {
		return false, nil // no drift on clean passes
	}

	// Pass 1: dirty fleet -> emits deferred notification #1
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), nil, false)
	mu.Lock()
	if deferredNotifs != 1 {
		t.Fatalf("expected 1 deferred notif, got %d", deferredNotifs)
	}
	mu.Unlock()

	// Pass 2: fleet becomes clean -> passes guards -> resets lastDeferredKey (L6)
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}
	monitor.mu.Unlock()
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), nil, false)

	// Pass 3: fleet becomes dirty again with same repo -> MUST emit deferred notification #2 (L6)
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: false, DirtyRepoPaths: []string{"/work/dirty"}}
	monitor.mu.Unlock()
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), nil, false)

	mu.Lock()
	if deferredNotifs != 2 {
		t.Fatalf("expected 2 deferred notifs (key was reset after clean pass), got %d (L6, N6)", deferredNotifs)
	}
	mu.Unlock()
}

func TestEagerToolUpdater_DedupeMultipleDirtyReposOrderAgnostic(t *testing.T) {
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)

	var deferredNotifs int
	var mu sync.Mutex
	notifFn := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		mu.Lock()
		defer mu.Unlock()
		if evt, ok := rec.Metadata["event"].(string); ok && (evt == "agent.tool.update_deferred" || evt == "agent.eager_update.deferred") {
			deferredNotifs++
		}
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		return UpdateResult{}, nil
	}, notifFn)

	// Pass 1: multiple dirty repos in order B, A
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{
		FleetGitClean:  false,
		DirtyRepoPaths: []string{"/work/repo-b", "/work/repo-a"},
	}
	monitor.mu.Unlock()
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), nil, false)

	mu.Lock()
	if deferredNotifs != 1 {
		t.Fatalf("expected 1 deferred notif after pass 1, got %d", deferredNotifs)
	}
	mu.Unlock()

	// Pass 2: same dirty repos but reported in reversed order A, B
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{
		FleetGitClean:  false,
		DirtyRepoPaths: []string{"/work/repo-a", "/work/repo-b"},
	}
	monitor.mu.Unlock()
	_, _, _ = updater.EvaluateAndTrigger(context.Background(), nil, false)

	mu.Lock()
	if deferredNotifs != 1 {
		t.Fatalf("expected still 1 deferred notif after permutation pass (deduped), got %d (F-1)", deferredNotifs)
	}
	mu.Unlock()
}

func TestRepoStateMonitor_LevelTriggeredCleanPasses(t *testing.T) {
	tempDir := t.TempDir()
	initTestGitRepo(t, tempDir)

	sup := New(time.Now)
	var onFleetCleanCalls int
	monitor := NewRepoStateMonitor(
		sup,
		"test-host",
		2*time.Minute,
		func() ([]RepoConfigItem, error) {
			return []RepoConfigItem{
				{Path: tempDir, Alias: "test-repo"},
			}, nil
		},
		nil,
		nil,
		func(snap FleetStateSnapshot) {
			onFleetCleanCalls++
		},
	)

	ctx := context.Background()

	// Pass 1: Repo is clean -> onFleetClean must fire
	snap1 := monitor.InspectFleet(ctx)
	if !snap1.FleetGitClean {
		t.Fatalf("expected clean fleet on pass 1, got dirty: %v", snap1.DirtyRepoPaths)
	}
	if onFleetCleanCalls != 1 {
		t.Fatalf("expected onFleetClean to be called once on pass 1, got %d", onFleetCleanCalls)
	}

	// Pass 2: Repo remains clean -> level trigger must fire AGAIN (consecutive clean pass, F7)
	snap2 := monitor.InspectFleet(ctx)
	if !snap2.FleetGitClean {
		t.Fatalf("expected clean fleet on pass 2, got dirty: %v", snap2.DirtyRepoPaths)
	}
	if onFleetCleanCalls != 2 {
		t.Fatalf("expected onFleetClean to be called twice across consecutive clean passes (level-triggered), got %d", onFleetCleanCalls)
	}
}

func TestEagerToolUpdater_Backoff(t *testing.T) {
	// R6, R7: Test calculateBackoff capping and shouldBackoff / recordFailure
	if b := calculateBackoff(0); b != 0 {
		t.Errorf("expected 0 for 0 attempts, got %v", b)
	}
	if b := calculateBackoff(1); b != 30*time.Second {
		t.Errorf("expected 30s for 1 attempt, got %v", b)
	}
	if b := calculateBackoff(2); b != 60*time.Second {
		t.Errorf("expected 60s for 2 attempts, got %v", b)
	}
	// Cap at 1h (R7)
	if b := calculateBackoff(20); b != 1*time.Hour {
		t.Errorf("expected 1h cap for 20 attempts, got %v", b)
	}

	updater := NewEagerToolUpdater(nil, nil, "test-host", nil, nil)
	// Initial state: no backoff
	if updater.shouldBackoff([]string{"my-tool"}, false) {
		t.Fatalf("expected no backoff initially")
	}

	// Record failure: should backoff immediately
	updater.recordToolFailure("my-tool")
	if !updater.shouldBackoff([]string{"my-tool"}, false) {
		t.Fatalf("expected shouldBackoff=true after failure")
	}
	// Force bypasses backoff
	if updater.shouldBackoff([]string{"my-tool"}, true) {
		t.Fatalf("expected force=true to bypass backoff")
	}

	// Record success: backoff cleared
	updater.recordToolSuccess("my-tool")
	if updater.shouldBackoff([]string{"my-tool"}, false) {
		t.Fatalf("expected shouldBackoff=false after success")
	}
}

func TestEagerToolUpdater_DeferralRetentionAndUnmergedDraining(t *testing.T) {
	// R4, R5, R6: Deferred forced/pinned requests must be retained across active harness runs
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var executions []struct {
		tools   []string
		force   bool
		version string
	}
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		var tv string
		if len(targetVersion) > 0 {
			tv = targetVersion[0]
		}
		executions = append(executions, struct {
			tools   []string
			force   bool
			version string
		}{
			tools:   append([]string(nil), targetTools...),
			force:   force,
			version: tv,
		})
		mu.Unlock()
		return UpdateResult{UpdatedTools: targetTools}, nil
	})

	ctx := context.Background()

	// 1. Simulate active harness blocking tool update
	tempDir := t.TempDir()
	if err := sup.TrySpawn(tempDir); err != nil {
		t.Fatalf("setup TrySpawn failed: %v", err)
	}

	// Request forced update for tool-a @ 0.1.0 while harness is running
	_, deferred, err := updater.EvaluateAndTriggerDetailed(ctx, []string{"tool-a"}, true, "0.1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deferred {
		t.Fatalf("expected request to be deferred while harness is active")
	}

	// Verify nothing was executed yet
	mu.Lock()
	if len(executions) != 0 {
		t.Fatalf("expected 0 executions while deferred, got %d", len(executions))
	}
	mu.Unlock()

	// 2. Harness finishes execution
	sup.MarkIdle(tempDir)

	// 3. Next periodic clean tick runs with targetTools=nil, force=false
	_, _, err = updater.EvaluateAndTrigger(ctx, nil, false)
	if err != nil {
		t.Fatalf("unexpected error on clean tick: %v", err)
	}

	// 4. Assert deferred request was retained and executed with original version and force flag (R4, R5)
	mu.Lock()
	defer mu.Unlock()
	if len(executions) < 1 {
		t.Fatalf("expected deferred request to be executed on clean tick, got %d executions", len(executions))
	}
	var found bool
	for _, ex := range executions {
		if len(ex.tools) == 1 && ex.tools[0] == "tool-a" {
			found = true
			if !ex.force {
				t.Errorf("expected force=true retained for deferred request, got false")
			}
			if ex.version != "0.1.0" {
				t.Errorf("expected version 0.1.0 retained for deferred request, got %q", ex.version)
			}
		}
	}
	if !found {
		t.Fatalf("expected deferred request for tool-a to be executed on clean tick, got executions: %v", executions)
	}
}

func TestEagerToolUpdater_SuitePartialFailureBackoffAndNotificationDedupe(t *testing.T) {
	// N1: Test that partial failures on unforced suite path engage backoff and deduplicate notifications
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var callCount int
	var mu sync.Mutex

	var emittedNotifications []controlplane.NotificationRecord
	var notifMu sync.Mutex

	postNotif := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		notifMu.Lock()
		emittedNotifications = append(emittedNotifications, rec)
		notifMu.Unlock()
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, postNotif)
	updater.ListSuiteTools = func(ctx context.Context) ([]string, error) {
		return []string{"bad-tool"}, nil
	}
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		return UpdateResult{
			FailedTools: []string{"bad-tool"},
		}, nil
	})

	ctx := context.Background()

	// Pass 1: unforced suite update (targetTools == nil)
	res1, def1, err1 := updater.EvaluateAndTriggerDetailed(ctx, nil, false)
	if err1 != nil || def1 {
		t.Fatalf("expected pass 1 to complete without deferral, err=%v, def=%v", err1, def1)
	}
	if len(res1.FailedTools) != 1 || res1.FailedTools[0] != "bad-tool" {
		t.Fatalf("expected failed tool 'bad-tool', got %v", res1.FailedTools)
	}

	mu.Lock()
	if callCount != 1 {
		t.Fatalf("expected 1 call to doUpdateDetailed, got %d", callCount)
	}
	mu.Unlock()

	notifMu.Lock()
	partialNotifCount := 0
	for _, n := range emittedNotifications {
		if n.Metadata["event"] == "agent.tool.update_partial_failure" {
			partialNotifCount++
		}
	}
	if partialNotifCount != 1 {
		t.Fatalf("expected 1 partial failure notification on pass 1, got %d", partialNotifCount)
	}
	notifMu.Unlock()

	// Verify tool and suite backoff state
	if !updater.IsToolInBackoff("bad-tool") {
		t.Fatalf("expected IsToolInBackoff('bad-tool')=true after failure")
	}

	// Pass 2: immediate second unforced suite pass (within backoff window)
	res2, def2, err2 := updater.EvaluateAndTriggerDetailed(ctx, nil, false)
	if err2 != nil || def2 {
		t.Fatalf("expected pass 2 to complete without deferral, err=%v, def=%v", err2, def2)
	}
	if len(res2.FailedTools) != 0 || len(res2.UpdatedTools) != 0 {
		t.Fatalf("expected empty result on backed off pass 2, got %+v", res2)
	}

	mu.Lock()
	if callCount != 1 {
		t.Fatalf("expected doUpdateDetailed to NOT be called on pass 2 due to backoff (N1), callCount=%d", callCount)
	}
	mu.Unlock()

	notifMu.Lock()
	partialNotifCountPass2 := 0
	for _, n := range emittedNotifications {
		if n.Metadata["event"] == "agent.tool.update_partial_failure" {
			partialNotifCountPass2++
		}
	}
	if partialNotifCountPass2 != 1 {
		t.Fatalf("expected no new partial failure notifications on pass 2 (deduped, N1), got %d", partialNotifCountPass2)
	}
	notifMu.Unlock()
}

func TestEagerToolUpdater_HealthyToolUpdatesWhileAnotherBackedOff(t *testing.T) {
	// N1-R: When tool-broken is backed off and tool-healthy is drifting,
	// unforced suite update should update tool-healthy and NOT attempt tool-broken.
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var attemptedTools []string
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)
	updater.ListSuiteTools = func(ctx context.Context) ([]string, error) {
		return []string{"tool-broken", "tool-healthy"}, nil
	}
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		attemptedTools = append([]string(nil), targetTools...)
		mu.Unlock()
		return UpdateResult{
			UpdatedTools: []string{"tool-healthy"},
		}, nil
	})

	// Pre-record failure for tool-broken so it is within backoff window
	updater.recordToolFailure("tool-broken")
	if !updater.IsToolInBackoff("tool-broken") {
		t.Fatalf("expected tool-broken to be in backoff")
	}

	res, def, err := updater.EvaluateAndTriggerDetailed(context.Background(), nil, false)
	if err != nil || def {
		t.Fatalf("expected update to succeed, err=%v def=%v", err, def)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attemptedTools) != 1 || attemptedTools[0] != "tool-healthy" {
		t.Fatalf("expected only tool-healthy to be attempted (N1-R), got: %v", attemptedTools)
	}
	if len(res.UpdatedTools) != 1 || res.UpdatedTools[0] != "tool-healthy" {
		t.Fatalf("expected tool-healthy in updated tools, got: %v", res.UpdatedTools)
	}
}

func TestEagerToolUpdater_QueueCapAndDedupe(t *testing.T) {
	// N5-R: Verify dedupe of identical pending requests and ErrQueueFull on overflow
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	blockCh := make(chan struct{})
	doneCh := make(chan struct{})
	var once sync.Once

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		once.Do(func() { close(doneCh) })
		<-blockCh
		return UpdateResult{UpdatedTools: targetTools}, nil
	})

	ctx := context.Background()

	bgDone := make(chan struct{})
	// Launch background update that holds the lock
	go func() {
		defer close(bgDone)
		_, _, _ = updater.EvaluateAndTriggerDetailed(ctx, []string{"running-tool"}, true)
	}()

	<-doneCh // ensure lock is held

	// 1. Deduplication: queue identical request multiple times
	for i := 0; i < 5; i++ {
		_, def, err := updater.EvaluateAndTriggerWithOpts(ctx, TriggerOptions{
			TargetTools: []string{"dedupe-tool"},
			Force:       true,
		})
		if !def || !errors.Is(err, ErrUpdateInProgress) {
			t.Fatalf("expected ErrUpdateInProgress, got err=%v def=%v", err, def)
		}
	}

	updater.mu.Lock()
	pendingCount := len(updater.pendingRequests)
	updater.mu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("expected deduplication to keep exactly 1 request in queue (N5), got %d", pendingCount)
	}

	// 2. Queue capacity: fill up to maxPendingRequests (32)
	for i := pendingCount; i < maxPendingRequests; i++ {
		toolName := fmt.Sprintf("fill-tool-%d", i)
		_, _, err := updater.EvaluateAndTriggerWithOpts(ctx, TriggerOptions{
			TargetTools: []string{toolName},
			Force:       true,
		})
		if !errors.Is(err, ErrUpdateInProgress) {
			t.Fatalf("expected ErrUpdateInProgress while under cap, got %v", err)
		}
	}

	// 3. Overflow beyond cap must return ErrQueueFull (N5-R)
	_, _, overflowErr := updater.EvaluateAndTriggerWithOpts(ctx, TriggerOptions{
		TargetTools: []string{"overflow-tool"},
		Force:       true,
	})
	if !errors.Is(overflowErr, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull on queue overflow (N5-R), got %v", overflowErr)
	}

	// Unblock background update and wait for completion (R5: no goroutine leak)
	close(blockCh)
	select {
	case <-bgDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for background update to complete")
	}
}

func TestEagerToolUpdater_BypassDirtySkipsBackoff(t *testing.T) {
	// R2, B-1: Operator-dispatched update (BypassDirty: true) must run even when tools are in backoff
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var attempted []string
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		attempted = append([]string(nil), targetTools...)
		mu.Unlock()
		return UpdateResult{UpdatedTools: targetTools}, nil
	})

	// Pre-record failure for tool-backed-off
	updater.recordToolFailure("tool-backed-off")
	if !updater.IsToolInBackoff("tool-backed-off") {
		t.Fatalf("expected tool-backed-off to be in backoff")
	}

	// 1. Unforced background run without BypassDirty: tool is skipped
	res1, def1, err1 := updater.EvaluateAndTriggerWithOpts(context.Background(), TriggerOptions{
		TargetTools: []string{"tool-backed-off"},
		Force:       false,
		BypassDirty: false,
	})
	if err1 != nil || def1 || len(res1.UpdatedTools) != 0 {
		t.Fatalf("expected background run to skip backed off tool, got res=%v def=%v err=%v", res1, def1, err1)
	}

	// 2. Unforced operator run WITH BypassDirty: tool is NOT skipped (B-1, R2)
	res2, def2, err2 := updater.EvaluateAndTriggerWithOpts(context.Background(), TriggerOptions{
		TargetTools: []string{"tool-backed-off"},
		Force:       false,
		BypassDirty: true,
	})
	if err2 != nil || def2 {
		t.Fatalf("expected BypassDirty run to succeed, err=%v def=%v", err2, def2)
	}
	mu.Lock()
	if len(attempted) != 1 || attempted[0] != "tool-backed-off" {
		t.Fatalf("expected tool-backed-off to be attempted via BypassDirty (R2), got: %v", attempted)
	}
	mu.Unlock()
	if len(res2.UpdatedTools) != 1 || res2.UpdatedTools[0] != "tool-backed-off" {
		t.Fatalf("expected tool-backed-off to be updated, got: %v", res2.UpdatedTools)
	}
}

func TestEagerToolUpdater_PrependTruncation(t *testing.T) {
	// R3: Prepend truncation caps queue at maxPendingRequests
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)

	var reqs []PendingToolRequest
	for i := 0; i < maxPendingRequests+10; i++ {
		reqs = append(reqs, PendingToolRequest{
			Tool: fmt.Sprintf("tool-%d", i),
		})
	}

	updater.mu.Lock()
	updater.prependPendingRequestsLocked(reqs)
	count := len(updater.pendingRequests)
	updater.mu.Unlock()

	if count != maxPendingRequests {
		t.Fatalf("expected prepend to truncate to %d, got %d", maxPendingRequests, count)
	}
}

func TestEagerToolUpdater_SuiteDisabledOnlyAgent(t *testing.T) {
	// R1: When updateSuite is false, ListSuiteTools returns only iazio-agent,
	// and backed-off tools do not trigger updates of the rest of the manifest.
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var attempted []string
	var mu sync.Mutex

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, nil)
	updater.ListSuiteTools = func(ctx context.Context) ([]string, error) {
		// When updateSuite is false, only iazio-agent is returned (R1)
		return []string{"iazio-agent"}, nil
	}
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		attempted = append([]string(nil), targetTools...)
		mu.Unlock()
		return UpdateResult{AgentUpdated: true}, nil
	})

	// Put another tool into backoff
	updater.recordToolFailure("some-other-tool")

	res, def, err := updater.EvaluateAndTriggerDetailed(context.Background(), nil, false)
	if err != nil || def {
		t.Fatalf("expected update to succeed, err=%v def=%v", err, def)
	}
	if !res.AgentUpdated {
		t.Fatalf("expected AgentUpdated to be true")
	}

	mu.Lock()
	defer mu.Unlock()
	// Must not have expanded to any other manifest tools
	for _, tool := range attempted {
		if tool != "iazio-agent" && tool != "" {
			t.Fatalf("expected only iazio-agent to be attempted when suite update is disabled (R1), got: %v", attempted)
		}
	}
}

func TestEagerToolUpdater_BackedOffToolOnlyDriftingDoesNotTriggerUpdate(t *testing.T) {
	// S1: When tool-broken is backed off and is the only drifting tool,
	// CheckDrift must be evaluated against effectiveTargets (healthy tools),
	// preventing update_started notification and doUpdateDetailed execution.
	sup := New(time.Now)
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}

	var updateCalled bool
	var notifStartedCalled bool
	var mu sync.Mutex

	postNotif := func(ctx context.Context, rec controlplane.NotificationRecord) error {
		mu.Lock()
		defer mu.Unlock()
		if rec.Metadata != nil && rec.Metadata["event"] == "agent.tool.update_started" {
			notifStartedCalled = true
		}
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", nil, postNotif)
	updater.ListSuiteTools = func(ctx context.Context) ([]string, error) {
		return []string{"tool-broken", "tool-healthy"}, nil
	}
	updater.CheckDrift = func(ctx context.Context, targetTools []string) (bool, error) {
		// If targetTools includes tool-broken, drift would be true.
		// If effectiveTargets was correctly passed, targetTools only contains tool-healthy (which has no drift).
		for _, t := range targetTools {
			if t == "tool-broken" {
				return true, nil // broken tool is out of date
			}
		}
		return false, nil // healthy tool is already up-to-date
	}
	updater.SetDetailedUpdateFunc(func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (UpdateResult, error) {
		mu.Lock()
		updateCalled = true
		mu.Unlock()
		return UpdateResult{}, nil
	})

	// Put tool-broken into backoff
	updater.recordToolFailure("tool-broken")

	res, def, err := updater.EvaluateAndTriggerDetailed(context.Background(), nil, false)
	if err != nil || def {
		t.Fatalf("unexpected err=%v def=%v", err, def)
	}
	if len(res.UpdatedTools) != 0 {
		t.Fatalf("expected 0 updated tools, got: %v", res.UpdatedTools)
	}

	mu.Lock()
	defer mu.Unlock()
	if notifStartedCalled {
		t.Fatalf("expected agent.tool.update_started NOT to be emitted when only backed-off tool drifts (S1)")
	}
	if updateCalled {
		t.Fatalf("expected doUpdateDetailed NOT to be called when only backed-off tool drifts (S1)")
	}
}

func TestInspectRepoGitState_RecursiveSubmodules(t *testing.T) {
	rootTemp := t.TempDir()
	initTestGitRepo(t, rootTemp)

	// Create a nested sub-repository
	subRepoPath := filepath.Join(rootTemp, "nested-subrepo")
	if err := os.MkdirAll(subRepoPath, 0755); err != nil {
		t.Fatalf("mkdir subrepo: %v", err)
	}
	initTestGitRepo(t, subRepoPath)

	// Add .gitmodules and submodule gitlink in root
	gitmodulesContent := `[submodule "nested-subrepo"]
	path = nested-subrepo
	url = https://example.com/nested-subrepo.git
`
	if err := os.WriteFile(filepath.Join(rootTemp, ".gitmodules"), []byte(gitmodulesContent), 0644); err != nil {
		t.Fatalf("write .gitmodules: %v", err)
	}

	cmdAdd := exec.Command("git", "-C", rootTemp, "add", ".gitmodules", "nested-subrepo")
	cmdAdd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=t@e.com", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=t@e.com")
	_ = cmdAdd.Run()
	cmdCommit := exec.Command("git", "-C", rootTemp, "commit", "-m", "add gitmodules and subrepo gitlink")
	cmdCommit.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=t@e.com", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=t@e.com")
	_ = cmdCommit.Run()

	ctx := context.Background()

	stClean := InspectRepoGitState(ctx, rootTemp, "fleet-workspace")
	if !stClean.IsClean || !stClean.SubmodulesClean || !stClean.RootClean {
		t.Fatalf("expected both root and subrepo clean, got: is_clean=%t root_clean=%t submodules_clean=%t (err=%s)",
			stClean.IsClean, stClean.RootClean, stClean.SubmodulesClean, stClean.Error)
	}
	if len(stClean.SubRepos) != 1 {
		t.Fatalf("expected 1 subrepo discovered, got %d", len(stClean.SubRepos))
	}

	// 2. Make subrepo dirty by adding untracked file
	subDirtyFile := filepath.Join(subRepoPath, "dirty.txt")
	if err := os.WriteFile(subDirtyFile, []byte("dirty content"), 0644); err != nil {
		t.Fatalf("write sub dirty file: %v", err)
	}

	stSubDirty := InspectRepoGitState(ctx, rootTemp, "fleet-workspace")
	if stSubDirty.IsClean {
		t.Fatalf("expected workspace to be marked DIRTY because nested sub-repository has untracked file")
	}
	if stSubDirty.SubmodulesClean {
		t.Fatalf("expected SubmodulesClean == false")
	}
	if len(stSubDirty.DirtySubmodules) != 1 || stSubDirty.DirtySubmodules[0] != "nested-subrepo" {
		t.Fatalf("expected DirtySubmodules to contain nested-subrepo, got: %v", stSubDirty.DirtySubmodules)
	}

	// 3. Clean subrepo, but add git lock to subrepo
	_ = os.Remove(subDirtyFile)
	subLock := filepath.Join(subRepoPath, ".git", "index.lock")
	if err := os.WriteFile(subLock, []byte(""), 0644); err != nil {
		t.Fatalf("write sub lock: %v", err)
	}

	stSubLock := InspectRepoGitState(ctx, rootTemp, "fleet-workspace")
	if stSubLock.IsClean {
		t.Fatalf("expected workspace dirty when subrepo has active index.lock")
	}
	if !stSubLock.HasActiveGitLock {
		t.Fatalf("expected HasActiveGitLock == true propagated from subrepo")
	}
	_ = os.Remove(subLock)

	// 4. Verify clean again
	stRestored := InspectRepoGitState(ctx, rootTemp, "fleet-workspace")
	if !stRestored.IsClean || !stRestored.SubmodulesClean {
		t.Fatalf("expected workspace clean after clearing lock, got: %v (err=%s)", stRestored.IsClean, stRestored.Error)
	}
}

func TestEagerToolUpdater_ReadinessReportAndGateIntegration(t *testing.T) {
	sup := New(time.Now)
	gate := NewHostExecutionAndUpdateGate()
	monitor := NewRepoStateMonitor(sup, "test-host", 2*time.Minute, nil, nil, nil, nil)
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}
	monitor.mu.Unlock()

	var reportedReadiness []EagerUpdateReadinessReport
	var readMu sync.Mutex
	reportFn := func(ctx context.Context, hostID string, rep EagerUpdateReadinessReport) error {
		readMu.Lock()
		reportedReadiness = append(reportedReadiness, rep)
		readMu.Unlock()
		return nil
	}

	updater := NewEagerToolUpdater(sup, monitor, "test-host", func(ctx context.Context, targetTools []string, force bool) (UpdateResult, error) {
		return UpdateResult{UpdatedTools: []string{"autopilot"}}, nil
	}, nil)
	updater.SetHostGate(gate)
	updater.SetReadinessReporter(reportFn)

	detector := NewMultiSignalExecutionDetector(sup, gate, nil, nil)
	detector.SetProcessScanner(func(repoPaths []string) ([]ActiveHarnessSignal, error) {
		return nil, nil
	})
	updater.SetExecutionDetector(detector)

	ctx := context.Background()

	// 1. Initial State: clean & idle -> UP_TO_DATE
	rep1 := updater.EvaluateReadiness(ctx)
	if rep1.EagerUpdateState != StateUpToDate {
		t.Fatalf("expected StateUpToDate, got %s", rep1.EagerUpdateState)
	}

	// 2. Active Harness via Gate Lease -> DEFERRED_HARNESS_ACTIVE
	relLease, err := gate.AcquireExecutionLease(ctx, "job-test-42", "/path/repo")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	rep2 := updater.EvaluateReadiness(ctx)
	if rep2.EagerUpdateState != StateDeferredHarnessActive {
		t.Fatalf("expected StateDeferredHarnessActive, got %s", rep2.EagerUpdateState)
	}
	if len(rep2.BlockingActiveJobs) == 0 || rep2.BlockingActiveJobs[0].JobID != "job-test-42" {
		t.Fatalf("expected BlockingActiveJobs to cite job-test-42, got: %+v", rep2.BlockingActiveJobs)
	}

	// Verify EvaluateAndTrigger is deferred
	_, def, err := updater.EvaluateAndTrigger(ctx, nil, false)
	if !def || err != nil {
		t.Fatalf("expected deferred=true when harness is active")
	}

	relLease()

	// 3. Dirty Repo -> DEFERRED_GIT_DIRTY
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{
		FleetGitClean:  false,
		DirtyRepoPaths: []string{"/path/fleet-workspace"},
		Repos: map[string]RepoGitState{
			"/path/fleet-workspace": {
				RepoPath:        "/path/fleet-workspace",
				IsClean:         false,
				RootClean:       true,
				SubmodulesClean: false,
				DirtySubmodules: []string{"sub-module-x"},
				SubRepos: map[string]SubRepoGitState{
					"sub-module-x": {
						SubRepoPath:         "/path/fleet-workspace/sub-module-x",
						RelPath:             "sub-module-x",
						DirtyFilesCount:     3,
						UntrackedFilesCount: 1,
						Branch:              "feature-branch",
					},
				},
			},
		},
	}
	monitor.mu.Unlock()

	rep3 := updater.EvaluateReadiness(ctx)
	if rep3.EagerUpdateState != StateDeferredGitDirty {
		t.Fatalf("expected StateDeferredGitDirty, got %s", rep3.EagerUpdateState)
	}
	if len(rep3.BlockingDirtyRepos) == 0 || rep3.BlockingDirtyRepos[0].SubRepoPath != "sub-module-x" {
		t.Fatalf("expected BlockingDirtyRepos to cite sub-module-x, got: %+v", rep3.BlockingDirtyRepos)
	}

	// 4. Clear dirty state -> UP_TO_DATE again
	monitor.mu.Lock()
	monitor.lastSnapshot = FleetStateSnapshot{FleetGitClean: true}
	monitor.mu.Unlock()

	rep4 := updater.EvaluateReadiness(ctx)
	if rep4.EagerUpdateState != StateUpToDate {
		t.Fatalf("expected StateUpToDate, got %s", rep4.EagerUpdateState)
	}

	// 5. Test TriggerEvaluation wake
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	updater.StartHybridTriggerLoop(loopCtx, nil)

	updater.TriggerEvaluation("test_event")
	time.Sleep(600 * time.Millisecond)

	readMu.Lock()
	defer readMu.Unlock()
	if len(reportedReadiness) < 4 {
		t.Errorf("expected at least 4 readiness reports posted, got %d", len(reportedReadiness))
	}
}

func TestEagerToolUpdater_AuxiliaryMethods(t *testing.T) {
	updater := NewEagerToolUpdater(nil, nil, "host-aux", nil, nil)
	if updater.IsUpdating() {
		t.Errorf("expected IsUpdating=false")
	}

	updater.recordSuiteFailure()
	updater.mu.Lock()
	if updater.suiteFailCount != 1 {
		t.Errorf("expected suiteFailCount=1, got %d", updater.suiteFailCount)
	}
	updater.mu.Unlock()

	updater.recordSuiteSuccess()
	updater.mu.Lock()
	if updater.suiteFailCount != 0 {
		t.Errorf("expected suiteFailCount=0, got %d", updater.suiteFailCount)
	}
	updater.mu.Unlock()

	last := updater.LastReadiness()
	if last.HostID != "host-aux" {
		t.Errorf("expected host-aux LastReadiness, got %+v", last)
	}
}

func TestRepoStateMonitor_Start(t *testing.T) {
	monitor := NewRepoStateMonitor(nil, "host-1", 5*time.Minute, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled immediately
	monitor.Start(ctx)
}

func TestDiscoverSubRepositories_GitModules(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "sub-mod")
	_ = os.MkdirAll(subDir, 0755)

	gitmodulesContent := `[submodule "sub-mod"]
	path = sub-mod
	url = https://example.com/repo.git
`
	_ = os.WriteFile(filepath.Join(tmpDir, ".gitmodules"), []byte(gitmodulesContent), 0644)

	discovered := DiscoverSubRepositories(tmpDir)
	found := false
	for _, p := range discovered {
		if p == "sub-mod" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected sub-mod discovered from .gitmodules, got %v", discovered)
	}
}

func TestInspectSingleGitDirectory_Gitfile(t *testing.T) {
	tmpDir := t.TempDir()
	actualGitDir := filepath.Join(tmpDir, "actual-git")
	_ = os.MkdirAll(actualGitDir, 0755)

	worktreeDir := filepath.Join(tmpDir, "worktree")
	_ = os.MkdirAll(worktreeDir, 0755)

	gitfile := filepath.Join(worktreeDir, ".git")
	_ = os.WriteFile(gitfile, []byte("gitdir: "+actualGitDir+"\n"), 0644)

	// Create index.lock in actualGitDir
	lockFile := filepath.Join(actualGitDir, "index.lock")
	_ = os.WriteFile(lockFile, []byte("lock"), 0644)

	res := inspectSingleGitDirectory(context.Background(), worktreeDir)
	if res.IsClean || !res.HasActiveGitLock {
		t.Fatalf("expected active git lock detected via gitfile redirection")
	}
}
