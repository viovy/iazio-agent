package supervisor

import (
	"context"
	"errors"
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
	if len(notifications) != 1 || notifications[0].Metadata["event"] != "agent.tool.update_deferred" {
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
	if len(notifications) != 2 || notifications[1].Metadata["event"] != "agent.tool.update_deferred" {
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
		if n.Metadata["event"] == "agent.tool.update_started" {
			foundStarted = true
		}
		if n.Metadata["event"] == "agent.tool.update_succeeded" {
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
