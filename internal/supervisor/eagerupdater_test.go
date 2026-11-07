package supervisor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
)

func TestEagerToolUpdater_EvaluateReadiness_DriftDetectedAndRestartPending(t *testing.T) {
	ctx := context.Background()
	sup := New(time.Now)
	rm := NewRepoStateMonitor(sup, "host-test", time.Minute, func() ([]RepoConfigItem, error) {
		return nil, nil
	}, nil, nil, nil)

	u := NewEagerToolUpdater(sup, rm, "host-test", nil, nil)
	u.InspectOutdatedTools = func(context.Context) ([]OutdatedToolInfo, error) {
		return []OutdatedToolInfo{{ToolName: "autopilot", InstalledVersion: "0.0.1", TargetVersion: "0.0.2"}}, nil
	}

	// 1. Outdated tools detected while idle -> DRIFT_DETECTED
	rep := u.EvaluateReadiness(ctx)
	if rep.EagerUpdateState != StateDriftDetected {
		t.Fatalf("expected EagerUpdateState=%s when outdated tools present, got %s", StateDriftDetected, rep.EagerUpdateState)
	}
	if rep.Details != "tool drift detected; eager update required before claiming jobs" {
		t.Fatalf("unexpected details for DRIFT_DETECTED: %q", rep.Details)
	}

	// 2. Restart pending -> RESTART_PENDING
	u.SetRestartPending(true)
	if !u.IsRestartPending() {
		t.Fatal("expected IsRestartPending() to be true")
	}
	repRestart := u.EvaluateReadiness(ctx)
	if repRestart.EagerUpdateState != StateRestartPending {
		t.Fatalf("expected EagerUpdateState=%s when restart pending, got %s", StateRestartPending, repRestart.EagerUpdateState)
	}
	if repRestart.Details != "agent self-update applied; restart pending" {
		t.Fatalf("unexpected details for RESTART_PENDING: %q", repRestart.Details)
	}
}

func TestEagerToolUpdater_EnsureUpToDateBeforeClaim_UpdatesOutdatedToolsAndUnblocks(t *testing.T) {
	ctx := context.Background()
	sup := New(time.Now)
	rm := NewRepoStateMonitor(sup, "host-test", time.Minute, func() ([]RepoConfigItem, error) {
		return nil, nil
	}, nil, nil, nil)
	gate := NewHostExecutionAndUpdateGate()

	var mu sync.Mutex
	outdated := []string{"autopilot", "iazio-harness"}
	var gateStateDuringUpdate GateState
	var reportedStates []EagerUpdateState

	u := NewEagerToolUpdater(sup, rm, "host-test", nil, func(context.Context, controlplane.NotificationRecord) error {
		return nil
	})
	u.SetHostGate(gate)
	u.SetReadinessReporter(func(_ context.Context, _ string, r EagerUpdateReadinessReport) error {
		mu.Lock()
		reportedStates = append(reportedStates, r.EagerUpdateState)
		mu.Unlock()
		return nil
	})
	u.InspectOutdatedTools = func(context.Context) ([]OutdatedToolInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		out := make([]OutdatedToolInfo, 0, len(outdated))
		for _, tool := range outdated {
			out = append(out, OutdatedToolInfo{ToolName: tool, InstalledVersion: "0.0.1", TargetVersion: "0.0.2"})
		}
		return out, nil
	}
	u.CheckDrift = func(context.Context, []string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return len(outdated) > 0, nil
	}
	u.SetDetailedUpdateFunc(func(_ context.Context, _ []string, _ bool, _ ...string) (UpdateResult, error) {
		mu.Lock()
		defer mu.Unlock()
		gateStateDuringUpdate = gate.State()
		updated := append([]string(nil), outdated...)
		outdated = nil
		return UpdateResult{UpdatedTools: updated}, nil
	})

	after, ok := u.EnsureUpToDateBeforeClaim(ctx)
	if !ok {
		t.Fatalf("expected EnsureUpToDateBeforeClaim to succeed and unblock claim, got ok=false report=%+v", after)
	}
	if after.EagerUpdateState != StateUpToDate {
		t.Fatalf("expected post-update state %s, got %s", StateUpToDate, after.EagerUpdateState)
	}
	if gateStateDuringUpdate != GateStateUpdatingTools {
		t.Fatalf("expected hostGate to hold maintenance lease (%s) during tool update, got %s", GateStateUpdatingTools, gateStateDuringUpdate)
	}
	if gate.State() != GateStateIdle {
		t.Fatalf("expected hostGate to return to %s after update completes, got %s", GateStateIdle, gate.State())
	}
}

func TestEagerToolUpdater_EnsureUpToDateBeforeClaim_BlocksWhenAgentUpdatedOrBusy(t *testing.T) {
	ctx := context.Background()
	sup := New(time.Now)
	rm := NewRepoStateMonitor(sup, "host-test", time.Minute, func() ([]RepoConfigItem, error) {
		return nil, nil
	}, nil, nil, nil)
	gate := NewHostExecutionAndUpdateGate()

	u := NewEagerToolUpdater(sup, rm, "host-test", nil, nil)
	u.SetHostGate(gate)
	u.InspectOutdatedTools = func(context.Context) ([]OutdatedToolInfo, error) {
		return []OutdatedToolInfo{{ToolName: "iazio-agent", InstalledVersion: "0.0.1", TargetVersion: "0.0.2"}}, nil
	}
	u.CheckDrift = func(context.Context, []string) (bool, error) {
		return true, nil
	}
	u.SetDetailedUpdateFunc(func(_ context.Context, _ []string, _ bool, _ ...string) (UpdateResult, error) {
		return UpdateResult{
			UpdatedTools: []string{"iazio-agent"},
			AgentUpdated: true,
		}, nil
	})

	after, ok := u.EnsureUpToDateBeforeClaim(ctx)
	if ok {
		t.Fatalf("expected EnsureUpToDateBeforeClaim to return false when AgentUpdated=true requires restart, got ok=true")
	}
	if after.EagerUpdateState != StateRestartPending {
		t.Fatalf("expected state %s after agent self-update, got %s", StateRestartPending, after.EagerUpdateState)
	}
}
