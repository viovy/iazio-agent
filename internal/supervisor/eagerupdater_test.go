package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
)

func makeTestDirFile(t *testing.T, p, content string) error {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return nil
}

func makeTestExecutable(t *testing.T, p, content string) error {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return nil
}

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

	// Already StateUpToDate (no outdated tools, not restart pending) -> returns (report, true) immediately
	u.SetRestartPending(false)
	u.InspectOutdatedTools = func(context.Context) ([]OutdatedToolInfo, error) {
		return nil, nil
	}
	upToDateRep, ok := u.EnsureUpToDateBeforeClaim(ctx)
	if !ok || upToDateRep.EagerUpdateState != StateUpToDate {
		t.Fatalf("expected EnsureUpToDateBeforeClaim on clean up-to-date host to return true, got ok=%v state=%s", ok, upToDateRep.EagerUpdateState)
	}

	// StateDeferredHarnessActive on a multi-repo host -> returns (report, true) so other idle worktrees are not deadlocked
	relExec, err := gate.AcquireExecutionLease(ctx, "job-active-1", "/work/repo")
	if err != nil {
		t.Fatalf("AcquireExecutionLease failed: %v", err)
	}
	if gate.State() != GateStateExecutingJobs || gate.ActiveCount() != 1 || len(gate.ActiveExecutions()) != 1 {
		t.Fatalf("unexpected gate state during execution: state=%s count=%d", gate.State(), gate.ActiveCount())
	}
	_ = gate.WaitingCount()
	u.SetExecutionDetector(NewMultiSignalExecutionDetector(sup, gate, nil, nil))
	busyRep, ok := u.EnsureUpToDateBeforeClaim(ctx)
	if !ok || busyRep.EagerUpdateState != StateDeferredHarnessActive {
		t.Fatalf("expected EnsureUpToDateBeforeClaim to allow multi-repo claim when StateDeferredHarnessActive, got ok=%v state=%s", ok, busyRep.EagerUpdateState)
	}
	gate.ReleaseExecutionLease("job-active-1")
	gate.ReleaseExecutionLease("") // no-op
	relExec()
	if gate.State() != GateStateIdle {
		t.Fatalf("expected GateStateIdle after ReleaseExecutionLease, got %s", gate.State())
	}

	// Stale suite failure backoff is cleared when InspectOutdatedTools confirms 0 outdated tools
	u.recordSuiteFailure()
	recoveredRep := u.EvaluateReadiness(ctx)
	if recoveredRep.EagerUpdateState != StateUpToDate {
		t.Fatalf("expected EvaluateReadiness to clear stale suite backoff when 0 tools are outdated, got %s", recoveredRep.EagerUpdateState)
	}

	// When in StateBackoffAfterFailure (e.g., InspectOutdatedTools failed after suite failure), EnsureUpToDateBeforeClaim fails open
	u.recordSuiteFailure()
	u.InspectOutdatedTools = func(context.Context) ([]OutdatedToolInfo, error) {
		return nil, os.ErrNotExist
	}
	backoffRep, backoffOK := u.EnsureUpToDateBeforeClaim(ctx)
	if !backoffOK || backoffRep.EagerUpdateState != StateBackoffAfterFailure {
		t.Fatalf("expected EnsureUpToDateBeforeClaim to fail-open on StateBackoffAfterFailure, got ok=%v state=%s", backoffOK, backoffRep.EagerUpdateState)
	}

	// RepoStateMonitor helpers & InspectWorktreeGitState coverage
	rm.SetAgyCooldownDuration(5 * time.Second)
	rm.SetIsPIDAlive(func(int) bool { return false })
	rm.Wait()
	st := InspectWorktreeGitState(t.TempDir())
	if !st.IsClean {
		t.Fatalf("expected uninitialized temp dir to be reported clean by InspectWorktreeGitState, got %+v", st)
	}
}

func TestRepoMonitor_HelpersAndStoryArtifactChecks(t *testing.T) {
	ctx := context.Background()

	// 1. extractPorcelainV2Path
	if got := extractPorcelainV2Path("? untracked/file.txt"); got != "untracked/file.txt" {
		t.Fatalf("expected untracked/file.txt, got %q", got)
	}
	if got := extractPorcelainV2Path("! ignored/file.txt"); got != "ignored/file.txt" {
		t.Fatalf("expected ignored/file.txt, got %q", got)
	}
	if got := extractPorcelainV2Path("1 .M N... 100644 100644 100644 abc def path/with spaces.go"); got != "path/with spaces.go" {
		t.Fatalf("expected 'path/with spaces.go', got %q", got)
	}
	if got := extractPorcelainV2Path("1 .M short"); got != "" {
		t.Fatalf("expected empty for short porcelain v2 line, got %q", got)
	}
	if got := extractPorcelainV2Path("# branch.head main"); got != "" {
		t.Fatalf("expected empty for comment line, got %q", got)
	}

	// 2. IsEphemeralArtifactPath
	for _, ep := range []string{
		"",
		".DS_Store",
		"./Thumbs.db",
		".session_story_id",
		"cov_auth",
		"sub/cov_unit.out",
		"TestResults/Diagnostics/pr-closeout-proof-123.json",
		".gradle/caches/foo",
		"sub/.gradle/bar",
		".dart_tool/package_config.json",
		"app/.dart_tool/foo",
		"build/tmp/test",
		"mod/build/tmp/out",
		"regular/source.go",
	} {
		_ = IsEphemeralArtifactPath(ep)
	}

	// 3. isStoryMarkedDoneOnDisk & isStoryExplicitlyInProgressOnDisk
	repoDir := t.TempDir()
	if isStoryMarkedDoneOnDisk(repoDir, "") || isStoryExplicitlyInProgressOnDisk(repoDir, "") {
		t.Fatal("expected false for empty storyID")
	}
	storiesDir := repoDir + "/docs/stories"
	_ = makeTestDirFile(t, storiesDir+"/iazio-0196-test.story.md", "---\nid: STORY-IAZIO-0196\nstatus: DONE\n---\n")
	_ = makeTestDirFile(t, storiesDir+"/iazio-0197-test.story.md", "---\nid: \"STORY-IAZIO-0197\"\nstatus: \"IN_PROGRESS\"\n---\n")

	if !isStoryMarkedDoneOnDisk(repoDir, "STORY-IAZIO-0196") {
		t.Fatal("expected STORY-IAZIO-0196 to be marked DONE on disk")
	}
	if isStoryMarkedDoneOnDisk(repoDir, "STORY-IAZIO-0197") {
		t.Fatal("expected STORY-IAZIO-0197 (IN_PROGRESS) not to be marked DONE on disk")
	}
	if !isStoryExplicitlyInProgressOnDisk(repoDir, "STORY-IAZIO-0197") {
		t.Fatal("expected STORY-IAZIO-0197 to be explicitly IN_PROGRESS on disk")
	}
	if isStoryExplicitlyInProgressOnDisk(repoDir, "STORY-IAZIO-0196") {
		t.Fatal("expected STORY-IAZIO-0196 not to be IN_PROGRESS on disk")
	}

	// 4. cleanOrRestoreSingleArtifact on untracked file
	if cleanOrRestoreSingleArtifact(ctx, repoDir, "") {
		t.Fatal("expected false for empty relPath")
	}
	_ = makeTestDirFile(t, repoDir+"/.session_story_id", "STORY-IAZIO-0196\n")
	if !cleanOrRestoreSingleArtifact(ctx, repoDir, ".session_story_id") {
		t.Fatal("expected cleanOrRestoreSingleArtifact to remove untracked .session_story_id")
	}

	// 5. isCleanOnMainSynced
	cleanMain := RepoGitState{
		IsClean:         true,
		RootClean:       true,
		SubmodulesClean: true,
		Branch:          "main",
		SubRepos: map[string]SubRepoGitState{
			"sub1": {IsClean: true},
		},
	}
	if !isCleanOnMainSynced(cleanMain) {
		t.Fatal("expected cleanMain to be synced")
	}
	dirtySub := cleanMain
	dirtySub.SubRepos = map[string]SubRepoGitState{"sub1": {IsClean: false}}
	if isCleanOnMainSynced(dirtySub) {
		t.Fatal("expected dirtySub not to be synced")
	}
	featureBr := cleanMain
	featureBr.Branch = "feat/x"
	if isCleanOnMainSynced(featureBr) {
		t.Fatal("expected featureBr not to be synced")
	}
	aheadMain := cleanMain
	aheadMain.Ahead = 1
	if isCleanOnMainSynced(aheadMain) {
		t.Fatal("expected aheadMain not to be synced")
	}
	notClean := cleanMain
	notClean.IsClean = false
	if isCleanOnMainSynced(notClean) {
		t.Fatal("expected notClean not to be synced")
	}

	// 6. DefaultShellGitUpdate & DefaultAgyEscalation with fake binaries in PATH
	fakeBinDir := t.TempDir()
	_ = makeTestExecutable(t, fakeBinDir+"/autopilot", "#!/bin/sh\necho ok-git-update\n")
	_ = makeTestExecutable(t, fakeBinDir+"/iazio-harness", "#!/bin/sh\necho ok-agy-escalation\n")
	t.Setenv("PATH", fakeBinDir)

	out1, err1 := DefaultShellGitUpdate(ctx, repoDir)
	if err1 != nil || out1 != "ok-git-update" {
		t.Fatalf("DefaultShellGitUpdate unexpected out=%q err=%v", out1, err1)
	}
	out2, err2 := DefaultAgyEscalation(ctx, repoDir, "run git-update", "diag failure")
	if err2 != nil || out2 != "ok-agy-escalation" {
		t.Fatalf("DefaultAgyEscalation unexpected out=%q err=%v", out2, err2)
	}
}

func TestReconcile_StrandedAndSupersededHelpers(t *testing.T) {
	ctx := context.Background()

	// 1. AutoHealSupersededWorktree
	if err := AutoHealSupersededWorktree(false, "feat/old", nil); err == nil {
		t.Fatal("expected error when not idle")
	}
	if err := AutoHealSupersededWorktree(true, "feat/old", nil); err == nil {
		t.Fatal("expected error when git runner is nil")
	}
	if err := AutoHealSupersededWorktree(true, "main", func(...string) error { return nil }); err != nil {
		t.Fatalf("expected nil for main branch, got %v", err)
	}
	var gitCalls [][]string
	err := AutoHealSupersededWorktree(true, "feat/old", func(args ...string) error {
		gitCalls = append(gitCalls, args)
		return nil
	})
	if err != nil || len(gitCalls) < 4 {
		t.Fatalf("expected AutoHealSupersededWorktree to run git commands, err=%v calls=%v", err, gitCalls)
	}

	// 2. DetectAutoHealableSIR & ArchiveSIR
	wt := t.TempDir()
	if healable, _, _, _ := DetectAutoHealableSIR(wt); healable {
		t.Fatal("expected false when no reports dir")
	}
	sirPath := wt + "/tmp/reports/intervention-001.json"
	_ = makeTestDirFile(t, sirPath, `{"id":"SIR-001","story_id":"STORY-1","category":"SUPERSEDED_BRANCH","auto_healable":true}`)
	healable, sirID, branch, err := DetectAutoHealableSIR(wt, func(string) (string, error) {
		return "feat/superseded", nil
	})
	if err != nil || !healable || sirID != "SIR-001" || branch != "feat/superseded" {
		t.Fatalf("unexpected DetectAutoHealableSIR result: healable=%v sirID=%q branch=%q err=%v", healable, sirID, branch, err)
	}
	if err := ArchiveSIR(wt, "SIR-001"); err != nil {
		t.Fatalf("ArchiveSIR failed: %v", err)
	}

	// 3. ReconcileStrandedCheckouts
	var abandonedJob string
	var remediatedWT string
	ReconcileStrandedCheckouts(
		ctx,
		"host-1",
		[]RepoState{
			{WorktreePath: ""},
			{WorktreePath: "/work/active", Lock: "RUNNING_HARNESS", RunningJobID: "job-1"},
			{WorktreePath: "/work/stranded", Lock: "RUNNING_HARNESS", RunningJobID: "job-2"},
		},
		func(w string) bool { return w == "/work/active" },
		func(_ context.Context, jobID, _ string) error {
			abandonedJob = jobID
			return nil
		},
		func(_ context.Context, _, w string) error {
			remediatedWT = w
			return nil
		},
		nil,
		nil,
	)
	if abandonedJob != "job-2" || remediatedWT != "/work/stranded" {
		t.Fatalf("expected stranded job-2 on /work/stranded to be abandoned and remediated, got job=%q wt=%q", abandonedJob, remediatedWT)
	}
}


