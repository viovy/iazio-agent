package supervisor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSpawnOnce(t *testing.T) {
	s := New(nil)
	if err := s.TrySpawn("/repos/leaf-01"); err != nil {
		t.Fatal(err)
	}
	if err := s.TrySpawn("/repos/leaf-01"); err == nil {
		t.Fatal("second spawn")
	}
	args := SpawnArgs("job-1", "https://example.test", "/repos/leaf-01", "/repos/docs-hub")
	for _, bad := range args {
		if bad == "--token" {
			t.Fatal("token flag")
		}
	}
}

func TestSweepSkipsPermission(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job-1.log")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	err := SweepSpool(dir, time.Now(), nil, func(string) error { return os.ErrPermission })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestDrain(t *testing.T) {
	if d := DecideDrain(false, DrainReject); d.Update || d.Cancel || d.Presence != "ONLINE" {
		t.Fatal(d)
	}
	if d := DecideDrain(false, DrainAbort); !d.Cancel || !d.Update || d.Reason != ReasonDrainTimeout || d.Presence != "ONLINE" {
		t.Fatal(d)
	}
	if d := DecideDrain(false, ""); d.Update || d.Cancel || d.Presence != "ONLINE" {
		t.Fatal(d)
	}
	if HostKind("") != "permanent" || HostKind("ephemeral") != "ephemeral" {
		t.Fatal("kind")
	}
}

type recordingRunner struct {
	argv []string
}

func (r *recordingRunner) Start(argv []string) (int, error) {
	r.argv = append([]string(nil), argv...)
	return 4, nil
}

func (r *recordingRunner) Signal(int, string) error { return nil }
func (r *recordingRunner) Wait(int) (int, error)     { return 0, nil }

func TestSpawnArgvOmitsToken(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	s := New(func() time.Time { return start })
	runner := &recordingRunner{}
	const token = "secret-token-value"
	if err := s.Spawn(runner, "job-1", "https://example.test", "/repos/leaf-01", "/repos/docs-hub", token); err != nil {
		t.Fatal(err)
	}
	if s.Lock("/repos/leaf-01") != LockRunning {
		t.Fatal(s.Lock("/repos/leaf-01"))
	}
	if err := s.TrySpawn("/repos/leaf-01"); err == nil {
		t.Fatal("second spawn")
	}
	want := SpawnArgs("job-1", "https://example.test", "/repos/leaf-01", "/repos/docs-hub")
	if len(runner.argv) != len(want) {
		t.Fatalf("%v", runner.argv)
	}
	for i := range want {
		if runner.argv[i] != want[i] || runner.argv[i] == token || runner.argv[i] == "--token" {
			t.Fatalf("%v", runner.argv)
		}
	}
	if !s.Deadline("/repos/leaf-01").Equal(start.Add(DefaultJobDuration)) {
		t.Fatal(s.Deadline("/repos/leaf-01"))
	}
}

func TestCoolingOff(t *testing.T) {
	cur := time.Unix(1_700_000_000, 0)
	s := New(func() time.Time { return cur })
	if err := s.TrySpawn("/repos/leaf-01"); err != nil {
		t.Fatal(err)
	}
	s.BeginCooling("/repos/leaf-01")
	cur = cur.Add(9 * time.Second)
	s.Promote()
	if s.Lock("/repos/leaf-01") != LockCooling {
		t.Fatal(s.Lock("/repos/leaf-01"))
	}
	cur = cur.Add(time.Second)
	s.Promote()
	if s.Lock("/repos/leaf-01") != LockIdle {
		t.Fatal(s.Lock("/repos/leaf-01"))
	}
}

func TestGracefulStop(t *testing.T) {
	var sigs []string
	var slept time.Duration
	GracefulStop(
		func(_ int, sig string) error {
			sigs = append(sigs, sig)
			return nil
		},
		func(int) bool { return true },
		func(d time.Duration) { slept = d },
		7,
	)
	if slept != TermGrace || len(sigs) != 2 || sigs[0] != "TERM" || sigs[1] != "KILL" {
		t.Fatalf("slept %s sigs %v", slept, sigs)
	}
}

func TestReconcileOrphans(t *testing.T) {
	var pruned []string
	res := Reconcile(
		[]Proc{
			{PID: 1, Name: "iazio-harness", Cwd: "/repos/leaf-01"},
			{PID: 2, Name: "agy", Cwd: "/repos/leaf-01"},
			{PID: 3, Name: "agent", Cwd: "/repos/leaf-01"},
			{PID: 4, Name: "opencode", Cwd: "/repos/leaf-01"},
			{PID: 5, Name: "iazio-agent", Cwd: "/repos/leaf-02"},
			{PID: 6, Name: "iazio-harness", Cwd: "/repos/other"},
		},
		[]Lease{
			{JobID: "job-a", Worktree: "/repos/leaf-01"},
			{JobID: "job-b", Worktree: "/repos/leaf-02"},
		},
		map[string]string{"/repos/leaf-01": " M file"},
		func(int, string) error { return nil },
		func(path string) error {
			pruned = append(pruned, path)
			return nil
		},
		func(string) error { return nil },
	)
	if len(res.Terminated) != 4 || len(pruned) != 1 || filepath.Clean(pruned[0]) != filepath.Clean("/repos/leaf-01") {
		t.Fatalf("%+v prune %v", res, pruned)
	}
	if !res.PauseQueue || res.HaltCode != "HALTED_DIRTY" || res.Healing != 0 {
		t.Fatal(res)
	}
	if res.Locks[cleanPath("/repos/leaf-01")] != LockIdle || len(res.Leases) != 2 || res.Leases[0].Reason != ReasonHostRestart || res.Leases[0].Healing != 0 {
		t.Fatal(res)
	}
}

func TestDiscardLockFailure(t *testing.T) {
	gitCalls := 0
	err := DiscardCheckout(true, true, func(d time.Duration) error {
		if d != HubLockTimeout {
			t.Fatalf("timeout %s", d)
		}
		return errors.New("locked")
	}, func(...string) error {
		gitCalls++
		return nil
	})
	if err == nil || gitCalls != 0 {
		t.Fatalf("err %v calls %d", err, gitCalls)
	}
	var got []string
	err = DiscardCheckout(true, false, nil, func(args ...string) error {
		got = append(got, args[0]+" "+args[1])
		return nil
	})
	if err != nil || len(got) != 2 || got[0] != "reset --hard" || got[1] != "clean -fd" {
		t.Fatalf("err %v got %v", err, got)
	}
}

func TestFetchDoesNotHalt(t *testing.T) {
	s := New(nil)
	s.RecordFetch(errors.New("fetch down"))
	if s.Halted() || len(s.FetchErrors()) != 1 {
		t.Fatal(s.FetchErrors())
	}
}

func TestProcessHostIDOnce(t *testing.T) {
	first := ProcessHostID("host-a", "id-1")
	second := ProcessHostID("host-b", "id-2")
	if first != "host-a-id-1" || second != first {
		t.Fatalf("first %s second %s", first, second)
	}
}

func TestOSRunnerSmoke(t *testing.T) {
	r := NewOSRunner()
	pid, err := r.Start([]string{"echo", "hello"})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if pid <= 0 {
		t.Fatalf("invalid pid: %d", pid)
	}
	code, err := r.Wait(pid)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if code != 0 {
		t.Fatalf("unexpected exit code: %d", code)
	}

	// Untracked PID returns -1 and error
	missingCode, missingErr := r.Wait(999999)
	if missingCode != -1 || missingErr == nil {
		t.Fatalf("expected -1 and error for missing pid, got %d, %v", missingCode, missingErr)
	}
}

func TestResolveBinaryAndAugmentedEnv(t *testing.T) {
	// Abs path is preserved
	abs := "/usr/bin/echo"
	if got := ResolveBinary(abs); got != abs {
		t.Fatalf("ResolveBinary(%q) = %q, want %q", abs, got, abs)
	}

	// AugmentedEnv sets PATH with user dirs
	env := AugmentedEnv([]string{"FOO=BAR"})
	hasPath := false
	for _, kv := range env {
		if len(kv) >= 5 && kv[:5] == "PATH=" {
			hasPath = true
			break
		}
	}
	if !hasPath {
		t.Fatalf("AugmentedEnv missing PATH: %v", env)
	}
}

func TestSupPID(t *testing.T) {
	s := New(nil)
	runner := &recordingRunner{}
	if err := s.Spawn(runner, "job-1", "https://api.test", "/repos/work", "/repos/hub", ""); err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}
	if pid := s.PID("/repos/work"); pid != 4 {
		t.Fatalf("expected PID 4, got %d", pid)
	}
}

func TestTickReapsDeadProcess(t *testing.T) {
	curr := time.Unix(1_700_000_000, 0)
	s := New(func() time.Time { return curr })
	s.alive = func(pid int) bool {
		return pid != 999
	}
	s.mu.Lock()
	s.locks["/repos/dead"] = LockRunning
	s.pids["/repos/dead"] = 999
	s.deadline["/repos/dead"] = curr.Add(1 * time.Hour)
	s.mu.Unlock()

	s.Tick()

	if lock := s.Lock("/repos/dead"); lock != LockCooling {
		t.Fatalf("expected lock to move to LockCooling after dead process reaped, got %s", lock)
	}

	curr = curr.Add(CoolingOff + time.Second)
	s.Promote()

	if lock := s.Lock("/repos/dead"); lock != LockIdle {
		t.Fatalf("expected lock to move to LockIdle after cooling off, got %s", lock)
	}
}

func TestReconcileHostCheckouts(t *testing.T) {
	var discarded []string
	var resumed []string
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	repos := []RepoState{
		{
			WorktreePath:   "/work/discard-me",
			Queue:          "PAUSED",
			Reason:         "HALTED_DIRTY",
			DiscardPending: true,
		},
		{
			WorktreePath:   "/work/clean-auto-resume",
			Queue:          "PAUSED",
			Reason:         "HALTED_DIRTY",
			DiscardPending: false,
		},
		{
			WorktreePath:   "/work/still-dirty",
			Queue:          "PAUSED",
			Reason:         "HALTED_DIRTY",
			DiscardPending: false,
		},
		{
			WorktreePath:   "/work/busy-running",
			Queue:          "PAUSED",
			Reason:         "HALTED_DIRTY",
			DiscardPending: true,
		},
		{
			WorktreePath:   "/work/disk-recovered",
			Queue:          "OPEN",
			Reason:         "HALTED_DISK",
			DiscardPending: false,
		},
		{
			WorktreePath:   "/work/disk-still-low",
			Queue:          "OPEN",
			Reason:         "HALTED_DISK",
			DiscardPending: false,
		},
	}

	isLockIdle := func(w string) bool {
		return w != "/work/busy-running"
	}
	discard := func(w string) error {
		discarded = append(discarded, w)
		return nil
	}
	isClean := func(w string) (bool, error) {
		if w == "/work/clean-auto-resume" {
			return true, nil
		}
		return false, nil
	}
	resume := func(w string) error {
		resumed = append(resumed, w)
		return nil
	}
	checkDisk := func(w string) (bool, error) {
		return w == "/work/disk-recovered", nil
	}

	ReconcileHostCheckouts(repos, isLockIdle, discard, isClean, resume, stdout, stderr, checkDisk)

	if len(discarded) != 1 || discarded[0] != "/work/discard-me" {
		t.Fatalf("expected discard on /work/discard-me, got: %v", discarded)
	}
	if len(resumed) != 3 || resumed[0] != "/work/discard-me" || resumed[1] != "/work/clean-auto-resume" || resumed[2] != "/work/disk-recovered" {
		t.Fatalf("expected resume on discard-me, clean-auto-resume, and disk-recovered, got: %v", resumed)
	}
	if !strings.Contains(stdout.String(), "discarding checkout for /work/discard-me") {
		t.Fatalf("missing discard message in stdout: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "worktree /work/clean-auto-resume is clean; resuming paused queue") {
		t.Fatalf("missing clean resume message in stdout: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "worktree /work/disk-recovered has sufficient disk space; resuming halted queue") {
		t.Fatalf("missing disk recovery message in stdout: %s", stdout.String())
	}
}

func TestReconcileHostCheckouts_Errors(t *testing.T) {
	// 1. Nil validation returns immediately without panic
	ReconcileHostCheckouts(nil, nil, nil, nil, nil, nil, nil)
	ReconcileHostCheckouts(nil, func(string) bool { return true }, nil, nil, nil, nil, nil)

	// 2. Error branches
	repos := []RepoState{
		{WorktreePath: ""}, // empty skipped
		{WorktreePath: "/work/discard-err", DiscardPending: true},
		{WorktreePath: "/work/clean-err", Queue: "PAUSED", Reason: "HALTED_DIRTY"},
		{WorktreePath: "/work/clean-resume-err", Queue: "PAUSED", Reason: "HALTED_DIRTY"},
		{WorktreePath: "/work/disk-err", Reason: "HALTED_DISK"},
		{WorktreePath: "/work/disk-resume-err", Reason: "HALTED_DISK"},
	}

	var stderr bytes.Buffer
	isLockIdle := func(string) bool { return true }
	discardErr := func(string) error { return errors.New("discard failed") }
	isCleanErr := func(w string) (bool, error) {
		if w == "/work/clean-err" {
			return false, errors.New("git status failed")
		}
		return true, nil
	}
	checkDiskErr := func(w string) (bool, error) {
		if w == "/work/disk-err" {
			return false, errors.New("statfs failed")
		}
		return true, nil
	}
	resumeErr := func(w string) error {
		if strings.Contains(w, "resume-err") {
			return errors.New("resume failed")
		}
		return nil
	}

	ReconcileHostCheckouts(repos, isLockIdle, discardErr, isCleanErr, resumeErr, nil, &stderr, checkDiskErr)

	errText := stderr.String()
	if !strings.Contains(errText, "discard checkout error") ||
		!strings.Contains(errText, "check clean status error") ||
		!strings.Contains(errText, "check disk status error") ||
		!strings.Contains(errText, "resume repo error") {
		t.Fatalf("missing expected error messages in stderr: %s", errText)
	}
}


func TestReconcileStrandedCheckouts(t *testing.T) {
	repos := []RepoState{
		{
			WorktreePath: "/repos/running-ok",
			Lock:         "RUNNING_HARNESS",
			RunningJobID: "job-active",
		},
		{
			WorktreePath: "/repos/stranded-job",
			Lock:         "RUNNING_HARNESS",
			RunningJobID: "job-dead",
		},
		{
			WorktreePath: "/repos/idle-normal",
			Lock:         "IDLE",
		},
		{
			WorktreePath: "/repos/idle-stale-job",
			Lock:         "IDLE",
			RunningJobID: "job-stale",
		},
	}
	var abandoned []string
	var remediated []string
	isLockActive := func(w string) bool {
		return w == "/repos/running-ok"
	}
	abandonJob := func(ctx context.Context, jobID, reason string) error {
		abandoned = append(abandoned, jobID+":"+reason)
		return nil
	}
	remediateRepo := func(ctx context.Context, hostID, worktree string) error {
		remediated = append(remediated, hostID+":"+worktree)
		return nil
	}

	ReconcileStrandedCheckouts(context.Background(), "mac-mini", repos, isLockActive, abandonJob, remediateRepo, nil, nil)

	if len(abandoned) != 1 || abandoned[0] != "job-dead:process_disappeared_on_host" {
		t.Fatalf("expected job-dead abandoned, got %v", abandoned)
	}
	if len(remediated) != 1 || remediated[0] != "mac-mini:/repos/stranded-job" {
		t.Fatalf("expected stranded-job remediated, got %v", remediated)
	}
}

func TestRunningJobs(t *testing.T) {
	s := New(nil)
	_ = s.TrySpawn("/repos/app1")
	s.mu.Lock()
	s.pids["/repos/app1"] = 1234
	s.jobIDs["/repos/app1"] = "job-42"
	s.mu.Unlock()

	running := s.RunningJobs()
	if len(running) != 1 {
		t.Fatalf("expected 1 running job, got %d", len(running))
	}
	if running[0].JobID != "job-42" || running[0].WorktreePath != "/repos/app1" || running[0].PID != 1234 {
		t.Fatalf("unexpected running job: %+v", running[0])
	}
	if s.JobID("/repos/app1") != "job-42" {
		t.Fatalf("expected job-42, got %s", s.JobID("/repos/app1"))
	}
}

func TestAutoHealSupersededWorktree(t *testing.T) {
	// 1. Refuse when not idle
	if err := AutoHealSupersededWorktree(false, "chore/test", nil); err == nil {
		t.Fatalf("expected error when not idle")
	}

	// 2. Refuse when runner nil
	if err := AutoHealSupersededWorktree(true, "chore/test", nil); err == nil {
		t.Fatalf("expected error when runner is nil")
	}

	// 3. No-op on main
	if err := AutoHealSupersededWorktree(true, "main", func(args ...string) error {
		t.Fatalf("git should not be called for main branch")
		return nil
	}); err != nil {
		t.Fatalf("unexpected error on main branch: %v", err)
	}

	// 4. Executed sequence on feature branch
	var gitCalls [][]string
	gitRunner := func(args ...string) error {
		gitCalls = append(gitCalls, args)
		return nil
	}
	err := AutoHealSupersededWorktree(true, "chore/iazio-0098-duplicate", gitRunner)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := [][]string{
		{"reset", "--hard"},
		{"clean", "-fd"},
		{"checkout", "main"},
		{"pull", "--ff-only"},
		{"branch", "-D", "chore/iazio-0098-duplicate"},
	}
	if len(gitCalls) != len(expected) {
		t.Fatalf("expected %d git calls, got %d: %v", len(expected), len(gitCalls), gitCalls)
	}
	for i, exp := range expected {
		if strings.Join(gitCalls[i], " ") != strings.Join(exp, " ") {
			t.Fatalf("call %d: expected %v, got %v", i, exp, gitCalls[i])
		}
	}
}

func TestDetectAutoHealableSIR_And_Archive(t *testing.T) {
	tmpDir := t.TempDir()
	reportsDir := filepath.Join(tmpDir, "tmp", "reports")
	if err := os.MkdirAll(reportsDir, 0755); err != nil {
		t.Fatalf("failed to create reports dir: %v", err)
	}

	sirJSON := `{
  "schema_version": 1,
  "id": "SIR-1051-12345",
  "job_id": "job-1051",
  "story_id": "STORY-APP-0098",
  "category": "SUPERSEDED_BRANCH",
  "auto_healable": true
}`
	sirPath := filepath.Join(reportsDir, "intervention-story-app-0098-job-1051.json")
	if err := os.WriteFile(sirPath, []byte(sirJSON), 0644); err != nil {
		t.Fatalf("failed to write test SIR: %v", err)
	}

	branchMock := func(w string) (string, error) {
		return "chore/iazio-0098-feature", nil
	}

	healable, sirID, branch, err := DetectAutoHealableSIR(tmpDir, branchMock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !healable {
		t.Fatalf("expected healable=true")
	}
	if sirID != "SIR-1051-12345" {
		t.Fatalf("expected SIR-1051-12345, got %s", sirID)
	}
	if branch != "chore/iazio-0098-feature" {
		t.Fatalf("expected chore/iazio-0098-feature, got %s", branch)
	}

	// Test Archive
	if err := ArchiveSIR(tmpDir, sirID); err != nil {
		t.Fatalf("failed to archive SIR: %v", err)
	}
	if _, err := os.Stat(sirPath + ".healed"); err != nil {
		t.Fatalf("expected archived file at %s.healed: %v", sirPath, err)
	}

	// Subsequent check should find no unhealed SIR
	healableAfter, _, _, _ := DetectAutoHealableSIR(tmpDir, branchMock)
	if healableAfter {
		t.Fatalf("expected healableAfter=false after archiving")
	}
}

func TestReconcileHostCheckouts_AutoHealSuperseded(t *testing.T) {
	tmpDir := t.TempDir()
	reportsDir := filepath.Join(tmpDir, "tmp", "reports")
	_ = os.MkdirAll(reportsDir, 0755)

	sirJSON := `{
  "id": "SIR-999",
  "category": "SUPERSEDED_BRANCH",
  "auto_healable": true
}`
	_ = os.WriteFile(filepath.Join(reportsDir, "intervention-test.json"), []byte(sirJSON), 0644)

	repos := []RepoState{
		{
			WorktreePath:   tmpDir,
			Queue:          "PAUSED",
			Reason:         "HALTED_DIRTY",
			DiscardPending: false,
		},
	}

	isLockIdle := func(w string) bool { return true }
	isClean := func(w string) (bool, error) { return false, nil }
	var resumed []string
	resume := func(w string) error {
		resumed = append(resumed, w)
		return nil
	}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	ReconcileHostCheckouts(repos, isLockIdle, nil, isClean, resume, stdout, stderr)

	// Since branch resolution in the real test worktree without git repo returns empty or main,
	// let's verify it handled cleanly without panic or errors.
	if len(stderr.String()) != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

type mockRunner struct {
	lastArgv []string
}

func (m *mockRunner) Start(argv []string) (int, error) {
	m.lastArgv = argv
	return 12345, nil
}

func (m *mockRunner) Signal(pid int, sig string) error {
	return nil
}

func (m *mockRunner) Wait(pid int) (int, error) {
	return 0, nil
}

func TestDockerRunner_Dispatch(t *testing.T) {
	mock := &mockRunner{}
	// 1. Without image, delegates directly to host
	drHost := NewDockerRunner(DockerConfig{}, mock)
	pid, err := drHost.Start([]string{"autopilot", "ase", "--story", "STORY-123"})
	if err != nil || pid != 12345 {
		t.Fatalf("expected host delegation, got pid=%d err=%v", pid, err)
	}
	if len(mock.lastArgv) != 4 || mock.lastArgv[0] != "autopilot" {
		t.Fatalf("unexpected host argv: %v", mock.lastArgv)
	}

	// 2. With image, constructs containerized invocation
	cfg := DockerConfig{
		Image:         "viovy/autopilot:latest",
		WorktreeMount: "/repos/my-project",
		EnvVars:       map[string]string{"ASE_MODE": "sandbox"},
	}
	drDocker := NewDockerRunner(cfg, mock)
	pidDocker, err := drDocker.Start([]string{"autopilot", "ase"})
	if err != nil || pidDocker != 12345 {
		t.Fatalf("expected docker invocation, got pid=%d err=%v", pidDocker, err)
	}
	if len(mock.lastArgv) < 6 || mock.lastArgv[0] != "docker" || mock.lastArgv[1] != "run" {
		t.Fatalf("expected docker run prefix, got: %v", mock.lastArgv)
	}
	joined := strings.Join(mock.lastArgv, " ")
	if !strings.Contains(joined, "viovy/autopilot:latest") || !strings.Contains(joined, "/repos/my-project:/workspace") {
		t.Fatalf("missing expected flags in docker argv: %s", joined)
	}
}

func TestSupervisor_ActiveJobs_HasRunningHarness_Reason(t *testing.T) {
	s := New(nil)

	// Initially idle
	if s.HasRunningHarness() {
		t.Fatal("expected no running harness initially")
	}

	_ = s.TrySpawn("/repos/app1")
	s.mu.Lock()
	s.pids["/repos/app1"] = 4321
	s.reasons["/repos/app1"] = "completed_ok"
	s.mu.Unlock()

	if !s.HasRunningHarness() {
		t.Fatal("expected running harness")
	}

	active := s.ActiveJobs()
	if len(active) != 1 || active["/repos/app1"] != 4321 {
		t.Fatalf("unexpected active jobs: %+v", active)
	}

	if s.Reason("/repos/app1") != "completed_ok" {
		t.Fatalf("expected completed_ok, got %s", s.Reason("/repos/app1"))
	}

	// MarkIdle
	s.MarkIdle("/repos/app1")
	if s.HasRunningHarness() {
		t.Fatal("expected no running harness after MarkIdle")
	}

	// Loop detector checks
	res := s.CheckLoop("edit", "some output")
	if res.IsLoop {
		t.Fatal("unexpected loop detected on first action")
	}
	s.ResetLoopDetector()
}

func TestResolveBinaryAndRunnerSignal(t *testing.T) {
	// 1. Resolve known binary
	resolved := ResolveBinary("git")
	if resolved == "" {
		t.Fatal("expected git to be resolved")
	}

	// 2. Resolve absolute path
	if ResolveBinary("/bin/echo") != "/bin/echo" {
		t.Fatal("expected absolute path unchanged")
	}

	// 3. Resolve from fake home .iazio/bin
	fakeHome := t.TempDir()
	iazioBin := filepath.Join(fakeHome, ".iazio", "bin")
	_ = os.MkdirAll(iazioBin, 0755)
	customTool := filepath.Join(iazioBin, "custom-iazio-tool")
	_ = os.WriteFile(customTool, []byte("#!/bin/sh\n"), 0755)

	t.Setenv("HOME", fakeHome)
	if got := ResolveBinary("custom-iazio-tool"); got != customTool {
		t.Fatalf("expected custom tool resolved from .iazio/bin, got %s", got)
	}

	// 4. Fallback when not found
	if got := ResolveBinary("non-existent-binary-12345"); got != "non-existent-binary-12345" {
		t.Fatalf("expected fallback to name, got %s", got)
	}

	// 5. Runner execution and signal
	runner := NewOSRunner()
	pid, err := runner.Start([]string{"sleep", "5"})
	if err == nil && pid > 0 {
		_ = runner.Signal(pid, "TERM")
		_, _ = runner.Wait(pid)
	}

	// 6. DockerRunner Signal and Wait
	mockHost := &mockRunner{}
	dr := &DockerRunner{
		host: mockHost,
		Config: DockerConfig{
			Image: "test-image:latest",
		},
	}
	_ = dr.Signal(1234, "TERM")
	code, err := dr.Wait(1234)
	if err != nil || code != 0 {
		t.Fatalf("unexpected DockerRunner wait: code=%d err=%v", code, err)
	}

	// 7. runner_unix functions
	postStartProc(nil)
	if !isProcessAlive(os.Getpid()) {
		t.Fatal("expected current pid to be alive")
	}
	if isProcessAlive(-1) {
		t.Fatal("expected -1 pid not alive")
	}
	if isProcessAlive(99999999) {
		t.Fatal("expected non-existent pid not alive")
	}
	if err := signalPID(0, "TERM"); err != nil {
		t.Fatalf("expected nil for pid <= 0: %v", err)
	}
	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err == nil {
		_ = signalPID(cmd.Process.Pid, "UNKNOWN_SIG")
		_ = signalPID(cmd.Process.Pid, "KILL")
		_ = cmd.Wait()
	}
}






