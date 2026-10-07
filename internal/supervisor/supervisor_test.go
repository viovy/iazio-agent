package supervisor

import (
	"bytes"
	"errors"
	"os"
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
	if res.Locks[filepath.Clean("/repos/leaf-01")] != LockIdle || len(res.Leases) != 2 || res.Leases[0].Reason != ReasonHostRestart || res.Leases[0].Healing != 0 {
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


