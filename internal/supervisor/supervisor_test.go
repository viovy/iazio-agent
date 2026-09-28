package supervisor

import (
	"errors"
	"os"
	"path/filepath"
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
	if len(res.Terminated) != 4 || len(pruned) != 1 || pruned[0] != "/repos/leaf-01" {
		t.Fatalf("%+v prune %v", res, pruned)
	}
	if !res.PauseQueue || res.HaltCode != "HALTED_DIRTY" || res.Healing != 0 {
		t.Fatal(res)
	}
	if res.Locks["/repos/leaf-01"] != LockIdle || len(res.Leases) != 2 || res.Leases[0].Reason != ReasonHostRestart || res.Leases[0].Healing != 0 {
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

