package supervisor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMultiSignalExecutionDetector_InMemorySignal(t *testing.T) {
	sup := New(time.Now)
	gate := NewHostExecutionAndUpdateGate()

	detector := NewMultiSignalExecutionDetector(sup, gate, func() ([]RepoConfigItem, error) {
		return []RepoConfigItem{{Path: "/repo1", Alias: "r1"}}, nil
	}, nil)
	detector.SetProcessScanner(func(repoPaths []string) ([]ActiveHarnessSignal, error) {
		return nil, nil
	})

	ctx := context.Background()

	// 1. Initial state: idle
	isExec, sigs := detector.DetectActiveExecutions(ctx)
	if isExec || len(sigs) > 0 {
		t.Fatalf("expected idle, got isExec=%t sigs=%v", isExec, sigs)
	}

	// 2. Gate lease active
	rel, err := gate.AcquireExecutionLease(ctx, "job-inmem", "/repo1")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	isExec, sigs = detector.DetectActiveExecutions(ctx)
	if !isExec || len(sigs) == 0 {
		t.Fatalf("expected active execution detected via gate lease")
	}
	if sigs[0].Source != "in_memory" || sigs[0].JobID != "job-inmem" {
		t.Errorf("unexpected signal: %+v", sigs[0])
	}

	rel()
	isExec, _ = detector.DetectActiveExecutions(ctx)
	if isExec {
		t.Fatalf("expected idle after releasing lease")
	}
}

func TestMultiSignalExecutionDetector_WorkspaceSentinel(t *testing.T) {
	tempDir := t.TempDir()
	autopilotDir := filepath.Join(tempDir, ".autopilot")
	if err := os.MkdirAll(autopilotDir, 0755); err != nil {
		t.Fatalf("mkdir .autopilot: %v", err)
	}

	detector := NewMultiSignalExecutionDetector(nil, nil, func() ([]RepoConfigItem, error) {
		return []RepoConfigItem{{Path: tempDir, Alias: "temp-repo"}}, nil
	}, nil)
	detector.SetProcessScanner(func(repoPaths []string) ([]ActiveHarnessSignal, error) {
		return nil, nil
	})

	ctx := context.Background()

	// 1. Write sentinel with PID 99999 (mock dead)
	detector.SetIsAlive(func(pid int) bool {
		return pid == 12345
	})

	sentinelFile := filepath.Join(autopilotDir, "active_job.json")
	_ = os.WriteFile(sentinelFile, []byte(`{"job_id":"job-dead","pid":99999}`), 0644)

	isExec, sigs := detector.DetectActiveExecutions(ctx)
	if isExec {
		t.Fatalf("expected dead PID sentinel to be ignored, got sigs=%v", sigs)
	}

	// 2. Write sentinel with PID 12345 (mock alive)
	_ = os.WriteFile(sentinelFile, []byte(`{"job_id":"job-live","pid":12345}`), 0644)
	isExec, sigs = detector.DetectActiveExecutions(ctx)
	if !isExec || len(sigs) == 0 {
		t.Fatalf("expected live PID sentinel to be detected")
	}
	if sigs[0].Source != "sentinel" || sigs[0].JobID != "job-live" || sigs[0].PID != 12345 {
		t.Errorf("unexpected signal: %+v", sigs[0])
	}

	// 3. Remove sentinel
	_ = os.Remove(sentinelFile)
	isExec, _ = detector.DetectActiveExecutions(ctx)
	if isExec {
		t.Fatalf("expected idle after removing sentinel")
	}
}

func TestMultiSignalExecutionDetector_ProcessTableAndControlPlane(t *testing.T) {
	detector := NewMultiSignalExecutionDetector(nil, nil, func() ([]RepoConfigItem, error) {
		return []RepoConfigItem{{Path: "/path/to/fleet-workspace", Alias: "fleet-workspace"}}, nil
	}, func(ctx context.Context) ([]ActiveHarnessSignal, error) {
		return []ActiveHarnessSignal{
			{
				Source:  "control_plane",
				JobID:   "job-cp-1",
				Details: "status: running",
			},
		}, nil
	})

	detector.SetProcessScanner(func(repoPaths []string) ([]ActiveHarnessSignal, error) {
		return []ActiveHarnessSignal{
			{
				Source:   "process_table",
				RepoPath: repoPaths[0],
				PID:      4567,
				Command:  fmt.Sprintf("iazio-harness run --job-id job-ps-1 --worktree-path %s", repoPaths[0]),
				Details:  "live process",
			},
		}, nil
	})

	ctx := context.Background()
	isExec, sigs := detector.DetectActiveExecutions(ctx)
	if !isExec || len(sigs) != 2 {
		t.Fatalf("expected 2 signals (process_table + control_plane), got: %v", sigs)
	}

	sources := make(map[string]bool)
	for _, s := range sigs {
		sources[s.Source] = true
	}
	if !sources["process_table"] || !sources["control_plane"] {
		t.Errorf("missing expected signal sources: %v", sources)
	}
}

func TestDefaultOSProcessScanner(t *testing.T) {
	sigs, err := DefaultOSProcessScanner([]string{"/nonexistent/repo/path"})
	if err != nil {
		t.Fatalf("DefaultOSProcessScanner failed: %v", err)
	}
	// On this host running the test, /nonexistent/repo/path should have 0 matches
	if len(sigs) != 0 {
		t.Errorf("expected 0 signals for nonexistent path, got %d", len(sigs))
	}
}
