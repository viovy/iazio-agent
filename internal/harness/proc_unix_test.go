//go:build !windows
// +build !windows

package harness

import (
	"os/exec"
	"testing"
)

func TestProcGroup_Unix(t *testing.T) {
	cmd := exec.Command("sleep", "1")
	setProcGroup(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("expected Setpgid=true, got %+v", cmd.SysProcAttr)
	}

	// 1. killProcGroup on nil / non-started cmd is safe
	if err := killProcGroup(nil); err != nil {
		t.Fatalf("expected nil for killProcGroup(nil), got %v", err)
	}
	if err := killProcGroup(cmd); err != nil {
		t.Fatalf("expected nil for unstarted cmd, got %v", err)
	}

	// 2. killProcGroup on running process group kills it
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start sleep: %v", err)
	}
	if err := killProcGroup(cmd); err != nil {
		t.Fatalf("killProcGroup failed: %v", err)
	}
	_ = cmd.Wait()

	// 3. killProcGroup on already terminated process (ESRCH) is non-fatal
	if err := killProcGroup(cmd); err != nil {
		t.Fatalf("expected non-fatal return for terminated process (ESRCH), got %v", err)
	}
}
