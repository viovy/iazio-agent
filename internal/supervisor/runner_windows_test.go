//go:build windows

package supervisor

import (
	"os"
	"os/exec"
	"testing"
)

func TestWindowsProcessAlive(t *testing.T) {
	if !isProcessAlive(os.Getpid()) {
		t.Fatalf("expected current PID %d to be alive", os.Getpid())
	}
	if isProcessAlive(0) {
		t.Fatalf("expected PID 0 to be dead")
	}
	if isProcessAlive(-1) {
		t.Fatalf("expected PID -1 to be dead")
	}

	// Spawn a command that exits immediately
	cmd := exec.Command("cmd.exe", "/c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start cmd: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("failed to wait for cmd: %v", err)
	}
	if isProcessAlive(pid) {
		t.Fatalf("expected terminated process %d to not be alive", pid)
	}
}
