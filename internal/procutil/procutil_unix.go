//go:build !windows

package procutil

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

// HideWindow is a no-op on non-Windows platforms and returns cmd unchanged.
func HideWindow(cmd *exec.Cmd) *exec.Cmd {
	return cmd
}

// Command creates an *exec.Cmd.
func Command(name string, arg ...string) *exec.Cmd {
	return exec.Command(name, arg...)
}

// CommandContext creates a context-bound *exec.Cmd.
func CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, arg...)
}

// IsPIDAlive checks whether a process with the given PID is currently alive via signal 0.
func IsPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if pid == os.Getpid() {
		return true
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func isWindowHidden(cmd *exec.Cmd) bool {
	return cmd != nil
}
