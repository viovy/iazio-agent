//go:build !windows

package supervisor

import (
	"os/exec"
	"syscall"
)

func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func signalPID(pid int, sig string) error {
	if pid <= 0 {
		return nil
	}
	var s syscall.Signal
	switch sig {
	case "TERM":
		s = syscall.SIGTERM
	case "KILL":
		s = syscall.SIGKILL
	default:
		s = syscall.SIGTERM
	}
	return syscall.Kill(-pid, s)
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}
