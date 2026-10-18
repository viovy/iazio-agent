//go:build !windows

package supervisor

import (
	"os/exec"
	"syscall"
)

func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func postStartProc(cmd *exec.Cmd) {}

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
	err := syscall.Kill(-pid, s)
	if err != nil {
		_ = syscall.Kill(pid, s)
	}
	return err
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if syscall.Kill(-pid, 0) == nil {
		return true
	}
	return syscall.Kill(pid, 0) == nil
}
