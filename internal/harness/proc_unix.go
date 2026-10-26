//go:build !windows

package harness

import (
	"os/exec"
	"syscall"
)

func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		if err == syscall.ESRCH {
			return nil
		}
		if singleErr := syscall.Kill(cmd.Process.Pid, syscall.SIGKILL); singleErr != nil && singleErr != syscall.ESRCH {
			return singleErr
		}
	}
	return nil
}
