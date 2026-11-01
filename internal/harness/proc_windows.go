//go:build windows

package harness

import (
	"os/exec"
	"strconv"
	"syscall"

	"github.com/viovy/iazio-agent/internal/procutil"
)

const createNoWindow = 0x08000000

func setProcGroup(cmd *exec.Cmd) {
	procutil.HideWindow(cmd)
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP | createNoWindow
}

func killProcGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return nil
	}
	killCmd := procutil.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
	return killCmd.Run()
}
