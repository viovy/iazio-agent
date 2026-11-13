//go:build windows

package supervisor

import (
	"os/exec"
	"strconv"
	"syscall"

	"github.com/viovy/iazio-core/pkg/procutil"
)

const supervisorCreateNoWindow = 0x08000000

func setSupervisorProcGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	procutil.HideWindow(cmd)
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP | supervisorCreateNoWindow
}

func killSupervisorProcGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return nil
	}
	killCmd := procutil.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
	return killCmd.Run()
}
