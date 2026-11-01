//go:build !windows

package main

import (
	"os/exec"

	"github.com/viovy/iazio-agent/internal/procutil"
)

func createWindowsNamedMutex(_ string) (uintptr, bool, func()) {
	return 1, false, func() {}
}

func detachWindowsConsole() {}

func buildWindowsSelfRestartCommand(targetBin string, args []string, env []string) (*exec.Cmd, *exec.Cmd) {
	var passArgs []string
	if len(args) > 1 {
		passArgs = args[1:]
	}
	spawnCmd := procutil.Command(targetBin, passArgs...)
	spawnCmd.Env = env
	schtasksCmd := procutil.Command("schtasks", "/Run", "/TN", "iazio-agent")
	return spawnCmd, schtasksCmd
}
