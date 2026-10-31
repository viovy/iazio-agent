//go:build !windows

package main

import "os/exec"

func createWindowsNamedMutex(_ string) (uintptr, bool, func()) {
	return 1, false, func() {}
}

func detachWindowsConsole() {}

func buildWindowsSelfRestartCommand(targetBin string, args []string, env []string) (*exec.Cmd, *exec.Cmd) {
	var passArgs []string
	if len(args) > 1 {
		passArgs = args[1:]
	}
	spawnCmd := exec.Command(targetBin, passArgs...)
	spawnCmd.Env = env
	schtasksCmd := exec.Command("schtasks", "/Run", "/TN", "iazio-agent")
	return spawnCmd, schtasksCmd
}
