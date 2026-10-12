//go:build windows

package supervisor

import (
	"os/exec"
	"strconv"
	"syscall"
)

func setProcGroup(cmd *exec.Cmd) {
	// Process group configuration on Windows if needed
}

func signalPID(pid int, sig string) error {
	if pid <= 0 {
		return nil
	}
	return exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid)).Run()
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	const processQueryLimitedInformation = 0x1000
	const stillActive = 259

	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)

	var exitCode uint32
	if err := syscall.GetExitCodeProcess(h, &exitCode); err != nil {
		return false
	}
	return exitCode == stillActive
}
