//go:build windows

package supervisor

import (
	"os"
	"os/exec"
	"strconv"
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
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p != nil
}
