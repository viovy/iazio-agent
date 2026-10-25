//go:build windows

package reaper

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// TerminateProcessGroup terminates the process and any child processes via taskkill on Windows.
func TerminateProcessGroup(pid int, gracePeriod time.Duration) error {
	if pid <= 0 {
		return nil
	}

	// Graceful terminate attempt
	_ = exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T").Run()

	deadline := time.Now().Add(gracePeriod)
	for time.Now().Before(deadline) {
		if !isPIDAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Forceful terminate
	_ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid), "/T").Run()
	return nil
}

func isPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil
}

func findOrphanPIDs(workspacePath, conversationID string) ([]int, error) {
	var results []int
	selfPID := os.Getpid()

	out, err := exec.Command("wmic", "process", "get", "ProcessId,CommandLine").Output()
	if err != nil {
		out, err = exec.Command("powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_Process | ForEach-Object { \"$($_.CommandLine) $($_.ProcessId)\" }").Output()
		if err != nil {
			return results, nil
		}
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 2 {
			continue
		}
		pidStr := fields[len(fields)-1]
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid <= 1 || pid == selfPID {
			continue
		}
		cmdline := strings.Join(fields[:len(fields)-1], " ")
		match := false
		if conversationID != "" && strings.Contains(cmdline, conversationID) {
			match = true
		}
		if workspacePath != "" && strings.Contains(cmdline, workspacePath) &&
			(strings.Contains(cmdline, "agy") || strings.Contains(cmdline, "iazio-harness") || strings.Contains(cmdline, "opencode")) {
			match = true
		}
		if match {
			results = append(results, pid)
		}
	}
	return results, nil
}
