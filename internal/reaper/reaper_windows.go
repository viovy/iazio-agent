//go:build windows

package reaper

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/viovy/iazio-agent/internal/procutil"
)

var (
	kernel32               = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess        = kernel32.NewProc("OpenProcess")
	procCloseHandle        = kernel32.NewProc("CloseHandle")
	procGetExitCodeProcess = kernel32.NewProc("GetExitCodeProcess")
)

const (
	processQueryLimitedInformation = 0x1000
	stillActive                    = 259
)

// TerminateProcessGroup terminates the process and any child processes via taskkill on Windows.
func TerminateProcessGroup(pid int, gracePeriod time.Duration) error {
	if pid <= 0 {
		return nil
	}

	// Graceful terminate attempt
	_ = procutil.Command("taskkill", "/PID", strconv.Itoa(pid), "/T").Run()

	deadline := time.Now().Add(gracePeriod)
	for time.Now().Before(deadline) {
		if !isPIDAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Forceful terminate
	_ = procutil.Command("taskkill", "/F", "/PID", strconv.Itoa(pid), "/T").Run()

	killDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(killDeadline) {
		if !isPIDAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	if isPIDAlive(pid) {
		return fmt.Errorf("process %d still alive after taskkill /F", pid)
	}
	return nil
}

func isPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	h, _, _ := procOpenProcess.Call(uintptr(processQueryLimitedInformation), 0, uintptr(pid))
	if h == 0 {
		return false
	}
	defer procCloseHandle.Call(h)
	var exitCode uint32
	r, _, _ := procGetExitCodeProcess.Call(h, uintptr(unsafe.Pointer(&exitCode)))
	if r == 0 {
		return false
	}
	return exitCode == stillActive
}

func findOrphanPIDs(workspacePath, conversationID string) ([]int, error) {
	var results []int
	selfPID := os.Getpid()

	out, err := procutil.Command("wmic", "process", "get", "ProcessId,CommandLine").Output()
	if err != nil {
		out, err = procutil.Command("powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_Process | ForEach-Object { \"$($_.CommandLine) $($_.ProcessId)\" }").Output()
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
