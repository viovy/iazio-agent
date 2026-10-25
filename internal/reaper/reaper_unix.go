//go:build !windows

package reaper

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// TerminateProcessGroup cleanly shuts down a process group by sending SIGTERM,
// waiting up to gracePeriod for graceful exit, and escalating to SIGKILL if processes remain.
func TerminateProcessGroup(pid int, gracePeriod time.Duration) error {
	if pid <= 0 {
		return nil
	}

	// 1. Send SIGTERM to process group
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	_ = syscall.Kill(pid, syscall.SIGTERM)

	deadline := time.Now().Add(gracePeriod)
	for time.Now().Before(deadline) {
		if !isPIDAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 2. Escalate to SIGKILL if still alive
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = syscall.Kill(pid, syscall.SIGKILL)
	time.Sleep(50 * time.Millisecond)

	return nil
}

func isPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Check /proc/<pid>/status for zombie state on Linux
	if data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status")); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, l := range lines {
			if strings.HasPrefix(l, "State:") {
				if strings.Contains(l, "Z") || strings.Contains(l, "zombie") {
					return false
				}
				break
			}
		}
		return true
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}

// findOrphanPIDs searches running processes whose cwd is inside workspacePath
// or whose command line mentions conversationID or workspacePath.
func findOrphanPIDs(workspacePath, conversationID string) ([]int, error) {
	var results []int
	selfPID := os.Getpid()
	parentPID := os.Getppid()

	// Linux /proc scanning
	procEntries, err := os.ReadDir("/proc")
	if err == nil {
		cleanWorkspace := filepath.Clean(workspacePath)
		for _, entry := range procEntries {
			if !entry.IsDir() {
				continue
			}
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 1 || pid == selfPID || pid == parentPID {
				continue
			}

			// Check cwd
			cwdLink, err := os.Readlink(filepath.Join("/proc", entry.Name(), "cwd"))
			if err == nil && workspacePath != "" {
				cleanCwd := filepath.Clean(cwdLink)
				if cleanCwd == cleanWorkspace || strings.HasPrefix(cleanCwd, cleanWorkspace+string(filepath.Separator)) {
					results = append(results, pid)
					continue
				}
			}

			// Check cmdline
			cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
			if err == nil && len(cmdlineBytes) > 0 {
				cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
				if conversationID != "" && strings.Contains(cmdline, conversationID) {
					results = append(results, pid)
					continue
				}
				if workspacePath != "" && strings.Contains(cmdline, workspacePath) &&
					(strings.Contains(cmdline, "agy") || strings.Contains(cmdline, "iazio-harness") || strings.Contains(cmdline, "opencode")) {
					results = append(results, pid)
					continue
				}
			}
		}
		return results, nil
	}

	// Fallback for macOS / Darwin where /proc is not mounted: use ps
	cmd := exec.Command("ps", "-eo", "pid,ppid,command")
	out, err := cmd.Output()
	if err != nil {
		return results, nil
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 1 || pid == selfPID || pid == parentPID {
			continue
		}
		cmdline := strings.Join(fields[2:], " ")
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
