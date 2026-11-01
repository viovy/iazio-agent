//go:build !windows

package reaper

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

// TerminateProcessGroup cleanly shuts down a process group by sending SIGTERM,
// waiting up to gracePeriod for graceful exit, and escalating to SIGKILL if processes remain.
func TerminateProcessGroup(pid int, gracePeriod time.Duration) error {
	if pid <= 0 {
		return nil
	}

	// 1. Send SIGTERM to process group (never signal iazio-agent's own PGID)
	myPgrp := syscall.Getpgrp()
	pgid, err := syscall.Getpgid(pid)
	if err == nil && pgid > 1 && pgid != myPgrp {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)

	deadline := time.Now().Add(gracePeriod)
	for time.Now().Before(deadline) {
		if !isPIDAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 2. Escalate to SIGKILL if still alive
	if err == nil && pgid > 1 && pgid != myPgrp {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	time.Sleep(50 * time.Millisecond)

	if isPIDAlive(pid) {
		return fmt.Errorf("process %d still alive after SIGKILL escalation", pid)
	}

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
	if err != nil {
		return false
	}
	// On macOS / BSD without /proc, check ps state for zombie ('Z')
	if out, psErr := procutil.Command("ps", "-o", "state=", "-p", strconv.Itoa(pid)).Output(); psErr == nil {
		st := strings.TrimSpace(string(out))
		if st == "" || strings.HasPrefix(st, "Z") {
			return false
		}
	}
	return true
}

// findOrphanPIDs searches running processes whose cwd is inside workspacePath,
// who hold open file handles inside workspacePath or conversation session stores,
// or whose command line mentions conversationID or workspacePath.
func findOrphanPIDs(workspacePath, conversationID string) ([]int, error) {
	seen := make(map[int]bool)
	var results []int
	selfPID := os.Getpid()
	parentPID := os.Getppid()

	addPID := func(pid int) {
		if pid <= 1 || pid == selfPID || pid == parentPID || seen[pid] {
			return
		}
		seen[pid] = true
		results = append(results, pid)
	}

	cleanWorkspace := ""
	if workspacePath != "" {
		cleanWorkspace = filepath.Clean(workspacePath)
	}

	// 1. Linux /proc scanning (if /proc is available)
	procEntries, err := os.ReadDir("/proc")
	if err == nil {
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
			if err == nil && cleanWorkspace != "" {
				cleanCwd := filepath.Clean(cwdLink)
				if cleanCwd == cleanWorkspace || strings.HasPrefix(cleanCwd, cleanWorkspace+string(filepath.Separator)) {
					addPID(pid)
					continue
				}
			}

			// Check cmdline
			cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
			if err == nil && len(cmdlineBytes) > 0 {
				cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
				if conversationID != "" && strings.Contains(cmdline, conversationID) {
					addPID(pid)
					continue
				}
				if cleanWorkspace != "" && strings.Contains(cmdline, cleanWorkspace) &&
					(strings.Contains(cmdline, "agy") || strings.Contains(cmdline, "iazio-harness") || strings.Contains(cmdline, "opencode")) {
					addPID(pid)
					continue
				}
			}
		}
	}

	// 2. lsof scanning for macOS/Darwin and robust cross-platform child process / cwd discovery
	if lsofBin, err := exec.LookPath("lsof"); err == nil {
		if cleanWorkspace != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			out, _ := procutil.CommandContext(ctx, lsofBin, "+D", cleanWorkspace, "-t").Output()
			cancel()
			for _, line := range strings.Split(string(out), "\n") {
				line = strings.TrimSpace(line)
				if pid, err := strconv.Atoi(line); err == nil {
					addPID(pid)
				}
			}
		}

		if conversationID != "" {
			home, _ := os.UserHomeDir()
			if home != "" {
				sessionPattern := filepath.Join(home, ".gemini", "antigravity-cli", "conversations", conversationID+"*")
				matches, _ := filepath.Glob(sessionPattern)
				for _, lockTarget := range matches {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					out, _ := procutil.CommandContext(ctx, lsofBin, lockTarget, "-t").Output()
					cancel()
					for _, line := range strings.Split(string(out), "\n") {
						line = strings.TrimSpace(line)
						if pid, err := strconv.Atoi(line); err == nil {
							addPID(pid)
						}
					}
				}
			}
		}
	}

	// 3. Fallback / complementary ps command scanning
	cmd := procutil.Command("ps", "-eo", "pid,ppid,command")
	out, err := cmd.Output()
	if err == nil {
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
			if cleanWorkspace != "" && strings.Contains(cmdline, cleanWorkspace) &&
				(strings.Contains(cmdline, "agy") || strings.Contains(cmdline, "iazio-harness") || strings.Contains(cmdline, "opencode")) {
				match = true
			}
			if match {
				addPID(pid)
			}
		}
	}

	return results, nil
}
