//go:build !windows

package supervisor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}

func postStartProc(cmd *exec.Cmd) {}

func signalPID(pid int, sig string) error {
	if pid <= 0 {
		return nil
	}
	var s syscall.Signal
	switch sig {
	case "TERM":
		s = syscall.SIGTERM
	case "KILL":
		s = syscall.SIGKILL
	default:
		s = syscall.SIGTERM
	}
	err := syscall.Kill(-pid, s)
	if err != nil {
		_ = syscall.Kill(pid, s)
	}
	return err
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if syscall.Kill(-pid, 0) == nil {
		return true
	}
	return syscall.Kill(pid, 0) == nil
}

// verifyProcessIdentity verifies that a running PID corresponds to the expected harness job.
// It inspects /proc/<pid>/cmdline (Linux) or ps output (macOS/BSD) to ensure the PID has
// not been recycled by an unrelated OS process across agent restarts.
func verifyProcessIdentity(pid int, expectedJobID string, startedAt time.Time) bool {
	if pid <= 0 || !isProcessAlive(pid) {
		return false
	}
	// Try /proc/<pid>/cmdline first (Linux)
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
		return matchesHarnessIdentity(string(data), expectedJobID)
	}
	// Fallback for macOS / Darwin / BSD without /proc
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := procutil.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err == nil {
		return matchesHarnessIdentity(string(out), expectedJobID)
	}
	// If neither /proc nor ps is available, reject if startedAt is zero or expired
	return !startedAt.IsZero() && time.Since(startedAt) < 24*time.Hour
}

func matchesHarnessIdentity(cmdline, expectedJobID string) bool {
	// A valid detached harness command includes "harness", "iazio", or the explicit job ID
	if strings.Contains(cmdline, "harness") ||
		strings.Contains(cmdline, "iazio") ||
		(expectedJobID != "" && strings.Contains(cmdline, expectedJobID)) {
		return true
	}
	return false
}
