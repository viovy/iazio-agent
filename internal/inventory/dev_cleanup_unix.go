//go:build !windows

package inventory

import "syscall"

func scheduleRebootDelete(path string) {
	// No-op on Unix
}

func terminatePID(pid int) {
	_ = syscall.Kill(pid, syscall.SIGTERM)
}

func forceKillPID(pid int) {
	_ = syscall.Kill(pid, syscall.SIGKILL)
}
