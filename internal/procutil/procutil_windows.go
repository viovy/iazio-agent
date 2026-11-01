//go:build windows

package procutil

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

// CreateNoWindowFlag is the Win32 CREATE_NO_WINDOW process creation flag (0x08000000).
const CreateNoWindowFlag = 0x08000000

const (
	processQueryLimitedInformation = 0x1000
	stillActiveExitCode            = 259
)

// HideWindow configures cmd to run without allocating or displaying a console window on Windows.
// Existing SysProcAttr fields and CreationFlags (such as CREATE_NEW_PROCESS_GROUP) are preserved.
func HideWindow(cmd *exec.Cmd) *exec.Cmd {
	if cmd == nil {
		return nil
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags |= CreateNoWindowFlag
	return cmd
}

// Command creates an *exec.Cmd with Windows console window creation suppressed.
func Command(name string, arg ...string) *exec.Cmd {
	return HideWindow(exec.Command(name, arg...))
}

// CommandContext creates a context-bound *exec.Cmd with Windows console window creation suppressed.
func CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	return HideWindow(exec.CommandContext(ctx, name, arg...))
}

// IsPIDAlive checks whether a process with the given PID is currently alive using Win32 kernel syscalls
// without spawning external subprocesses (such as tasklist.exe).
func IsPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if pid == os.Getpid() {
		return true
	}
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(h, &exitCode); err != nil {
		return false
	}
	return exitCode == stillActiveExitCode
}

func isWindowHidden(cmd *exec.Cmd) bool {
	return cmd != nil && cmd.SysProcAttr != nil && cmd.SysProcAttr.HideWindow && cmd.SysProcAttr.CreationFlags&CreateNoWindowFlag != 0
}
