//go:build windows

package main

import (
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

func createWindowsNamedMutex(name string) (uintptr, bool, func()) {
	k32 := syscall.NewLazyDLL("kernel32.dll")
	namePtr, _ := syscall.UTF16PtrFromString(name)
	ret, _, err := k32.NewProc("CreateMutexW").Call(0, 1, uintptr(unsafe.Pointer(namePtr)))
	alreadyExists := err == syscall.Errno(183)
	if ret != 0 && alreadyExists {
		waitTimeout := uintptr(0)
		if os.Getenv("IAZIO_HANDOFF_PID") != "" {
			waitTimeout = 15000
		}
		if waitTimeout > 0 {
			waitRet, _, _ := k32.NewProc("WaitForSingleObject").Call(ret, waitTimeout)
			// 0 = WAIT_OBJECT_0, 0x80 = WAIT_ABANDONED
			if waitRet == 0 || waitRet == 0x80 {
				alreadyExists = false
			}
		}
	}
	closeFn := func() {
		if ret != 0 {
			_, _, _ = k32.NewProc("ReleaseMutex").Call(ret)
			_, _, _ = k32.NewProc("CloseHandle").Call(ret)
		}
	}
	return ret, alreadyExists, closeFn
}

func detachWindowsConsole() {
	_, _, _ = syscall.NewLazyDLL("kernel32.dll").NewProc("FreeConsole").Call()
}

// buildWindowsSelfRestartCommand constructs the detached background command and scheduled-task fallback
// used on Windows when syscall.Exec is unsupported (EWINDOWS).
func buildWindowsSelfRestartCommand(targetBin string, args []string, env []string) (*exec.Cmd, *exec.Cmd) {
	var passArgs []string
	if len(args) > 1 {
		passArgs = args[1:]
	}
	spawnCmd := exec.Command(targetBin, passArgs...)
	spawnCmd.Env = env
	spawnCmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000,
		HideWindow:    true,
	}
	schtasksCmd := exec.Command("schtasks", "/Run", "/TN", "iazio-agent")
	schtasksCmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: 0x08000000,
		HideWindow:    true,
	}
	return spawnCmd, schtasksCmd
}
