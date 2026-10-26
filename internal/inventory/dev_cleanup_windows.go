//go:build windows

package inventory

import (
	"os/exec"
	"strconv"
	"syscall"
	"unsafe"
)

func scheduleRebootDelete(path string) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	moveFileEx := kernel32.NewProc("MoveFileExW")
	if moveFileEx != nil {
		pathPtr, err := syscall.UTF16PtrFromString(path)
		if err == nil {
			const MOVEFILE_DELAY_UNTIL_REBOOT = 0x4
			_, _, _ = moveFileEx.Call(uintptr(unsafe.Pointer(pathPtr)), 0, uintptr(MOVEFILE_DELAY_UNTIL_REBOOT))
		}
	}
}

func terminatePID(pid int) {
	_ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid)).Run()
}

func forceKillPID(pid int) {
	// On Windows, taskkill /F already force-terminates
}
