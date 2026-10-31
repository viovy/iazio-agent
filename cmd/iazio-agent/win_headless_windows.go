//go:build windows

package main

import (
	"os"
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
