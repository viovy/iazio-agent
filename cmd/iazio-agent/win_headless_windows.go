//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

func createWindowsNamedMutex(name string) (uintptr, bool, func()) {
	k32 := syscall.NewLazyDLL("kernel32.dll")
	namePtr, _ := syscall.UTF16PtrFromString(name)
	ret, _, err := k32.NewProc("CreateMutexW").Call(0, 0, uintptr(unsafe.Pointer(namePtr)))
	alreadyExists := err == syscall.Errno(183)
	closeFn := func() {
		if ret != 0 {
			_, _, _ = k32.NewProc("CloseHandle").Call(ret)
		}
	}
	return ret, alreadyExists, closeFn
}

func detachWindowsConsole() {
	_, _, _ = syscall.NewLazyDLL("kernel32.dll").NewProc("FreeConsole").Call()
}
