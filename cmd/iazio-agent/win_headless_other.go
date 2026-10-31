//go:build !windows

package main

func createWindowsNamedMutex(_ string) (uintptr, bool, func()) {
	return 1, false, func() {}
}

func detachWindowsConsole() {}
