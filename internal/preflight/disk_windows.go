//go:build windows

package preflight

import (
	"syscall"
	"unsafe"
)

var (
	kernel32            = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceExW = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// FreeSpace returns available bytes on the filesystem containing path.
func FreeSpace(path string) (uint64, error) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailable uint64
	r1, _, errCall := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(ptr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)
	if r1 == 0 {
		return 0, errCall
	}
	return freeBytesAvailable, nil
}
