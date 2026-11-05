//go:build windows

package repolock

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

func tryLockFile(f *os.File) error {
	if f == nil {
		return syscall.EINVAL
	}
	var ol syscall.Overlapped
	r1, _, e1 := procLockFileEx.Call(
		f.Fd(),
		uintptr(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		if e1 != nil && e1 != syscall.Errno(0) {
			return e1
		}
		return ErrLockAcquisitionFailed
	}
	return nil
}

func unlockFile(f *os.File) error {
	if f == nil {
		return syscall.EINVAL
	}
	var ol syscall.Overlapped
	r1, _, e1 := procUnlockFileEx.Call(
		f.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		if e1 != nil && e1 != syscall.Errno(0) {
			return e1
		}
		return syscall.EINVAL
	}
	return nil
}
