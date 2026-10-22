//go:build windows

package repolock

import (
	"os"
)

func tryLockFile(f *os.File) error {
	// Windows file open mode handles sharing or fallback
	return nil
}

func unlockFile(f *os.File) error {
	return nil
}
