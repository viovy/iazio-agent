//go:build !windows

package preflight

import (
	"syscall"
)

// FreeSpace returns available bytes on the filesystem containing path.
func FreeSpace(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}
