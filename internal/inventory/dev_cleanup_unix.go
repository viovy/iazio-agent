//go:build !windows

package inventory

func scheduleRebootDelete(path string) {
	// No-op on Unix
}
