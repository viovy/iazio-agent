//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

func installExecutableSafe(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	return installExecutableSafeDirect(destPath, data)
}

func installExecutableSafeDirect(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp.%d", destPath, time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, data, 0755); err != nil {
		return err
	}

	// Unlink existing destination first on Unix/macOS to ensure inode change
	// and prevent macOS amfid code-signature cache corruption.
	_ = os.Remove(destPath)
	if err := os.Rename(tmpPath, destPath); err != nil {
		// Fallback to copy if cross-device boundary
		if writeErr := os.WriteFile(destPath, data, 0755); writeErr != nil {
			_ = os.Remove(tmpPath)
			return writeErr
		}
		_ = os.Remove(tmpPath)
	}

	// On macOS Apple Silicon, ad-hoc codesign binary so amfid does not kill it with SIGKILL
	if runtime.GOOS == "darwin" {
		_ = procutil.Command("codesign", "--force", "--sign", "-", destPath).Run()
	}

	return nil
}

func cleanStaleBackups(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.Contains(name, ".old.") {
			fullPath := filepath.Join(dir, name)
			if info, err := ent.Info(); err == nil {
				if time.Since(info.ModTime()) > 5*time.Minute {
					_ = os.Remove(fullPath)
				}
			}
		}
	}
}
