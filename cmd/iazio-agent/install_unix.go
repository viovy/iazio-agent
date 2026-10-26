//go:build !windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func installExecutableSafe(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	if err := installExecutableSafeDirect(destPath, data); err != nil {
		return err
	}

	// Synchronize active running process executable on Unix if installed from alternate location
	if currentExe, err := os.Executable(); err == nil && currentExe != "" {
		if resolvedCurrent, err := filepath.EvalSymlinks(currentExe); err == nil {
			currentExe = resolvedCurrent
		}
		cleanCurrent := filepath.Clean(currentExe)
		cleanDest := filepath.Clean(destPath)
		if filepath.Base(cleanCurrent) == filepath.Base(cleanDest) && cleanCurrent != cleanDest {
			if err := installExecutableSafeDirect(cleanCurrent, data); err != nil {
				return fmt.Errorf("failed to synchronize running executable at %s: %w", cleanCurrent, err)
			}
		}
	}

	// Synchronize alternate binary directories on Unix to prevent service / PATH shadowing
	if home := os.Getenv("HOME"); home != "" {
		for _, altDir := range []string{
			filepath.Join(home, ".local", "bin"),
			filepath.Join(home, ".iazio", "bin"),
			filepath.Join(home, ".gemini", "antigravity-cli", "bin"),
		} {
			altPath := filepath.Join(altDir, filepath.Base(destPath))
			if filepath.Clean(altPath) == filepath.Clean(destPath) {
				continue
			}
			if _, statErr := os.Stat(altPath); statErr == nil {
				if err := installExecutableSafeDirect(altPath, data); err != nil {
					return fmt.Errorf("failed to synchronize existing alternate binary at %s: %w", altPath, err)
				}
			} else if _, statErr := os.Stat(altDir); statErr == nil {
				if err := installExecutableSafeDirect(altPath, data); err != nil {
					return fmt.Errorf("failed to initialize alternate binary at %s: %w", altPath, err)
				}
			}
		}
	}
	return nil
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
		_ = exec.Command("codesign", "--force", "--sign", "-", destPath).Run()
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
