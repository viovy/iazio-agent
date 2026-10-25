//go:build windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func installExecutableSafe(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	cleanStaleBackups(dir)
	if err := installExecutableSafeDirect(destPath, data); err != nil {
		return err
	}

	// Synchronize active running process executable if installed from alternate location
	if currentExe, err := os.Executable(); err == nil && currentExe != "" {
		if strings.EqualFold(filepath.Base(currentExe), filepath.Base(destPath)) &&
			!strings.EqualFold(filepath.Clean(currentExe), filepath.Clean(destPath)) {
			if err := installExecutableSafeDirect(currentExe, data); err != nil {
				return fmt.Errorf("failed to synchronize running executable at %s: %w", currentExe, err)
			}
		}
	}

	// Synchronize %USERPROFILE%\bin and %USERPROFILE%\.local\bin to prevent PATH shadowing
	userProfile := os.Getenv("USERPROFILE")
	if userProfile != "" {
		binDir1 := filepath.Join(userProfile, "bin")
		binDir2 := filepath.Join(userProfile, ".local", "bin")
		var altPath string
		if strings.HasPrefix(strings.ToLower(destPath), strings.ToLower(binDir1)) {
			altPath = filepath.Join(binDir2, filepath.Base(destPath))
		} else if strings.HasPrefix(strings.ToLower(destPath), strings.ToLower(binDir2)) {
			altPath = filepath.Join(binDir1, filepath.Base(destPath))
		}
		if altPath != "" {
			if _, statErr := os.Stat(altPath); statErr == nil {
				if err := installExecutableSafeDirect(altPath, data); err != nil {
					return fmt.Errorf("failed to synchronize existing alternate binary at %s: %w", altPath, err)
				}
			} else if _, statErr := os.Stat(filepath.Dir(altPath)); statErr == nil {
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
	if _, err := os.Stat(destPath); err == nil {
		f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			backupPath := fmt.Sprintf("%s.old.%d.exe", strings.TrimSuffix(destPath, ".exe"), time.Now().UnixNano())
			if renameErr := os.Rename(destPath, backupPath); renameErr != nil {
				return fmt.Errorf("file locked and rename failed: %w", renameErr)
			}
		} else {
			_ = f.Close()
		}
	}
	return os.WriteFile(destPath, data, 0755)
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
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}
