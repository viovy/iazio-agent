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
	return installExecutableSafeDirect(destPath, data)
}

func installExecutableSafeDirect(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	cleanStaleBackups(dir)

	// Write new binary to an atomic staging file first
	tmpPath := fmt.Sprintf("%s.new.%d.exe", strings.TrimSuffix(destPath, ".exe"), time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, data, 0755); err != nil {
		return fmt.Errorf("write staging executable: %w", err)
	}

	// If destPath exists, rename it out of the way to .old.<timestamp>.exe.
	// On Windows NTFS, running executables CAN be renamed, but CANNOT be overwritten in-place.
	if _, err := os.Stat(destPath); err == nil {
		backupPath := fmt.Sprintf("%s.old.%d.exe", strings.TrimSuffix(destPath, ".exe"), time.Now().UnixNano())
		if renameErr := os.Rename(destPath, backupPath); renameErr != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("file locked and rename failed: %w", renameErr)
		}
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename staging binary to destination failed: %w", err)
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
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}
