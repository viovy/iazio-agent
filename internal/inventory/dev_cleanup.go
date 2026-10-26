package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DevBinaryCandidate represents an identified non-canonical or dev executable.
type DevBinaryCandidate struct {
	Path        string `json:"path"`
	ToolName    string `json:"tool_name"`
	VariantType string `json:"variant_type"` // dev_suffix, staging_extension, shadow_duplicate
	PID         int    `json:"pid,omitempty"`
}

// CleanupReport summarizes the results of dev and shadow binary cleanup.
type CleanupReport struct {
	DeletedPaths           []string `json:"deleted_paths"`
	KilledPIDs             []int    `json:"killed_pids"`
	FailedPaths            []string `json:"failed_paths"`
	CanonicalPathsVerified []string `json:"canonical_paths_verified"`
}

// ManagedTools are the core fleet tools subject to eager update and dev binary cleanup.
var ManagedTools = []string{
	"iazio-agent",
	"iazio-harvester",
	"iazio-mcp",
	"iazio-harness",
	"autopilot",
}

type shaCacheEntry struct {
	modTime   time.Time
	size      int64
	commitSHA string
}

var (
	shaCacheMu sync.RWMutex
	shaCache   = make(map[string]shaCacheEntry)
)

// InspectToolCommitSHA probes the binary using --version --json, falling back to version string or sha256.
func InspectToolCommitSHA(binaryPath string) string {
	fi, err := os.Stat(binaryPath)
	if err != nil {
		return ""
	}

	shaCacheMu.RLock()
	cached, ok := shaCache[binaryPath]
	shaCacheMu.RUnlock()
	if ok && cached.modTime.Equal(fi.ModTime()) && cached.size == fi.Size() {
		return cached.commitSHA
	}

	setCache := func(result string) string {
		shaCacheMu.Lock()
		shaCache[binaryPath] = shaCacheEntry{
			modTime:   fi.ModTime(),
			size:      fi.Size(),
			commitSHA: result,
		}
		shaCacheMu.Unlock()
		return result
	}

	// Fast path: check .version sidecar if present and contains hex SHA
	if sidecar, err := os.ReadFile(binaryPath + ".version"); err == nil {
		s := strings.TrimSpace(string(sidecar))
		if len(s) >= 7 && len(s) <= 40 {
			isHex := true
			for _, r := range s {
				if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
					isHex = false
					break
				}
			}
			if isHex {
				return setCache(s)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "--version", "--json")
	if out, err := cmd.Output(); err == nil {
		var v struct {
			CommitSHA string `json:"commit_sha"`
		}
		if jsonErr := json.Unmarshal(out, &v); jsonErr == nil && v.CommitSHA != "" && v.CommitSHA != "unknown" {
			return setCache(v.CommitSHA)
		}
	}

	// Try running without --json
	cmd2 := exec.CommandContext(ctx, binaryPath, "--version")
	if out, err := cmd2.Output(); err == nil {
		outStr := string(out)
		if idx := strings.Index(outStr, "+"); idx != -1 {
			shaPart := strings.TrimSpace(outStr[idx+1:])
			if shaEnd := strings.IndexAny(shaPart, " \t\r\n"); shaEnd != -1 {
				shaPart = shaPart[:shaEnd]
			}
			if len(shaPart) >= 7 {
				return setCache(shaPart)
			}
		}
	}

	// Check .version sidecar if present
	if sidecar, err := os.ReadFile(binaryPath + ".version"); err == nil {
		s := strings.TrimSpace(string(sidecar))
		if len(s) >= 7 && len(s) <= 40 {
			return setCache(s)
		}
	}

	// Fallback to SHA-256 fingerprint (bounded to first 64KB for performance)
	f, err := os.Open(binaryPath)
	if err == nil {
		defer f.Close()
		buf := make([]byte, 64*1024)
		n, _ := f.Read(buf)
		if n > 0 {
			h := sha256.Sum256(buf[:n])
			return setCache(hex.EncodeToString(h[:]))
		}
	}

	return setCache(fmt.Sprintf("size:%d-mod:%d", fi.Size(), fi.ModTime().Unix()))
}

// ScanDevAndShadowBinaries identifies non-canonical name variants and duplicate shadow binaries.
func ScanDevAndShadowBinaries(searchDirs []string, canonicalBinDir string) []DevBinaryCandidate {
	var candidates []DevBinaryCandidate
	seenPaths := make(map[string]bool)

	cleanCanonical := ""
	if canonicalBinDir != "" {
		cleanCanonical = filepath.Clean(canonicalBinDir)
		if runtime.GOOS == "windows" {
			cleanCanonical = strings.ToLower(cleanCanonical)
		}
	}

	toolSet := make(map[string]bool)
	for _, t := range ManagedTools {
		toolSet[t] = true
	}

	// Regex for dev suffix: <tool>[-_](dev|test|debug|local|old|bak|copy|v[0-9]|darwin|linux|windows|arm64|amd64).*
	devSuffixRegexes := make(map[string]*regexp.Regexp)
	// Regex for staging extension: <tool>\.(old|bak|new|tmp|del).* or \.iazio-.*
	stagingExtRegexes := make(map[string]*regexp.Regexp)

	for _, t := range ManagedTools {
		devSuffixRegexes[t] = regexp.MustCompile(fmt.Sprintf(`(?i)^%s[-_](dev|test|debug|local|old|bak|copy|v[0-9]|darwin|linux|windows|arm64|amd64).*`, regexp.QuoteMeta(t)))
		stagingExtRegexes[t] = regexp.MustCompile(fmt.Sprintf(`(?i)^%s\.(old|bak|new|tmp|del|exe\.old|exe\.bak).*`, regexp.QuoteMeta(t)))
	}
	dotIazioRegex := regexp.MustCompile(`(?i)^\.iazio-.*`)

	for _, dir := range searchDirs {
		cleanDir := filepath.Clean(dir)
		if cleanDir == "" || cleanDir == "." {
			continue
		}
		dirKey := cleanDir
		if runtime.GOOS == "windows" {
			dirKey = strings.ToLower(cleanDir)
		}

		isCanonicalDir := (cleanCanonical != "" && dirKey == cleanCanonical)

		entries, err := os.ReadDir(cleanDir)
		if err != nil {
			continue
		}

		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}

			fileName := ent.Name()
			fullPath := filepath.Join(cleanDir, fileName)
			pathKey := fullPath
			if runtime.GOOS == "windows" {
				pathKey = strings.ToLower(fullPath)
			}
			if seenPaths[pathKey] {
				continue
			}

			// Clean extension for matching
			baseWithoutExt := fileName
			if runtime.GOOS == "windows" {
				if strings.HasSuffix(strings.ToLower(baseWithoutExt), ".exe") {
					baseWithoutExt = baseWithoutExt[:len(baseWithoutExt)-4]
				}
			}

			for _, t := range ManagedTools {
				matched := false
				variant := ""

				// 1. Exact canonical name match
				if strings.EqualFold(baseWithoutExt, t) {
					if !isCanonicalDir {
						matched = true
						variant = "shadow_duplicate"
					}
				} else if devSuffixRegexes[t].MatchString(baseWithoutExt) {
					matched = true
					variant = "dev_suffix"
				} else if stagingExtRegexes[t].MatchString(fileName) {
					matched = true
					variant = "staging_extension"
				} else if dotIazioRegex.MatchString(fileName) && strings.Contains(fileName, t) {
					matched = true
					variant = "staging_extension"
				}

				if matched {
					seenPaths[pathKey] = true
					candidates = append(candidates, DevBinaryCandidate{
						Path:        fullPath,
						ToolName:    t,
						VariantType: variant,
					})
					break
				}
			}
		}
	}

	return candidates
}

// FindPIDsExecutingFile locates running OS processes whose binary image points to filePath.
func FindPIDsExecutingFile(filePath string) []int {
	cleanPath, err := filepath.Abs(filePath)
	if err != nil {
		cleanPath = filepath.Clean(filePath)
	}

	selfPID := os.Getpid()
	seen := make(map[int]bool)
	var pids []int

	addPID := func(pid int) {
		if pid <= 1 || pid == selfPID || seen[pid] {
			return
		}
		seen[pid] = true
		pids = append(pids, pid)
	}

	// 1. Linux /proc/*/exe scanning
	if procEntries, err := os.ReadDir("/proc"); err == nil {
		for _, ent := range procEntries {
			if !ent.IsDir() {
				continue
			}
			pid, parseErr := strconv.Atoi(ent.Name())
			if parseErr != nil {
				continue
			}
			exeLink := filepath.Join("/proc", ent.Name(), "exe")
			if target, readErr := os.Readlink(exeLink); readErr == nil {
				if filepath.Clean(target) == cleanPath {
					addPID(pid)
				}
			}
		}
		return pids
	}

	// 2. macOS / Darwin: lsof -t
	if runtime.GOOS == "darwin" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "lsof", "-t", cleanPath).Output(); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if pid, err := strconv.Atoi(strings.TrimSpace(line)); err == nil {
					addPID(pid)
				}
			}
		}
		return pids
	}

	// 3. Windows tasklist / WMI
	if runtime.GOOS == "windows" {
		baseName := filepath.Base(cleanPath)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "wmic", "process", "where", fmt.Sprintf("ExecutablePath='%s'", strings.ReplaceAll(cleanPath, `\`, `\\`)), "get", "ProcessId")
		if out, err := cmd.Output(); err == nil {
			lines := strings.Split(string(out), "\n")
			for _, l := range lines {
				if pid, err := strconv.Atoi(strings.TrimSpace(l)); err == nil {
					addPID(pid)
				}
			}
		}
		_ = baseName
		return pids
	}

	return pids
}

// CleanupDevBinaries unlocks, terminates running processes for, and removes dev/shadow binaries.
func CleanupDevBinaries(ctx context.Context, candidates []DevBinaryCandidate, canonicalBinDir string) (CleanupReport, error) {
	report := CleanupReport{
		DeletedPaths:           make([]string, 0),
		KilledPIDs:             make([]int, 0),
		FailedPaths:            make([]string, 0),
		CanonicalPathsVerified: make([]string, 0),
	}

	cleanCanonical := ""
	if canonicalBinDir != "" {
		cleanCanonical = filepath.Clean(canonicalBinDir)
		if runtime.GOOS == "windows" {
			cleanCanonical = strings.ToLower(cleanCanonical)
		}
	}

	// Record canonical binaries verified
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	if cleanCanonical != "" {
		for _, t := range ManagedTools {
			cPath := filepath.Join(canonicalBinDir, t+ext)
			if _, err := os.Stat(cPath); err == nil {
				report.CanonicalPathsVerified = append(report.CanonicalPathsVerified, cPath)
			}
		}
	}

	for _, cand := range candidates {
		targetPath := cand.Path

		// Absolute safety check: Never delete a file inside canonical directory that has the exact canonical name
		cleanTarget := filepath.Clean(targetPath)
		targetDirKey := filepath.Dir(cleanTarget)
		if runtime.GOOS == "windows" {
			targetDirKey = strings.ToLower(targetDirKey)
		}

		baseName := filepath.Base(cleanTarget)
		if runtime.GOOS == "windows" && strings.HasSuffix(strings.ToLower(baseName), ".exe") {
			baseName = baseName[:len(baseName)-4]
		}
		if cleanCanonical != "" && targetDirKey == cleanCanonical && strings.EqualFold(baseName, cand.ToolName) {
			continue // skip canonical
		}

		// 1. Terminate any locking processes executing this binary
		pids := FindPIDsExecutingFile(targetPath)
		for _, pid := range pids {
			if runtime.GOOS == "windows" {
				_ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid)).Run()
			} else {
				_ = syscall.Kill(pid, syscall.SIGTERM)
			}
			report.KilledPIDs = append(report.KilledPIDs, pid)
		}
		if len(pids) > 0 {
			time.Sleep(100 * time.Millisecond)
			for _, pid := range pids {
				if runtime.GOOS != "windows" {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}

		// 2. Clear read-only permissions
		_ = os.Chmod(targetPath, 0755)

		// 3. Remove file
		err := os.Remove(targetPath)
		if err != nil && !os.IsNotExist(err) {
			// Windows locked file fallback: move to temp and schedule reboot deletion
			if runtime.GOOS == "windows" {
				tempTrash := filepath.Join(os.TempDir(), fmt.Sprintf("iazio-trash-%d.del", time.Now().UnixNano()))
				if renameErr := os.Rename(targetPath, tempTrash); renameErr == nil {
					scheduleRebootDelete(tempTrash)
					err = nil
				}
			}
		}

		if err == nil || os.IsNotExist(err) {
			report.DeletedPaths = append(report.DeletedPaths, targetPath)
			InvalidateVersionCache(targetPath)
			_ = os.Remove(targetPath + ".version")
		} else {
			report.FailedPaths = append(report.FailedPaths, fmt.Sprintf("%s: %v", targetPath, err))
		}
	}

	return report, nil
}
