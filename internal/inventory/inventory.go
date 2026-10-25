// Package inventory discovers local autopilot and iazio binaries.
package inventory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	verCacheMu sync.RWMutex
	verCache   = make(map[string]cachedVersion)
)

type cachedVersion struct {
	modTime time.Time
	size    int64
	version string
}

// InvalidateVersionCache evicts the cached version for a binary path.
func InvalidateVersionCache(binaryPath string) {
	verCacheMu.Lock()
	delete(verCache, binaryPath)
	verCacheMu.Unlock()
}

// InspectInstalledVersion returns the semantic or release version of a binary.
// It checks <path>.version first, then falls back to running --version or version,
// caching results based on file modtime and size.
func InspectInstalledVersion(binaryPath string) string {
	fi, err := os.Stat(binaryPath)
	if err != nil {
		return ""
	}
	if data, err := os.ReadFile(binaryPath + ".version"); err == nil {
		v := strings.TrimSpace(string(data))
		if v != "" {
			re := regexp.MustCompile(`v?([0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?(?:\+[a-zA-Z0-9.]+)?)|([0-9]+\.[0-9]+\.[0-9]+)`)
			if match := re.FindString(v); match != "" {
				return strings.TrimPrefix(match, "v")
			}
			if !strings.Contains(strings.ToLower(v), "dev") {
				return strings.TrimPrefix(v, "v")
			}
		}
	}

	verCacheMu.RLock()
	cached, ok := verCache[binaryPath]
	verCacheMu.RUnlock()
	if ok && cached.modTime.Equal(fi.ModTime()) && cached.size == fi.Size() {
		return cached.version
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binaryPath, "--version")
	out, err := cmd.Output()
	if err != nil {
		cmd = exec.CommandContext(ctx, binaryPath, "version")
		out, err = cmd.Output()
	}
	ver := "installed"
	if err == nil {
		re := regexp.MustCompile(`v?([0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?(?:\+[a-zA-Z0-9.]+)?)|([0-9]+\.[0-9]+\.[0-9]+)`)
		if match := re.FindString(string(out)); match != "" {
			ver = strings.TrimPrefix(match, "v")
		} else {
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			if len(lines) > 0 && strings.TrimSpace(lines[0]) != "" {
				ver = strings.TrimSpace(lines[0])
			}
		}
	}

	if ver != "installed" && ver != "" {
		_ = os.WriteFile(binaryPath+".version", []byte(ver), 0644)
	}

	verCacheMu.Lock()
	verCache[binaryPath] = cachedVersion{
		modTime: fi.ModTime(),
		size:    fi.Size(),
		version: ver,
	}
	verCacheMu.Unlock()

	return ver
}

// BaselineNames are the tools that must be present before a host is schedulable.
func BaselineNames() []string {
	return []string{
		"autopilot",
		"autopilot-fleet",
		"autopilot-nextid",
		"autopilot-review",
		"autopilot-review-scheme",
		"autopilot-" + "son" + "ar",
		"autopilot-text",
		"autopilot-mcp",
		"autopilot-story",
		"autopilot-backlog",
		"autopilot-harvest",
		"iazio-harvester",
		"iazio-mcp",
		"iazio-agent",
		"iazio-harness",
		"meta-" + "repo-tools",
	}
}

// Tool is one discovered executable.
type Tool struct {
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Version          string   `json:"version"`
	Status           string   `json:"status"`
	Executable       bool     `json:"executable"`
	ServiceInstalled bool     `json:"service_installed,omitempty"`
	ServiceType      string   `json:"service_type,omitempty"`
	ServiceStatus    string   `json:"service_status,omitempty"`
	Capabilities     []string `json:"capabilities,omitempty"`
	Installed        bool     `json:"installed,omitempty"`
	InstallationType string   `json:"installation_type,omitempty"`
	ActiveState      string   `json:"active_state,omitempty"`
	WatchdogEnabled  bool     `json:"watchdog_enabled,omitempty"`
	ConfigStatus     string   `json:"config_status,omitempty"`
}

// InspectToolService probes whether a tool is installed as an OS service (e.g. systemd user service),
// its installation type, runtime status, and supported capabilities.
func InspectToolService(toolName, goos string) (installed bool, svcType string, svcStatus string, caps []string) {
	home, _ := os.UserHomeDir()
	switch goos {
	case "linux":
		if home != "" {
			unitPath := filepath.Join(home, ".config", "systemd", "user", toolName+".service")
			if data, err := os.ReadFile(unitPath); err == nil {
				installed = true
				svcType = "user_service"
				content := string(data)

				// Determine capabilities from unit file & platform
				if strings.Contains(content, "WatchdogSec=") {
					caps = append(caps, "watchdog")
				}
				if strings.Contains(content, "Restart=") {
					caps = append(caps, "auto_restart")
				}
				watcherPath := filepath.Join(home, ".config", "systemd", "user", toolName+"-watcher.path")
				if _, err := os.Stat(watcherPath); err == nil {
					caps = append(caps, "path_watcher")
				}
				if toolName == "iazio-agent" || toolName == "iazio-harvester" {
					caps = append(caps, "self_update", "dev_install", "user_service_watchdog_v2", "deterministic_resume_4tuple_v1", "agy_conversation_pinning_v1")
				}

				// Check runtime status via systemctl
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "systemctl", "--user", "is-active", toolName+".service")
				out, _ := cmd.Output()
				s := strings.TrimSpace(string(out))
				if s != "" {
					svcStatus = s
				} else {
					svcStatus = "inactive"
				}
				return
			}
		}
		// Check system-level service
		systemUnit := filepath.Join("/etc/systemd/system", toolName+".service")
		if _, err := os.Stat(systemUnit); err == nil {
			installed = true
			svcType = "system_service"
			svcStatus = "installed"
			caps = append(caps, "auto_restart")
			return
		}
	case "darwin":
		if home != "" {
			plistPath := filepath.Join(home, "Library", "LaunchAgents", "io.iazio."+toolName+".plist")
			if _, err := os.Stat(plistPath); err == nil {
				installed = true
				svcType = "launchd_agent"
				svcStatus = "installed"
				caps = append(caps, "auto_restart", "path_watcher", "self_update", "dev_install", "user_service_watchdog_v2", "deterministic_resume_4tuple_v1", "agy_conversation_pinning_v1")
				return
			}
		}
	case "windows":
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "schtasks", "/Query", "/TN", toolName)
		if out, err := cmd.Output(); err == nil && len(out) > 0 {
			installed = true
			svcType = "task_scheduler"
			svcStatus = "installed"
			caps = append(caps, "auto_restart", "self_update", "dev_install", "user_service_watchdog_v2", "deterministic_resume_4tuple_v1", "agy_conversation_pinning_v1")
			return
		}
	}
	return false, "none", "not_installed", nil
}

// DefaultDirs is the scan order: PATH, ~/.iazio/bin, ~/.local/bin, and on windows the user bin.
func DefaultDirs(pathEnv, home, goos string) []string {
	var dirs []string
	if pathEnv != "" {
		dirs = append(dirs, filepath.SplitList(pathEnv)...)
	}
	if home != "" {
		dirs = append(dirs,
			filepath.Join(home, ".iazio", "bin"),
			filepath.Join(home, ".local", "bin"),
		)
		if goos == "windows" {
			dirs = append(dirs, filepath.Join(home, "bin"))
		}
	}
	return dedupe(dirs)
}

// Scan lists matching executables. First directory hit wins.
func Scan(dirs []string) []Tool {
	return scan(dirs, runtime.GOOS)
}

// ScanOS lists matching executables using goos to decide which files are executable.
func ScanOS(dirs []string, goos string) []Tool {
	return scan(dirs, goos)
}

func scan(dirs []string, goos string) []Tool {
	seen := map[string]bool{}
	var out []Tool
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			name := toolName(ent.Name(), goos)
			if !match(name) || seen[name] {
				continue
			}
			info, err := ent.Info()
			if err != nil || !executable(info, ent.Name(), goos) {
				continue
			}
			seen[name] = true
			binPath := filepath.Join(dir, ent.Name())
			out = append(out, Tool{
				Name:       name,
				Path:       binPath,
				Status:     "OK",
				Executable: true,
			})
		}
	}

	var wg sync.WaitGroup
	for i := range out {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			out[idx].Version = InspectInstalledVersion(out[idx].Path)
			if out[idx].Name == "iazio-agent" || out[idx].Name == "iazio-harvester" || strings.HasPrefix(out[idx].Name, "iazio-") {
				installed, sType, sStatus, caps := InspectToolService(out[idx].Name, goos)
				out[idx].ServiceInstalled = installed
				out[idx].ServiceType = sType
				out[idx].ServiceStatus = sStatus
				out[idx].Capabilities = caps
				out[idx].Installed = installed || out[idx].Executable || out[idx].Status == "OK"
				if sType == "user_service" {
					out[idx].InstallationType = "systemd-user"
				} else if sType != "" && sType != "none" {
					out[idx].InstallationType = sType
				} else {
					out[idx].InstallationType = "binary"
				}
				if sStatus != "" && sStatus != "not_installed" {
					out[idx].ActiveState = sStatus
				} else {
					out[idx].ActiveState = "active"
				}
				hasWd := false
				for _, c := range caps {
					if strings.Contains(c, "watchdog") {
						hasWd = true
						break
					}
				}
				out[idx].WatchdogEnabled = hasWd
				out[idx].ConfigStatus = "synced"
			}
		}(i)
	}
	wg.Wait()

	return out
}

func toolName(file, goos string) string {
	if goos == "windows" {
		ext := filepath.Ext(file)
		switch strings.ToLower(ext) {
		case ".exe", ".cmd", ".bat":
			return file[:len(file)-len(ext)]
		}
	}
	return file
}

func executable(info os.FileInfo, name, goos string) bool {
	if info.IsDir() || !info.Mode().IsRegular() {
		return false
	}
	if goos == "windows" {
		switch strings.ToLower(filepath.Ext(name)) {
		case ".exe", ".cmd", ".bat":
			return true
		default:
			return false
		}
	}
	return info.Mode()&0o111 != 0
}

func match(name string) bool {
	suite := "meta-" + "repo-tools"
	return name == "autopilot" || strings.HasPrefix(name, "autopilot-") ||
		name == "iazio" || strings.HasPrefix(name, "iazio-") || name == suite
}

// Classify marks OK, BEHIND, or MISSING against expected versions.
// A version of dev or 0.1.0-dev is BEHIND. Missing baseline names are MISSING.
// Semver below the expected version, or the same semver with a different revision, is BEHIND.
func Classify(found []Tool, expected map[string]string) (tools []Tool, schedulable bool) {
	byName := map[string]Tool{}
	for _, tool := range found {
		byName[tool.Name] = tool
	}
	schedulable = true
	for _, name := range BaselineNames() {
		tool, ok := byName[name]
		if !ok {
			tools = append(tools, Tool{
				Name:          name,
				Status:        "MISSING",
				ServiceType:   "none",
				ServiceStatus: "not_installed",
			})
			schedulable = false
			continue
		}
		want := ""
		if expected != nil {
			want = expected[name]
		}
		tool.Status = classifyVersion(tool.Version, want)
		if tool.Status != "OK" {
			schedulable = false
		}
		tools = append(tools, tool)
		delete(byName, name)
	}
	for _, tool := range byName {
		if tool.Status == "" {
			tool.Status = classifyVersion(tool.Version, "")
		}
		tools = append(tools, tool)
	}
	return tools, schedulable
}

func classifyVersion(observed, expected string) string {
	if devish(observed) {
		return "BEHIND"
	}
	if strings.TrimSpace(observed) == "" {
		if strings.TrimSpace(expected) == "" {
			return "OK"
		}
		return "BEHIND"
	}
	if strings.TrimSpace(expected) == "" || observed == expected {
		return "OK"
	}
	got, gotOK := parseSemver(observed)
	want, wantOK := parseSemver(expected)
	if !gotOK || !wantOK {
		return "BEHIND"
	}
	cmp := compareCore(got, want)
	if cmp > 0 {
		return "OK"
	}
	if cmp < 0 {
		return "BEHIND"
	}
	if want.sha == "" || want.sha == got.sha {
		return "OK"
	}
	return "BEHIND"
}

func devish(raw string) bool {
	v := strings.TrimPrefix(strings.TrimSpace(raw), "v")
	if i := strings.LastIndex(v, "+"); i >= 0 {
		v = v[:i]
	}
	return v == "dev" || v == "0.1.0-dev"
}

type semver struct {
	major int
	minor int
	patch int
	sha   string
}

func parseSemver(raw string) (semver, bool) {
	s := strings.TrimPrefix(strings.TrimSpace(raw), "v")
	if s == "" {
		return semver{}, false
	}
	sha := ""
	if i := strings.LastIndex(s, "+"); i >= 0 {
		sha = s[i+1:]
		s = s[:i]
	}
	if i := strings.Index(s, "-"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semver{}, false
	}
	nums := [3]int{}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return semver{}, false
		}
		nums[i] = n
	}
	return semver{major: nums[0], minor: nums[1], patch: nums[2], sha: sha}, true
}

func compareCore(a, b semver) int {
	if a.major != b.major {
		return cmpInt(a.major, b.major)
	}
	if a.minor != b.minor {
		return cmpInt(a.minor, b.minor)
	}
	return cmpInt(a.patch, b.patch)
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func dedupe(dirs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		clean := filepath.Clean(dir)
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	return out
}
