// Package agyhooks sanitizes user-level Antigravity CLI (~/.gemini/config) plugin
// state and detects stale PreToolUse plugin hook directory failures.
package agyhooks

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const datacloudTelemetryPlugin = "googlecloudtools.datacloud_telemetry"

var sanitizeMu sync.Mutex

// Result summarizes changes made while sanitizing ~/.gemini/config.
type Result struct {
	RemovedPluginDirs []string `json:"removed_plugin_dirs,omitempty"`
	DisabledPlugins   []string `json:"disabled_plugins,omitempty"`
	Changed           bool     `json:"changed"`
}

var (
	// Matches: command failed: chdir /Users/romeo/.gemini/config/plugins/googlecloudtools.datacloud_telemetry: no such file or directory
	staleChdirHookRe = regexp.MustCompile(`(?i)command\s+failed:\s*chdir\s+\S*[/\\]\.gemini[/\\]config[/\\]plugins[/\\]([A-Za-z0-9_.-]+):\s*no\s+such\s+file\s+or\s+directory`)
	// Matches: JSON hook "jsonhook__googlecloudtools.datacloud_telemetry_PreToolUse_0_0" failed
	staleJSONHookNameRe = regexp.MustCompile(`(?is)json\s+hook\s+"jsonhook__([A-Za-z0-9_.-]+?)_(?:PreToolUse|PostToolUse|Stop|SubagentStop|Notification|UserPromptSubmit)[^"]*"\s+failed`)
)

// IsTestProcess reports whether the current process is executing under `go test`.
// It intentionally avoids importing the `testing` package so production binaries
// do not expose `-test.*` CLI flags.
func IsTestProcess() bool {
	if flag.Lookup("test.v") != nil {
		return true
	}
	if len(os.Args) > 0 {
		base := strings.ToLower(filepath.Base(os.Args[0]))
		if strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe") || strings.HasPrefix(base, "___test") {
			return true
		}
		for _, a := range os.Args[1:] {
			if strings.HasPrefix(a, "-test.") {
				return true
			}
		}
	}
	return false
}

// ResolveAgentHome resolves the home directory to sanitize from getenv, falling
// back to os.UserHomeDir() only when not executing under `go test`.
func ResolveAgentHome(getenv func(string) string, isTest bool) string {
	if getenv != nil {
		if h := strings.TrimSpace(getenv("HOME")); h != "" {
			return h
		}
		if up := strings.TrimSpace(getenv("USERPROFILE")); up != "" {
			return up
		}
	}
	if isTest {
		return ""
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(home)
}

// ResolveEnvHome extracts HOME / USERPROFILE from a child environment slice.
// Falls back to os.UserHomeDir() only outside `go test`.
func ResolveEnvHome(env []string) string {
	var homeVal, userProfileVal string
	for _, entry := range env {
		idx := strings.IndexByte(entry, '=')
		if idx <= 0 {
			continue
		}
		k := entry[:idx]
		v := strings.TrimSpace(entry[idx+1:])
		switch {
		case k == "HOME" || (runtime.GOOS == "windows" && strings.EqualFold(k, "HOME")):
			if v != "" {
				homeVal = v
			}
		case k == "USERPROFILE" || (runtime.GOOS == "windows" && strings.EqualFold(k, "USERPROFILE")):
			if v != "" {
				userProfileVal = v
			}
		}
	}
	if homeVal != "" {
		return homeVal
	}
	if userProfileVal != "" {
		return userProfileVal
	}
	return ResolveAgentHome(nil, IsTestProcess())
}

// SanitizeUserGeminiHooks cleans broken/stale Antigravity CLI plugin directories
// and pins googlecloudtools.datacloud_telemetry (plus missing non-marketplace
// plugins) disabled in <homeDir>/.gemini/config/config.json. It is a pure no-op
// when <homeDir> is empty in tests or when .gemini/config does not exist.
func SanitizeUserGeminiHooks(homeDir string) (Result, error) {
	var res Result
	home := strings.TrimSpace(homeDir)
	if home == "" {
		if IsTestProcess() {
			return res, nil
		}
		resolved, err := os.UserHomeDir()
		if err != nil || strings.TrimSpace(resolved) == "" {
			return res, nil
		}
		home = resolved
	}

	configDir := filepath.Join(home, ".gemini", "config")
	cfgDirInfo, err := os.Stat(configDir)
	if err != nil || !cfgDirInfo.IsDir() {
		return res, nil
	}

	sanitizeMu.Lock()
	defer sanitizeMu.Unlock()

	pluginsDir := filepath.Join(configDir, "plugins")
	if entries, err := os.ReadDir(pluginsDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			fullPath := filepath.Join(pluginsDir, name)

			if lfi, lerr := os.Lstat(fullPath); lerr == nil && (lfi.Mode()&os.ModeSymlink != 0) {
				if _, serr := os.Stat(fullPath); serr != nil {
					if rErr := os.Remove(fullPath); rErr == nil {
						res.RemovedPluginDirs = append(res.RemovedPluginDirs, fullPath)
						res.Changed = true
					}
					continue
				}
			}

			if name == datacloudTelemetryPlugin {
				if rErr := os.RemoveAll(fullPath); rErr == nil {
					res.RemovedPluginDirs = append(res.RemovedPluginDirs, fullPath)
					res.Changed = true
				}
				continue
			}

			sfi, serr := os.Stat(fullPath)
			if serr != nil || !sfi.IsDir() {
				continue
			}

			hooksPath := filepath.Join(fullPath, "hooks.json")
			if data, rErr := os.ReadFile(hooksPath); rErr == nil {
				if isBrokenPluginHooksJSON(fullPath, data) {
					if remErr := os.RemoveAll(fullPath); remErr == nil {
						res.RemovedPluginDirs = append(res.RemovedPluginDirs, fullPath)
						res.Changed = true
					}
				}
			}
		}
	}

	configPath := filepath.Join(configDir, "config.json")
	origInfo, statErr := os.Stat(configPath)
	if statErr == nil && !origInfo.IsDir() {
		raw, rErr := os.ReadFile(configPath)
		if rErr != nil {
			return res, rErr
		}
		root := make(map[string]any)
		if len(bytes.TrimSpace(raw)) > 0 {
			if uErr := json.Unmarshal(raw, &root); uErr != nil {
				return res, fmt.Errorf("parse %s: %w", configPath, uErr)
			}
		}
		if root == nil {
			root = make(map[string]any)
		}

		pluginsMap, _ := root["plugins"].(map[string]any)
		if pluginsMap == nil {
			pluginsMap = make(map[string]any)
		}

		var configChanged bool
		dtEntry, _ := pluginsMap[datacloudTelemetryPlugin].(map[string]any)
		if dtEntry == nil {
			dtEntry = make(map[string]any)
		}
		if en, hasEn := dtEntry["enabled"].(bool); !hasEn || en {
			dtEntry["enabled"] = false
			pluginsMap[datacloudTelemetryPlugin] = dtEntry
			root["plugins"] = pluginsMap
			configChanged = true
			res.DisabledPlugins = append(res.DisabledPlugins, datacloudTelemetryPlugin)
		}

		names := make([]string, 0, len(pluginsMap))
		for k := range pluginsMap {
			if k != datacloudTelemetryPlugin {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			pm, ok := pluginsMap[name].(map[string]any)
			if !ok || pm == nil {
				continue
			}
			if _, hasInstalledFrom := pm["installedFrom"]; hasInstalledFrom {
				continue
			}
			if _, dErr := os.Stat(filepath.Join(pluginsDir, name)); os.IsNotExist(dErr) {
				if en, hasEn := pm["enabled"].(bool); hasEn && en {
					pm["enabled"] = false
					configChanged = true
					res.DisabledPlugins = append(res.DisabledPlugins, name)
				}
			}
		}

		if configChanged {
			mode := origInfo.Mode().Perm()
			if mode == 0 {
				mode = 0o600
			}
			if wErr := writeJSONAtomic(configDir, configPath, root, mode); wErr != nil {
				return res, wErr
			}
			res.Changed = true
		}
	} else if os.IsNotExist(statErr) && len(res.RemovedPluginDirs) > 0 {
		root := map[string]any{
			"plugins": map[string]any{
				datacloudTelemetryPlugin: map[string]any{
					"enabled": false,
				},
			},
		}
		if wErr := writeJSONAtomic(configDir, configPath, root, 0o600); wErr != nil {
			return res, wErr
		}
		res.DisabledPlugins = append(res.DisabledPlugins, datacloudTelemetryPlugin)
		res.Changed = true
	}

	return res, nil
}

func writeJSONAtomic(dir, targetPath string, payload map[string]any, mode os.FileMode) error {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')

	tmpFile, err := os.CreateTemp(dir, ".config.json.tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Write(encoded); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func isBrokenPluginHooksJSON(pluginDir string, raw []byte) bool {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	var cmds []string
	collectHookCommands(doc, &cmds)
	if len(cmds) == 0 {
		return false
	}
	cleanPluginDir := filepath.Clean(pluginDir)
	for _, cmdStr := range cmds {
		if !commandReferencesOnlyMissingPluginPaths(cleanPluginDir, cmdStr) {
			return false
		}
	}
	return true
}

func collectHookCommands(node any, out *[]string) {
	switch v := node.(type) {
	case map[string]any:
		if rawCmd, ok := v["command"].(string); ok {
			typ, _ := v["type"].(string)
			if strings.TrimSpace(typ) == "" || strings.EqualFold(strings.TrimSpace(typ), "command") {
				if trimmed := strings.TrimSpace(rawCmd); trimmed != "" {
					*out = append(*out, trimmed)
				}
			}
		}
		for _, child := range v {
			collectHookCommands(child, out)
		}
	case []any:
		for _, item := range v {
			collectHookCommands(item, out)
		}
	}
}

func commandReferencesOnlyMissingPluginPaths(cleanPluginDir, cmdStr string) bool {
	expanded := strings.NewReplacer(
		"${CLAUDE_PLUGIN_ROOT}/", "",
		"${CLAUDE_PLUGIN_ROOT}\\", "",
		"${CLAUDE_PLUGIN_ROOT}", ".",
		"${GEMINI_PLUGIN_ROOT}/", "",
		"${GEMINI_PLUGIN_ROOT}\\", "",
		"${GEMINI_PLUGIN_ROOT}", ".",
		"${PLUGIN_ROOT}/", "",
		"${PLUGIN_ROOT}\\", "",
		"${PLUGIN_ROOT}", ".",
	).Replace(cmdStr)

	tokens := splitCommandTokens(expanded)
	var hasInternalPath bool
	var hasMissingPath bool
	for _, tok := range tokens {
		if tok == "||" || tok == "&&" || tok == ";" || tok == "|" {
			break
		}
		if strings.HasPrefix(tok, "-") || strings.Contains(tok, "://") {
			continue
		}
		candidate, inside := resolvePluginCandidatePath(cleanPluginDir, tok)
		if !inside {
			continue
		}
		hasInternalPath = true
		_, err := os.Stat(candidate)
		if err == nil {
			return false
		}
		if os.IsNotExist(err) {
			hasMissingPath = true
			continue
		}
		return false
	}
	return hasInternalPath && hasMissingPath
}

func resolvePluginCandidatePath(cleanPluginDir, tok string) (string, bool) {
	normalized := strings.ReplaceAll(tok, "\\", string(filepath.Separator))
	var candidate string
	if filepath.IsAbs(normalized) {
		candidate = filepath.Clean(normalized)
	} else {
		if !looksLikeRelativeFilePath(normalized) {
			return "", false
		}
		candidate = filepath.Clean(filepath.Join(cleanPluginDir, normalized))
	}
	rel, err := filepath.Rel(cleanPluginDir, candidate)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return candidate, true
}

func looksLikeRelativeFilePath(tok string) bool {
	if strings.Contains(tok, "/") || strings.Contains(tok, "\\") {
		return true
	}
	switch strings.ToLower(filepath.Ext(tok)) {
	case ".js", ".mjs", ".cjs", ".ts", ".py", ".sh", ".ps1", ".cmd", ".bat":
		return true
	default:
		return false
	}
}

func splitCommandTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	var quote rune
	inToken := false
	flush := func() {
		if inToken {
			tokens = append(tokens, cur.String())
			cur.Reset()
			inToken = false
		}
	}
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
			inToken = true
		case r == '"' || r == '\'':
			quote = r
			inToken = true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		default:
			cur.WriteRune(r)
			inToken = true
		}
	}
	flush()
	return tokens
}

// DetectStalePluginHookFailure detects Antigravity PreToolUse/hook failures caused
// by a missing ~/.gemini/config/plugins/<plugin> working directory.
func DetectStalePluginHookFailure(texts ...string) (matched bool, pluginName string) {
	for _, text := range texts {
		if m, p := matchStaleHookText(text); m {
			return true, p
		}
	}
	if len(texts) > 1 {
		if m, p := matchStaleHookText(strings.Join(texts, "\n")); m {
			return true, p
		}
	}
	return false, ""
}

func matchStaleHookText(text string) (bool, string) {
	if strings.TrimSpace(text) == "" {
		return false, ""
	}
	if m := staleChdirHookRe.FindStringSubmatch(text); len(m) == 2 {
		return true, m[1]
	}
	if m := staleJSONHookNameRe.FindStringSubmatch(text); len(m) == 2 {
		lower := strings.ToLower(text)
		if strings.Contains(lower, "chdir") && strings.Contains(lower, "no such file or directory") {
			return true, m[1]
		}
	}
	return false, ""
}
