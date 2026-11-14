package agyhooks

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestResolveAgentHome_IsolatedUnderGoTest(t *testing.T) {
	if !IsTestProcess() {
		t.Fatalf("expected IsTestProcess() == true under go test")
	}
	if got := ResolveAgentHome(func(string) string { return "" }, true); got != "" {
		t.Fatalf("expected empty resolved home when getenv empty in test, got %q", got)
	}
	tmp := t.TempDir()
	got := ResolveAgentHome(func(k string) string {
		if k == "HOME" {
			return tmp
		}
		return ""
	}, true)
	if got != tmp {
		t.Fatalf("expected %q, got %q", tmp, got)
	}

	res, err := SanitizeUserGeminiHooks("")
	if err != nil {
		t.Fatalf("SanitizeUserGeminiHooks(\"\") failed: %v", err)
	}
	if res.Changed {
		t.Fatalf("expected no-op for empty home in test, got %+v", res)
	}
}

func TestSanitizeUserGeminiHooks_NoOpWhenGeminiMissing(t *testing.T) {
	home := t.TempDir()
	res, err := SanitizeUserGeminiHooks(home)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Changed || len(res.RemovedPluginDirs) != 0 || len(res.DisabledPlugins) != 0 {
		t.Fatalf("expected no-op result, got %+v", res)
	}
	if _, statErr := os.Stat(filepath.Join(home, ".gemini")); !os.IsNotExist(statErr) {
		t.Fatalf("expected .gemini not to be created, statErr=%v", statErr)
	}
}

func TestSanitizeUserGeminiHooks_PrunesStalePreservesValidAndPins0600(t *testing.T) {
	home := t.TempDir()
	cfgDir := filepath.Join(home, ".gemini", "config")
	pluginsDir := filepath.Join(cfgDir, "plugins")

	dtDir := filepath.Join(pluginsDir, "googlecloudtools.datacloud_telemetry")
	brokenCustom := filepath.Join(pluginsDir, "broken.custom_hook")
	validDak := filepath.Join(pluginsDir, "data-agent-kit-plugin")
	for _, d := range []string{
		dtDir,
		brokenCustom,
		filepath.Join(validDak, "hooks"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	if err := os.WriteFile(filepath.Join(dtDir, "hooks.json"), []byte(`{"PreToolUse":[{"type":"command","command":"node hooks/dt.js"}]}`), 0o644); err != nil {
		t.Fatalf("write dt hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenCustom, "hooks.json"), []byte(`{"PreToolUse":[{"type":"command","command":"node hooks/missing.js"}]}`), 0o644); err != nil {
		t.Fatalf("write broken hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(validDak, "hooks", "telemetry_hook.js"), []byte("// valid\n"), 0o644); err != nil {
		t.Fatalf("write valid dak script: %v", err)
	}
	if err := os.WriteFile(filepath.Join(validDak, "hooks.json"), []byte(`{"dak-telemetry":{"PreToolUse":[{"matcher":"*","hooks":[{"type":"command","command":"node hooks/telemetry_hook.js || exit 0"}]}]}}`), 0o644); err != nil {
		t.Fatalf("write valid dak hooks.json: %v", err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Symlink(filepath.Join(home, "nonexistent-target"), filepath.Join(pluginsDir, "dangling-link")); err != nil {
			t.Fatalf("create dangling symlink: %v", err)
		}
	}

	cfgPath := filepath.Join(cfgDir, "config.json")
	initialCfg := `{
  "userSettings": {
    "theme": "dark"
  },
  "plugins": {
    "googlecloudtools.datacloud_telemetry": {
      "enabled": true,
      "customField": "keep"
    },
    "data-agent-kit-plugin": {
      "enabled": true
    },
    "missing-manual-plugin": {
      "enabled": true
    },
    "flutter": {
      "enabled": true,
      "installedFrom": "marketplace"
    }
  }
}
`
	if err := os.WriteFile(cfgPath, []byte(initialCfg), 0o600); err != nil {
		t.Fatalf("write config.json: %v", err)
	}

	res, err := SanitizeUserGeminiHooks(home)
	if err != nil {
		t.Fatalf("SanitizeUserGeminiHooks failed: %v", err)
	}
	if !res.Changed {
		t.Fatalf("expected res.Changed == true, got %+v", res)
	}
	if _, err := os.Stat(dtDir); !os.IsNotExist(err) {
		t.Fatalf("expected %s removed", dtDir)
	}
	if _, err := os.Stat(brokenCustom); !os.IsNotExist(err) {
		t.Fatalf("expected %s removed", brokenCustom)
	}
	if _, err := os.Stat(validDak); err != nil {
		t.Fatalf("expected valid marketplace plugin preserved: %v", err)
	}

	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat config.json: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", info.Mode().Perm())
	}

	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config.json: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal config.json: %v", err)
	}
	if us, ok := parsed["userSettings"].(map[string]any); !ok || us["theme"] != "dark" {
		t.Fatalf("expected userSettings preserved, got %#v", parsed["userSettings"])
	}
	pluginsMap := parsed["plugins"].(map[string]any)
	dtCfg := pluginsMap["googlecloudtools.datacloud_telemetry"].(map[string]any)
	if dtCfg["enabled"] != false || dtCfg["customField"] != "keep" {
		t.Fatalf("unexpected dt entry: %#v", dtCfg)
	}
	if pluginsMap["data-agent-kit-plugin"].(map[string]any)["enabled"] != true {
		t.Fatalf("expected valid plugin enabled=true")
	}
	if pluginsMap["missing-manual-plugin"].(map[string]any)["enabled"] != false {
		t.Fatalf("expected missing manual plugin disabled")
	}
	if pluginsMap["flutter"].(map[string]any)["enabled"] != true {
		t.Fatalf("expected installedFrom plugin preserved")
	}

	// Idempotent second pass.
	res2, err := SanitizeUserGeminiHooks(home)
	if err != nil {
		t.Fatalf("second sanitize pass failed: %v", err)
	}
	if res2.Changed {
		t.Fatalf("expected second pass no-op, got %+v", res2)
	}
}

func TestDetectStalePluginHookFailure(t *testing.T) {
	matched, plugin := DetectStalePluginHookFailure(
		`JSON hook "jsonhook__googlecloudtools.datacloud_telemetry_PreToolUse_0_0" failed:`,
		`command failed: chdir /Users/romeo/.gemini/config/plugins/googlecloudtools.datacloud_telemetry: no such file or directory`,
	)
	if !matched || plugin != "googlecloudtools.datacloud_telemetry" {
		t.Fatalf("expected match for googlecloudtools.datacloud_telemetry, got matched=%v plugin=%q", matched, plugin)
	}
	if matched2, _ := DetectStalePluginHookFailure("normal output without hook failure"); matched2 {
		t.Fatalf("unexpected match on benign text")
	}
}
