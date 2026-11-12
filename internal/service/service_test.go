package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

func newNonTempTestHome(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(wd, "testdata", "homes")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(base, "home-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("IAZIO_TEST_MODE", "")
	t.Setenv("IAZIO_DISABLE_SERVICE_CONTROL", "")
	t.Setenv("GO_TEST", "")
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
		_ = os.Remove(base)
		_ = os.Remove(filepath.Join(wd, "testdata"))
	})
	return dir
}

func TestUnitContainsNoninteractive(t *testing.T) {
	text := UnitText("linux", "/usr/bin/iazio-agent")
	if !contains(text, "IAZIO_AGENT_NONINTERACTIVE=1") {
		t.Fatal(text)
	}
	plist := UnitText("darwin", "bin")
	if !contains(plist, "RunAtLoad") || !contains(plist, "KeepAlive") || !contains(plist, "io.iazio.iazio-agent") {
		t.Fatal(plist)
	}
}

func TestInstallDarwinBootstrapsUserAgent(t *testing.T) {
	t.Setenv("IAZIO_HARNESS_API_URL", "http://127.0.0.1:8090")
	home := newNonTempTestHome(t)
	var got []string
	err := InstallDarwinAt(home, "/Users/romeo/.local/bin/iazio-agent", func(name string, args ...string) error {
		got = append(got, name+" "+strings.Join(args, " "))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	plist := filepath.Join(home, "Library", "LaunchAgents", "io.iazio.iazio-agent.plist")
	body, err := os.ReadFile(plist)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, needle := range []string{"RunAtLoad", "KeepAlive", "<string>run</string>", "IAZIO_AGENT_NONINTERACTIVE", "IAZIO_HARNESS_API_URL", "ThrottleInterval"} {
		if !strings.Contains(text, needle) {
			t.Fatalf("missing %s in %s", needle, text)
		}
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "launchctl bootstrap") || !strings.Contains(joined, plist) {
		t.Fatal(joined)
	}
}

func TestDryRunText(t *testing.T) {
	t.Setenv("IAZIO_HARNESS_API_URL", "http://127.0.0.1:8090")
	tests := []struct {
		goos string
		need string
	}{
		{goos: "darwin", need: "IAZIO_HARNESS_API_URL"},
		{goos: "linux", need: "Environment=IAZIO_HARNESS_API_URL=http://127.0.0.1:8090"},
		{goos: "windows", need: "IAZIO_HARNESS_API_URL=http://127.0.0.1:8090"},
	}
	for _, tt := range tests {
		t.Run(tt.goos, func(t *testing.T) {
			called := false
			text, err := Apply(tt.goos, "install", "/opt/iazio-agent", true, false, func(string, ...string) error {
				called = true
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if called {
				t.Fatal("dry-run executed")
			}
			if !contains(text, "iazio-agent") || !contains(text, tt.need) {
				t.Fatal(text)
			}
		})
	}
	win, err := Apply("windows", "install", "/opt/iazio-agent", true, true, func(string, ...string) error {
		t.Fatal("dry-run executed")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !contains(win, "/F") {
		t.Fatal(win)
	}
}

func TestInstallLinuxAndWindowsUnits(t *testing.T) {
	home := newNonTempTestHome(t)
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}
	if err := InstallLinuxAt(home, "/usr/local/bin/iazio-agent", run); err != nil {
		t.Fatal(err)
	}
	unit, err := os.ReadFile(filepath.Join(home, ".config", "systemd", "user", "iazio-agent.service"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(unit), "WantedBy=default.target") || !strings.Contains(string(unit), "RestartSec=") || !strings.Contains(string(unit), "Restart=always") || !strings.Contains(string(unit), "WatchdogSec=30s") {
		t.Fatal(string(unit))
	}
	joined := strings.Join(cmds, "\n")
	if !strings.Contains(joined, "enable --now iazio-agent.service") || !strings.Contains(joined, "loginctl enable-linger") {
		t.Fatal(joined)
	}
	cmds = nil
	if err := InstallWindowsAt(home, `C:\bin\iazio-agent.exe`, "romeo", run); err != nil {
		t.Fatal(err)
	}
	xml, err := os.ReadFile(filepath.Join(home, ".iazio", "tasks", "iazio-agent.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(xml) < 2 || xml[0] != 0xFF || xml[1] != 0xFE {
		t.Fatalf("task XML must be UTF-16LE with BOM, got %d bytes", len(xml))
	}
	decoded := decodeUTF16LE(xml[2:])
	if !strings.Contains(decoded, "<UserId>romeo</UserId>") || !strings.Contains(decoded, "RestartOnFailure") {
		t.Fatal(decoded)
	}
	if !strings.Contains(decoded, `id="Author"`) || !strings.Contains(decoded, "LeastPrivilege") {
		t.Fatal(decoded)
	}
	if !strings.Contains(decoded, "MultipleInstancesPolicy") || !strings.Contains(decoded, "TimeTrigger") {
		t.Fatal(decoded)
	}
	if !strings.Contains(strings.Join(cmds, "\n"), "schtasks /Create") {
		t.Fatal(cmds)
	}
	if !strings.Contains(strings.Join(cmds, "\n"), "schtasks /Run") {
		t.Fatal(cmds)
	}
	if msg := WSLSystemdBlock(); strings.Contains(msg, "WSL") && !strings.Contains(msg, "wsl.conf") {
		t.Fatal(msg)
	}
}

func TestWSLSystemdBlockSentence(t *testing.T) {
	got := wslSystemdBlock("6.6.87.2-microsoft-standard-WSL2", true, false)
	if !strings.Contains(got, "wsl.conf") || !strings.Contains(got, "systemd=true") {
		t.Fatal(got)
	}
	if wslSystemdBlock("6.8.0-generic", true, false) != "" {
		t.Fatal("non-WSL release must not block")
	}
	if wslSystemdBlock("microsoft", true, true) != "" {
		t.Fatal("WSL with systemd must not block")
	}
	if wslSystemdBlock("", false, false) != "" {
		t.Fatal("missing osrelease must not block")
	}
}

func TestIsWSLWith(t *testing.T) {
	// From WSL_DISTRO_NAME
	if !IsWSLWith(func(k string) string {
		if k == "WSL_DISTRO_NAME" {
			return "Debian"
		}
		return ""
	}, nil) {
		t.Fatal("expected WSL detected from WSL_DISTRO_NAME")
	}

	// From WSL_INTEROP
	if !IsWSLWith(func(k string) string {
		if k == "WSL_INTEROP" {
			return "/run/WSL/10_interop"
		}
		return ""
	}, nil) {
		t.Fatal("expected WSL detected from WSL_INTEROP")
	}

	// From osrelease
	if !IsWSLWith(nil, func() ([]byte, error) {
		return []byte("6.6.87.2-microsoft-standard-WSL2"), nil
	}) {
		t.Fatal("expected WSL detected from osrelease containing microsoft")
	}

	// Non-WSL
	if IsWSLWith(func(string) string { return "" }, func() ([]byte, error) {
		return []byte("6.8.0-generic"), nil
	}) {
		t.Fatal("expected non-WSL not detected as WSL")
	}
}

func TestWindowsLifecycleCommands(t *testing.T) {
	var got []string
	run := func(name string, args ...string) error {
		got = append(got, name+" "+strings.Join(args, " "))
		return nil
	}
	cases := map[string]string{
		"start":     "schtasks /Run /TN iazio-agent",
		"stop":      "schtasks /End /TN iazio-agent",
		"uninstall": "schtasks /Delete /TN iazio-agent /F",
	}
	for action, want := range cases {
		got = nil
		if _, err := Apply("windows", action, "bin", false, false, run); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("%s: got %v", action, got)
		}
	}
}

func TestActionTextAndApply_EdgeCases(t *testing.T) {
	// 1. ActionText default action is install
	txt := ActionText("darwin", "", "bin", false)
	if !strings.Contains(txt, "io.iazio.iazio-agent") {
		t.Fatalf("expected darwin plist for default action, got: %s", txt)
	}

	// 2. ActionText windows install with force and API URL
	t.Setenv("IAZIO_HARNESS_API_URL", "http://harness.api")
	txtWin := ActionText("windows", "install", "bin", true)
	if !strings.Contains(txtWin, "/F") || !strings.Contains(txtWin, "IAZIO_HARNESS_API_URL=http://harness.api") {
		t.Fatalf("expected /F and API URL in windows install, got: %s", txtWin)
	}

	// 3. ActionText windows other actions
	txtWinStatus := ActionText("windows", "status", "bin", false)
	if !strings.Contains(txtWinStatus, "schtasks /Query") {
		t.Fatalf("expected Query, got: %s", txtWinStatus)
	}

	// 4. Apply with nil execFn returns text without error
	res, err := Apply("darwin", "start", "bin", false, false, nil)
	if err != nil || res == "" {
		t.Fatalf("expected success with nil execFn: res=%s err=%v", res, err)
	}

	// 5. Apply with dry=true does not invoke execFn
	res, err = Apply("linux", "status", "bin", true, false, func(string, ...string) error {
		t.Fatal("should not be called when dry is true")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 6. Apply when execFn fails
	_, err = Apply("linux", "start", "bin", false, false, func(string, ...string) error {
		return errors.New("command failed")
	})
	if err == nil {
		t.Fatal("expected error when execFn fails")
	}
}

func TestSuiteAllowed(t *testing.T) {
	tests := []struct {
		name    string
		running bool
		on      string
		expired bool
		allow   bool
	}{
		{name: "idle", allow: true},
		{name: "reject", running: true, on: "reject", expired: true, allow: false},
		{name: "abort waiting", running: true, on: "abort", allow: false},
		{name: "abort expired", running: true, on: "abort", expired: true, allow: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SuiteAllowed(tt.running, tt.on, tt.expired); got != tt.allow {
				t.Fatalf("allow %v, want %v", got, tt.allow)
			}
		})
	}
}

func TestUserDaemonLinuxFallback(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "bin", "iazio-agent")
	_ = os.MkdirAll(filepath.Dir(bin), 0o755)
	_ = os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0o755)

	var runs []string
	run := func(name string, args ...string) error {
		runs = append(runs, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallUserDaemonAt(home, bin, run); err != nil {
		t.Fatal(err)
	}
	runner := filepath.Join(home, ".iazio", "daemon", "runner.sh")
	if _, err := os.Stat(runner); err != nil {
		t.Fatalf("runner script missing: %v", err)
	}
	body, _ := os.ReadFile(runner)
	if !strings.Contains(string(body), "IAZIO_AGENT_NONINTERACTIVE=1") || !strings.Contains(string(body), "IAZIO_HARNESS_API_URL") {
		t.Fatalf("unexpected runner content: %s", string(body))
	}

	if err := UninstallUserDaemonAt(home, run); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runner); !os.IsNotExist(err) {
		t.Fatalf("runner should be removed after uninstall: %v", err)
	}
	if !strings.Contains(strings.Join(runs, " "), "crontab") {
		t.Fatalf("expected crontab command recorded in runs: %v", runs)
	}
}

func TestResolveElasticsearchURL(t *testing.T) {
	t.Setenv("IAZIO_ELASTICSEARCH_URL", "https://custom.es.local:9200")
	if got := resolveElasticsearchURL(); got != "https://custom.es.local:9200" {
		t.Fatalf("expected custom ES URL, got %q", got)
	}
	t.Setenv("IAZIO_ELASTICSEARCH_URL", "")
	home := newNonTempTestHome(t)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOME", home)
	cfgDir := filepath.Join(home, ".iazio")
	_ = os.MkdirAll(cfgDir, 0o755)
	_ = os.WriteFile(filepath.Join(cfgDir, "agent.json"), []byte(`{"elasticsearch_url":"https://agent.json.es:9200"}`), 0o600)
	if got := resolveElasticsearchURL(); got != "https://agent.json.es:9200" {
		t.Fatalf("expected agent.json ES URL, got %q", got)
	}
}

func TestResolveHarnessAPIURLDefaults(t *testing.T) {
	t.Setenv("IAZIO_HARNESS_API_URL", "")
	home := newNonTempTestHome(t)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOME", home)

	url := resolveHarnessAPIURL()
	if url != DefaultFleetAPI {
		t.Fatalf("expected DefaultFleetAPI %q, got %q", DefaultFleetAPI, url)
	}

	// Now set in config.json
	cfgDir := filepath.Join(home, ".iazio")
	_ = os.MkdirAll(cfgDir, 0o755)
	_ = os.WriteFile(filepath.Join(cfgDir, "config.json"), []byte(`{"endpoints":{"harness_api":"https://custom.fleet.local"}}`), 0o600)
	if got := resolveHarnessAPIURL(); got != "https://custom.fleet.local" {
		t.Fatalf("expected custom URL from config.json, got %q", got)
	}
}

func TestBuildInstallAndUninstallCrontab(t *testing.T) {
	runnerPath := "/tmp/runner.sh"
	existing := "# existing comment\n0 * * * * backup.sh\n* * * * * pgrep -f \"iazio-agent run\" >/dev/null || nohup /bin/sh \"/old/runner.sh\" >/dev/null 2>&1 &\n"

	installed := BuildInstallCrontab(existing, runnerPath)
	if !strings.Contains(installed, "0 * * * * backup.sh") {
		t.Errorf("expected backup.sh preserved, got %q", installed)
	}
	if strings.Contains(installed, "/old/runner.sh") {
		t.Errorf("expected /old/runner.sh removed, got %q", installed)
	}
	if !strings.Contains(installed, runnerPath) {
		t.Errorf("expected new runnerPath %q present, got %q", runnerPath, installed)
	}
	if !strings.Contains(installed, "@reboot nohup /bin/sh") {
		t.Errorf("expected @reboot entry present, got %q", installed)
	}

	uninstalled := BuildUninstallCrontab(installed)
	if strings.Contains(uninstalled, "iazio-agent") {
		t.Errorf("expected iazio-agent stripped, got %q", uninstalled)
	}
	if !strings.Contains(uninstalled, "0 * * * * backup.sh") {
		t.Errorf("expected backup.sh preserved after uninstall, got %q", uninstalled)
	}

	emptyUninstalled := BuildUninstallCrontab(BuildInstallCrontab("", runnerPath))
	if emptyUninstalled != "" {
		t.Errorf("expected empty string when only iazio-agent was present, got %q", emptyUninstalled)
	}
}

func TestControlArgv_AllPlatforms(t *testing.T) {
	// Darwin
	for _, act := range []string{"install", "uninstall", "start", "stop", "restart"} {
		args := controlArgv("darwin", act, false)
		if len(args) == 0 || args[0] != "launchctl" {
			t.Fatalf("expected launchctl for darwin %s, got %v", act, args)
		}
	}

	// Windows
	for _, act := range []string{"install", "start", "stop", "restart", "uninstall", "status"} {
		args := controlArgv("windows", act, true)
		if len(args) == 0 || args[0] != "schtasks" {
			t.Fatalf("expected schtasks for windows %s, got %v", act, args)
		}
	}

	// Linux
	for _, act := range []string{"install", "start", "stop", "restart", "status"} {
		args := controlArgv("linux", act, true)
		if len(args) == 0 || args[0] != "systemctl" {
			t.Fatalf("expected systemctl for linux %s, got %v", act, args)
		}
	}
}

func TestDaemonPIDLifecycle(t *testing.T) {
	home := newNonTempTestHome(t)
	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	_ = os.MkdirAll(filepath.Dir(pidPath), 0755)

	// Non-existent PID file
	if isDaemonPIDAlive(pidPath) {
		t.Fatal("expected false for missing pid file")
	}
	alive, _ := StatusUserDaemonAt(home)
	if alive {
		t.Fatal("expected false for StatusUserDaemonAt with missing file")
	}

	// Invalid PID content
	_ = os.WriteFile(pidPath, []byte("invalid-pid"), 0644)
	if isDaemonPIDAlive(pidPath) {
		t.Fatal("expected false for invalid pid file")
	}
	alive, _ = StatusUserDaemonAt(home)
	if alive {
		t.Fatal("expected false for StatusUserDaemonAt with invalid pid")
	}
	_ = StopUserDaemonAt(home)
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatal("expected invalid pid file cleaned up by StopUserDaemonAt")
	}

	// Alive PID (current process)
	myPID := os.Getpid()
	_ = os.WriteFile(pidPath, []byte(strconv.Itoa(myPID)), 0644)
	if !isDaemonPIDAlive(pidPath) {
		t.Fatal("expected true for current process PID")
	}
	alive, gotPID := StatusUserDaemonAt(home)
	if !alive || gotPID != myPID {
		t.Fatalf("expected alive=true and pid=%d, got alive=%v pid=%d", myPID, alive, gotPID)
	}
}

func TestAgentLogPath(t *testing.T) {
	path := agentLogPath()
	if !strings.Contains(path, "agent.log") {
		t.Fatalf("unexpected agentLogPath: %s", path)
	}
}

func TestInstallErrors(t *testing.T) {
	home := newNonTempTestHome(t)

	// InstallDarwinAt errors
	if err := InstallDarwinAt(home, "", func(string, ...string) error { return nil }); err == nil {
		t.Fatal("expected error on empty binary for InstallDarwinAt")
	}
	if err := InstallDarwinAt(home, "bin", nil); err == nil {
		t.Fatal("expected error on nil runner for InstallDarwinAt")
	}

	// InstallLinuxAt errors
	if err := InstallLinuxAt(home, "bin", nil); err == nil {
		t.Fatal("expected error on nil runner for InstallLinuxAt")
	}

	// InstallWindowsAt errors
	if err := InstallWindowsAt(home, "bin", "user", nil); err == nil {
		t.Fatal("expected error on nil runner for InstallWindowsAt")
	}

	// windowsLogonUser
	t.Setenv("USERNAME", "winuser")
	if windowsLogonUser() != "winuser" {
		t.Fatal("expected winuser")
	}
	t.Setenv("USERNAME", "")
	t.Setenv("USER", "unixuser")
	if windowsLogonUser() != "unixuser" {
		t.Fatal("expected unixuser")
	}

	// execCommand
	if err := execCommand("echo", "ok"); err != nil {
		t.Fatalf("expected nil for echo: %v", err)
	}
	if err := execCommand("nonexistent_service_cmd_xyz"); err == nil {
		t.Fatal("expected error on nonexistent command")
	}

	// IsWSL and IsSystemdAvailable
	_ = IsWSL()
	_ = IsSystemdAvailable()

	// StartUserDaemonAt branches with mock runner
	testHome := newNonTempTestHome(t)
	pidFile := filepath.Join(testHome, ".iazio", "agent.pid")
	_ = os.MkdirAll(filepath.Dir(pidFile), 0755)
	_ = os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644)
	// 1. Alive PID returns nil immediately
	if err := StartUserDaemonAt(testHome, "bin", func(string, ...string) error { return errors.New("should not run") }); err != nil {
		t.Fatal(err)
	}

	// 2. Not alive, runnerPath exists, calls run
	_ = os.Remove(pidFile)
	runnerFile := filepath.Join(testHome, ".iazio", "daemon", "runner.sh")
	_ = os.MkdirAll(filepath.Dir(runnerFile), 0755)
	_ = os.WriteFile(runnerFile, []byte("#!/bin/sh\n"), 0755)
	var startedCmd string
	err := StartUserDaemonAt(testHome, "bin", func(name string, args ...string) error {
		startedCmd = name + " " + strings.Join(args, " ")
		return nil
	})
	if err != nil || !strings.Contains(startedCmd, runnerFile) {
		t.Fatalf("expected runner script executed, got err=%v cmd=%s", err, startedCmd)
	}
}

func TestInstallDarwinAt(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "bin", "iazio-agent")

	if err := InstallDarwinAt(home, "", func(string, ...string) error { return nil }); err == nil {
		t.Fatal("expected error for empty bin")
	}
	if err := InstallDarwinAt(home, bin, nil); err == nil {
		t.Fatal("expected error for nil runner")
	}

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallDarwinAt(home, bin, runner); err != nil {
		t.Fatalf("InstallDarwinAt failed: %v", err)
	}

	plistPath := filepath.Join(home, "Library", "LaunchAgents", agentLabel+".plist")
	if _, err := os.Stat(plistPath); err != nil {
		t.Fatalf("plist missing: %v", err)
	}
	if len(commands) < 3 {
		t.Fatalf("expected at least 3 launchctl commands, got %v", commands)
	}

	// Test bootstrap failure error handling
	failBootstrap := func(name string, args ...string) error {
		if len(args) > 0 && args[0] == "bootstrap" {
			return errors.New("bootstrap failed")
		}
		return nil
	}
	if err := InstallDarwinAt(home, bin, failBootstrap); err == nil {
		t.Fatal("expected error when bootstrap fails")
	}

	// Test enable failure error handling
	failEnable := func(name string, args ...string) error {
		if len(args) > 0 && args[0] == "enable" {
			return errors.New("enable failed")
		}
		return nil
	}
	if err := InstallDarwinAt(home, bin, failEnable); err == nil {
		t.Fatal("expected error when enable fails")
	}
}

func TestInstallLinuxAt(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "bin", "iazio-agent")

	if err := InstallLinuxAt(home, bin, nil); err == nil {
		t.Fatal("expected error for nil runner")
	}

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallLinuxAt(home, bin, runner); err != nil {
		t.Fatalf("InstallLinuxAt failed: %v", err)
	}

	unitPath := filepath.Join(home, ".config", "systemd", "user", "iazio-agent.service")
	if _, err := os.Stat(unitPath); err != nil {
		t.Fatalf("service unit missing: %v", err)
	}
	if len(commands) != 3 {
		t.Fatalf("expected 3 systemctl/loginctl commands, got %v", commands)
	}

	// Test command failure error propagation
	failRunner := func(name string, args ...string) error {
		return errors.New("command failed")
	}
	if err := InstallLinuxAt(home, bin, failRunner); err == nil {
		t.Fatal("expected error when runner fails")
	}
}

func TestInstallWindowsAt(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "bin", "iazio-agent.exe")

	if err := InstallWindowsAt(home, bin, "DOMAIN\\user", nil); err == nil {
		t.Fatal("expected error for nil runner")
	}

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallWindowsAt(home, bin, "DOMAIN\\user", runner); err != nil {
		t.Fatalf("InstallWindowsAt failed: %v", err)
	}

	batPath := filepath.Join(home, ".iazio", "tasks", "iazio-agent.bat")
	xmlPath := filepath.Join(home, ".iazio", "tasks", "iazio-agent.xml")
	if _, err := os.Stat(batPath); err == nil {
		t.Fatalf("bat should not exist: %v", batPath)
	}
	if _, err := os.Stat(xmlPath); err != nil {
		t.Fatalf("xml missing: %v", err)
	}
	xmlData, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("read xml: %v", err)
	}
	if len(xmlData) < 2 || xmlData[0] != 0xFF || xmlData[1] != 0xFE {
		t.Fatalf("expected UTF-16LE with BOM, got %d bytes", len(xmlData))
	}
	xmlStr := decodeUTF16LE(xmlData[2:])
	if !strings.Contains(xmlStr, "<LogonType>S4U</LogonType>") {
		t.Fatalf("expected S4U logon type in xml, got: %s", xmlStr)
	}
	if !strings.Contains(xmlStr, "<Hidden>true</Hidden>") {
		t.Fatalf("expected <Hidden>true</Hidden> in xml, got: %s", xmlStr)
	}
	if !strings.Contains(xmlStr, bin) {
		t.Fatalf("expected direct command %s in xml, got: %s", bin, xmlStr)
	}
	if !strings.Contains(xmlStr, "run --log-file") {
		t.Fatalf("expected run --log-file argument in xml, got: %s", xmlStr)
	}
	if len(commands) != 2 || !strings.Contains(commands[0], "schtasks /Create") {
		t.Fatalf("unexpected schtasks command: %v", commands)
	}

	// Verify S4U access-denied fallback to InteractiveToken and default bin resolution
	createAttempts := 0
	fallbackRunner := func(name string, args ...string) error {
		if len(args) > 0 && args[0] == "/Create" {
			createAttempts++
			if createAttempts == 1 {
				return fmt.Errorf("ERROR: Access is denied")
			}
		}
		return nil
	}
	if err := InstallWindowsAt(home, "", "DOMAIN\\user", fallbackRunner); err != nil {
		t.Fatalf("InstallWindowsAt S4U fallback failed: %v", err)
	}
	if createAttempts != 2 {
		t.Fatalf("expected 2 schtasks /Create attempts on S4U access denied, got %d", createAttempts)
	}

	// Test reconcileWindowsServiceConfiguration directly (missing XML -> installs, then healthy XML -> starts stopped task)
	tasksDir := filepath.Join(home, ".iazio", "tasks")
	_ = os.WriteFile(filepath.Join(tasksDir, "iazio-harness.bat"), []byte("bad"), 0o644)
	_ = os.Remove(xmlPath)

	origStatusCmd := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCmd }()

	if err := reconcileWindowsServiceConfiguration(home, "", runner); err != nil {
		t.Fatalf("reconcileWindowsServiceConfiguration install branch failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tasksDir, "iazio-harness.bat")); !os.IsNotExist(err) {
		t.Fatalf("expected iazio-harness.bat to be purged")
	}

	// Now XML exists with S4U, PT1M, and iazio-agent.exe; simulate schtasks query showing task stopped
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("TaskName: \\iazio-agent\nTask To Run: C:\\Users\\user\\bin\\iazio-agent.exe run\nStatus: Ready\n"), nil
	}
	if err := reconcileWindowsServiceConfiguration(home, bin, runner); err != nil {
		t.Fatalf("reconcileWindowsServiceConfiguration healthy branch failed: %v", err)
	}

	// Simulate schtasks query showing legacy .bat -> triggers reinstall
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("TaskName: \\iazio-agent\nTask To Run: C:\\Users\\user\\.iazio\\tasks\\iazio-agent.bat\nStatus: Running\n"), nil
	}
	if err := reconcileWindowsServiceConfiguration(home, bin, runner); err != nil {
		t.Fatalf("reconcileWindowsServiceConfiguration legacy bat branch failed: %v", err)
	}
}

func TestDaemonPIDLifecycleExtended(t *testing.T) {
	home := newNonTempTestHome(t)
	pidDir := filepath.Join(home, ".iazio")
	_ = os.MkdirAll(pidDir, 0o755)
	pidFile := filepath.Join(pidDir, "agent.pid")

	// Status when file doesn't exist
	alive, pid := StatusUserDaemonAt(home)
	if alive || pid != 0 {
		t.Fatalf("expected not alive when no pid file, got %v, %d", alive, pid)
	}

	// Stop when file doesn't exist
	if err := StopUserDaemonAt(home); err != nil {
		t.Fatal(err)
	}

	// Status when corrupt PID
	_ = os.WriteFile(pidFile, []byte("invalid-pid"), 0o600)
	alive, pid = StatusUserDaemonAt(home)
	if alive || pid != 0 {
		t.Fatalf("expected not alive on invalid pid")
	}
	_ = StopUserDaemonAt(home)
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatal("expected corrupt pid file cleaned up by StopUserDaemonAt")
	}

	// Status when negative or zero PID
	_ = os.WriteFile(pidFile, []byte("0"), 0o600)
	alive, pid = StatusUserDaemonAt(home)
	if alive || pid != 0 {
		t.Fatalf("expected not alive on zero pid")
	}
	_ = StopUserDaemonAt(home)

	// Status with current process PID (alive)
	myPID := strconv.Itoa(os.Getpid())
	_ = os.WriteFile(pidFile, []byte(myPID), 0o600)
	alive, pid = StatusUserDaemonAt(home)
	if !alive || pid != os.Getpid() {
		t.Fatalf("expected alive for current pid, got alive=%v pid=%d", alive, pid)
	}

	// StartUserDaemonAt when already alive
	err := StartUserDaemonAt(home, "bin", func(string, ...string) error {
		t.Fatal("should not invoke runner if already alive")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(pidFile)
}

func TestSetupUserCrontab(t *testing.T) {
	var runArgs []string
	runner := func(name string, args ...string) error {
		runArgs = append(runArgs, name+" "+strings.Join(args, " "))
		return nil
	}
	if err := setupUserCrontab("/tmp/runner.sh", runner); err != nil {
		t.Fatal(err)
	}
	if len(runArgs) == 0 || !strings.Contains(runArgs[0], "crontab -") {
		t.Fatalf("expected crontab runner invocation, got %v", runArgs)
	}
}

func TestInstallWrapperFunctions(t *testing.T) {
	fakeHome := newNonTempTestHome(t)
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)

	origExec := execCommand
	t.Cleanup(func() {
		execCommand = origExec
	})
	execCommand = func(string, ...string) error {
		return nil
	}

	bin := filepath.Join(fakeHome, "bin", "iazio-agent")

	// 1. InstallDarwin
	if err := InstallDarwin(bin); err != nil {
		t.Fatalf("InstallDarwin failed: %v", err)
	}

	// 2. InstallUserDaemon
	if err := InstallUserDaemon(bin); err != nil {
		t.Fatalf("InstallUserDaemon failed: %v", err)
	}

	// 3. InstallLinux
	if err := InstallLinux(bin); err != nil {
		t.Fatalf("InstallLinux failed: %v", err)
	}

	// 4. InstallWindows
	if err := InstallWindows(bin); err != nil {
		t.Fatalf("InstallWindows failed: %v", err)
	}
}

func TestResilienceUnitsAndWatcher(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".iazio", "bin", "iazio-agent")

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallLinuxResilience(home, bin, runner); err != nil {
		t.Fatalf("InstallLinuxResilience failed: %v", err)
	}

	unitDir := filepath.Join(home, ".config", "systemd", "user")
	for _, unitName := range []string{
		"iazio-agent-watcher.path",
		"iazio-agent-watcher.service",
		"iazio-agent-watchdog.timer",
		"iazio-agent-watchdog.service",
	} {
		p := filepath.Join(unitDir, unitName)
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("expected unit file %s: %v", unitName, err)
		}
		if len(content) == 0 {
			t.Fatalf("unit file %s is empty", unitName)
		}
	}

	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "enable --now iazio-agent-watcher.path") ||
		!strings.Contains(joined, "enable --now iazio-agent-watchdog.timer") {
		t.Fatalf("expected watcher and watchdog enable commands, got: %s", joined)
	}
}

func TestPauseResumeAndReviveCheck(t *testing.T) {
	home := newNonTempTestHome(t)
	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	// 1. Pause
	if err := PauseService(home, runner); err != nil {
		t.Fatalf("PauseService failed: %v", err)
	}
	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("expected lock file: %v", err)
	}

	// 2. ReviveCheck while paused -> should do nothing (no start command)
	commands = nil
	if err := ReviveCheck(home, runner); err != nil {
		t.Fatalf("ReviveCheck failed: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected 0 commands while paused, got %v", commands)
	}

	// 3. Resume
	if err := ResumeService(home, runner); err != nil {
		t.Fatalf("ResumeService failed: %v", err)
	}
	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatal("expected lock file to be removed")
	}
}

func TestDevInstallHotSwap(t *testing.T) {
	home := newNonTempTestHome(t)
	srcDir := newNonTempTestHome(t)
	srcBin := filepath.Join(srcDir, "iazio-agent-dev")
	if err := os.WriteFile(srcBin, []byte("#!/bin/sh\necho dev\n"), 0755); err != nil {
		t.Fatal(err)
	}

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := DevInstall(srcBin, home, runner); err != nil {
		t.Fatalf("DevInstall failed: %v", err)
	}

	dest := filepath.Join(home, ".local", "bin", "iazio-agent")
	if runtime.GOOS == "windows" {
		dest = filepath.Join(home, "bin", "iazio-agent.exe")
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("installed binary not found: %v", err)
	}
	if !strings.Contains(string(data), "echo dev") {
		t.Fatalf("unexpected content in installed binary: %s", string(data))
	}
	// Verify secondary binary in .iazio/bin was purged
	secondaryDest := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	if _, err := os.Stat(secondaryDest); !os.IsNotExist(err) {
		t.Fatalf("expected secondary binary in .iazio/bin to be purged, got: %v", err)
	}
	joined := strings.Join(commands, "\n")
	if runtime.GOOS == "darwin" {
		if !strings.Contains(joined, "bootstrap") || !strings.Contains(joined, "kickstart") {
			t.Fatalf("expected bootstrap and kickstart commands on darwin, got:\n%s", joined)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func decodeUTF16LE(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	units := make([]uint16, len(b)/2)
	for i := range units {
		units[i] = uint16(b[i*2]) | uint16(b[i*2+1])<<8
	}
	return string(utf16.Decode(units))
}

func TestDarwinResilienceUnits(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	logPath := filepath.Join(home, ".iazio", "logs", "agent.log")

	watcherPlist := DarwinWatcherPlist(bin, logPath)
	if !strings.Contains(watcherPlist, watcherLabel) {
		t.Fatalf("expected watcher label, got %s", watcherPlist)
	}
	if !strings.Contains(watcherPlist, "<key>WatchPaths</key>") || !strings.Contains(watcherPlist, bin) {
		t.Fatalf("expected WatchPaths with %s, got %s", bin, watcherPlist)
	}
	if !strings.Contains(watcherPlist, "revive-check") {
		t.Fatalf("expected revive-check in watcher plist, got %s", watcherPlist)
	}

	watchdogPlist := DarwinWatchdogPlist(bin, logPath)
	if !strings.Contains(watchdogPlist, watchdogLabel) {
		t.Fatalf("expected watchdog label, got %s", watchdogPlist)
	}
	if !strings.Contains(watchdogPlist, "<key>StartInterval</key>\n\t<integer>120</integer>") {
		t.Fatalf("expected StartInterval 120, got %s", watchdogPlist)
	}
	if !strings.Contains(watchdogPlist, "revive-check") {
		t.Fatalf("expected revive-check, got %s", watchdogPlist)
	}

	var commands []string
	runner := func(name string, args ...string) error {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallDarwinResilience(home, bin, runner); err != nil {
		t.Fatalf("InstallDarwinResilience failed: %v", err)
	}

	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	if _, err := os.Stat(filepath.Join(launchAgentsDir, watcherLabel+".plist")); err != nil {
		t.Fatalf("watcher plist missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(launchAgentsDir, watchdogLabel+".plist")); err != nil {
		t.Fatalf("watchdog plist missing: %v", err)
	}

	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "bootstrap") || !strings.Contains(joined, "enable") {
		t.Fatalf("expected bootstrap and enable commands, got:\n%s", joined)
	}

	// Uninstall resilience
	commands = nil
	if err := UninstallDarwinResilience(home, runner); err != nil {
		t.Fatalf("UninstallDarwinResilience failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(launchAgentsDir, watcherLabel+".plist")); !os.IsNotExist(err) {
		t.Fatalf("expected watcher plist removed")
	}
	if _, err := os.Stat(filepath.Join(launchAgentsDir, watchdogLabel+".plist")); !os.IsNotExist(err) {
		t.Fatalf("expected watchdog plist removed")
	}
}

func TestReviveCheckDarwin(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	_ = os.MkdirAll(filepath.Dir(bin), 0755)
	_ = os.WriteFile(bin, []byte("#!/bin/sh\n"), 0755)

	var runCommands []string
	runner := func(name string, args ...string) error {
		runCommands = append(runCommands, name+" "+strings.Join(args, " "))
		return nil
	}

	// 1. Service is dead (status check returns exited/no pid) -> ReviveCheck triggers kickstart
	restore := SetExecCommandForTest(runner, func(name string, args ...string) ([]byte, error) {
		return []byte("job state = exited\nlast exit code = 78"), nil
	})
	defer restore()

	if err := ReviveCheck(home, runner); err != nil {
		t.Fatalf("ReviveCheck failed: %v", err)
	}
	joined := strings.Join(runCommands, "\n")
	if runtime.GOOS == "darwin" {
		if !strings.Contains(joined, "bootout") || !strings.Contains(joined, "bootstrap") || !strings.Contains(joined, "kickstart") {
			t.Fatalf("expected bootout, bootstrap, and kickstart on dead darwin service, got:\n%s", joined)
		}
	}

	// 2. Service is running -> ReviveCheck does nothing
	runCommands = nil
	restore2 := SetExecCommandForTest(runner, func(name string, args ...string) ([]byte, error) {
		return []byte("pid = 4567\nstate = running"), nil
	})
	defer restore2()

	if err := ReviveCheck(home, runner); err != nil {
		t.Fatalf("ReviveCheck running failed: %v", err)
	}
	if runtime.GOOS == "darwin" && len(runCommands) != 0 {
		t.Fatalf("expected 0 commands when service running, got: %v", runCommands)
	}
}

func TestDarwinPlistHardening(t *testing.T) {
	plist := darwinPlist("/opt/iazio/bin/iazio-agent", "/opt/iazio/logs/agent.log")
	if !strings.Contains(plist, "<key>AbandonProcessGroup</key>\n\t<true/>") {
		t.Errorf("expected AbandonProcessGroup true in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>ThrottleInterval</key>\n\t<integer>3</integer>") {
		t.Errorf("expected ThrottleInterval 3s in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>PathState</key>") || !strings.Contains(plist, "maintenance.lock") {
		t.Errorf("expected PathState maintenance.lock in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<false/>") {
		t.Errorf("expected <false/> for maintenance.lock PathState in plist, got:\n%s", plist)
	}
	if strings.Contains(plist, "SuccessfulExit") {
		t.Errorf("expected SuccessfulExit to be absent for Restart=always parity in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>SoftResourceLimits</key>") || !strings.Contains(plist, "<integer>65536</integer>") {
		t.Errorf("expected SoftResourceLimits in plist, got:\n%s", plist)
	}
}

func TestParseSchtasksStatus(t *testing.T) {
	out := "Folder: \\\nTaskName: \\iazio-agent\nStatus: Running\n"
	running, status, ok := ParseSchtasksStatus(out)
	if !ok || !running || status != "Running" {
		t.Fatalf("unexpected result: ok=%v, running=%v, status=%s", ok, running, status)
	}

	outReady := "Status: Ready\n"
	running, status, ok = ParseSchtasksStatus(outReady)
	if !ok || running || status != "Ready" {
		t.Fatalf("unexpected ready result: ok=%v, running=%v, status=%s", ok, running, status)
	}

	outEmpty := "No status here\n"
	running, status, ok = ParseSchtasksStatus(outEmpty)
	if ok || running || status != "" {
		t.Fatalf("expected not ok for empty status")
	}
}

func TestPauseAndResumeService(t *testing.T) {
	home := newNonTempTestHome(t)
	runner := func(name string, args ...string) error {
		return nil
	}
	if err := PauseService(home, runner); err != nil {
		t.Fatalf("PauseService failed: %v", err)
	}
	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("expected maintenance.lock to exist")
	}
	if err := ResumeService(home, runner); err != nil {
		t.Fatalf("ResumeService failed: %v", err)
	}
	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatalf("expected maintenance.lock to be removed")
	}
}

func TestUserDaemonAt_Helpers(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".local", "bin", "iazio-agent")
	_ = os.MkdirAll(filepath.Dir(bin), 0755)
	_ = os.WriteFile(bin, []byte("#!/bin/sh\n"), 0755)

	runner := func(name string, args ...string) error {
		return nil
	}

	alive, pid := StatusUserDaemonAt(home)
	if alive || pid != 0 {
		t.Fatalf("expected inactive daemon")
	}

	if err := StartUserDaemonAt(home, bin, runner); err != nil {
		t.Fatalf("StartUserDaemonAt failed: %v", err)
	}

	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	_ = os.WriteFile(pidPath, []byte("99999999\n"), 0644)
	alive, _ = StatusUserDaemonAt(home)
	if alive {
		t.Fatalf("expected non-existent pid to be not alive")
	}

	_ = StopUserDaemonAt(home)
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("expected agent.pid to be removed")
	}

	if err := UninstallUserDaemonAt(home, runner); err != nil {
		t.Fatalf("UninstallUserDaemonAt failed: %v", err)
	}
}

func TestCopyBinaryAtomic(t *testing.T) {
	tmp := newNonTempTestHome(t)
	src := filepath.Join(tmp, "src.bin")
	_ = os.WriteFile(src, []byte("atomic-bytes"), 0755)

	if err := copyBinaryAtomic(src, src); err != nil {
		t.Fatalf("expected no-op: %v", err)
	}

	dest := filepath.Join(tmp, "dest.bin")
	if err := copyBinaryAtomic(src, dest); err != nil {
		t.Fatalf("copyBinaryAtomic failed: %v", err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "atomic-bytes" {
		t.Fatalf("unexpected content: %s", string(data))
	}
}

func TestWindowsLogonUser(t *testing.T) {
	t.Setenv("USERNAME", "winuser")
	if u := windowsLogonUser(); u != "winuser" {
		t.Fatalf("expected winuser, got %s", u)
	}
	t.Setenv("USERNAME", "")
	t.Setenv("USER", "unixuser")
	if u := windowsLogonUser(); u != "unixuser" {
		t.Fatalf("expected unixuser, got %s", u)
	}
}

func TestInstallLinuxResilience(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "iazio-agent")
	var calls []string
	runner := func(name string, args ...string) error {
		calls = append(calls, name+" "+strings.Join(args, " "))
		return nil
	}
	if err := InstallLinuxResilience(home, bin, runner); err != nil {
		t.Fatalf("InstallLinuxResilience failed: %v", err)
	}
	if len(calls) < 3 {
		t.Fatalf("expected at least 3 systemctl calls, got %v", calls)
	}
}

func TestIsSystemdAvailable(t *testing.T) {
	_ = IsSystemdAvailable()
}

func TestInstallDarwinResilience(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, "iazio-agent")
	var calls []string
	runner := func(name string, args ...string) error {
		calls = append(calls, name+" "+strings.Join(args, " "))
		return nil
	}
	if err := InstallDarwinResilience(home, bin, runner); err != nil {
		t.Fatalf("InstallDarwinResilience failed: %v", err)
	}
	if len(calls) < 4 {
		t.Fatalf("expected at least 4 launchctl calls, got %v", calls)
	}
}

func TestDevInstall_DefaultExecutable(t *testing.T) {
	home := newNonTempTestHome(t)
	runner := func(name string, args ...string) error {
		return nil
	}
	if err := DevInstall("", home, runner); err != nil {
		t.Fatalf("DevInstall with default executable failed: %v", err)
	}
}

func TestReviveCheck_LinuxInactive(t *testing.T) {
	home := newNonTempTestHome(t)
	var runCalls []string
	runner := func(name string, args ...string) error {
		runCalls = append(runCalls, name+" "+strings.Join(args, " "))
		return nil
	}
	restore := SetExecCommandForTest(runner, func(name string, args ...string) ([]byte, error) {
		return nil, errors.New("inactive")
	})
	defer restore()

	_ = ReviveCheck(home, runner)
	_ = ReviveCheck("", runner)
}

func TestPauseAndResume_EmptyHome(t *testing.T) {
	runner := func(name string, args ...string) error { return nil }
	_ = PauseService("", runner)
	_ = ResumeService("", runner)
}

func TestSetupUserCrontab_DefaultRunner(t *testing.T) {
	var applied string
	restore := SetCrontabCommandForTest(
		func() ([]byte, error) {
			return []byte("# existing crontab\n* * * * * echo hi\n"), nil
		},
		func(stdin string) error {
			applied = stdin
			return nil
		},
		nil,
	)
	defer restore()

	if err := setupUserCrontab("/usr/local/bin/iazio-runner.sh", nil); err != nil {
		t.Fatalf("setupUserCrontab failed: %v", err)
	}
	if !strings.Contains(applied, "/usr/local/bin/iazio-runner.sh") {
		t.Fatalf("expected runner in crontab, got:\n%s", applied)
	}

	restoreErr := SetCrontabCommandForTest(
		func() ([]byte, error) { return nil, errors.New("no cron") },
		func(stdin string) error { return errors.New("cannot apply") },
		nil,
	)
	defer restoreErr()
	if err := setupUserCrontab("/usr/local/bin/iazio-runner.sh", nil); err == nil {
		t.Fatal("expected error from crontab apply")
	}
}

func TestStartUserDaemonAt_DirectExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("user daemon shell runner uses /bin/sh on unix")
	}
	home := newNonTempTestHome(t)
	daemonDir := filepath.Join(home, ".iazio", "daemon")
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		t.Fatal(err)
	}
	runnerScript := filepath.Join(daemonDir, "runner.sh")
	if err := os.WriteFile(runnerScript, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := StartUserDaemonAt(home, "/bin/iazio-agent", nil); err != nil {
		t.Fatalf("StartUserDaemonAt with nil runner failed: %v", err)
	}
}

func TestUninstallUserDaemonAt_DefaultCrontab(t *testing.T) {
	home := newNonTempTestHome(t)
	pidFile := filepath.Join(home, ".iazio", "agent.pid")
	_ = os.MkdirAll(filepath.Dir(pidFile), 0o755)
	_ = os.WriteFile(pidFile, []byte("99999999"), 0o644)

	var removed bool
	var applied string
	restore := SetCrontabCommandForTest(
		func() ([]byte, error) {
			return []byte(BuildInstallCrontab("", "/tmp/runner.sh")), nil
		},
		func(stdin string) error {
			applied = stdin
			return nil
		},
		func() error {
			removed = true
			return nil
		},
	)
	defer restore()

	if err := UninstallUserDaemonAt(home); err != nil {
		t.Fatalf("UninstallUserDaemonAt failed: %v", err)
	}
	if !removed {
		t.Fatal("expected crontab -r to be invoked when crontab becomes empty")
	}

	restoreNonEmpty := SetCrontabCommandForTest(
		func() ([]byte, error) {
			return []byte("# user job\n* * * * * echo hi\n" + BuildInstallCrontab("", "/tmp/runner.sh")), nil
		},
		func(stdin string) error {
			applied = stdin
			return nil
		},
		nil,
	)
	defer restoreNonEmpty()

	if err := UninstallUserDaemonAt(home); err != nil {
		t.Fatalf("UninstallUserDaemonAt with non-empty crontab failed: %v", err)
	}
	if !strings.Contains(applied, "echo hi") {
		t.Fatalf("expected other user jobs preserved in crontab, got:\n%s", applied)
	}
}

func TestInstallDarwinResilience_ErrorCases(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := "/usr/local/bin/iazio-agent"

	err1 := InstallDarwinResilience(home, bin, func(name string, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "bootstrap") && strings.Contains(strings.Join(args, " "), "watcher") {
			return errors.New("fail bootstrap watcher")
		}
		return nil
	})
	if err1 == nil || !strings.Contains(err1.Error(), "fail bootstrap watcher") {
		t.Fatalf("expected bootstrap watcher error, got %v", err1)
	}

	err2 := InstallDarwinResilience(home, bin, func(name string, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "enable") && strings.Contains(strings.Join(args, " "), "watcher") {
			return errors.New("fail enable watcher")
		}
		return nil
	})
	if err2 == nil || !strings.Contains(err2.Error(), "fail enable watcher") {
		t.Fatalf("expected enable watcher error, got %v", err2)
	}

	err3 := InstallDarwinResilience(home, bin, func(name string, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "bootstrap") && strings.Contains(strings.Join(args, " "), "watchdog") {
			return errors.New("fail bootstrap watchdog")
		}
		return nil
	})
	if err3 == nil || !strings.Contains(err3.Error(), "fail bootstrap watchdog") {
		t.Fatalf("expected bootstrap watchdog error, got %v", err3)
	}

	err4 := InstallDarwinResilience(home, bin, func(name string, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "enable") && strings.Contains(strings.Join(args, " "), "watchdog") {
			return errors.New("fail enable watchdog")
		}
		return nil
	})
	if err4 == nil || !strings.Contains(err4.Error(), "fail enable watchdog") {
		t.Fatalf("expected enable watchdog error, got %v", err4)
	}
}

func TestInstallLinuxResilience_ErrorCase(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := "/usr/local/bin/iazio-agent"

	err := InstallLinuxResilience(home, bin, func(name string, args ...string) error {
		if name == "systemctl" && len(args) > 0 && args[len(args)-1] == "daemon-reload" {
			return errors.New("daemon reload error")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "daemon reload error") {
		t.Fatalf("expected daemon reload error, got %v", err)
	}
}

func TestReviveCheck_ExpiredLock(t *testing.T) {
	home := newNonTempTestHome(t)
	lockDir := filepath.Join(home, ".iazio")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	lockFile := filepath.Join(lockDir, "maintenance.lock")
	if err := os.WriteFile(lockFile, []byte("paused"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-3 * time.Hour)
	_ = os.Chtimes(lockFile, oldTime, oldTime)

	var runCalled bool
	runner := func(name string, args ...string) error {
		runCalled = true
		return nil
	}
	_ = runCalled
	_ = ReviveCheck(home, runner)
	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatal("expected expired maintenance.lock to be removed")
	}
}

func TestPauseAndResume_DefaultRunner(t *testing.T) {
	home := newNonTempTestHome(t)
	var runCalls []string
	restore := SetExecCommandForTest(func(name string, args ...string) error {
		runCalls = append(runCalls, name+" "+strings.Join(args, " "))
		return nil
	})
	defer restore()

	if err := PauseService(home, nil); err != nil {
		t.Fatalf("PauseService failed: %v", err)
	}
	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("expected maintenance.lock to exist: %v", err)
	}

	if err := ResumeService(home, nil); err != nil {
		t.Fatalf("ResumeService failed: %v", err)
	}
	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatalf("expected maintenance.lock to be removed")
	}

	if IsSystemdAvailable() && len(runCalls) == 0 {
		t.Fatal("expected default execCommand to be invoked on systemd host")
	}
}

func TestWindowsLogonUser_EnvVariants(t *testing.T) {
	t.Setenv("USERNAME", "alice")
	if u := windowsLogonUser(); u != "alice" {
		t.Fatalf("expected alice, got %q", u)
	}
	t.Setenv("USERNAME", "")
	t.Setenv("USER", "bob")
	if u := windowsLogonUser(); u != "bob" {
		t.Fatalf("expected bob, got %q", u)
	}
	t.Setenv("USERNAME", "")
	t.Setenv("USER", "")
	u := windowsLogonUser()
	if u == "" {
		t.Fatal("expected non-empty user from current user fallback")
	}
}

func TestReconcileServiceConfiguration(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	// 1. Initial Linux reconcile: writes missing files and rewrites .iazio/bin -> .local/bin
	if err := reconcileLinuxServiceConfiguration(home, bin, run); err != nil {
		t.Fatalf("reconcileLinuxServiceConfiguration failed: %v", err)
	}

	unitFile := filepath.Join(home, ".config", "systemd", "user", "iazio-agent.service")
	data, err := os.ReadFile(unitFile)
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	if !strings.Contains(string(data), "WatchdogSec=30s") {
		t.Fatalf("expected WatchdogSec=30s in reconciled unit, got:\n%s", string(data))
	}

	// 2. Corrupted unit without WatchdogSec
	_ = os.WriteFile(unitFile, []byte("[Service]\nExecStart=/some/bin\n"), 0644)
	cmds = nil
	if err := reconcileLinuxServiceConfiguration(home, bin, run); err != nil {
		t.Fatalf("reconcileLinuxServiceConfiguration failed: %v", err)
	}
	data, _ = os.ReadFile(unitFile)
	if !strings.Contains(string(data), "WatchdogSec=30s") {
		t.Fatalf("expected WatchdogSec=30s after healing, got:\n%s", string(data))
	}
}

func TestReconcileDarwinServiceConfiguration_SelfBootoutGuard(t *testing.T) {
	home := newNonTempTestHome(t)
	canonicalBin := filepath.Join(home, ".local", "bin", "iazio-agent")
	harnessBin := filepath.Join(home, ".local", "bin", "iazio-harness")
	if err := os.MkdirAll(filepath.Dir(canonicalBin), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(canonicalBin, []byte("#!/bin/sh\n"), 0o755)
	_ = os.WriteFile(harnessBin, []byte("#!/bin/sh\n"), 0o755)

	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(launchAgentsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	plistPath := filepath.Join(launchAgentsDir, DarwinLabel+".plist")
	// Write a drifted plist pointing to ~/.iazio/bin/iazio-agent
	_ = os.WriteFile(plistPath, []byte("<plist><string>"+filepath.Join(home, ".iazio", "bin", "iazio-agent")+"</string></plist>"), 0o644)

	origStatusCheck := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCheck }()
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte(fmt.Sprintf(
			"state = running\npid = %d\npath = /private/var/folders/l8/7kps/T/TestE2E_Gov3799/001/Library/LaunchAgents/io.iazio.iazio-agent.plist\nprogram = /var/folders/l8/7kps/T/TestE2E_Gov3799/001/.local/bin/iazio-agent\nlast exit code = 78: EX_CONFIG\n",
			os.Getpid(),
		)), nil
	}

	t.Setenv("XPC_SERVICE_NAME", DarwinLabel)
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := reconcileDarwinServiceConfiguration(home, filepath.Join(home, ".iazio", "bin", "iazio-agent"), run); err != nil {
		t.Fatalf("reconcileDarwinServiceConfiguration failed: %v", err)
	}

	// Verify plist on disk was updated to canonical .local/bin/iazio-agent
	updatedPlist, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read updated plist: %v", err)
	}
	if !strings.Contains(string(updatedPlist), canonicalBin) {
		t.Fatalf("expected updated plist to reference %s, got:\n%s", canonicalBin, string(updatedPlist))
	}

	// Verify launchctl bootout was NEVER called on io.iazio.iazio-agent
	for _, c := range cmds {
		if strings.Contains(c, "bootout") && strings.Contains(c, DarwinLabel) && !strings.Contains(c, DarwinWatcherLabel) && !strings.Contains(c, DarwinWatchdogLabel) {
			t.Fatalf("must NEVER call launchctl bootout on %s when running inside the service, got command: %s", DarwinLabel, c)
		}
	}

	// Verify any legacy iazio-harness plist is purged per ADR-002
	harnessPlistPath := filepath.Join(launchAgentsDir, "io.iazio.iazio-harness.plist")
	if _, err := os.Stat(harnessPlistPath); !os.IsNotExist(err) {
		t.Fatalf("ADR-002 violation: expected harness plist %s not to be installed, err=%v", harnessPlistPath, err)
	}
}

func TestInstallDarwinResilience_WatchdogSelfBootoutGuard(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".local", "bin", "iazio-agent")

	origStatusCheck := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCheck }()
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("state = running\n"), nil
	}

	t.Setenv("XPC_SERVICE_NAME", DarwinWatchdogLabel)
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	// First call writes plists, must NOT bootout watchdog when XPC_SERVICE_NAME == DarwinWatchdogLabel
	if err := InstallDarwinResilience(home, bin, run); err != nil {
		t.Fatalf("InstallDarwinResilience failed: %v", err)
	}
	for _, c := range cmds {
		if strings.Contains(c, "bootout") && strings.Contains(c, DarwinWatchdogLabel) {
			t.Fatalf("must not bootout %s when XPC_SERVICE_NAME == %s, got: %s", DarwinWatchdogLabel, DarwinWatchdogLabel, c)
		}
	}

	// Second call with unchanged plists and already loaded services should not call bootout at all
	cmds = nil
	t.Setenv("XPC_SERVICE_NAME", "")
	if err := InstallDarwinResilience(home, bin, run); err != nil {
		t.Fatalf("second InstallDarwinResilience failed: %v", err)
	}
	for _, c := range cmds {
		if strings.Contains(c, "bootout") {
			t.Fatalf("expected no bootout when plists unchanged and already loaded, got: %s", c)
		}
	}
}

func TestLinuxUnitHardening(t *testing.T) {
	text := UnitText("linux", "/usr/bin/iazio-agent")
	needles := []string{
		"StartLimitIntervalSec=0",
		"Nice=10",
		"IOSchedulingClass=idle",
		"IOSchedulingPriority=7",
		"EnvironmentFile=-%h/.iazio/agent.env",
		"StandardOutput=journal",
		"StandardError=journal",
		"RestartSec=3s",
		"KillMode=process",
		"WatchdogSec=30s",
		"OOMScoreAdjust=-500",
	}
	for _, needle := range needles {
		if !strings.Contains(text, needle) {
			t.Errorf("missing %q in linux unit:\n%s", needle, text)
		}
	}
}

func TestReviveCheck_MaintenanceLockActive(t *testing.T) {
	tmpDir := newNonTempTestHome(t)
	lockDir := filepath.Join(tmpDir, ".iazio")
	_ = os.MkdirAll(lockDir, 0755)
	lockFile := filepath.Join(lockDir, "maintenance.lock")
	_ = os.WriteFile(lockFile, []byte("active"), 0644)

	err := ReviveCheck(tmpDir, func(name string, args ...string) error { return nil })
	if err != nil {
		t.Fatalf("expected nil for active maintenance lock, got %v", err)
	}
}

func TestLegacyHarnessUnitsPurgedOnLinuxReconcile(t *testing.T) {
	home := newNonTempTestHome(t)
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	legacyUnits := []string{
		"iazio-harness.service",
		"iazio-harness-watchdog.timer",
		"iazio-harness-watchdog.service",
		"iazio-harness-watcher.path",
		"iazio-harness-watcher.service",
	}
	for _, u := range legacyUnits {
		if err := os.WriteFile(filepath.Join(unitDir, u), []byte("[Unit]\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var cmds []string
	runner := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}
	if err := reconcileLinuxServiceConfiguration(home, filepath.Join(home, ".local", "bin", "iazio-agent"), runner); err != nil {
		t.Fatalf("reconcileLinuxServiceConfiguration failed: %v", err)
	}
	for _, u := range legacyUnits {
		if _, err := os.Stat(filepath.Join(unitDir, u)); !os.IsNotExist(err) {
			t.Fatalf("expected legacy unit %s to be deleted, got err=%v", u, err)
		}
	}
}

func TestRestartToolService(t *testing.T) {
	var cmds []string
	runner := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	_ = RestartToolService("iazio-agent", runner)
	_ = RestartToolService("iazio-harness", runner)
	_ = RestartToolService("autopilot", runner)

	txt := ActionText("linux", "reconcile", "/bin/iazio-agent", false)
	if !strings.Contains(txt, "reconcile user service") {
		t.Fatalf("expected reconcile text, got: %s", txt)
	}
}

func TestReconcileServiceConfiguration_AllBranches(t *testing.T) {
	home := newNonTempTestHome(t)
	binDir := filepath.Join(home, ".iazio", "bin")
	_ = os.MkdirAll(binDir, 0755)
	agentBin := filepath.Join(binDir, "iazio-agent")
	_ = os.WriteFile(agentBin, []byte("#!/bin/sh\n"), 0755)
	harnessBin := filepath.Join(binDir, "iazio-harness")
	_ = os.WriteFile(harnessBin, []byte("#!/bin/sh\n"), 0755)

	var cmds []string
	runner := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	err := ReconcileServiceConfiguration(home, agentBin, runner)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Run again when files already exist
	err = ReconcileServiceConfiguration(home, agentBin, runner)
	if err != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err)
	}

	// ReviveCheck without maintenance lock
	_ = ReviveCheck(home, runner)
}

func TestIsTempOrTestPath_And_ServiceControlDisabled(t *testing.T) {
	if !IsTempOrTestPath("/private/var/folders/l8/7kpsgc7s/T/TestE2E_Gov3799/001") {
		t.Fatal("expected /private/var/folders/.../TestE2E_... to be detected as temp/test path")
	}
	if !IsTempOrTestPath("/var/folders/l8/7kpsgc7s/T/TestE2E_Gov3799/001") {
		t.Fatal("expected /var/folders/... to be detected as temp/test path")
	}
	if !IsTempOrTestPath("/tmp/test-home-123") {
		t.Fatal("expected /tmp/... to be detected as temp/test path")
	}
	if !IsTempOrTestPath(t.TempDir()) {
		t.Fatal("expected t.TempDir() to be detected as temp/test path")
	}
	if IsTempOrTestPath("/Users/romeo") || IsTempOrTestPath("/home/romeo") {
		t.Fatal("expected real user home not to be flagged as temp/test path")
	}
	if IsTempOrTestPath("") {
		t.Fatal("expected empty path not to be flagged as temp/test path")
	}
	if !IsServiceControlDisabled() {
		t.Fatal("expected IsServiceControlDisabled() to be true inside go test process")
	}
}

func TestInstallDarwinResilience_HealsPoisonedTempPathAndExConfig78(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".local", "bin", "iazio-agent")
	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	logPath := filepath.Join(home, ".iazio", "logs", "agent.log")
	if err := os.MkdirAll(launchAgentsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatal(err)
	}

	// Pre-populate canonical watcher and watchdog plists on disk so watcherChanged/watchdogChanged are false.
	watcherPlistPath := filepath.Join(launchAgentsDir, watcherLabel+".plist")
	watchdogPlistPath := filepath.Join(launchAgentsDir, watchdogLabel+".plist")
	if err := os.WriteFile(watcherPlistPath, []byte(DarwinWatcherPlist(bin, logPath)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(watchdogPlistPath, []byte(DarwinWatchdogPlist(bin, logPath)), 0o644); err != nil {
		t.Fatal(err)
	}

	// Simulate launchd in-memory state poisoned by a deleted TestE2E temp dir with 78: EX_CONFIG (spawn failed).
	origStatusCheck := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCheck }()
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte(
			"gui/501/io.iazio.iazio-agent-watcher = {\n" +
				"\tpath = /private/var/folders/l8/7kps/T/TestE2E_Gov3799/001/Library/LaunchAgents/io.iazio.iazio-agent-watcher.plist\n" +
				"\tprogram = /var/folders/l8/7kps/T/TestE2E_Gov3799/001/.local/bin/iazio-agent\n" +
				"\tjob state = spawn failed\n" +
				"\tlast exit code = 78: EX_CONFIG\n" +
				"\tproperties = { penalty box }\n" +
				"}\n",
		), nil
	}

	t.Setenv("XPC_SERVICE_NAME", "")
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := InstallDarwinResilience(home, bin, run); err != nil {
		t.Fatalf("InstallDarwinResilience failed: %v", err)
	}

	joined := strings.Join(cmds, "\n")
	for _, required := range []string{
		"bootout gui/" + strconv.Itoa(os.Getuid()) + "/" + watcherLabel,
		"bootstrap gui/" + strconv.Itoa(os.Getuid()) + " " + watcherPlistPath,
		"enable gui/" + strconv.Itoa(os.Getuid()) + "/" + watcherLabel,
		"bootout gui/" + strconv.Itoa(os.Getuid()) + "/" + watchdogLabel,
		"bootstrap gui/" + strconv.Itoa(os.Getuid()) + " " + watchdogPlistPath,
		"enable gui/" + strconv.Itoa(os.Getuid()) + "/" + watchdogLabel,
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("expected %q when healing poisoned in-memory launchd plist path / EX_CONFIG, got:\n%s", required, joined)
		}
	}
}

func TestReconcileDarwinServiceConfiguration_PurgesInMemoryOrphanHarnessAndDeterministicPath(t *testing.T) {
	home := newNonTempTestHome(t)
	bin := filepath.Join(home, ".local", "bin", "iazio-agent")
	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	logPath := filepath.Join(home, ".iazio", "logs", "agent.log")
	if err := os.MkdirAll(launchAgentsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatal(err)
	}

	// Verify darwinPlist generates identical content regardless of caller PATH.
	t.Setenv("PATH", "/custom/conda/bin:/usr/bin")
	plist1 := darwinPlist(bin, logPath)
	t.Setenv("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
	plist2 := darwinPlist(bin, logPath)
	if plist1 != plist2 {
		t.Fatalf("expected deterministic darwinPlist regardless of caller PATH, got:\n%s\nvs\n%s", plist1, plist2)
	}

	// Pre-write canonical plists on disk; do NOT create io.iazio.iazio-harness.plist on disk (already deleted).
	agentPlistPath := filepath.Join(launchAgentsDir, DarwinLabel+".plist")
	if err := os.WriteFile(agentPlistPath, []byte(plist1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(launchAgentsDir, watcherLabel+".plist"), []byte(DarwinWatcherPlist(bin, logPath)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(launchAgentsDir, watchdogLabel+".plist"), []byte(DarwinWatchdogPlist(bin, logPath)), 0o644); err != nil {
		t.Fatal(err)
	}

	// Simulate io.iazio.iazio-agent healthy in launchd AND orphan io.iazio.iazio-harness still loaded in launchd memory.
	origStatusCheck := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCheck }()
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "print" {
			target := args[1]
			if strings.HasSuffix(target, "/io.iazio.iazio-harness") ||
				strings.HasSuffix(target, "/io.iazio.iazio-harness-watcher") ||
				strings.HasSuffix(target, "/io.iazio.iazio-harness-watchdog") {
				return []byte(target + " = {\n\tstate = waiting\n\truns = 12747\n\tlast exit code = 1\n}\n"), nil
			}
			if strings.HasSuffix(target, "/"+DarwinLabel) {
				return []byte(fmt.Sprintf("%s = {\n\tstate = running\n\tpid = 99999\n\tpath = %s\n\tprogram = %s\n}\n", target, agentPlistPath, bin)), nil
			}
			return []byte("state = waiting\n"), nil
		}
		return nil, errors.New("unexpected command")
	}

	t.Setenv("XPC_SERVICE_NAME", "")
	var cmds []string
	run := func(name string, args ...string) error {
		cmds = append(cmds, name+" "+strings.Join(args, " "))
		return nil
	}

	if err := reconcileDarwinServiceConfiguration(home, bin, run); err != nil {
		t.Fatalf("reconcileDarwinServiceConfiguration failed: %v", err)
	}

	joined := strings.Join(cmds, "\n")
	// Must bootout in-memory orphan io.iazio.iazio-harness jobs even though .plist files are absent on disk
	for _, label := range []string{"io.iazio.iazio-harness", "io.iazio.iazio-harness-watcher", "io.iazio.iazio-harness-watchdog"} {
		expectedBootout := "bootout gui/" + strconv.Itoa(os.Getuid()) + "/" + label
		if !strings.Contains(joined, expectedBootout) {
			t.Fatalf("expected %q for orphan in-memory harness LaunchAgent, got:\n%s", expectedBootout, joined)
		}
	}
	// Must NOT bootout healthy running io.iazio.iazio-agent
	if strings.Contains(joined, "bootout gui/"+strconv.Itoa(os.Getuid())+"/"+DarwinLabel+"\n") ||
		strings.HasSuffix(joined, "bootout gui/"+strconv.Itoa(os.Getuid())+"/"+DarwinLabel) {
		t.Fatalf("must not bootout healthy running %s, got:\n%s", DarwinLabel, joined)
	}

	// Verify A1, A3, A4, F1, F2, F6:
	// 1. Running service with historical last exit code = 78 or penalty box is NOT flagged as drifted.
	runningWithHistory := fmt.Sprintf("state = running\npid = 99999\npath = %s\nprogram = %s\nlast exit code = 78: EX_CONFIG\nproperties = { penalty box }\n", agentPlistPath, bin)
	if isDarwinLaunchdServiceDrifted(runningWithHistory, agentPlistPath, bin) {
		t.Fatal("expected running service with matching path/program and historical exit 78 not to be flagged as drifted")
	}

	// 1b. Non-running idle StartInterval job with "penalty box" alone (no exit 78 / spawn failed) is NOT flagged as drifted (Finding A4).
	idlePenaltyBox := fmt.Sprintf("state = waiting\npath = %s\nprogram = %s\nproperties = { penalty box }\n", agentPlistPath, bin)
	if isDarwinLaunchdServiceDrifted(idlePenaltyBox, agentPlistPath, bin) {
		t.Fatal("expected idle StartInterval job with penalty box alone not to be flagged as drifted")
	}

	// 1c. Exact PID parsing does not prefix-match pid = 12 against pid = 123 (Finding A4).
	if hasExactDarwinPID("state = running\npid = 123\n", 12) {
		t.Fatal("expected hasExactDarwinPID not to prefix-match pid=12 against pid=123")
	}
	if !hasExactDarwinPID("state = running\npid = 123\n", 123) {
		t.Fatal("expected hasExactDarwinPID to match exact pid=123")
	}

	// 2a. Self-identity first (Finding A1): when pid == os.Getpid() and state = running,
	// isRunningInsideDarwinAgentService returns true even if path is drifted, preventing self-bootout.
	t.Setenv("XPC_SERVICE_NAME", DarwinLabel)
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte(fmt.Sprintf("state = running\npid = %d\npath = /private/var/folders/poisoned/io.iazio.iazio-agent.plist\nprogram = %s\n", os.Getpid(), bin)), nil
	}
	if !isRunningInsideDarwinAgentService("gui/501/"+DarwinLabel, agentPlistPath, bin) {
		t.Fatal("expected isRunningInsideDarwinAgentService=true when loaded pid equals os.Getpid() even if path is drifted (Finding A1 self-bootout guard)")
	}
	// Verify reconcileDarwinServiceConfiguration never boots out io.iazio.iazio-agent when loaded pid == os.Getpid() even with drifted path.
	cmds = nil
	if err := reconcileDarwinServiceConfiguration(home, bin, run); err != nil {
		t.Fatalf("reconcileDarwinServiceConfiguration inside running self-pid agent failed: %v", err)
	}
	if strings.Contains(strings.Join(cmds, "\n"), "bootout gui/"+strconv.Itoa(os.Getuid())+"/"+DarwinLabel+"\n") ||
		strings.HasSuffix(strings.Join(cmds, "\n"), "bootout gui/"+strconv.Itoa(os.Getuid())+"/"+DarwinLabel) {
		t.Fatalf("must NEVER bootout own running launchd job when pid == os.Getpid(), got:\n%s", strings.Join(cmds, "\n"))
	}

	// 2b. When pid != os.Getpid() (e.g. child/CLI process with XPC_SERVICE_NAME inherited) and plist path is drifted,
	// isRunningInsideDarwinAgentService returns false (AC2.3).
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte(fmt.Sprintf("state = running\npid = 99999\npath = /private/var/folders/poisoned/io.iazio.iazio-agent.plist\nprogram = %s\n", bin)), nil
	}
	if isRunningInsideDarwinAgentService("gui/501/"+DarwinLabel, agentPlistPath, bin) {
		t.Fatal("expected isRunningInsideDarwinAgentService=false when pid != os.Getpid() and loaded plist path is drifted")
	}
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte(fmt.Sprintf("state = running\npid = 99999\npath = %s\nprogram = %s\n", agentPlistPath, bin)), nil
	}
	if !isRunningInsideDarwinAgentService("gui/501/"+DarwinLabel, agentPlistPath, bin) {
		t.Fatal("expected isRunningInsideDarwinAgentService=true when loaded plist path matches canonical and state=running")
	}

	// 3. Reconciling when io.iazio.iazio-agent itself is drifted (temp path / EX_CONFIG) boots out, bootstraps, and enables it.
	t.Setenv("XPC_SERVICE_NAME", "")
	cmds = nil
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "print" && strings.HasSuffix(args[1], "/"+DarwinLabel) {
			return []byte("state = spawn failed\npath = /private/var/folders/TestE2E_123/io.iazio.iazio-agent.plist\nlast exit code = 78: EX_CONFIG\n"), nil
		}
		return []byte("state = waiting\n"), nil
	}
	if err := reconcileDarwinServiceConfiguration(home, bin, run); err != nil {
		t.Fatalf("reconcileDarwinServiceConfiguration on drifted agent failed: %v", err)
	}
	joinedDriftedAgent := strings.Join(cmds, "\n")
	for _, req := range []string{
		"bootout gui/" + strconv.Itoa(os.Getuid()) + "/" + DarwinLabel,
		"bootstrap gui/" + strconv.Itoa(os.Getuid()) + " " + agentPlistPath,
		"enable gui/" + strconv.Itoa(os.Getuid()) + "/" + DarwinLabel,
	} {
		if !strings.Contains(joinedDriftedAgent, req) {
			t.Fatalf("expected %q when healing drifted primary agent, got:\n%s", req, joinedDriftedAgent)
		}
	}

	// 4. insideWatcher guard: when XPC_SERVICE_NAME == DarwinWatcherLabel, InstallDarwinResilience never boots out watcher.
	t.Setenv("XPC_SERVICE_NAME", DarwinWatcherLabel)
	cmds = nil
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		return []byte("state = running\npath = /private/var/folders/drifted/watcher.plist\n"), nil
	}
	if err := InstallDarwinResilience(home, bin, run); err != nil {
		t.Fatalf("InstallDarwinResilience insideWatcher failed: %v", err)
	}
	if strings.Contains(strings.Join(cmds, "\n"), "bootout gui/"+strconv.Itoa(os.Getuid())+"/"+watcherLabel) {
		t.Fatalf("must never bootout watcher from insideWatcher context, got:\n%s", strings.Join(cmds, "\n"))
	}

	// 5. resolveDarwinPlistHome fallback when logPath is not under .iazio/logs
	if gotHome := resolveDarwinPlistHome(filepath.Join("/Users/testuser", ".local", "bin", "iazio-agent"), "/var/log/custom.log"); gotHome == "" {
		t.Fatal("expected non-empty resolved home from resolveDarwinPlistHome fallback")
	}
}

func TestInstallDarwinAt_TempHomeSuppressesExplicitRunner(t *testing.T) {
	nonTempHome := newNonTempTestHome(t)
	t.Setenv("HOME", nonTempHome)
	t.Setenv("IAZIO_TEST_MODE", "")
	t.Setenv("IAZIO_DISABLE_SERVICE_CONTROL", "")
	t.Setenv("GO_TEST", "")

	origStatusCheck := statusCheckCommand
	defer func() { statusCheckCommand = origStatusCheck }()
	var statusProbes []string
	statusCheckCommand = func(name string, args ...string) ([]byte, error) {
		statusProbes = append(statusProbes, name+" "+strings.Join(args, " "))
		return []byte("state = running\n"), nil
	}

	tempHome := t.TempDir()
	var recorded []string
	recordingRunner := func(name string, args ...string) error {
		recorded = append(recorded, name+" "+strings.Join(args, " "))
		return nil
	}

	binPath := filepath.Join(nonTempHome, ".local", "bin", "iazio-agent")
	if err := InstallDarwinAt(tempHome, binPath, recordingRunner); err != nil {
		t.Fatalf("InstallDarwinAt with tempHome failed: %v", err)
	}
	if err := reconcileDarwinServiceConfiguration(tempHome, binPath, recordingRunner); err != nil {
		t.Fatalf("reconcileDarwinServiceConfiguration with tempHome failed: %v", err)
	}
	if len(recorded) != 0 {
		t.Fatalf("expected zero launchctl mutations to reach explicit runner when home is a temp path, got: %v", recorded)
	}
	if len(statusProbes) != 0 {
		t.Fatalf("expected zero launchctl print status probes to reach statusCheckCommand when home is a temp path, got: %v", statusProbes)
	}
	if err := CheckExplicitServiceControlAllowed(tempHome, binPath); !errors.Is(err, ErrServiceControlSuppressed) {
		t.Fatalf("expected ErrServiceControlSuppressed for tempHome, got: %v", err)
	}
}
