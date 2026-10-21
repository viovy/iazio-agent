package service

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"
)

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
	home := t.TempDir()
	var got []string
	err := InstallDarwinAt(home, "/Users/romeo/.iazio/bin/iazio-agent", func(name string, args ...string) error {
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
	home := t.TempDir()
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
	if !strings.Contains(string(unit), "WantedBy=default.target") || !strings.Contains(string(unit), "RestartSec=3s") || !strings.Contains(string(unit), "Restart=always") {
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()

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
	testHome := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	if _, err := os.Stat(batPath); err != nil {
		t.Fatalf("bat missing: %v", err)
	}
	if _, err := os.Stat(xmlPath); err != nil {
		t.Fatalf("xml missing: %v", err)
	}
	if len(commands) != 2 || !strings.Contains(commands[0], "schtasks /Create") {
		t.Fatalf("unexpected schtasks command: %v", commands)
	}
}

func TestDaemonPIDLifecycleExtended(t *testing.T) {
	home := t.TempDir()
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
	fakeHome := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
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
	home := t.TempDir()
	srcDir := t.TempDir()
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

	dest := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("installed binary not found: %v", err)
	}
	if !strings.Contains(string(data), "echo dev") {
		t.Fatalf("unexpected content in installed binary: %s", string(data))
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
	home := t.TempDir()
	bin := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	logPath := filepath.Join(home, ".iazio", "logs", "agent.log")

	watcherPlist := DarwinWatcherPlist(bin, logPath)
	if !strings.Contains(watcherPlist, watcherLabel) {
		t.Fatalf("expected watcher label, got %s", watcherPlist)
	}
	if !strings.Contains(watcherPlist, "<key>WatchPaths</key>") || !strings.Contains(watcherPlist, bin) {
		t.Fatalf("expected WatchPaths with %s, got %s", bin, watcherPlist)
	}
	if !strings.Contains(watcherPlist, "kickstart") {
		t.Fatalf("expected kickstart in watcher plist, got %s", watcherPlist)
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
	home := t.TempDir()
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
	if runtime.GOOS == "darwin" && !strings.Contains(joined, "kickstart") {
		t.Fatalf("expected kickstart on dead darwin service, got:\n%s", joined)
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
	if !strings.Contains(plist, "<key>ThrottleInterval</key>\n\t<integer>3</integer>") {
		t.Errorf("expected ThrottleInterval 3s in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>PathState</key>") || !strings.Contains(plist, "maintenance.lock") {
		t.Errorf("expected PathState maintenance.lock in plist, got:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>SoftResourceLimits</key>") || !strings.Contains(plist, "<integer>65536</integer>") {
		t.Errorf("expected SoftResourceLimits in plist, got:\n%s", plist)
	}
}
