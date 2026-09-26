package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	for _, needle := range []string{"RunAtLoad", "KeepAlive", "<string>run</string>", "IAZIO_AGENT_NONINTERACTIVE", "ThrottleInterval"} {
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
	tests := []struct {
		goos string
		need string
	}{
		{goos: "darwin", need: "IAZIO_AGENT_NONINTERACTIVE"},
		{goos: "linux", need: "Environment=IAZIO_AGENT_NONINTERACTIVE=1"},
		{goos: "windows", need: "IAZIO_AGENT_NONINTERACTIVE=1"},
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
	if !strings.Contains(string(unit), "WantedBy=default.target") || !strings.Contains(string(unit), "RestartSec=60s") {
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
	if !strings.Contains(string(xml), "<UserId>romeo</UserId>") || !strings.Contains(string(xml), "RestartOnFailure") {
		t.Fatal(string(xml))
	}
	if !strings.Contains(strings.Join(cmds, "\n"), "schtasks /Create") {
		t.Fatal(cmds)
	}
	if msg := WSLSystemdBlock(); strings.Contains(msg, "WSL") && !strings.Contains(msg, "wsl.conf") {
		t.Fatal(msg)
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
