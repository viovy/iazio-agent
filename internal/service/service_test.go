package service

import "testing"

func TestUnitContainsNoninteractive(t *testing.T) {
	text := UnitText("linux", "/usr/bin/iazio-agent")
	if !contains(text, "IAZIO_AGENT_NONINTERACTIVE=1") {
		t.Fatal(text)
	}
	if !contains(UnitText("darwin", "bin"), "RunAtLoad") {
		t.Fatal("plist")
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
