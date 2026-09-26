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

func TestSuiteAllowed(t *testing.T) {
	if SuiteAllowed(true, "reject", true) {
		t.Fatal("reject must not update")
	}
	if !SuiteAllowed(true, "abort", true) {
		t.Fatal("abort")
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
