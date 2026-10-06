package inventory

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestClassifyBehindAndMissing(t *testing.T) {
	found := []Tool{{Name: "autopilot", Version: "dev", Path: "/bin/autopilot"}}
	tools, ok := Classify(found, map[string]string{"autopilot": "1.2.3"})
	if ok {
		t.Fatal("expected unschedulable")
	}
	var missing, behind int
	for _, tool := range tools {
		switch tool.Status {
		case "MISSING":
			missing++
		case "BEHIND":
			behind++
		}
	}
	if missing == 0 || behind == 0 {
		t.Fatalf("missing=%d behind=%d", missing, behind)
	}
}

func TestScanFirstHit(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	binName := "autopilot"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	writeExe(t, filepath.Join(a, binName))
	writeExe(t, filepath.Join(b, binName))
	got := Scan([]string{a, b})
	if len(got) != 1 || got[0].Path != filepath.Join(a, binName) {
		t.Fatalf("%+v", got)
	}
}

func TestClassifyVersions(t *testing.T) {
	const sha = "abc123"
	tests := []struct {
		name string
		ver  string
		exp  string
		st   string
	}{
		{name: "dev", ver: "dev", exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "dev build", ver: "0.1.0-dev", exp: "1.2.3", st: "BEHIND"},
		{name: "same revision", ver: "1.2.3+" + sha, exp: "1.2.3+" + sha, st: "OK"},
		{name: "other revision", ver: "1.2.3+fff", exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "older", ver: "1.2.2+" + sha, exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "newer", ver: "1.2.4+" + sha, exp: "1.2.3+" + sha, st: "OK"},
		{name: "empty pin", ver: "1.2.3", exp: "", st: "OK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tools, ok := Classify([]Tool{{Name: "autopilot", Version: tt.ver, Path: "/bin/autopilot", Executable: true}}, map[string]string{"autopilot": tt.exp})
			var got string
			for _, tool := range tools {
				if tool.Name == "autopilot" {
					got = tool.Status
				}
			}
			if got != tt.st {
				t.Fatalf("status %s, want %s", got, tt.st)
			}
			if ok {
				t.Fatal("a partial baseline must be unschedulable")
			}
		})
	}
}

func TestDefaultDirsOrder(t *testing.T) {
	home := t.TempDir()
	first := t.TempDir()
	second := t.TempDir()
	pathEnv := first + string(os.PathListSeparator) + second
	got := DefaultDirs(pathEnv, home, "linux")
	wantPrefix := []string{
		first,
		second,
		filepath.Join(home, ".local", "bin"),
		filepath.Join(home, ".iazio", "bin"),
	}
	if len(got) != len(wantPrefix) {
		t.Fatalf("%v", got)
	}
	for i := range wantPrefix {
		if got[i] != wantPrefix[i] {
			t.Fatalf("dir %d = %s, want %s", i, got[i], wantPrefix[i])
		}
	}
	win := DefaultDirs("", home, "windows")
	if win[len(win)-1] != filepath.Join(home, "bin") {
		t.Fatalf("%v", win)
	}
}

func TestScanWindowsUserBin(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "bin", "autopilot.exe")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := ScanOS(DefaultDirs("", home, "windows"), "windows")
	if len(got) != 1 || got[0].Name != "autopilot" || !got[0].Executable {
		t.Fatalf("%+v", got)
	}
}

func writeExe(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}
