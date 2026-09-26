package inventory

import (
	"os"
	"path/filepath"
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
	writeExe(t, filepath.Join(a, "autopilot"))
	writeExe(t, filepath.Join(b, "autopilot"))
	got := Scan([]string{a, b})
	if len(got) != 1 || got[0].Path != filepath.Join(a, "autopilot") {
		t.Fatalf("%+v", got)
	}
}

func writeExe(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}
