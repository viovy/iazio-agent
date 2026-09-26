package supervisor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpawnOnce(t *testing.T) {
	s := New(nil)
	if err := s.TrySpawn("/repos/leaf-01"); err != nil {
		t.Fatal(err)
	}
	if err := s.TrySpawn("/repos/leaf-01"); err == nil {
		t.Fatal("second spawn")
	}
	args := SpawnArgs("job-1", "https://example.test", "/repos/leaf-01", "/repos/docs-hub")
	for _, bad := range args {
		if bad == "--token" {
			t.Fatal("token flag")
		}
	}
}

func TestSweepSkipsPermission(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job-1.log")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	err := SweepSpool(dir, time.Now(), nil, func(string) error { return os.ErrPermission })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestDrain(t *testing.T) {
	if d := DecideDrain(false, DrainReject); d.Update || d.Cancel || d.Presence != "ONLINE" {
		t.Fatal(d)
	}
	if d := DecideDrain(false, DrainAbort); !d.Cancel || !d.Update {
		t.Fatal(d)
	}
	if HostKind("") != "permanent" || HostKind("ephemeral") != "ephemeral" {
		t.Fatal("kind")
	}
	_ = errors.New
}
