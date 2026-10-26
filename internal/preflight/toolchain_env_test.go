package preflight

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestHydrateToolchainEnv_Darwin(t *testing.T) {
	fakeHome := func() (string, error) {
		return "/Users/testuser", nil
	}
	currentPath := "/usr/bin:/bin"
	got := HydrateToolchainEnvWith("darwin", currentPath, fakeHome, nil)

	expectedDirs := []string{
		"/Users/testuser/.local/bin",
		"/Users/testuser/bin",
		"/Users/testuser/go/bin",
		"/opt/homebrew/bin",
		"/opt/homebrew/sbin",
		"/usr/local/go/bin",
		"/usr/local/bin",
		"/usr/bin",
		"/bin",
	}

	for _, dir := range expectedDirs {
		if !strings.Contains(got, dir) {
			t.Errorf("expected PATH to contain %q, got: %s", dir, got)
		}
	}
}

func TestHydrateToolchainEnv_Linux(t *testing.T) {
	fakeHome := func() (string, error) {
		return "/home/testuser", nil
	}
	currentPath := "/usr/bin:/bin"
	got := HydrateToolchainEnvWith("linux", currentPath, fakeHome, nil)

	expectedDirs := []string{
		"/home/testuser/.local/bin",
		"/home/testuser/bin",
		"/home/testuser/go/bin",
		"/usr/local/go/bin",
		"/usr/local/bin",
	}

	for _, dir := range expectedDirs {
		if !strings.Contains(got, dir) {
			t.Errorf("expected PATH to contain %q, got: %s", dir, got)
		}
	}
}

func TestHydrateToolchainEnv_Windows(t *testing.T) {
	fakeHome := func() (string, error) {
		return `C:\Users\testuser`, nil
	}
	getenv := func(key string) string {
		switch key {
		case "USERPROFILE":
			return `C:\Users\testuser`
		case "LOCALAPPDATA":
			return `C:\Users\testuser\AppData\Local`
		case "APPDATA":
			return `C:\Users\testuser\AppData\Roaming`
		}
		return ""
	}
	currentPath := `C:\Windows\System32;C:\Windows`
	got := HydrateToolchainEnvWith("windows", currentPath, fakeHome, getenv)

	expectedDirs := []string{
		filepath.Clean(`C:\Users\testuser\AppData\Local\Programs\iazio\bin`),
		filepath.Clean(`C:\Users\testuser\bin`),
		filepath.Clean(`C:\Users\testuser\AppData\Roaming\npm`),
		filepath.Clean(`C:\Program Files\Go\bin`),
		filepath.Clean(`C:\Program Files\Git\cmd`),
	}

	for _, dir := range expectedDirs {
		if !strings.Contains(strings.ToLower(got), strings.ToLower(dir)) {
			t.Errorf("expected Windows PATH to contain %q, got: %s", dir, got)
		}
	}
}

func TestHydrateToolchainEnv_Deduplication(t *testing.T) {
	fakeHome := func() (string, error) {
		return "/home/testuser", nil
	}
	currentPath := "/usr/local/bin:/usr/bin:/bin"
	got := HydrateToolchainEnvWith("linux", currentPath, fakeHome, nil)

	// Count occurrences of /usr/local/bin
	count := strings.Count(got, "/usr/local/bin")
	if count != 1 {
		t.Errorf("expected exactly 1 occurrence of /usr/local/bin, got %d in %s", count, got)
	}
}
