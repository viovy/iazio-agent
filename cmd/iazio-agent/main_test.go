package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)


func TestFormatVersion(t *testing.T) {
	origV, origC, origB := version, commit, branch
	t.Cleanup(func() {
		version, commit, branch = origV, origC, origB
	})
	version, commit, branch = "dev", "unknown", "unknown"
	if formatVersion() != "iazio-agent dev" {
		t.Fatal(formatVersion())
	}
	version = "1.2.3"
	commit = "abcdef1234567890"
	branch = "release-marker"
	got := formatVersion()
	if got != "iazio-agent 1.2.3+abcdef1" || strings.Contains(got, branchName()) {
		t.Fatal(got)
	}
}

func TestExecuteCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, []string{"version"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "iazio-agent ") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	err := execute(ctx, []string{"auth", "login"}, &stdout, &stderr, func(k string) string {
		if k == "IAZIO_AGENT_NONINTERACTIVE" {
			return "1"
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "noninteractive") {
		t.Fatal(err)
	}
	stdout.Reset()
	cfg := filepath.Join(t.TempDir(), "agent.json")
	err = execute(ctx, []string{"auth", "login"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "XPC_SERVICE_NAME":
			return "gui"
		case "IAZIO_AGENT_CONFIG":
			return cfg
		default:
			return ""
		}
	})
	if err == nil || strings.Contains(err.Error(), "noninteractive") {
		t.Fatal(err)
	}
	stdout.Reset()
	if err = execute(ctx, []string{"service", "install", "--dry-run"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "IAZIO_AGENT_NONINTERACTIVE") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err = execute(ctx, []string{"update", "--suite"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "update suite") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err = execute(ctx, []string{"run"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "permanent") {
		t.Fatal(err, stdout.String())
	}
}

func TestExecuteRunConnected(t *testing.T) {
	var heartbeatReceived bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeatReceived = true
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/poll"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	err := execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !heartbeatReceived {
		t.Fatalf("expected heartbeat to be received")
	}
}

func TestExecuteRunDefaultHostID(t *testing.T) {
	var registeredHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 4 {
				registeredHost = parts[len(parts)-2]
			}
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/poll"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	err := execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		if k == "IAZIO_HARNESS_API_URL" {
			return srv.URL
		}
		return ""
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if registeredHost == "" {
		t.Fatalf("expected registered host from hostname default, got empty")
	}
}

func TestResolveHostID(t *testing.T) {
	// 1. Explicit env var wins unconditionally
	got := resolveHostID(func(k string) string {
		if k == "IAZIO_AGENT_HOST_ID" {
			return "explicit-host"
		}
		return ""
	}, func() (string, error) {
		return "ignored-pc", nil
	}, func() bool {
		return true
	})
	if got != "explicit-host" {
		t.Fatalf("expected explicit-host, got %s", got)
	}

	// 2. Non-WSL host derives clean lowercase hostname without suffix
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "My-PC.localdomain", nil
	}, func() bool {
		return false
	})
	if got != "my-pc" {
		t.Fatalf("expected my-pc, got %s", got)
	}

	// 3. WSL host appends -wsl
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "PC1", nil
	}, func() bool {
		return true
	})
	if got != "pc1-wsl" {
		t.Fatalf("expected pc1-wsl, got %s", got)
	}

	// 4. WSL host already suffixed with -wsl does not duplicate
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "PC1-WSL", nil
	}, func() bool {
		return true
	})
	if got != "pc1-wsl" {
		t.Fatalf("expected pc1-wsl, got %s", got)
	}

	// 5. Hostname lookup failure defaults to runner or runner-wsl
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "", errors.New("lookup failed")
	}, func() bool {
		return false
	})
	if got != "runner" {
		t.Fatalf("expected runner, got %s", got)
	}

	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "", errors.New("lookup failed")
	}, func() bool {
		return true
	})
	if got != "runner-wsl" {
		t.Fatalf("expected runner-wsl, got %s", got)
	}

	// 6. Special characters in hostname are sanitized to lowercase alphanumeric and dashes
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "My_PC!Test-01.lan", nil
	}, func() bool {
		return false
	})
	if got != "mypctest-01" {
		t.Fatalf("expected mypctest-01, got %s", got)
	}

	// 7. Whitespace-only hostname defaults to runner / runner-wsl
	got = resolveHostID(func(string) string { return "" }, func() (string, error) {
		return "   ", nil
	}, func() bool {
		return true
	})
	if got != "runner-wsl" {
		t.Fatalf("expected runner-wsl, got %s", got)
	}

	// 8. Explicit IAZIO_AGENT_HOST_ID with whitespace and special chars is sanitized
	got = resolveHostID(func(k string) string {
		if k == "IAZIO_AGENT_HOST_ID" {
			return "  My_Agent!01  "
		}
		return ""
	}, func() (string, error) {
		return "ignored", nil
	}, func() bool {
		return true
	})
	if got != "myagent01" {
		t.Fatalf("expected myagent01, got %s", got)
	}
}

func TestRunUpdate_ManifestIntegration(t *testing.T) {
	manifestHit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/fleet/manifest") {
			manifestHit = true
			if r.URL.Query().Get("profile") != "developer-workstation" {
				t.Errorf("expected profile developer-workstation, got %s", r.URL.Query().Get("profile"))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ManifestResponse{
				Profile: "developer-workstation",
				InstallRoots: ManifestInstallRoots{
					Unix:    ManifestInstallRootPaths{Primary: "~/.local/bin"},
					Windows: ManifestInstallRootPaths{Primary: `C:\bin`},
				},
				Tools: []ManifestToolItem{
					{
						Name:        "iazio-agent",
						Repository:  "viovy/iazio-agent",
						BinaryName:  "iazio-agent",
						Tier:        "baseline",
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	err := execute(ctx, []string{
		"update",
		"--suite",
		"--profile", "developer-workstation",
		"--api", srv.URL,
	}, &stdout, &stderr, func(string) string { return "" })
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}
	if !strings.Contains(stdout.String(), "update suite") {
		t.Errorf("expected stdout to contain 'update suite', got %q", stdout.String())
	}
	if !manifestHit {
		t.Error("expected manifest endpoint to be fetched")
	}
}

func TestExecuteRunProfileBinding(t *testing.T) {
	var registeredProfile string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			registeredProfile = body["profile"]
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/poll"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run", "--profile", "workstation-pro"}, &stdout, &stderr, func(k string) string {
		if k == "IAZIO_HARNESS_API_URL" {
			return srv.URL
		}
		return ""
	})

	if registeredProfile != "workstation-pro" {
		t.Errorf("expected registered profile 'workstation-pro', got %q", registeredProfile)
	}
}

func TestSyncLocalEndpointsAndResolveLocalAPI(t *testing.T) {
	tempHome := t.TempDir()
	origHome := os.Getenv("USERPROFILE")
	if origHome == "" {
		origHome = os.Getenv("HOME")
	}
	// On Windows UserHomeDir checks USERPROFILE, on Unix HOME
	os.Setenv("USERPROFILE", tempHome)
	os.Setenv("HOME", tempHome)
	t.Cleanup(func() {
		os.Setenv("USERPROFILE", origHome)
		os.Setenv("HOME", origHome)
	})

	eps := map[string]string{
		"harness_api": "https://harness.example.com",
		"iazio_api":   "https://api.example.com",
	}
	if err := syncLocalEndpoints(eps, "custom-prof"); err != nil {
		t.Fatalf("syncLocalEndpoints failed: %v", err)
	}

	gotAPI := resolveLocalAPI(func(string) string { return "" })
	if gotAPI != "https://harness.example.com" {
		t.Errorf("expected resolved API from config https://harness.example.com, got %q", gotAPI)
	}
}

