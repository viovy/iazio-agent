package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestParseSchtasksStatus(t *testing.T) {
	running, status, ok := parseSchtasksStatus("HostName: DESKTOP\nTaskName: \\iazio-agent\nStatus: Running\n")
	if !ok || !running || !strings.EqualFold(status, "Running") {
		t.Fatalf("running case: ok=%v running=%v status=%q", ok, running, status)
	}
	running, status, ok = parseSchtasksStatus("Status: Ready\r\n")
	if !ok || running || status != "Ready" {
		t.Fatalf("ready case: ok=%v running=%v status=%q", ok, running, status)
	}
	_, _, ok = parseSchtasksStatus("TaskName: \\iazio-agent\nLast Result: 0\n")
	if ok {
		t.Fatal("expected missing Status field to yield ok=false")
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
	if err := execute(ctx, []string{"--version"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "iazio-agent ") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err := execute(ctx, []string{"-v"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "iazio-agent ") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err := execute(ctx, []string{"--help"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "usage: iazio-agent") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err := execute(ctx, []string{"-h"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "usage: iazio-agent") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err := execute(ctx, []string{"help"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "usage: iazio-agent") {
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
	var heartbeatReceived atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeatReceived.Store(true)
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
		for i := 0; i < 60; i++ {
			if heartbeatReceived.Load() {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
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
	if !heartbeatReceived.Load() {
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
		// Same PATH inventory cost as TestExecuteRunConnected on Windows.
		time.Sleep(1500 * time.Millisecond)
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

func TestRunProbe(t *testing.T) {
	ctx := context.Background()

	// 1. Test probe default text output
	var stdout, stderr bytes.Buffer
	err := execute(ctx, []string{"probe"}, &stdout, &stderr, func(string) string { return "" })
	if err != nil {
		t.Fatalf("execute probe failed: %v", err)
	}
	outStr := stdout.String()
	if !strings.Contains(outStr, "host=") || !strings.Contains(outStr, "version=") {
		t.Errorf("unexpected probe output: %q", outStr)
	}

	// 2. Test probe --json output
	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, []string{"probe", "--json"}, &stdout, &stderr, func(string) string { return "" })
	if err != nil {
		t.Fatalf("execute probe --json failed: %v", err)
	}
	var res ProbeResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal probe JSON output: %v\nOutput: %s", err, stdout.String())
	}
	if res.HostID == "" || res.OS == "" || res.Arch == "" {
		t.Errorf("incomplete probe result: %+v", res)
	}

	// 3. Test probe --help
	stdout.Reset()
	err = execute(ctx, []string{"probe", "--help"}, &stdout, &stderr, func(string) string { return "" })
	if err != nil || !strings.Contains(stdout.String(), "usage: iazio-agent probe") {
		t.Errorf("expected usage on probe --help, got err=%v out=%q", err, stdout.String())
	}

	// 4. Test probe with healthy mock API
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, []string{"probe", "--json", "--check-api"}, &stdout, &stderr, func(k string) string {
		if k == "IAZIO_AGENT_API" {
			return srv.URL
		}
		return ""
	})
	if err != nil {
		t.Fatalf("execute probe with mock API failed: %v", err)
	}
	var apiRes ProbeResult
	if err := json.Unmarshal(stdout.Bytes(), &apiRes); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if apiRes.ControlPlaneStatus != "OK" {
		t.Errorf("expected control plane status OK, got %q", apiRes.ControlPlaneStatus)
	}
}

func TestInstallExecutableSafeDirect(t *testing.T) {
	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "sub", "test-bin")
	data := []byte("#!/bin/sh\necho ok\n")

	if err := installExecutableSafeDirect(targetPath, data); err != nil {
		t.Fatalf("installExecutableSafeDirect failed: %v", err)
	}

	read, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("failed to read installed binary: %v", err)
	}
	if string(read) != string(data) {
		t.Fatalf("content mismatch: got %q, want %q", string(read), string(data))
	}

	// Overwrite existing
	newData := []byte("#!/bin/sh\necho updated\n")
	if err := installExecutableSafeDirect(targetPath, newData); err != nil {
		t.Fatalf("overwrite failed: %v", err)
	}
	read, _ = os.ReadFile(targetPath)
	if string(read) != string(newData) {
		t.Fatalf("updated content mismatch: got %q, want %q", string(read), string(newData))
	}
}

func TestInstallExecutableSafe_AlternatePathSync(t *testing.T) {
	tmpDir := t.TempDir()
	fakeHome := filepath.Join(tmpDir, "home")
	binDir1 := filepath.Join(fakeHome, ".local", "bin")
	binDir2 := filepath.Join(fakeHome, ".iazio", "bin")
	if err := os.MkdirAll(binDir1, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(binDir2, 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", fakeHome)

	target1 := filepath.Join(binDir1, "iazio-agent")
	target2 := filepath.Join(binDir2, "iazio-agent")
	// Pre-create target2 to simulate an existing alternate binary that must be kept in sync
	if err := os.WriteFile(target2, []byte("old-binary"), 0755); err != nil {
		t.Fatal(err)
	}

	data := []byte("new-binary-content")
	if err := installExecutableSafe(target1, data); err != nil {
		t.Fatalf("installExecutableSafe failed: %v", err)
	}

	// Verify primary was written
	if c, _ := os.ReadFile(target1); string(c) != string(data) {
		t.Fatalf("primary not updated: %q", string(c))
	}
	// Verify alternate was synchronized
	if c, _ := os.ReadFile(target2); string(c) != string(data) {
		t.Fatalf("alternate not synchronized: %q", string(c))
	}
}

func TestScanRepoFleetStatus_FailClosed(t *testing.T) {
	ctx := context.Background()

	// 1. Invalid or non-existent path fails closed (returns false)
	if scanRepoFleetStatus(ctx, filepath.Join(t.TempDir(), "nonexistent")) {
		t.Fatal("expected non-existent dir to fail closed")
	}

	// 2. Directory without git repo fails closed (returns false)
	emptyDir := t.TempDir()
	if scanRepoFleetStatus(ctx, emptyDir) {
		t.Fatal("expected non-git dir to fail closed")
	}

	// 3. Git repo check
	repoDir := t.TempDir()
	if err := exec.Command("git", "init", repoDir).Run(); err != nil {
		t.Skip("git not available")
	}
	_ = exec.Command("git", "-C", repoDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.email", "test@test.local").Run()

	file := filepath.Join(repoDir, "file.txt")
	if err := os.WriteFile(file, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	// With untracked file, must return false (dirty)
	if scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected dirty repo to return false")
	}

	// Commit file
	_ = exec.Command("git", "-C", repoDir, "add", "file.txt").Run()
	_ = exec.Command("git", "-C", repoDir, "commit", "-m", "init").Run()

	// Clean repo must return true
	if !scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected clean committed repo to return true")
	}
}


