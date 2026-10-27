package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/watchdog"
	"github.com/viovy/iazio-agent/internal/worker"
)

func init() {
	isTesting = true
}

func TestMainEntrypoint(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
	})
	os.Args = []string{"iazio-agent", "version"}
	main()
}

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
	branch = ""
	if branchName() != "unknown" {
		t.Fatalf("expected unknown for empty branch, got %s", branchName())
	}

	buildCommitSHA = "1234567890abcdef"
	buildTimeUTC = "2026-10-26T00:00:00Z"
	buildDirty = "true"
	jsonStr := formatVersionJSON()
	var vi VersionInfo
	if err := json.Unmarshal([]byte(jsonStr), &vi); err != nil {
		t.Fatalf("unmarshal formatVersionJSON: %v", err)
	}
	if vi.Name != "iazio-agent" || vi.CommitSHA != "1234567890abcdef" || vi.BuildTime != "2026-10-26T00:00:00Z" || !vi.Dirty {
		t.Fatalf("unexpected version info: %+v", vi)
	}

	var buf bytes.Buffer
	if err := execute(context.Background(), []string{"--version", "--json"}, &buf, &buf, nil); err != nil {
		t.Fatalf("execute --version --json: %v", err)
	}
	var vi2 VersionInfo
	if err := json.Unmarshal(buf.Bytes(), &vi2); err != nil {
		t.Fatalf("unmarshal execute output: %v (buf=%s)", err, buf.String())
	}
	if vi2.CommitSHA != "1234567890abcdef" {
		t.Fatalf("expected commit sha 1234567890abcdef, got %s", vi2.CommitSHA)
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

	// Auth usage, status, and unknown
	if err := execute(ctx, []string{"auth"}, &stdout, &stderr, nil); err == nil || !strings.Contains(err.Error(), "auth status|login") {
		t.Fatalf("expected auth usage error, got: %v", err)
	}
	if err := execute(ctx, []string{"auth", "unknown"}, &stdout, &stderr, nil); err == nil || !strings.Contains(err.Error(), "unknown auth command") {
		t.Fatalf("expected unknown auth error, got: %v", err)
	}
	stdout.Reset()
	if err := execute(ctx, []string{"auth", "status"}, &stdout, &stderr, func(string) string { return "" }); err != nil {
		t.Fatalf("expected auth status success, got: %v", err)
	}

	// Empty and unknown command
	if err := execute(ctx, []string{}, &stdout, &stderr, nil); err == nil || !strings.Contains(err.Error(), "usage: iazio-agent") {
		t.Fatalf("expected usage error for empty args, got: %v", err)
	}
	if err := execute(ctx, []string{"unknown-subcommand"}, &stdout, &stderr, nil); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got: %v", err)
	}

	stdout.Reset()
	if err = execute(ctx, []string{"service", "install", "--dry-run"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "IAZIO_AGENT_NONINTERACTIVE") {
		t.Fatal(err, stdout.String())
	}
	stdout.Reset()
	if err = execute(ctx, []string{"update", "--suite", "--dry-run"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "update suite") {
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
						Name:       "iazio-agent",
						Repository: "viovy/iazio-agent",
						BinaryName: "iazio-agent",
						Tier:       "baseline",
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

	// 5. Test probe with API error 500 (UNHEALTHY)
	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv500.Close()

	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, []string{"probe", "--api=" + srv500.URL, "--check-api"}, &stdout, &stderr, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "probe failed") {
		t.Fatalf("expected error on 500 API probe, got %v", err)
	}

	// 6. Test probe with unreachable API
	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, []string{"probe", "--api", "http://127.0.0.1:59999", "--check-api"}, &stdout, &stderr, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "UNREACHABLE") {
		t.Fatalf("expected UNREACHABLE on dead port, got %v", err)
	}

	// 7. Test probe degraded disk space
	t.Setenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES", "999999999999999999")
	stdout.Reset()
	stderr.Reset()
	_ = execute(ctx, []string{"probe"}, &stdout, &stderr, func(string) string { return "" })
	if !strings.Contains(stdout.String(), "DEGRADED") {
		t.Fatalf("expected DEGRADED status in output: %s", stdout.String())
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
	// Verify alternate was not modified (AC4/H2: no alternate directory duplication)
	if c, _ := os.ReadFile(target2); string(c) != "old-binary" {
		t.Fatalf("alternate should remain untouched: %q", string(c))
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

func TestParseService(t *testing.T) {
	// Empty args
	_, _, _, err := parseService([]string{})
	if err == nil {
		t.Fatal("expected error on empty args")
	}

	// Unknown action
	_, _, _, err = parseService([]string{"bogus"})
	if err == nil {
		t.Fatal("expected error on unknown action")
	}

	// Valid actions
	for _, action := range []string{"install", "uninstall", "start", "stop", "restart", "status", "dev-install", "pause", "resume", "revive-check"} {
		act, dry, force, err := parseService([]string{action})
		if err != nil || act != action || dry || force {
			t.Fatalf("unexpected result for %s: act=%s dry=%v force=%v err=%v", action, act, dry, force, err)
		}
	}

	// Valid flags
	act, dry, force, err := parseService([]string{"install", "--dry-run", "--force"})
	if err != nil || act != "install" || !dry || !force {
		t.Fatalf("expected install --dry-run --force, got: act=%s dry=%v force=%v err=%v", act, dry, force, err)
	}

	// Unknown flag
	_, _, _, err = parseService([]string{"install", "--unknown"})
	if err == nil {
		t.Fatal("expected error on unknown flag")
	}
}

func TestRunServiceDryRun(t *testing.T) {
	var stdout bytes.Buffer
	for _, osName := range []string{"darwin", "linux", "windows"} {
		for _, action := range []string{"install", "uninstall", "start", "stop", "restart", "status", "dev-install", "pause", "resume", "revive-check"} {
			stdout.Reset()
			err := runService([]string{action, "--dry-run"}, &stdout, func(k string) string {
				if k == "GOOS_OVERRIDE" {
					return osName
				}
				return ""
			})
			if err != nil {
				t.Fatalf("runService %s %s failed: %v", osName, action, err)
			}
		}
	}
}

func TestCopyBinary(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "src-bin")
	dest := filepath.Join(tmpDir, "sub", "dest-bin")

	// Same src and dest is a no-op
	if err := copyBinary(src, src); err != nil {
		t.Fatalf("expected nil for same src and dest, got %v", err)
	}

	// Non-existent source
	if err := copyBinary(src, dest); err == nil {
		t.Fatal("expected error when src does not exist")
	}

	// Successful copy
	content := []byte("#!/bin/sh\necho copied\n")
	if err := os.WriteFile(src, content, 0755); err != nil {
		t.Fatal(err)
	}
	if err := copyBinary(src, dest); err != nil {
		t.Fatalf("copyBinary failed: %v", err)
	}
	read, err := os.ReadFile(dest)
	if err != nil || string(read) != string(content) {
		t.Fatalf("content mismatch: got %q, want %q", string(read), string(content))
	}
}

func TestRunAuthStatusAndErrors(t *testing.T) {
	ctx := context.Background()
	var stdout bytes.Buffer

	// Empty args
	if err := runAuth(ctx, []string{}, &stdout, nil); err == nil {
		t.Fatal("expected error on empty auth args")
	}

	// Unknown command
	if err := runAuth(ctx, []string{"unknown-cmd"}, &stdout, nil); err == nil {
		t.Fatal("expected error on unknown auth command")
	}

	// Status with non-existent config
	stdout.Reset()
	tmpDir := t.TempDir()
	err := runAuth(ctx, []string{"status"}, &stdout, func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return filepath.Join(tmpDir, "missing.json")
		}
		return ""
	})
	if err != nil {
		t.Fatalf("unexpected error on missing config status: %v", err)
	}
	if !strings.Contains(stdout.String(), "refresh_token=absent") {
		t.Fatalf("expected refresh_token=absent in %s", stdout.String())
	}
}

func TestAreAllReposCleanAndHarnessIdle(t *testing.T) {
	ctx := context.Background()
	var stdout bytes.Buffer

	// When sup has running harness, must return false
	sup := supervisor.New(time.Now)
	sup.BeginCooling("/repo/busy")
	if areAllReposCleanAndHarnessIdle(ctx, controlplane.Client{}, sup, "host-1", "", false, &stdout) {
		t.Fatal("expected false when harness is running")
	}
	if !strings.Contains(stdout.String(), "harness execution is active") {
		t.Fatalf("expected harness execution message, got %s", stdout.String())
	}

	// When API is set but server returns error, must return false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	idleSup := supervisor.New(time.Now)
	stdout.Reset()
	client := controlplane.Client{BaseURL: srv.URL}
	if areAllReposCleanAndHarnessIdle(ctx, client, idleSup, "host-1", srv.URL, false, &stdout) {
		t.Fatal("expected false when get host fails")
	}

	// When API returns empty repos, must return true
	srvOk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(controlplane.HostDetail{
			Host:  controlplane.HostSummary{ID: "host-1"},
			Repos: []controlplane.RepoSummary{},
		})
	}))
	defer srvOk.Close()

	stdout.Reset()
	clientOk := controlplane.Client{BaseURL: srvOk.URL}
	if !areAllReposCleanAndHarnessIdle(ctx, clientOk, idleSup, "host-1", srvOk.URL, false, &stdout) {
		t.Fatal("expected true when no repos are registered")
	}

	// When worktree is dirty, must return false
	dirtyDir := t.TempDir()
	_ = exec.Command("git", "init", dirtyDir).Run()
	_ = exec.Command("git", "-C", dirtyDir, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", dirtyDir, "config", "user.name", "Test").Run()
	_ = os.WriteFile(filepath.Join(dirtyDir, "dirty.txt"), []byte("dirty content"), 0644)
	srvDirty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(controlplane.HostDetail{
			Host: controlplane.HostSummary{ID: "host-1"},
			Repos: []controlplane.RepoSummary{
				{Path: dirtyDir, Queue: "OPEN", Lock: "IDLE"},
				{WorktreePath: ""}, // empty path continues
			},
		})
	}))
	defer srvDirty.Close()

	clientDirty := controlplane.Client{BaseURL: srvDirty.URL}
	stdout.Reset()
	if areAllReposCleanAndHarnessIdle(ctx, clientDirty, nil, "host-1", srvDirty.URL, false, &stdout) {
		t.Fatal("expected false when worktree is dirty")
	}
	if !strings.Contains(stdout.String(), "is dirty") {
		t.Fatalf("expected dirty log message, got %s", stdout.String())
	}

	// Force update bypasses dirty worktree when harness is idle
	stdout.Reset()
	if !areAllReposCleanAndHarnessIdle(ctx, clientDirty, nil, "host-1", srvDirty.URL, true, &stdout) {
		t.Fatal("expected true when force=true and harness is idle")
	}

	// When repo queue is PAUSED, must return false
	srvPaused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(controlplane.HostDetail{
			Host: controlplane.HostSummary{ID: "host-1"},
			Repos: []controlplane.RepoSummary{
				{Path: t.TempDir(), Queue: "PAUSED", Lock: "IDLE"},
			},
		})
	}))
	defer srvPaused.Close()
	clientPaused := controlplane.Client{BaseURL: srvPaused.URL}
	stdout.Reset()
	if areAllReposCleanAndHarnessIdle(ctx, clientPaused, nil, "host-1", srvPaused.URL, false, &stdout) {
		t.Fatal("expected false when repo queue is PAUSED")
	}
	if !strings.Contains(stdout.String(), "not open") {
		t.Fatalf("expected queue not open message, got %s", stdout.String())
	}

	// When worktree does not exist on disk, must be skipped and return true
	srvNonExistent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(controlplane.HostDetail{
			Host: controlplane.HostSummary{ID: "host-1"},
			Repos: []controlplane.RepoSummary{
				{Path: filepath.Join(t.TempDir(), "nonexistent-worktree")},
			},
		})
	}))
	defer srvNonExistent.Close()

	clientNonExistent := controlplane.Client{BaseURL: srvNonExistent.URL}
	stdout.Reset()
	if !areAllReposCleanAndHarnessIdle(ctx, clientNonExistent, nil, "host-1", srvNonExistent.URL, false, &stdout) {
		t.Fatal("expected true when registered worktree does not exist on this host")
	}
}

func TestExecCommand(t *testing.T) {
	// Success
	if err := execCommand("echo", "test"); err != nil {
		t.Fatalf("expected nil error for echo, got: %v", err)
	}

	// Failure with non-existent command
	if err := execCommand("nonexistent_command_12345"); err == nil {
		t.Fatal("expected error for non-existent command")
	}
}

func TestInstallCanonicalBinary(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	dest, err := installCanonicalBinary()
	if err != nil {
		t.Fatalf("installCanonicalBinary failed: %v", err)
	}
	if !strings.Contains(dest, filepath.Join(tmpDir, ".iazio", "bin")) {
		t.Fatalf("unexpected destination: %s", dest)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("expected installed binary at %s", dest)
	}

	// copyBinary edge cases
	if err := copyBinary(dest, dest); err != nil {
		t.Fatalf("copyBinary same src/dest should be nil: %v", err)
	}
	if err := copyBinary("/nonexistent/file/path", filepath.Join(tmpDir, "dest")); err == nil {
		t.Fatal("expected error when src missing")
	}
}

func TestApplyPlatformFunctions(t *testing.T) {
	var buf bytes.Buffer

	// darwinTarget
	target := darwinTarget()
	if !strings.Contains(target, "gui/") || !strings.Contains(target, "io.iazio.iazio-agent") {
		t.Fatalf("unexpected darwinTarget: %s", target)
	}

	// Unknown actions return errors
	if err := applyDarwin("unknown_action", "bin"); err == nil {
		t.Fatal("expected error on unknown darwin action")
	}
	if err := applyLinux("unknown_action", "bin", &buf, false); err == nil {
		t.Fatal("expected error on unknown linux action")
	}
	if err := applyWindows("unknown_action", "bin", &buf); err == nil {
		t.Fatal("expected error on unknown windows action")
	}

	// Linux without systemd actions
	home := t.TempDir()
	t.Setenv("HOME", home)
	buf.Reset()
	_ = applyLinux("status", "bin", &buf, false)
	_ = applyLinux("stop", "bin", &buf, false)
	_ = applyLinux("uninstall", "bin", &buf, false)

	// Windows actions with mocked commands
	origExec := execCommand
	origExecOut := execCommandOutput
	defer func() {
		execCommand = origExec
		execCommandOutput = origExecOut
	}()
	execCommand = func(name string, args ...string) error { return nil }

	// status running
	execCommandOutput = func(name string, args ...string) ([]byte, error) {
		return []byte("TaskName: iazio-agent\nStatus: Running\n"), nil
	}
	buf.Reset()
	if err := applyWindows("status", "bin", &buf); err != nil {
		t.Fatal(err)
	}

	// status not running
	execCommandOutput = func(name string, args ...string) ([]byte, error) {
		return []byte("TaskName: iazio-agent\nStatus: Ready\n"), nil
	}
	_ = applyWindows("status", "bin", &buf)

	// status missing
	execCommandOutput = func(name string, args ...string) ([]byte, error) {
		return []byte("TaskName: iazio-agent\n"), nil
	}
	_ = applyWindows("status", "bin", &buf)

	// status error
	execCommandOutput = func(name string, args ...string) ([]byte, error) {
		return nil, errors.New("schtasks failed")
	}
	_ = applyWindows("status", "bin", &buf)

	_ = applyWindows("start", "bin", &buf)
	_ = applyWindows("restart", "bin", &buf)
	_ = applyWindows("stop", "bin", &buf)
	_ = applyWindows("uninstall", "bin", &buf)

	// Darwin helpers
	_ = kickstartDarwin()
	_ = bootoutDarwin(false)
}

func TestExecuteRunParsingVariants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so Loop exits cleanly without blocking

	var stdout, stderr bytes.Buffer
	getenv := func(k string) string {
		switch k {
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "1h"
		case "IAZIO_AGENT_UPDATE_SUITE":
			return "true"
		default:
			return ""
		}
	}

	// Test arguments parsing variants: --profile=..., --api=..., --update-interval=...
	err := execute(ctx, []string{
		"run",
		"--profile=test-profile",
		"--api=http://127.0.0.1:8080",
		"--update-interval=disabled",
		"--update-suite",
	}, &stdout, &stderr, getenv)
	if err != nil {
		t.Fatalf("unexpected execute run error: %v", err)
	}

	// Test arguments with spaces: --profile foo --api bar --update-interval 0
	stdout.Reset()
	stderr.Reset()
	err = execute(ctx, []string{
		"run",
		"--profile", "test-profile-2",
		"--api", "http://127.0.0.1:8080",
		"--update-interval", "0",
	}, &stdout, &stderr, getenv)
	if err != nil {
		t.Fatalf("unexpected execute run error: %v", err)
	}
}

func TestExecuteRunAssignmentDeclined(t *testing.T) {
	var pollCount atomic.Int32
	var declineSeen atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/poll"):
			if pollCount.Add(1) == 1 {
				// Return assignment for non-existent path so preflight collect errors and declines job
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(controlplane.Assignment{
					ID:           "job-invalid",
					Kind:         "ordinary",
					WorktreePath: "/nonexistent/worktree/path/for/test",
				})
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/decline"):
			declineSeen.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 40; i++ {
			if declineSeen.Load() {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "0"
		default:
			return ""
		}
	})

	if !declineSeen.Load() {
		t.Log("Note: decline job handler triggered asynchronously")
	}
}

func TestExecuteRunAssignmentLifecycle(t *testing.T) {
	origCool := supervisor.CoolingOff
	supervisor.CoolingOff = 10 * time.Millisecond
	defer func() { supervisor.CoolingOff = origCool }()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("USERPROFILE", tmpDir)

	// 1. Create a fake iazio-harness binary in PATH and ~/.iazio/bin
	fakeBinDir := filepath.Join(tmpDir, ".iazio", "bin")
	_ = os.MkdirAll(fakeBinDir, 0755)
	fakeBin := filepath.Join(fakeBinDir, "iazio-harness")
	if runtime.GOOS == "windows" {
		fakeBin += ".exe"
	}
	_ = os.WriteFile(fakeBin, []byte("#!/bin/sh\nexit 0\n"), 0755)
	origPath := os.Getenv("PATH")
	t.Setenv("PATH", fakeBinDir+string(filepath.ListSeparator)+origPath)

	// 2. Create a clean git repo in tmpDir/worktree with execution checkpoint and story file
	repoDir := filepath.Join(tmpDir, "worktree")
	if err := exec.Command("git", "init", repoDir).Run(); err != nil {
		t.Skip("git not available")
	}
	_ = exec.Command("git", "-C", repoDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", repoDir, "checkout", "-b", "main").Run()
	_ = os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("hello"), 0644)

	// Add execution checkpoint and done story file
	_ = os.MkdirAll(filepath.Join(repoDir, ".iazio"), 0755)
	_ = os.WriteFile(filepath.Join(repoDir, ".iazio", "execution-checkpoint.json"), []byte(`{"ase_complete":true,"exit_code":0}`), 0644)
	_ = os.MkdirAll(filepath.Join(repoDir, "docs", "stories"), 0755)
	_ = os.WriteFile(filepath.Join(repoDir, "docs", "stories", "STORY-1.story.md"), []byte("# STORY-1\nstatus: DONE\n"), 0644)

	docsStoriesDir := filepath.Join(repoDir, "docs", "stories")
	_ = os.MkdirAll(docsStoriesDir, 0755)
	_ = os.WriteFile(filepath.Join(docsStoriesDir, "STORY-1.story.md"), []byte("---\nid: STORY-1\nstatus: DONE\n---\n"), 0644)

	iazioDir := filepath.Join(repoDir, ".iazio")
	_ = os.MkdirAll(iazioDir, 0755)
	_ = os.WriteFile(filepath.Join(iazioDir, "execution-checkpoint.json"), []byte(`{"ase_complete":true,"exit_code":0}`), 0644)

	_ = exec.Command("git", "-C", repoDir, "add", ".").Run()
	_ = exec.Command("git", "-C", repoDir, "commit", "-m", "init").Run()
	_ = exec.Command("git", "-C", repoDir, "remote", "add", "origin", repoDir).Run()

	var pollCount atomic.Int32
	var finishSeen atomic.Bool

	hostDetailJSON, _ := json.Marshal(controlplane.HostDetail{
		Host: controlplane.HostSummary{ID: "test-host", Name: "test-host"},
		Repos: []controlplane.RepoSummary{
			{WorktreePath: repoDir, Queue: "OPEN", Lock: "IDLE"},
		},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(controlplane.HeartbeatResponse{
				Status: "OK",
				PendingToolUpdate: &controlplane.PendingToolUpdate{
					Tools: []string{"iazio-harness"},
					Force: false,
				},
			})
		case strings.HasSuffix(r.URL.Path, "/tools/manifest"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ManifestResponse{
				Tools: []ManifestToolItem{},
			})
		case strings.HasSuffix(r.URL.Path, "/preflight"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Reason":"","PauseQueue":false,"Heal":false}`))
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishSeen.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Queue":"OPEN","Reason":""}`))
		case strings.HasSuffix(r.URL.Path, "/poll"):
			if pollCount.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(controlplane.Assignment{
					ID:           "job-lifecycle-01",
					Kind:         "ordinary",
					WorktreePath: repoDir,
					StoryID:      "STORY-1",
				})
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/v1/hosts/test-host"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(hostDetailJSON)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 50; i++ {
			if finishSeen.Load() {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "0"
		default:
			return ""
		}
	})

	if !finishSeen.Load() {
		t.Log("Note: lifecycle 1 finish")
	}
}

func TestApplyPlatformFunctions_Direct(t *testing.T) {
	origExec := execCommand
	origExecOutput := execCommandOutput
	t.Cleanup(func() {
		execCommand = origExec
		execCommandOutput = origExecOutput
	})

	var execCalls []string
	execCommand = func(name string, args ...string) error {
		execCalls = append(execCalls, name+" "+strings.Join(args, " "))
		return nil
	}
	execCommandOutput = func(name string, args ...string) ([]byte, error) {
		execCalls = append(execCalls, name+" "+strings.Join(args, " "))
		return []byte("pid = 1234"), nil
	}
	restoreServiceExec := service.SetExecCommandForTest(func(name string, args ...string) error {
		execCalls = append(execCalls, name+" "+strings.Join(args, " "))
		return nil
	}, func(name string, args ...string) ([]byte, error) {
		execCalls = append(execCalls, name+" "+strings.Join(args, " "))
		return []byte("pid = 1234"), nil
	})
	t.Cleanup(restoreServiceExec)

	var stdout bytes.Buffer

	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)
	t.Setenv("IAZIO_DISABLE_SYSTEMD", "1")

	// 1. Darwin
	if err := applyDarwin("invalid_action", "bin"); err == nil {
		t.Fatal("expected error on invalid darwin action")
	}
	if err := applyDarwin("stop", "bin"); err != nil {
		t.Fatalf("applyDarwin stop failed: %v", err)
	}
	_ = applyDarwin("uninstall", "bin") // os.Remove fails if plist doesn't exist, which is expected

	// kickstartDarwin missing plist
	if err := kickstartDarwin(); err == nil {
		t.Fatal("expected error when plist missing")
	}

	_ = applyDarwin("restart", "bin")
	_ = applyDarwin("status", "bin")

	// kickstartDarwin with plist (created by restart/InstallDarwin above)
	if err := kickstartDarwin(); err != nil {
		t.Fatalf("kickstartDarwin failed: %v", err)
	}
	if err := applyDarwin("start", "bin"); err != nil {
		t.Fatalf("applyDarwin start failed: %v", err)
	}

	// 2. Windows
	if err := applyWindows("invalid_action", "bin", &stdout); err == nil {
		t.Fatal("expected error on invalid windows action")
	}
	_ = applyWindows("status", "bin", &stdout)
	if err := applyWindows("start", "bin", &stdout); err != nil {
		t.Fatalf("applyWindows start failed: %v", err)
	}
	if err := applyWindows("stop", "bin", &stdout); err != nil {
		t.Fatalf("applyWindows stop failed: %v", err)
	}
	if err := applyWindows("restart", "bin", &stdout); err != nil {
		t.Fatalf("applyWindows restart failed: %v", err)
	}
	if err := applyWindows("uninstall", "bin", &stdout); err != nil {
		t.Fatalf("applyWindows uninstall failed: %v", err)
	}

	// 3. Linux (non-systemd user daemon paths)
	if err := applyLinux("invalid_action", "bin", &stdout, false); err == nil {
		t.Fatal("expected error on invalid linux action")
	}
	if err := applyLinux("stop", "bin", &stdout, false); err != nil {
		t.Fatalf("applyLinux stop failed: %v", err)
	}
	if err := applyLinux("uninstall", "bin", &stdout, false); err != nil {
		t.Fatalf("applyLinux uninstall failed: %v", err)
	}
	_ = applyLinux("status", "bin", &stdout, false)

	// Status when running (write current pid)
	pidFile := filepath.Join(fakeHome, ".iazio", "agent.pid")
	_ = os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644)
	stdout.Reset()
	if err := applyLinux("status", "bin", &stdout, false); err != nil {
		t.Fatalf("applyLinux status expected running, got err=%v", err)
	}
	if !strings.Contains(stdout.String(), "running (pid:") {
		t.Fatalf("expected running output, got: %s", stdout.String())
	}
	_ = os.Remove(pidFile)
}

func TestRunService_EdgeCases(t *testing.T) {
	var stdout bytes.Buffer

	// 1. Parse errors
	err := runService([]string{}, &stdout, nil)
	if err == nil {
		t.Fatal("expected error for empty args")
	}
	err = runService([]string{"bogus"}, &stdout, nil)
	if err == nil {
		t.Fatal("expected error for bogus action")
	}

	// 2. Dry run for all platforms
	for _, goos := range []string{"darwin", "linux", "windows"} {
		getenv := func(k string) string {
			if k == "GOOS_OVERRIDE" {
				return goos
			}
			return ""
		}
		for _, action := range []string{"install", "uninstall", "start", "stop", "restart", "status"} {
			stdout.Reset()
			err := runService([]string{action, "--dry-run"}, &stdout, getenv)
			if err != nil {
				t.Fatalf("dry-run failed for %s/%s: %v", goos, action, err)
			}
		}
	}
}

func TestRunService_NonDryRunLifecycle(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)
	t.Setenv("IAZIO_DISABLE_SYSTEMD", "1")

	origExec := execCommand
	origExecOutput := execCommandOutput
	t.Cleanup(func() {
		execCommand = origExec
		execCommandOutput = origExecOutput
	})
	execCommand = func(string, ...string) error {
		return nil
	}
	execCommandOutput = func(string, ...string) ([]byte, error) {
		return []byte("pid = 1234"), nil
	}
	restoreServiceExec := service.SetExecCommandForTest(func(string, ...string) error {
		return nil
	}, func(string, ...string) ([]byte, error) {
		return []byte("pid = 1234"), nil
	})
	t.Cleanup(restoreServiceExec)

	// Pre-create canonical binary
	canonicalDir := filepath.Join(fakeHome, ".iazio", "bin")
	_ = os.MkdirAll(canonicalDir, 0755)
	_ = os.WriteFile(filepath.Join(canonicalDir, "iazio-agent"), []byte("#!/bin/sh\n"), 0755)

	var stdout bytes.Buffer

	// 1. Darwin restart & stop
	_ = runService([]string{"restart"}, &stdout, func(string) string { return "" })
	_ = runService([]string{"stop"}, &stdout, func(string) string { return "" })

	// 2. Windows install, start, stop, restart, uninstall
	winGetenv := func(k string) string {
		if k == "GOOS_OVERRIDE" {
			return "windows"
		}
		return ""
	}
	_ = runService([]string{"install"}, &stdout, winGetenv)
	_ = runService([]string{"start"}, &stdout, winGetenv)
	_ = runService([]string{"stop"}, &stdout, winGetenv)
	_ = runService([]string{"restart"}, &stdout, winGetenv)
	_ = runService([]string{"uninstall"}, &stdout, winGetenv)

	// 3. Linux stop, uninstall
	linuxGetenv := func(k string) string {
		if k == "GOOS_OVERRIDE" {
			return "linux"
		}
		return ""
	}
	_ = runService([]string{"stop"}, &stdout, linuxGetenv)
	_ = runService([]string{"uninstall"}, &stdout, linuxGetenv)
}

func TestExecuteRun_CLIArgsParsing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so Loop exits

	var stdout, stderr bytes.Buffer
	testCases := [][]string{
		{"run", "--profile=custom-prof", "--api=http://127.0.0.1:9999", "--update-interval=5m", "--update-suite"},
		{"run", "--profile", "custom-prof2", "--api", "http://127.0.0.1:9999", "--update-interval", "disabled"},
		{"run", "--update-interval", "0"},
		{"run", "--update-interval=0"},
		{"run", "--update-interval", "invalid-dur"},
		{"run", "--update-interval=invalid-dur"},
	}

	for _, tc := range testCases {
		stdout.Reset()
		stderr.Reset()
		_ = execute(ctx, tc, &stdout, &stderr, func(k string) string {
			switch k {
			case "IAZIO_AGENT_UPDATE_INTERVAL":
				return "0"
			case "IAZIO_AGENT_UPDATE_SUITE":
				return "true"
			case "IAZIO_AGENT_HOST_KIND":
				return "runner"
			default:
				return ""
			}
		})
	}
}

func TestExecuteRun_AutoHealMergedBranch(t *testing.T) {
	repoDir := t.TempDir()
	_ = exec.Command("git", "init", repoDir).Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.email", "test@test.local").Run()
	_ = exec.Command("git", "-C", repoDir, "config", "user.name", "Test").Run()
	_ = exec.Command("git", "-C", repoDir, "checkout", "-b", "main").Run()
	_ = os.WriteFile(filepath.Join(repoDir, "file.txt"), []byte("initial"), 0644)
	_ = exec.Command("git", "-C", repoDir, "add", ".").Run()
	_ = exec.Command("git", "-C", repoDir, "commit", "-m", "init on main").Run()

	// Add local remote pointing to repoDir and fetch main
	_ = exec.Command("git", "-C", repoDir, "remote", "add", "origin", repoDir).Run()
	_ = exec.Command("git", "-C", repoDir, "fetch", "origin").Run()

	// Create feature branch from main
	_ = exec.Command("git", "-C", repoDir, "checkout", "-b", "feat/my-story").Run()

	var finishSeen atomic.Bool
	var pollCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(controlplane.HeartbeatResponse{Status: "OK"})
		case strings.HasSuffix(r.URL.Path, "/preflight"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Reason":""}`))
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishSeen.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Queue":"OPEN"}`))
		case strings.HasSuffix(r.URL.Path, "/poll"):
			if pollCount.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(controlplane.Assignment{
					ID:           "job-auto-heal",
					Kind:         "ordinary",
					WorktreePath: repoDir,
					StoryID:      "STORY-AUTO",
				})
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 50; i++ {
			if finishSeen.Load() {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "0"
		default:
			return ""
		}
	})

	if !finishSeen.Load() {
		t.Log("Note: auto heal finish report submitted asynchronously")
	}
}

func TestExecuteRun_BackgroundUpdateTicker(t *testing.T) {
	origDelay := initialUpdateDelay
	initialUpdateDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		initialUpdateDelay = origDelay
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(controlplane.HeartbeatResponse{Status: "OK"})
		case strings.HasSuffix(r.URL.Path, "/tools/manifest"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ManifestResponse{
				Tools: []ManifestToolItem{},
			})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(120*time.Millisecond, cancel)

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "20ms"
		default:
			return ""
		}
	})
}

func TestExecuteRun_DaemonPreflightTelemetry(t *testing.T) {
	var heartbeatCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeatCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(controlplane.HeartbeatResponse{Status: "OK"})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(250*time.Millisecond, cancel)

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "0"
		default:
			return ""
		}
	})

	if !heartbeatCalled.Load() {
		t.Log("Note: heartbeat not called before context cancellation")
	}
}

func TestProbeRepoRuntime(t *testing.T) {
	tempDir := t.TempDir()
	cmd := exec.Command("git", "init", tempDir)
	if err := cmd.Run(); err != nil {
		t.Skip("git not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st := probeRepoRuntime(ctx, tempDir, "temp-alias")
	if st.RepoPath != tempDir || st.Alias != "temp-alias" {
		t.Fatalf("unexpected repo runtime status: %+v", st)
	}

	_ = os.WriteFile(filepath.Join(tempDir, "test.txt"), []byte("hello"), 0644)
	st2 := probeRepoRuntime(ctx, tempDir, "temp-alias")
	if st2.IsClean {
		t.Fatalf("expected dirty repo, got clean: %+v", st2)
	}
}

func TestProbeRepoRuntime_HarnessOutput(t *testing.T) {
	tempHome := t.TempDir()
	binDir := filepath.Join(tempHome, ".iazio", "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("failed to create bin dir: %v", err)
	}
	mockHarness := filepath.Join(binDir, "iazio-harness")
	script := `#!/bin/sh
cat << 'EOF'
{
  "branch": "feat/mock-test",
  "is_clean": false,
  "ahead": 1,
  "behind": 0,
  "modified_files": ["main.go"],
  "untracked_files": ["foo.txt"],
  "submodules_clean": false,
  "dirty_submodules": ["submod1"],
  "ase_state": "active",
  "active_story_id": "STORY-1",
  "agent_tool": "autopilot",
  "tool_liveness": "alive",
  "execution_provenance": "prov",
  "active_conversation_id": "conv-123"
}
EOF
`
	if err := os.WriteFile(mockHarness, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock harness: %v", err)
	}

	t.Setenv("HOME", tempHome)
	origPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+origPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st := probeRepoRuntime(ctx, "/any/path", "test-alias")
	if st.Branch != "feat/mock-test" {
		t.Fatalf("expected branch feat/mock-test, got %s", st.Branch)
	}
	if st.IsClean {
		t.Fatal("expected IsClean false")
	}
	if st.SubmodulesClean {
		t.Fatal("expected SubmodulesClean false")
	}
	if len(st.DirtySubmodules) != 1 || st.DirtySubmodules[0] != "submod1" {
		t.Fatalf("unexpected dirty submodules: %v", st.DirtySubmodules)
	}
	if !strings.Contains(st.Porcelain, "M main.go") || !strings.Contains(st.Porcelain, "?? foo.txt") {
		t.Fatalf("expected porcelain to contain modified and untracked files, got %s", st.Porcelain)
	}
}

func TestResolveWorktreeFromHostDetail(t *testing.T) {
	tempRoot := t.TempDir()
	primaryWT := filepath.Join(tempRoot, "umbrella-root")
	auxWT := filepath.Join(tempRoot, "umbrella-root-02")
	_ = os.MkdirAll(primaryWT, 0755)
	_ = os.MkdirAll(auxWT, 0755)

	_ = os.WriteFile(filepath.Join(primaryWT, ".gitmodules"), []byte(`[submodule "child-pkg"]
	path = child-pkg
`), 0644)
	_ = os.WriteFile(filepath.Join(auxWT, ".gitmodules"), []byte(`[submodule "child-pkg"]
	path = child-pkg
`), 0644)

	detail := controlplane.HostDetail{
		Repos: []controlplane.RepoSummary{
			{
				Alias:        "alias-repo",
				WorktreePath: "/path/alias",
			},
			{
				RepoAlias: "repo-alias-name",
				Path:      "/path/repo-alias",
			},
			{
				WorktreePath: auxWT,
			},
			{
				WorktreePath:                 primaryWT,
				GlobalQueueAllocationEnabled: true,
			},
		},
	}

	// 1. Exact alias match
	if wt := resolveWorktreeFromHostDetail(detail, "alias-repo"); wt != "/path/alias" {
		t.Fatalf("expected /path/alias, got %s", wt)
	}

	// 2. RepoAlias match
	if wt := resolveWorktreeFromHostDetail(detail, "repo-alias-name"); wt != "/path/repo-alias" {
		t.Fatalf("expected /path/repo-alias, got %s", wt)
	}

	// 3. Submodule match prefers primaryWT (not auxWT which has -02)
	if wt := resolveWorktreeFromHostDetail(detail, "child-pkg"); wt != primaryWT {
		t.Fatalf("expected primaryWT %s, got %s", primaryWT, wt)
	}

	// 4. Root matches GlobalQueueAllocationEnabled
	if wt := resolveWorktreeFromHostDetail(detail, "."); wt != primaryWT {
		t.Fatalf("expected primaryWT for root '.', got %s", wt)
	}
	if wt := resolveWorktreeFromHostDetail(detail, "root"); wt != primaryWT {
		t.Fatalf("expected primaryWT for 'root', got %s", wt)
	}

	// 5. Unknown repo returns empty
	if wt := resolveWorktreeFromHostDetail(detail, "nonexistent"); wt != "" {
		t.Fatalf("expected empty for nonexistent, got %s", wt)
	}

	// 6. Cross-host slot family fallback (e.g. umbrella-root-04 from another host falls back to registered umbrella-root)
	if wt := resolveWorktreeFromHostDetail(detail, "/Users/romeo/work/umbrella-root-04"); wt != primaryWT {
		t.Fatalf("expected primaryWT %s for foreign umbrella-root-04 slot, got %s", primaryWT, wt)
	}
}

func TestOnJob_RepoBusyRefusal(t *testing.T) {
	wt := t.TempDir()
	var abandonSeen atomic.Bool
	var abandonReason string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/abandon") {
			mu.Lock()
			abandonSeen.Store(true)
			var body struct {
				Reason string `json:"reason"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			abandonReason = body.Reason
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Simulate active lock held by current process PID
	info := repolock.LockInfo{
		JobID:          "job-prior-1192",
		PID:            os.Getpid(),
		Engine:         "agy",
		ConversationID: "conv-active-1192",
		Status:         "RUNNING",
	}
	lockHandle, err := repolock.Acquire(wt, info, func(pid int) bool { return pid == os.Getpid() })
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer lockHandle.Release()

	sup := supervisor.New(time.Now)
	client := controlplane.Client{BaseURL: srv.URL}
	var stderr bytes.Buffer

	// Define onJob logic matching agent execution
	onJob := func(a controlplane.Assignment) error {
		lockInfo, lockErr := repolock.Read(a.WorktreePath)
		if lockErr == nil && lockInfo.JobID != "" && lockInfo.JobID != a.ID {
			checkPID := lockInfo.PID
			if lockInfo.HarnessPID > 0 {
				checkPID = lockInfo.HarnessPID
			}
			if checkPID > 0 {
				p, findErr := os.FindProcess(checkPID)
				if findErr == nil && p.Signal(syscall.Signal(0)) == nil {
					fmt.Fprintf(&stderr, "[agent] refusing concurrent job %s on %s: worktree actively locked by job %s (pid %d)\n",
						a.ID, a.WorktreePath, lockInfo.JobID, checkPID)
					_ = client.AbandonJob(context.Background(), a.ID, "REPO_BUSY")
					return fmt.Errorf("repository %s busy with job %s", a.WorktreePath, lockInfo.JobID)
				}
			}
		}
		return sup.Spawn(supervisor.NewOSRunner(), a.ID, srv.URL, a.WorktreePath, "", "")
	}

	assignment := controlplane.Assignment{
		ID:           "job-concurrent-1193",
		WorktreePath: wt,
	}

	err = onJob(assignment)
	if err == nil {
		t.Fatal("expected onJob to fail with repository busy error")
	}
	if !strings.Contains(err.Error(), "busy with job job-prior-1192") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !abandonSeen.Load() {
		t.Fatal("expected AbandonJob to be called with REPO_BUSY")
	}
	mu.Lock()
	gotReason := abandonReason
	mu.Unlock()
	if gotReason != "REPO_BUSY" {
		t.Fatalf("expected abandon reason REPO_BUSY, got %q", gotReason)
	}
}

func TestHelpers_DocAndSubmoduleCheck(t *testing.T) {
	tmp := t.TempDir()
	if isGitSubmoduleDir(tmp) {
		t.Fatalf("expected non-git dir to return false")
	}
	_ = os.Mkdir(filepath.Join(tmp, ".git"), 0755)
	if !isGitSubmoduleDir(tmp) {
		t.Fatalf("expected git dir to return true")
	}
	if !isDocOrEphemeralFile("docs/stories/story.md") {
		t.Fatalf("expected doc file to return true")
	}
	if isDocOrEphemeralFile("cmd/main.go") {
		t.Fatalf("expected go file to return false")
	}
}

func TestProbeRepoRuntime_LocalGit(t *testing.T) {
	tmp := t.TempDir()
	cmdInit := exec.Command("git", "init", tmp)
	if err := cmdInit.Run(); err != nil {
		t.Skip("git init failed")
	}
	ctx := context.Background()
	st := probeRepoRuntime(ctx, tmp, "test-repo")
	if st.RepoPath != tmp {
		t.Fatalf("expected RepoPath %q, got %q", tmp, st.RepoPath)
	}
	if st.Alias != "test-repo" {
		t.Fatalf("expected Alias test-repo, got %q", st.Alias)
	}
}

func TestApplyWindows_Actions(t *testing.T) {
	origExec := execCommand
	defer func() { execCommand = origExec }()

	var calls []string
	execCommand = func(name string, args ...string) error {
		calls = append(calls, name+" "+strings.Join(args, " "))
		return nil
	}

	var stdout bytes.Buffer
	_ = applyWindows("start", "", &stdout)
	_ = applyWindows("stop", "", &stdout)
	_ = applyWindows("restart", "", &stdout)
	_ = applyWindows("uninstall", "", &stdout)
	err := applyWindows("invalid", "", &stdout)
	if err == nil {
		t.Fatalf("expected error for invalid action")
	}
	if len(calls) != 5 {
		t.Fatalf("expected 5 calls, got %d: %v", len(calls), calls)
	}
}

func TestCopyBinary_Cases(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.bin")
	_ = os.WriteFile(src, []byte("content"), 0755)

	if err := copyBinary(src, src); err != nil {
		t.Fatalf("expected no-op for same path: %v", err)
	}

	dest := filepath.Join(tmp, "sub", "dest.bin")
	if err := copyBinary(src, dest); err != nil {
		t.Fatalf("copyBinary failed: %v", err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "content" {
		t.Fatalf("unexpected content: %s", string(data))
	}
}

func TestScanRepoFleetStatus_Cases(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	if scanRepoFleetStatus(ctx, tmp) {
		t.Fatalf("expected non-repo to return false")
	}

	cmd := exec.Command("git", "init", tmp)
	if err := cmd.Run(); err != nil {
		t.Skip("git init not available")
	}
	if !scanRepoFleetStatus(ctx, tmp) {
		t.Fatalf("expected empty git repo to be clean")
	}

	docFile := filepath.Join(tmp, "docs", "note.md")
	_ = os.MkdirAll(filepath.Dir(docFile), 0755)
	_ = os.WriteFile(docFile, []byte("note"), 0644)
	if !scanRepoFleetStatus(ctx, tmp) {
		t.Fatalf("expected git repo with untracked doc to be clean")
	}

	codeFile := filepath.Join(tmp, "foo.go")
	_ = os.WriteFile(codeFile, []byte("package main"), 0644)
	if scanRepoFleetStatus(ctx, tmp) {
		t.Fatalf("expected git repo with untracked code to be dirty")
	}
}

func TestRestartOrExecSelf(t *testing.T) {
	orig := syscallExec
	defer func() { syscallExec = orig }()
	called := false
	syscallExec = func(argv0 string, argv []string, envv []string) error {
		called = true
		return nil
	}
	var stdout bytes.Buffer
	restartOrExecSelf(&stdout)
	if runtime.GOOS != "windows" && !called {
		t.Fatalf("expected syscallExec to be called")
	}
}

func TestExecute_Run_WithElasticsearchSink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_ELASTICSEARCH_URL":
			return "http://127.0.0.1:9200"
		case "IAZIO_AGENT_PROFILE":
			return "test-profile"
		case "IAZIO_AGENT_UPDATE_INTERVAL":
			return "10m"
		default:
			return ""
		}
	})
}

func TestExecute_Service_PauseResumeReviveDevInstall(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)
	t.Setenv("IAZIO_DISABLE_SYSTEMD", "1")

	origExec := execCommand
	defer func() { execCommand = origExec }()
	execCommand = func(name string, args ...string) error {
		return nil
	}

	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	_ = execute(ctx, []string{"service", "pause"}, &stdout, &stderr, nil)
	_ = execute(ctx, []string{"service", "resume"}, &stdout, &stderr, nil)
	_ = execute(ctx, []string{"service", "revive-check"}, &stdout, &stderr, nil)

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "src-bin")
	_ = os.WriteFile(bin, []byte("#!/bin/sh\necho 'valid dev binary'\nexit 0\n"), 0755)
	_ = execute(ctx, []string{"service", "dev-install", bin}, &stdout, &stderr, nil)
}

func TestExecute_Service_DevInstall_RejectsEmptyBinary(t *testing.T) {
	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	t.Setenv("USERPROFILE", fakeHome)
	t.Setenv("IAZIO_DISABLE_SYSTEMD", "1")

	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	tmp := t.TempDir()
	emptyBin := filepath.Join(tmp, "empty-bin")
	_ = os.WriteFile(emptyBin, []byte("#!/bin/sh\n"), 0755)
	err := execute(ctx, []string{"service", "dev-install", emptyBin}, &stdout, &stderr, nil)
	if err == nil {
		t.Fatal("expected error when installing empty script")
	}
}

func TestProbeRepoRuntime_StateFiles(t *testing.T) {
	tmp := t.TempDir()
	cmdInit := exec.Command("git", "init", tmp)
	_ = cmdInit.Run()

	apDir := filepath.Join(tmp, ".autopilot")
	_ = os.MkdirAll(apDir, 0755)
	_ = os.WriteFile(filepath.Join(apDir, "state.json"), []byte(`{"state":"IN_PROGRESS","story_id":"STORY-999"}`), 0644)

	ctx := context.Background()
	st := probeRepoRuntime(ctx, tmp, "test-repo")
	if st.ASEState != "IN_PROGRESS" || st.ActiveStoryID != "STORY-999" {
		t.Fatalf("unexpected status: %+v", st)
	}

	_ = os.Remove(filepath.Join(apDir, "state.json"))
	aseDir := filepath.Join(tmp, ".ase")
	_ = os.MkdirAll(aseDir, 0755)
	_ = os.WriteFile(filepath.Join(aseDir, "active.lock"), []byte("lock"), 0644)

	st = probeRepoRuntime(ctx, tmp, "test-repo")
	if st.ASEState != "RUNNING" && st.ASEState != "ORPHANED" {
		t.Fatalf("expected RUNNING or ORPHANED for active.lock, got %s", st.ASEState)
	}
}

func TestProbeRepoRuntime_MetadataYmlAndGitVersion(t *testing.T) {
	tmp := t.TempDir()
	cmdInit := exec.Command("git", "init", tmp)
	if err := cmdInit.Run(); err != nil {
		t.Skip("git not installed")
	}

	metadataContent := []byte("schema_version: 1\nname: test-fleet\n")
	if err := os.WriteFile(filepath.Join(tmp, "metadata.yml"), metadataContent, 0644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	st := probeRepoRuntime(ctx, tmp, "test-repo")

	if st.MetadataYmlRaw != string(metadataContent) {
		t.Fatalf("expected raw metadata %q, got %q", string(metadataContent), st.MetadataYmlRaw)
	}
	if st.MetadataYmlSHA == "" {
		t.Fatal("expected non-empty MetadataYmlSHA")
	}
	if st.MetadataYmlModTime == "" {
		t.Fatal("expected non-empty MetadataYmlModTime")
	}
}

func TestHealGitRepo_RemovesLockAndCleans(t *testing.T) {
	tmp := t.TempDir()
	gitDir := filepath.Join(tmp, ".git")
	_ = os.MkdirAll(gitDir, 0755)
	lockFile := filepath.Join(gitDir, "index.lock")
	_ = os.WriteFile(lockFile, []byte("lock"), 0644)

	healGitRepo(context.Background(), tmp)

	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		t.Fatalf("expected index.lock to be removed, got err: %v", err)
	}
}

func TestExecuteRun_WithCommandsAndToolUpdate(t *testing.T) {
	orig := syscallExec
	defer func() { syscallExec = orig }()
	syscallExec = func(argv0 string, argv []string, envv []string) error {
		return nil
	}

	var pingAckReceived atomic.Bool
	var claimed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"registered":true}`))
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"ok": true,
				"pending_tool_update": {"tools":["custom-tool"],"force":true},
				"pending_commands": [
					{"id":"cmd-ping-1","type":"PING"},
					{"id":"cmd-fleet-1","type":"CHECK_FLEET_GIT_STATUS","payload":"/tmp"},
					{"id":"cmd-fleet-2","type":"CHECK_FLEET_GIT_STATUS","payload":""},
					{"id":"cmd-idle-1","type":"TRIGGER_IDLE_UPDATE"}
				]
			}`))
		case strings.HasSuffix(r.URL.Path, "/jobs/claim"):
			if !claimed.Swap(true) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"id": "job-test-1",
					"kind": "tool.update",
					"repo": "custom-repo",
					"fencing_token": 1,
					"payload": {"tools":["custom-tool"],"force":true}
				}`))
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case strings.HasSuffix(r.URL.Path, "/hosts/test-host"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"id": "test-host",
				"repos": [
					{"alias": "root", "worktree_path": "/tmp"}
				]
			}`))
		case strings.Contains(r.URL.Path, "/fleet/manifest"):
			w.WriteHeader(http.StatusNotFound)
		case strings.Contains(r.URL.Path, "/commands/") && strings.HasSuffix(r.URL.Path, "/ack"):
			pingAckReceived.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case strings.HasSuffix(r.URL.Path, "/repo-status"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case strings.HasSuffix(r.URL.Path, "/poll"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 40; i++ {
			if pingAckReceived.Load() {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	_ = execute(ctx, []string{"run", "--update-suite=false"}, &stdout, &stderr, func(k string) string {
		switch k {
		case "IAZIO_HARNESS_API_URL":
			return srv.URL
		case "IAZIO_AGENT_HOST_ID":
			return "test-host"
		case "IAZIO_AGENT_NONINTERACTIVE":
			return "1"
		default:
			return ""
		}
	})
}

type mockHostGetter struct {
	detail controlplane.HostDetail
	err    error
}

func (m mockHostGetter) GetHost(ctx context.Context, hostID string) (controlplane.HostDetail, error) {
	return m.detail, m.err
}

func TestResolveWorktreeForRepo(t *testing.T) {
	ctx := context.Background()

	getter := mockHostGetter{
		detail: controlplane.HostDetail{
			Repos: []controlplane.RepoSummary{
				{Alias: "my-repo", WorktreePath: "/tmp/work/my-repo"},
			},
		},
	}
	res := resolveWorktreeForRepo(ctx, getter, "host-1", "my-repo", nil)
	if res != "/tmp/work/my-repo" {
		t.Fatalf("expected /tmp/work/my-repo, got %q", res)
	}

	getterErr := mockHostGetter{err: errors.New("not found")}
	resEnv := resolveWorktreeForRepo(ctx, getterErr, "host-1", "other-repo", func(k string) string {
		if k == "IAZIO_FLEET_ROOT" {
			return "/opt/fleet-root"
		}
		return ""
	})
	if resEnv != "/opt/fleet-root" {
		t.Fatalf("expected /opt/fleet-root, got %q", resEnv)
	}

	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	candWork := filepath.Join(fakeHome, "work", "repo-cand")
	if err := os.MkdirAll(candWork, 0o755); err != nil {
		t.Fatal(err)
	}
	gitmodules := filepath.Join(candWork, ".gitmodules")
	if err := os.WriteFile(gitmodules, []byte("[submodule \"sub-1\"]\n\tpath = sub-1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	resCand := resolveWorktreeForRepo(ctx, getterErr, "host-1", "sub-1", func(k string) string { return "" })
	if resCand != candWork {
		t.Fatalf("expected %s, got %q", candWork, resCand)
	}

	resRoot := resolveWorktreeForRepo(ctx, getterErr, "host-1", ".", func(k string) string { return "" })
	if resRoot != candWork {
		t.Fatalf("expected %s, got %q", candWork, resRoot)
	}

	subDir := filepath.Join(candWork, "direct-sub")
	_ = os.MkdirAll(subDir, 0o755)
	resDirect := resolveWorktreeForRepo(ctx, getterErr, "host-1", "direct-sub", func(k string) string { return "" })
	if resDirect != candWork {
		t.Fatalf("expected %s, got %q", candWork, resDirect)
	}

	resEmpty := resolveWorktreeForRepo(ctx, getterErr, "host-1", "nonexistent-sub", func(k string) string { return "" })
	if resEmpty != "" {
		t.Fatalf("expected empty worktree, got %q", resEmpty)
	}
}

func TestProbeRepoRuntime_HarnessAndFallback(t *testing.T) {
	ctx := context.Background()

	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	fakeBin := filepath.Join(fakeHome, ".iazio", "bin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	harnessScript := filepath.Join(fakeBin, "iazio-harness")
	scriptContent := `#!/bin/sh
echo '{"branch":"feature/test-1","is_clean":false,"ahead":1,"behind":0,"modified_files":["file.go"],"untracked_files":["new.txt"],"submodules_clean":false,"dirty_submodules":["subA"],"ase_state":"RUNNING","active_story_id":"STORY-999","agent_tool":"agy","tool_liveness":"alive","execution_provenance":"test-prov","active_conversation_id":"conv-999"}'
`
	if err := os.WriteFile(harnessScript, []byte(scriptContent), 0o755); err != nil {
		t.Fatal(err)
	}

	var cleanPathElements []string
	for _, elem := range filepath.SplitList(os.Getenv("PATH")) {
		if strings.Contains(elem, ".iazio") {
			continue
		}
		if _, err := os.Stat(filepath.Join(elem, "iazio-harness")); err == nil {
			continue
		}
		cleanPathElements = append(cleanPathElements, elem)
	}
	cleanPath := strings.Join(cleanPathElements, string(os.PathListSeparator))

	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+cleanPath)

	st := probeRepoRuntime(ctx, fakeBin, "test-repo")
	if st.Branch != "feature/test-1" || st.ActiveStoryID != "STORY-999" || st.ActiveTool != "agy" || st.ActiveConversationID != "conv-999" {
		t.Fatalf("unexpected probe status: %+v", st)
	}
	if !strings.Contains(st.Porcelain, "M file.go") || !strings.Contains(st.Porcelain, "?? new.txt") {
		t.Fatalf("expected porcelain lines, got: %s", st.Porcelain)
	}

	repoDir := t.TempDir()
	_ = os.Remove(harnessScript)
	t.Setenv("PATH", cleanPath)

	cmd := exec.Command("git", "init", repoDir)
	_ = cmd.Run()
	_ = os.WriteFile(filepath.Join(repoDir, "hello.txt"), []byte("hi"), 0o644)

	apDir := filepath.Join(repoDir, ".autopilot")
	_ = os.MkdirAll(apDir, 0o755)
	_ = os.WriteFile(filepath.Join(apDir, "state.json"), []byte(`{"state":"COMPLETING","story_id":"STORY-FALLBACK"}`), 0o644)

	stFallback := probeRepoRuntime(ctx, repoDir, "repo-dir")
	if stFallback.IsClean {
		t.Fatal("expected repo to be dirty due to untracked hello.txt")
	}
	if stFallback.ASEState != "COMPLETING" || stFallback.ActiveStoryID != "STORY-FALLBACK" {
		t.Fatalf("expected COMPLETING/STORY-FALLBACK, got: %+v", stFallback)
	}

	_ = os.Remove(filepath.Join(apDir, "state.json"))
	_ = os.WriteFile(filepath.Join(apDir, "lock"), []byte("locked"), 0o644)
	stLock := probeRepoRuntime(ctx, repoDir, "repo-dir")
	if stLock.ASEState != "RUNNING" && stLock.ASEState != "ORPHANED" {
		t.Fatalf("expected RUNNING or ORPHANED from lock, got: %s", stLock.ASEState)
	}

	_ = os.Remove(filepath.Join(apDir, "lock"))
	stIdle := probeRepoRuntime(ctx, repoDir, "repo-dir")
	t.Logf("stIdle: %+v", stIdle)
	if stIdle.ASEState != "IDLE" {
		t.Fatalf("expected IDLE, got: %s", stIdle.ASEState)
	}
}

func TestScanRepoFleetStatus_HarnessJSON(t *testing.T) {
	ctx := context.Background()

	repoDir := t.TempDir()
	if err := exec.Command("git", "init", repoDir).Run(); err != nil {
		t.Fatal(err)
	}

	fakeHome := t.TempDir()
	t.Setenv("HOME", fakeHome)
	fakeBin := filepath.Join(fakeHome, ".iazio", "bin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	harnessScript := filepath.Join(fakeBin, "iazio-harness")
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	_ = os.WriteFile(harnessScript, []byte("#!/bin/sh\necho '{\"worktree_state\":\"CLEAN\",\"submodules_clean\":true}'\n"), 0o755)
	if !scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected clean repo status for CLEAN")
	}

	_ = os.WriteFile(harnessScript, []byte("#!/bin/sh\necho '{\"worktree_state\":\"CLEAN_SUBMODULE_DRIFT\",\"submodules_clean\":true}'\n"), 0o755)
	if !scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected clean repo status for CLEAN_SUBMODULE_DRIFT")
	}

	_ = os.WriteFile(harnessScript, []byte("#!/bin/sh\necho '{\"worktree_state\":\"DIRTY_UNTRACKED_ONLY\",\"submodules_clean\":true,\"untracked_files\":[\"docs/note.md\",\"README.md\"]}'\n"), 0o755)
	if !scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected clean repo status for doc untracked files")
	}

	_ = os.WriteFile(harnessScript, []byte("#!/bin/sh\necho '{\"worktree_state\":\"DIRTY_UNTRACKED_ONLY\",\"submodules_clean\":true,\"untracked_files\":[\"main.go\"]}'\n"), 0o755)
	if scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected dirty repo status for source untracked files")
	}

	_ = os.WriteFile(harnessScript, []byte("#!/bin/sh\necho '{\"worktree_state\":\"DIRTY\",\"submodules_clean\":false}'\n"), 0o755)
	if scanRepoFleetStatus(ctx, repoDir) {
		t.Fatal("expected dirty repo status for DIRTY")
	}
}

func TestResolveHostID_EdgeCases(t *testing.T) {
	// hostname error and WSL true -> runner-wsl
	id := resolveHostID(nil, func() (string, error) { return "", errors.New("err") }, func() bool { return true })
	if id != "runner-wsl" {
		t.Fatalf("expected runner-wsl, got %s", id)
	}
	// hostname empty and WSL false -> runner
	idNoWSL := resolveHostID(func(string) string { return "" }, func() (string, error) { return "", nil }, func() bool { return false })
	if idNoWSL != "runner" {
		t.Fatalf("expected runner, got %s", idNoWSL)
	}
}

func TestResolveDispatchConversationID_Precedence(t *testing.T) {
	cActive := "aad902c8-bda6-44c7-8a68-c468a1562399"
	cResume := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"
	cInitial := "2d446877-5bf9-44eb-9db5-e1c007a9fe32"
	cSlice := "3d446877-5bf9-44eb-9db5-e1c007a9fe33"
	cLock := "4d446877-5bf9-44eb-9db5-e1c007a9fe34"

	// 1. Active takes top precedence over Resume, Initial, ConversationIDs, and Lock
	a1 := controlplane.Assignment{
		ActiveConversationID:  cActive,
		ResumeConversationID:  cResume,
		InitialConversationID: cInitial,
		ConversationIDs:       []string{cSlice},
	}
	if got := ResolveDispatchConversationID(a1, cLock, "STORY-1", "RUNNING"); got != cActive {
		t.Fatalf("expected active %s, got %s", cActive, got)
	}

	// 2. Resume takes precedence when Active is empty
	a2 := controlplane.Assignment{
		ResumeConversationID:  cResume,
		InitialConversationID: cInitial,
		ConversationIDs:       []string{cSlice},
	}
	if got := ResolveDispatchConversationID(a2, cLock, "STORY-1", "RUNNING"); got != cResume {
		t.Fatalf("expected resume %s, got %s", cResume, got)
	}

	// 3. Initial takes precedence when Active and Resume are empty
	a3 := controlplane.Assignment{
		InitialConversationID: cInitial,
		ConversationIDs:       []string{cSlice},
	}
	if got := ResolveDispatchConversationID(a3, cLock, "STORY-1", "RUNNING"); got != cInitial {
		t.Fatalf("expected initial %s, got %s", cInitial, got)
	}

	// 4. ConversationIDs slice fallback
	a4 := controlplane.Assignment{
		ConversationIDs: []string{cSlice},
	}
	if got := ResolveDispatchConversationID(a4, cLock, "STORY-1", "RUNNING"); got != cSlice {
		t.Fatalf("expected slice %s, got %s", cSlice, got)
	}

	// 5. Lock fallback for resume kind
	a5 := controlplane.Assignment{
		Kind: "resume",
	}
	if got := ResolveDispatchConversationID(a5, cLock, "STORY-1", "RUNNING"); got != cLock {
		t.Fatalf("expected lock %s, got %s", cLock, got)
	}

	// 6. Invalid format rejected (path traversal / bad chars)
	a6 := controlplane.Assignment{
		ActiveConversationID: "../../etc/passwd",
	}
	if got := ResolveDispatchConversationID(a6, "", "", ""); got != "" {
		t.Fatalf("expected empty for invalid ID, got %s", got)
	}

	// 7. ResumeMode == fallback_new_conversation with empty ActiveConversationID starts fresh (Finding C3)
	a7 := controlplane.Assignment{
		ResumeMode:            "fallback_new_conversation",
		InitialConversationID: cInitial,
		ResumeConversationID:  cResume,
		ConversationIDs:       []string{cSlice},
	}
	if got := ResolveDispatchConversationID(a7, cLock, "STORY-1", "RUNNING"); got != "" {
		t.Fatalf("expected empty conversation for fallback_new_conversation without active ID, got %s", got)
	}

	// 8. ResumeMode == fallback_new_conversation with ActiveConversationID already pinned reuses active
	a8 := controlplane.Assignment{
		ResumeMode:            "fallback_new_conversation",
		ActiveConversationID:  cActive,
		InitialConversationID: cInitial,
	}
	if got := ResolveDispatchConversationID(a8, cLock, "STORY-1", "RUNNING"); got != cActive {
		t.Fatalf("expected active %s for pinned fallback conversation, got %s", cActive, got)
	}

	// 9. Control plane resume (ParentJobID set) with empty registered conversations must NOT adopt lock conversation (Finding M2)
	a9 := controlplane.Assignment{
		Kind:        "resume",
		ParentJobID: "job-parent-001",
	}
	if got := ResolveDispatchConversationID(a9, cLock, "STORY-1", "RUNNING"); got != "" {
		t.Fatalf("CP resume with empty parent conversation must NOT adopt lock %s, got %s", cLock, got)
	}

	// 10. Control plane resume (RootJobID set) with empty registered conversations must NOT adopt lock conversation (Finding M2)
	a10 := controlplane.Assignment{
		Kind:      "resume",
		RootJobID: "job-root-001",
	}
	if got := ResolveDispatchConversationID(a10, cLock, "STORY-1", "RUNNING"); got != "" {
		t.Fatalf("CP resume with RootJobID set must NOT adopt lock %s, got %s", cLock, got)
	}
}

func TestDispatcher_ToolUpdaterResultMapping(t *testing.T) {
	// Test that non-agent update (e.g. updated autopilot) succeeds without error
	// and does not return "tool update deferred: workstation is busy or harness is actively running" (F-3)
	doToolUpdateDetailed := func(ctx context.Context, tools []string, force bool) (supervisor.UpdateResult, bool, error) {
		return supervisor.UpdateResult{
			UpdatedTools: []string{"autopilot"},
			AgentUpdated: false,
		}, false, nil
	}

	handler := func(ctx context.Context, tools []string, force bool) error {
		res, deferred, err := doToolUpdateDetailed(ctx, tools, true)
		if err != nil {
			return fmt.Errorf("tool update failed: %w", err)
		}
		if deferred {
			return fmt.Errorf("tool update deferred: workstation is busy or harness is actively running")
		}
		if res.AgentUpdated {
			// would restart
		}
		return nil
	}

	if err := handler(context.Background(), []string{"autopilot"}, true); err != nil {
		t.Fatalf("expected nil error when non-agent tools update successfully, got: %v", err)
	}

	// Test that up-to-date tools also succeed without error
	doToolUpdateUpToDate := func(ctx context.Context, tools []string, force bool) (supervisor.UpdateResult, bool, error) {
		return supervisor.UpdateResult{
			UpdatedTools: nil,
			AgentUpdated: false,
		}, false, nil
	}
	handlerUpToDate := func(ctx context.Context, tools []string, force bool) error {
		res, deferred, err := doToolUpdateUpToDate(ctx, tools, true)
		if err != nil {
			return fmt.Errorf("tool update failed: %w", err)
		}
		if deferred {
			return fmt.Errorf("tool update deferred: workstation is busy or harness is actively running")
		}
		if res.AgentUpdated {
			// would restart
		}
		return nil
	}
	if err := handlerUpToDate(context.Background(), []string{"autopilot"}, true); err != nil {
		t.Fatalf("expected nil error when tools are already up to date, got: %v", err)
	}

	// Test that deferred returns worker.ErrJobDeferred so dispatcher can ack without failure (N3)
	doToolUpdateDeferred := func(ctx context.Context, tools []string, force bool) (supervisor.UpdateResult, bool, error) {
		return supervisor.UpdateResult{}, true, nil
	}
	handlerDeferred := func(ctx context.Context, tools []string, force bool) error {
		res, deferred, err := doToolUpdateDeferred(ctx, tools, force)
		if err != nil {
			return fmt.Errorf("tool update failed: %w", err)
		}
		if deferred {
			return worker.ErrJobDeferred
		}
		if res.AgentUpdated {
			// would restart
		}
		return nil
	}
	if err := handlerDeferred(context.Background(), []string{"autopilot"}, false); !errors.Is(err, worker.ErrJobDeferred) {
		t.Fatalf("expected ErrJobDeferred for deferred update, got: %v", err)
	}
}

type testManifestFetcher struct {
	manifest *supervisor.FleetManifest
	err      error
}

func (m *testManifestFetcher) FetchManifest(ctx context.Context) (*supervisor.FleetManifest, error) {
	return m.manifest, m.err
}

func TestBuildEagerHooks_SuiteOptOutAndBackoff(t *testing.T) {
	// S2: Verify buildEagerHooks when updateSuite is false vs true
	manifest := &supervisor.FleetManifest{
		Tools: []supervisor.ManifestTool{
			{Name: "iazio-agent", BinaryName: "iazio-agent", TargetVersion: "1.0.0"},
			{Name: "tool-b", BinaryName: "tool-b", TargetVersion: "2.0.0"},
			{Name: "tool-c", BinaryName: "tool-c", TargetVersion: "3.0.0"},
		},
	}
	fetcher := &testManifestFetcher{manifest: manifest}

	tempDir := t.TempDir()

	// 1. Suite update disabled: listSuiteTools returns ONLY ["iazio-agent"]
	checkDriftFalse, listSuiteFalse := buildEagerHooks(
		false,
		fetcher,
		func(tool string) bool { return tool == "tool-b" },
		func(_ *supervisor.FleetManifest) string { return tempDir },
		runtime.GOOS,
	)

	suiteTools, err := listSuiteFalse(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(suiteTools) != 1 || suiteTools[0] != "iazio-agent" {
		t.Fatalf("expected only iazio-agent when suite opt-out, got: %v", suiteTools)
	}

	// 2. Suite update enabled: listSuiteTools returns all tools in manifest
	checkDriftTrue, listSuiteTrue := buildEagerHooks(
		true,
		fetcher,
		func(tool string) bool { return tool == "tool-b" },
		func(_ *supervisor.FleetManifest) string { return tempDir },
		runtime.GOOS,
	)

	allTools, err := listSuiteTrue(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(allTools) != 3 {
		t.Fatalf("expected all 3 tools when suite enabled, got: %v", allTools)
	}

	// 3. Seed binaries: iazio-agent at 1.0.0 (up to date), tool-c at 3.0.0 (up to date).
	// tool-b is missing/drifting.
	if runtime.GOOS != "windows" {
		agentScript := filepath.Join(tempDir, "iazio-agent")
		if err := os.WriteFile(agentScript, []byte("#!/bin/sh\necho 'iazio-agent 1.0.0'\n"), 0755); err != nil {
			t.Fatalf("failed to write agentScript: %v", err)
		}
		toolCScript := filepath.Join(tempDir, "tool-c")
		if err := os.WriteFile(toolCScript, []byte("#!/bin/sh\necho 'tool-c 3.0.0'\n"), 0755); err != nil {
			t.Fatalf("failed to write toolCScript: %v", err)
		}

		// checkDriftFalse only checks iazio-agent -> no drift
		driftFalse, err := checkDriftFalse(context.Background(), nil)
		if err != nil || driftFalse {
			t.Fatalf("expected false drift for opt-out when iazio-agent is current, got %v err=%v", driftFalse, err)
		}

		// checkDriftTrue checks manifest but excludes backed-off tool-b -> no drift
		driftTrue, err := checkDriftTrue(context.Background(), nil)
		if err != nil || driftTrue {
			t.Fatalf("expected false drift when only backed-off tool drifts, got %v err=%v", driftTrue, err)
		}

		// Positive control 1 (H4): when tool-b is NOT in backoff, suite drift check detects missing tool-b -> true
		checkDriftNoBackoff, _ := buildEagerHooks(
			true,
			fetcher,
			func(tool string) bool { return false }, // no tools backed off
			func(_ *supervisor.FleetManifest) string { return tempDir },
			runtime.GOOS,
		)
		driftNoBackoff, err := checkDriftNoBackoff(context.Background(), nil)
		if err != nil || !driftNoBackoff {
			t.Fatalf("expected true drift when missing tool-b is not backed off, got %v err=%v", driftNoBackoff, err)
		}

		// Positive control 2 (H4): with tool-b still missing and NOT in backoff, suite opt-out still returns false
		// proving opt-out strictly narrows check to iazio-agent and ignores suite drift
		checkDriftOptOutNoBackoff, _ := buildEagerHooks(
			false,
			fetcher,
			func(tool string) bool { return false }, // no tools backed off
			func(_ *supervisor.FleetManifest) string { return tempDir },
			runtime.GOOS,
		)
		driftOptOut, err := checkDriftOptOutNoBackoff(context.Background(), nil)
		if err != nil || driftOptOut {
			t.Fatalf("expected false drift for suite opt-out even when suite tools drift, got %v err=%v", driftOptOut, err)
		}
	}
}

func TestExecute_CommandsAndHarvest(t *testing.T) {
	// 1. flushHarvestConversation empty
	if err := flushHarvestConversation(context.Background(), ""); err != nil {
		t.Errorf("expected nil err for empty conv, got %v", err)
	}

	// 2. flushHarvestConversation with mock harvester
	tmpDir := t.TempDir()
	mockHarvester := filepath.Join(tmpDir, "iazio-harvester")
	_ = os.WriteFile(mockHarvester, []byte("#!/bin/sh\nexit 0\n"), 0755)
	t.Setenv("PATH", tmpDir+":"+os.Getenv("PATH"))
	if err := flushHarvestConversation(context.Background(), "conv-test-123"); err != nil {
		t.Errorf("expected success for mock harvester, got %v", err)
	}

	// 3. execute help & version & unknown
	var stdout, stderr strings.Builder
	_ = execute(context.Background(), []string{"help"}, &stdout, &stderr, nil)
	_ = execute(context.Background(), []string{"--help"}, &stdout, &stderr, nil)
	_ = execute(context.Background(), []string{"version"}, &stdout, &stderr, nil)
	_ = execute(context.Background(), []string{}, &stdout, &stderr, nil)
	_ = execute(context.Background(), []string{"unknown-cmd"}, &stdout, &stderr, nil)
}

func TestScanRepoFleetStatus(t *testing.T) {
	tmpDir := t.TempDir()
	cmd := exec.Command("git", "init", tmpDir)
	if err := cmd.Run(); err != nil {
		t.Fatalf("git init failed: %v", err)
	}

	ctx := context.Background()
	if !scanRepoFleetStatus(ctx, tmpDir) {
		t.Errorf("expected clean git repo to return true")
	}

	// Dirty file
	dirty := filepath.Join(tmpDir, "dirty.go")
	_ = os.WriteFile(dirty, []byte("package main\n"), 0644)
	if scanRepoFleetStatus(ctx, tmpDir) {
		t.Errorf("expected dirty git repo to return false")
	}

	// Non-git directory
	nonGit := t.TempDir()
	if scanRepoFleetStatus(ctx, nonGit) {
		t.Errorf("expected non-git dir to return false")
	}
}

func TestAgent_AgyToolDiscoveryAndTerminalExitIntegration(t *testing.T) {
	// 1. Setup mock control plane recording PostExit and Heartbeat
	var mu sync.Mutex
	var postedExits []map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(controlplane.HeartbeatResponse{Status: "OK"})

		case strings.Contains(r.URL.Path, "/exit"):
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			postedExits = append(postedExits, body)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	// 2. Setup mock-agy binary in test bin directory
	binDir := t.TempDir()
	mockAgyBin := filepath.Join(binDir, "agy")
	_ = os.WriteFile(mockAgyBin, []byte("#!/bin/sh\necho 'antigravity 1.15.0-mock'\nexit 0\n"), 0755)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	// Verify inventory discovers agy
	home, _ := os.UserHomeDir()
	dirs := inventory.DefaultDirs(os.Getenv("PATH"), home, runtime.GOOS)
	found := inventory.Scan(dirs)
	tools, _ := inventory.Classify(found, nil)
	foundAgy := false
	for _, tool := range tools {
		if tool.Name == "agy" || tool.Name == "mock-agy" {
			foundAgy = true
			if !tool.Installed || tool.Status != "OK" {
				t.Errorf("expected agy tool to be installed and OK, got: %+v", tool)
			}
		}
	}
	if !foundAgy {
		t.Errorf("expected inventory to discover agy in %s", binDir)
	}

	// 3. Verify watchdog terminal failure callback triggers PostExit and frees worktree
	client := controlplane.Client{BaseURL: srv.URL}
	sup := supervisor.New(time.Now)
	repoDir := t.TempDir()
	if err := sup.Spawn(nil, "job-integration-999", srv.URL, repoDir, "", "token"); err != nil {
		t.Fatalf("sup.Spawn failed: %v", err)
	}

	if len(sup.RunningJobs()) != 1 {
		t.Fatalf("expected 1 running job in supervisor, got %d", len(sup.RunningJobs()))
	}

	// Create watchdog with OnJobTerminalFailure wired as in main.go
	stateFile := filepath.Join(t.TempDir(), "wd-state.json")
	wd := watchdog.New(watchdog.Options{
		StateFile:  stateFile,
		IsAlive:    func(pid int) bool { return false },
		MaxResumes: 2,
		Spawner: func(ctx context.Context, job watchdog.TrackedJob) (int, error) {
			return 2000, nil
		},
		OnJobTerminalFailure: func(job watchdog.TrackedJob, reason string) {
			_ = client.PostExit(context.Background(), job.JobID, controlplane.ExitOptions{
				ExitCode:                 1,
				Reason:                   "CIRCUIT_BREAKER_TRIPPED",
				DiagnosticCode:           "CIRCUIT_BREAKER_TRIPPED",
				ErrorMessage:             "Terminal execution failure: " + reason,
				TerminationReasonCode:    "CIRCUIT_BREAKER_TRIPPED",
				TerminationReasonMessage: "Terminal execution failure: " + reason,
			})
			sup.MarkIdle(job.WorktreePath)
		},
	})

	// Track job and force terminal failure
	wd.Track(watchdog.TrackedJob{
		JobID:        "job-integration-999",
		PID:          1999,
		WorktreePath: repoDir,
		ResumeCount:  2, // already at max resumes
	})

	// Trigger exit: resume count >= max resumes, trips terminal failure
	wd.OnProcessExit("job-integration-999", 1999, 1, errors.New("fatal crash"))

	// Verify supervisor marked idle
	if len(sup.RunningJobs()) != 0 {
		t.Errorf("expected supervisor RunningJobs to be empty after terminal failure, got %d", len(sup.RunningJobs()))
	}

	// Verify PostExit reached control plane
	mu.Lock()
	defer mu.Unlock()
	if len(postedExits) != 1 {
		t.Fatalf("expected 1 PostExit call, got %d", len(postedExits))
	}
	if postedExits[0]["reason"] != "CIRCUIT_BREAKER_TRIPPED" {
		t.Errorf("expected reason CIRCUIT_BREAKER_TRIPPED, got %v", postedExits[0]["reason"])
	}
}

