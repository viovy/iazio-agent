package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/service"
)

func TestVerifyChecksum(t *testing.T) {
	data := []byte("binary-payload-data")
	sum := sha256.Sum256(data)
	hashStr := hex.EncodeToString(sum[:])

	checksumsText := fmt.Sprintf("%s  iazio-agent-darwin-arm64\n%s  other-file\n", hashStr, "deadbeef12345678")

	// 1. Valid matching checksum
	if err := verifyChecksum(data, "iazio-agent-darwin-arm64", checksumsText); err != nil {
		t.Fatalf("expected valid checksum, got: %v", err)
	}

	// 2. Mismatched checksum
	badData := []byte("corrupted-payload")
	if err := verifyChecksum(badData, "iazio-agent-darwin-arm64", checksumsText); err == nil {
		t.Fatal("expected checksum mismatch error")
	}

	// 3. File not in checksums text fails closed (returns error, G2)
	if err := verifyChecksum(data, "non-existent-file", checksumsText); err == nil {
		t.Fatal("expected error when file not found in checksums (fail-closed)")
	}
}

func createTarGz(t *testing.T, filename string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	hdr := &tar.Header{
		Name: filename,
		Mode: 0755,
		Size: int64(len(content)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	_ = tw.Close()
	_ = gw.Close()
	return buf.Bytes()
}

func createZip(t *testing.T, filename string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	w, err := zw.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
	_ = zw.Close()
	return buf.Bytes()
}

func TestExtractBinary_TarGz(t *testing.T) {
	content := []byte("#!/bin/sh\necho agent\n")
	archive := createTarGz(t, "iazio-agent", content)

	// Success case
	out, err := extractBinary(archive, "iazio-agent-darwin-arm64.tar.gz", "iazio-agent", "darwin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(content) {
		t.Fatalf("content mismatch: got %q, want %q", string(out), string(content))
	}

	// Missing binary in archive
	_, err = extractBinary(archive, "iazio-agent-darwin-arm64.tar.gz", "wrong-binary", "darwin")
	if err == nil || !strings.Contains(err.Error(), "not found in tar.gz") {
		t.Fatalf("expected not found error, got %v", err)
	}

	// Corrupted archive
	_, err = extractBinary([]byte("not a gzip"), "file.tar.gz", "iazio-agent", "darwin")
	if err == nil {
		t.Fatal("expected error on invalid tar.gz")
	}
}

func TestExtractBinary_Zip(t *testing.T) {
	content := []byte("windows-exe-content")
	archive := createZip(t, "iazio-agent.exe", content)

	// Success case
	out, err := extractBinary(archive, "iazio-agent-windows-amd64.zip", "iazio-agent", "windows")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != string(content) {
		t.Fatalf("content mismatch: got %q, want %q", string(out), string(content))
	}

	// Missing binary in zip
	_, err = extractBinary(archive, "archive.zip", "missing-binary", "windows")
	if err == nil || !strings.Contains(err.Error(), "not found in zip") {
		t.Fatalf("expected not found error, got %v", err)
	}

	// Corrupted zip
	_, err = extractBinary([]byte("not a zip"), "archive.zip", "iazio-agent", "windows")
	if err == nil {
		t.Fatal("expected error on invalid zip")
	}

	// Unsupported format
	_, err = extractBinary(content, "binary.exe", "iazio-agent", "windows")
	if err == nil || !strings.Contains(err.Error(), "unsupported archive format") {
		t.Fatalf("expected unsupported archive format error, got %v", err)
	}
}

func TestFindMatchingAsset(t *testing.T) {
	rel := &gitHubRelease{
		TagName: "v1.5.0",
		Assets: []gitHubAsset{
			{Name: "iazio-agent_1.5.0_checksums.txt"},
			{Name: "iazio-agent_1.5.0_signature.sig"},
			{Name: "iazio-agent_1.5.0_darwin_arm64.tar.gz", BrowserDownloadURL: "https://example.com/darwin-arm64"},
			{Name: "iazio-agent_1.5.0_linux_x86_64.tar.gz", BrowserDownloadURL: "https://example.com/linux-amd64"},
			{Name: "iazio-agent_1.5.0_windows_amd64.zip", BrowserDownloadURL: "https://example.com/win-amd64"},
		},
	}

	// Direct match
	asset, err := findMatchingAsset(rel, "iazio-agent", "darwin", "arm64")
	if err != nil || asset.Name != "iazio-agent_1.5.0_darwin_arm64.tar.gz" {
		t.Fatalf("unexpected asset for darwin/arm64: %+v, err: %v", asset, err)
	}

	// AMD64 / x86_64 fallback
	asset, err = findMatchingAsset(rel, "iazio-agent", "linux", "amd64")
	if err != nil || asset.Name != "iazio-agent_1.5.0_linux_x86_64.tar.gz" {
		t.Fatalf("unexpected asset for linux/amd64: %+v, err: %v", asset, err)
	}

	// No match
	_, err = findMatchingAsset(rel, "iazio-agent", "freebsd", "riscv64")
	if err == nil || !strings.Contains(err.Error(), "no asset matching") {
		t.Fatalf("expected no match error, got %v", err)
	}
}

func TestFindMatchingAsset_MultiComponentSuite(t *testing.T) {
	rel := &gitHubRelease{
		TagName: "v0.0.479",
		Assets: []gitHubAsset{
			{Name: "autopilot-backlog_0.0.479_linux_amd64.tar.gz"},
			{Name: "autopilot-fleet_0.0.479_linux_amd64.tar.gz"},
			{Name: "autopilot-story_0.0.479_linux_amd64.tar.gz"},
			{Name: "autopilot-plugins-internal-build-release_0.0.479_linux_amd64.tar.gz"},
		},
	}

	// 1. Specific tool matches its dedicated component asset, NOT the first asset (backlog)
	assetFleet, err := findMatchingAsset(rel, "autopilot-fleet", "linux", "amd64")
	if err != nil || assetFleet.Name != "autopilot-fleet_0.0.479_linux_amd64.tar.gz" {
		t.Fatalf("expected fleet archive, got %+v, err: %v", assetFleet, err)
	}

	assetStory, err := findMatchingAsset(rel, "autopilot-story", "linux", "amd64")
	if err != nil || assetStory.Name != "autopilot-story_0.0.479_linux_amd64.tar.gz" {
		t.Fatalf("expected story archive, got %+v, err: %v", assetStory, err)
	}

	assetBacklog, err := findMatchingAsset(rel, "autopilot-backlog", "linux", "amd64")
	if err != nil || assetBacklog.Name != "autopilot-backlog_0.0.479_linux_amd64.tar.gz" {
		t.Fatalf("expected backlog archive, got %+v, err: %v", assetBacklog, err)
	}

	// 2. Unmatched tool falls back to combined bundle
	assetBundle, err := findMatchingAsset(rel, "autopilot-extra-plugin", "linux", "amd64")
	if err != nil || assetBundle.Name != "autopilot-plugins-internal-build-release_0.0.479_linux_amd64.tar.gz" {
		t.Fatalf("expected bundle fallback, got %+v, err: %v", assetBundle, err)
	}
}

func TestFindChecksumsAsset(t *testing.T) {
	rel := &gitHubRelease{
		Assets: []gitHubAsset{
			{Name: "iazio-agent_darwin_arm64.tar.gz"},
			{Name: "sha256sums.txt"},
		},
	}
	cs := findChecksumsAsset(rel)
	if cs == nil || cs.Name != "sha256sums.txt" {
		t.Fatalf("expected sha256sums.txt, got %+v", cs)
	}

	rel2 := &gitHubRelease{
		Assets: []gitHubAsset{
			{Name: "iazio-agent_checksums.txt"},
		},
	}
	cs2 := findChecksumsAsset(rel2)
	if cs2 == nil || cs2.Name != "iazio-agent_checksums.txt" {
		t.Fatalf("expected iazio-agent_checksums.txt, got %+v", cs2)
	}

	relEmpty := &gitHubRelease{}
	if findChecksumsAsset(relEmpty) != nil {
		t.Fatal("expected nil checksum asset")
	}
}

func TestResolveInstallDir(t *testing.T) {
	roots := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Primary: "~/.custom/bin",
		},
		Windows: ManifestInstallRootPaths{
			Primary: "%USERPROFILE%/custom/bin",
		},
	}

	if runtime.GOOS == "windows" {
		dir := resolveInstallDir(roots, func(k string) string {
			if k == "USERPROFILE" {
				return `C:\Users\test`
			}
			return ""
		})
		if !strings.Contains(dir, `custom\bin`) {
			t.Fatalf("unexpected windows install dir: %s", dir)
		}
	} else {
		dir := resolveInstallDir(roots, func(k string) string {
			if k == "HOME" {
				return "/home/test"
			}
			return ""
		})
		if dir != "/home/test/.custom/bin" {
			t.Fatalf("unexpected unix install dir: %s", dir)
		}

		// Canonical default when empty primary
		rootsEmpty := ManifestInstallRoots{
			Unix: ManifestInstallRootPaths{Primary: ""},
		}
		dirDefault := resolveInstallDir(rootsEmpty, func(k string) string {
			if k == "HOME" {
				return "/home/test"
			}
			return ""
		})
		if dirDefault != "/home/test/.local/bin" {
			t.Fatalf("expected default /home/test/.local/bin, got %s", dirDefault)
		}
	}
}

func TestResolveGitHubToken(t *testing.T) {
	tok := resolveGitHubToken(func(k string) string {
		if k == "GITHUB_TOKEN" {
			return "tok-gh"
		}
		return ""
	})
	if tok != "tok-gh" {
		t.Fatalf("expected tok-gh, got %q", tok)
	}

	tok2 := resolveGitHubToken(func(k string) string {
		if k == "GH_TOKEN" {
			return "tok-cli"
		}
		return ""
	})
	if tok2 != "tok-cli" {
		t.Fatalf("expected tok-cli, got %q", tok2)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFetchRelease(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			path := r.URL.Path
			var body []byte
			status := http.StatusOK
			switch {
			case strings.HasSuffix(path, "/releases/latest"):
				body, _ = json.Marshal(gitHubRelease{
					TagName: "v2.0.0",
					Assets:  []gitHubAsset{{Name: "test-tool.tar.gz"}},
				})
			case strings.HasSuffix(path, "/releases/tags/1.0.0"):
				body, _ = json.Marshal(gitHubRelease{
					TagName: "1.0.0",
				})
			case strings.HasSuffix(path, "/releases/tags/v3.0.0"):
				body, _ = json.Marshal(gitHubRelease{
					TagName: "v3.0.0",
				})
			default:
				status = http.StatusNotFound
			}
			return &http.Response{
				StatusCode: status,
				Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	repo := "viovy/test-tool"
	ctx := context.Background()
	// 1. Fetch latest
	rel, err := fetchRelease(ctx, client, repo, "latest", "dummy-tok")
	if err != nil || rel.TagName != "v2.0.0" {
		t.Fatalf("fetch latest failed: %v, rel: %+v", err, rel)
	}

	// 2. Fetch specific tag
	rel, err = fetchRelease(ctx, client, repo, "1.0.0", "")
	if err != nil || rel.TagName != "1.0.0" {
		t.Fatalf("fetch specific tag failed: %v, rel: %+v", err, rel)
	}

	// 3. Fetch with v-prefix fallback
	rel, err = fetchRelease(ctx, client, repo, "3.0.0", "")
	if err != nil || rel.TagName != "v3.0.0" {
		t.Fatalf("fetch with v-prefix fallback failed: %v, rel: %+v", err, rel)
	}
}

func TestDownloadAsset(t *testing.T) {
	content := []byte("downloaded-file-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	ctx := context.Background()
	client := srv.Client()

	// Download by BrowserDownloadURL
	asset := &gitHubAsset{
		Name:               "tool.tar.gz",
		BrowserDownloadURL: srv.URL + "/download",
	}
	data, err := downloadAsset(ctx, client, asset, "")
	if err != nil || string(data) != string(content) {
		t.Fatalf("download failed: %v, got %q", err, string(data))
	}

	// Download with token using URL
	assetWithURL := &gitHubAsset{
		Name: "tool.tar.gz",
		URL:  srv.URL + "/api-download",
	}
	data, err = downloadAsset(ctx, client, assetWithURL, "token-123")
	if err != nil || string(data) != string(content) {
		t.Fatalf("download with token failed: %v, got %q", err, string(data))
	}
}

func TestCleanStaleBackups(t *testing.T) {
	tmpDir := t.TempDir()

	oldFile := filepath.Join(tmpDir, "iazio-agent.old.100.exe")
	if err := os.WriteFile(oldFile, []byte("old"), 0755); err != nil {
		t.Fatal(err)
	}
	// Change mtime to 10 minutes ago
	oldTime := time.Now().Add(-10 * time.Minute)
	_ = os.Chtimes(oldFile, oldTime, oldTime)

	freshFile := filepath.Join(tmpDir, "iazio-agent.old.200.exe")
	if err := os.WriteFile(freshFile, []byte("fresh"), 0755); err != nil {
		t.Fatal(err)
	}

	keepFile := filepath.Join(tmpDir, "keep.txt")
	_ = os.WriteFile(keepFile, []byte("keep"), 0644)

	cleanStaleBackups(tmpDir)

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatal("expected old backup to be removed")
	}
	if _, err := os.Stat(freshFile); err != nil {
		t.Fatal("expected fresh backup to remain")
	}
	if _, err := os.Stat(keepFile); err != nil {
		t.Fatal("expected keepFile to remain")
	}
}

func TestRunUpdateFlags(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	// Flag parsing errors
	err := runUpdate(ctx, []string{"--invalid-flag"}, &stdout, &stderr, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "unknown update flag") {
		t.Fatalf("expected unknown flag error, got %v", err)
	}

	// Parse valid flags in dry-run mode
	stdout.Reset()
	stderr.Reset()
	err = runUpdate(ctx, []string{
		"--dry-run",
		"--force",
		"--tool", "iazio-agent",
		"--tool=autopilot",
		"--profile", "developer",
		"--profile=developer",
		"--api", "http://localhost:8080",
		"--api=http://localhost:8080",
		"--bin-dir", "/tmp/bin",
		"--bin-dir=/tmp/bin",
		"--on-timeout", "reject",
		"--on-timeout=reject",
		"--drain-expired",
	}, &stdout, &stderr, func(string) string { return "" })
	if err != nil {
		t.Fatalf("unexpected error in dry-run update: %v", err)
	}
	if !strings.Contains(stdout.String(), "update") {
		t.Fatalf("expected stdout to contain 'update', got %q", stdout.String())
	}
}

func TestInspectInstalledVersion(t *testing.T) {
	tmpDir := t.TempDir()
	bin := filepath.Join(tmpDir, "dummy-bin")
	_ = os.WriteFile(bin, []byte("bin"), 0755)
	_ = os.WriteFile(bin+".version", []byte("1.2.3\n"), 0644)

	ver := inspectInstalledVersion(bin)
	if ver != "1.2.3" {
		t.Fatalf("expected 1.2.3, got %q", ver)
	}
}

func TestUpdateSingleTool_Lifecycle(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "iazio-dummy")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	// 1. Tool already up to date with force=false
	_ = os.WriteFile(binPath, []byte("echo binary"), 0755)
	_ = os.WriteFile(binPath+".version", []byte("1.0.0"), 0644)

	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			rel := gitHubRelease{
				TagName: "v1.0.0",
				Assets:  []gitHubAsset{{Name: "iazio-dummy_1.0.0_darwin_arm64.tar.gz"}},
			}
			b, _ := json.Marshal(rel)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(b)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	var stdout, stderr bytes.Buffer
	tool := ManifestToolItem{
		Name:       "iazio-dummy",
		Repository: "viovy/iazio-dummy",
		BinaryName: "iazio-dummy",
	}

	updated, err := updateSingleTool(ctx, client, tool, binDir, "", false, &stdout, &stderr)
	if err != nil || updated {
		t.Fatalf("expected not updated, got updated=%v err=%v", updated, err)
	}
	if !strings.Contains(stdout.String(), "up-to-date") {
		t.Fatalf("expected stdout to mention up-to-date, got %s", stdout.String())
	}

	// 2. Tool out of date -> installs new version
	archiveBinName := "iazio-dummy"
	if runtime.GOOS == "windows" {
		archiveBinName = "iazio-dummy.exe"
	}
	tarContent := createTarGz(t, archiveBinName, []byte("#!/bin/sh\necho updated\n"))
	sum := sha256.Sum256(tarContent)
	sumText := fmt.Sprintf("%s  iazio-dummy_2.0.0_%s_%s.tar.gz\n", hex.EncodeToString(sum[:]), runtime.GOOS, runtime.GOARCH)

	client2 := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			path := r.URL.Path
			switch {
			case strings.HasSuffix(path, "/releases/latest"):
				rel := gitHubRelease{
					TagName: "v2.0.0",
					Assets: []gitHubAsset{
						{
							Name:               fmt.Sprintf("iazio-dummy_2.0.0_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH),
							BrowserDownloadURL: "https://example.com/asset.tar.gz",
						},
						{
							Name:               "checksums.txt",
							BrowserDownloadURL: "https://example.com/checksums.txt",
						},
					},
				}
				b, _ := json.Marshal(rel)
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(b))}, nil
			case strings.HasSuffix(path, "checksums.txt"):
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte(sumText)))}, nil
			default:
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(tarContent))}, nil
			}
		}),
	}

	stdout.Reset()
	stderr.Reset()
	updated, err = updateSingleTool(ctx, client2, tool, binDir, "", true, &stdout, &stderr)
	if err != nil || !updated {
		t.Fatalf("expected updated=true, got updated=%v err=%v stderr=%s", updated, err, stderr.String())
	}
	if v, _ := os.ReadFile(binPath + ".version"); string(v) != "2.0.0" {
		t.Fatalf("expected version 2.0.0, got %q", string(v))
	}
}

func TestPerformUpdate_ManifestBranches(t *testing.T) {
	ctx := context.Background()
	var stdout, stderr bytes.Buffer

	// 1. Dry run returns false immediately
	opts := UpdateOptions{DryRun: true}
	updated, err := performUpdate(ctx, opts, &stdout, &stderr, nil)
	if err != nil || updated {
		t.Fatalf("expected false nil for dry-run, got %v %v", updated, err)
	}

	// 2. HTTP 500 error from manifest
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srvErr.Close()

	opts500 := UpdateOptions{
		API:     srvErr.URL,
		Profile: "default",
	}
	_, err = performUpdate(ctx, opts500, &stdout, &stderr, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("expected HTTP 500 error, got: %v", err)
	}

	// 3. HTTP 404 from manifest falls back to default baseline manifest
	srv404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv404.Close()

	opts404 := UpdateOptions{
		API:         srv404.URL,
		Profile:     "baseline",
		TargetTools: []string{"nonexistent-tool"},
	}
	_, err = performUpdate(ctx, opts404, &stdout, &stderr, nil)
	if err != nil {
		t.Fatalf("unexpected error on 404 fallback: %v", err)
	}
}

func TestInstallExecutableSafe_ReverseSync(t *testing.T) {
	tmpDir := t.TempDir()
	fakeHome := filepath.Join(tmpDir, "home")
	binDir1 := filepath.Join(fakeHome, ".local", "bin")
	binDir2 := filepath.Join(fakeHome, ".iazio", "bin")
	_ = os.MkdirAll(binDir1, 0755)
	_ = os.MkdirAll(binDir2, 0755)

	t.Setenv("HOME", fakeHome)

	target1 := filepath.Join(binDir1, "iazio-agent")
	target2 := filepath.Join(binDir2, "iazio-agent")

	data := []byte("reverse-sync-bytes")
	// Install to binDir2; target2 is installed exclusively per AC4/H2
	if err := installExecutableSafe(target2, data); err != nil {
		t.Fatalf("installExecutableSafe reverse failed: %v", err)
	}

	if c, _ := os.ReadFile(target2); string(c) != string(data) {
		t.Fatalf("target2 mismatch: %q", string(c))
	}
	if _, err := os.Stat(target1); !os.IsNotExist(err) {
		t.Fatalf("target1 should not be initialized per AC4/H2")
	}
}

func TestInstallExecutableSafe_ExistingAltPath(t *testing.T) {
	tmpDir := t.TempDir()
	fakeHome := filepath.Join(tmpDir, "home")
	binDir1 := filepath.Join(fakeHome, ".local", "bin")
	binDir2 := filepath.Join(fakeHome, ".iazio", "bin")
	_ = os.MkdirAll(binDir1, 0755)
	_ = os.MkdirAll(binDir2, 0755)

	t.Setenv("HOME", fakeHome)

	target1 := filepath.Join(binDir1, "iazio-agent")
	target2 := filepath.Join(binDir2, "iazio-agent")

	// Pre-create target2 as an existing file
	_ = os.WriteFile(target2, []byte("old-bytes"), 0755)

	data := []byte("new-sync-bytes")
	// Install to binDir1; target2 must not be modified per AC4/H2
	if err := installExecutableSafe(target1, data); err != nil {
		t.Fatalf("installExecutableSafe failed: %v", err)
	}

	if c, _ := os.ReadFile(target1); string(c) != string(data) {
		t.Fatalf("target1 mismatch: %q", string(c))
	}
	if c, _ := os.ReadFile(target2); string(c) != "old-bytes" {
		t.Fatalf("target2 should remain untouched per AC4/H2: %q", string(c))
	}
}

func TestCleanStaleBackups_StaleFileRemoved(t *testing.T) {
	dir := t.TempDir()

	// Non-existent directory returns early
	cleanStaleBackups(filepath.Join(dir, "nonexistent"))

	// Create fresh and stale backup files
	fresh := filepath.Join(dir, "agent.old.fresh.exe")
	stale := filepath.Join(dir, "agent.old.stale.exe")
	_ = os.WriteFile(fresh, []byte("fresh"), 0644)
	_ = os.WriteFile(stale, []byte("stale"), 0644)

	past := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(stale, past, past)

	cleanStaleBackups(dir)

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("expected stale file to be deleted")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("expected fresh file to remain")
	}
}

func TestUpdateSingleTool_ErrorsAndRepoPrefix(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()
	binDir := t.TempDir()
	var stdout, stderr bytes.Buffer

	// 1. Tool repo without slash prepends viovy/ and fails release fetch gracefully
	client404 := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("not found")),
				Header:     make(http.Header),
			}, nil
		}),
	}
	toolNoSlash := ManifestToolItem{
		Name:       "custom-tool",
		Repository: "custom-tool-repo", // no slash
		BinaryName: "",                 // empty binary name falls back to tool.Name
	}
	updated, err := updateSingleTool(ctx, client404, toolNoSlash, binDir, "", false, &stdout, &stderr)
	if err == nil || updated {
		t.Fatalf("expected error on 404 fetch release, got err=%v updated=%v", err, updated)
	}

	// 2. Matching asset error (no asset matching OS/arch)
	clientNoAsset := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			rel := gitHubRelease{
				TagName: "v1.2.0",
				Assets: []gitHubAsset{
					{Name: "unrelated-file.txt"},
				},
			}
			b, _ := json.Marshal(rel)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(b)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	toolMatchErr := ManifestToolItem{
		Name:       "iazio-custom-no-source",
		Repository: "viovy/iazio-mcp",
		BinaryName: "iazio-custom-no-source",
	}
	updated, err = updateSingleTool(ctx, clientNoAsset, toolMatchErr, binDir, "", true, &stdout, &stderr)
	if err == nil || updated {
		t.Fatalf("expected error when no asset matches, got err=%v updated=%v", err, updated)
	}
}

func TestResolveLocalAPI_Extended(t *testing.T) {
	// 1. Env override
	got := resolveLocalAPI(func(k string) string {
		if k == "IAZIO_HARNESS_API_URL" {
			return "http://env-api.local"
		}
		return ""
	})
	if got != "http://env-api.local" {
		t.Fatalf("expected env-api.local, got %s", got)
	}

	// 2. config.json harness_api
	tmpHome := t.TempDir()
	iazioDir := filepath.Join(tmpHome, ".iazio")
	_ = os.MkdirAll(iazioDir, 0755)
	cfgFile := filepath.Join(iazioDir, "config.json")
	_ = os.WriteFile(cfgFile, []byte(`{"endpoints":{"harness_api":"http://cfg-harness.local"}}`), 0600)

	t.Setenv("HOME", tmpHome)
	got = resolveLocalAPI(func(string) string { return "" })
	if got != "http://cfg-harness.local" {
		t.Fatalf("expected cfg-harness.local, got %s", got)
	}

	// 3. config.json harness-api (hyphenated)
	_ = os.WriteFile(cfgFile, []byte(`{"endpoints":{"harness-api":"http://hyphen-harness.local"}}`), 0600)
	got = resolveLocalAPI(func(string) string { return "" })
	if got != "http://hyphen-harness.local" {
		t.Fatalf("expected hyphen-harness.local, got %s", got)
	}

	// 4. api_url file
	_ = os.Remove(cfgFile)
	_ = os.WriteFile(filepath.Join(iazioDir, "api_url"), []byte("http://file-api.local\n"), 0600)
	got = resolveLocalAPI(func(string) string { return "" })
	if got != "http://file-api.local" {
		t.Fatalf("expected file-api.local, got %s", got)
	}

	// 5. default fallback
	_ = os.Remove(filepath.Join(iazioDir, "api_url"))
	got = resolveLocalAPI(func(string) string { return "" })
	if got != service.DefaultFleetAPI {
		t.Fatalf("expected DefaultFleetAPI, got %q", got)
	}
}

func TestResolveInstallDir_Extended(t *testing.T) {
	home := t.TempDir()
	getenv := func(k string) string {
		if k == "HOME" {
			return home
		}
		return ""
	}

	// 1. primary starting with ~/
	roots := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Primary: "~/mytools/bin",
		},
	}
	got := resolveInstallDir(roots, getenv)
	want := filepath.Join(home, "mytools/bin")
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}

	// 2. absolute primary
	roots = ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Primary: "/usr/local/custom/bin",
		},
	}
	got = resolveInstallDir(roots, getenv)
	if got != "/usr/local/custom/bin" {
		t.Fatalf("expected /usr/local/custom/bin, got %s", got)
	}

	// 3. empty primary, uses default ~/.local/bin
	roots = ManifestInstallRoots{}
	got = resolveInstallDir(roots, getenv)
	want = filepath.Join(home, ".local", "bin")
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestPerformUpdateWithResult_FallbackToSingleAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/fleet/manifest") {
			manifest := ManifestResponse{
				Profile: "generic",
				Tools: []ManifestToolItem{
					{Name: "other-tool", Repository: "viovy/other-tool", BinaryName: "other-tool"},
				},
			}
			_ = json.NewEncoder(w).Encode(manifest)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	tmpHome := t.TempDir()
	opts := UpdateOptions{
		Suite:   false,
		API:     srv.URL,
		Profile: "generic",
		BinDir:  t.TempDir(),
	}
	var stdout, stderr bytes.Buffer
	_, _ = performUpdateWithResult(ctx, opts, &stdout, &stderr, func(k string) string {
		if k == "HOME" {
			return tmpHome
		}
		return ""
	})
}

func TestResolveInstallDir_Variants(t *testing.T) {
	fakeHome := "/home/testuser"
	getenv := func(k string) string {
		switch k {
		case "HOME":
			return fakeHome
		case "USERPROFILE":
			return "C:\\Users\\testuser"
		default:
			return ""
		}
	}

	// 1. Tilde expansion
	rootsTilde := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Primary: "~/.local/bin",
		},
	}
	dir := resolveInstallDir(rootsTilde, getenv)
	if runtime.GOOS != "windows" {
		expected := filepath.Join(fakeHome, ".local", "bin")
		if dir != expected {
			t.Fatalf("expected %s, got %s", expected, dir)
		}
	}

	// 2. Absolute path primary
	rootsAbs := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Primary: "/opt/bin",
		},
	}
	dirAbs := resolveInstallDir(rootsAbs, getenv)
	if runtime.GOOS != "windows" && dirAbs != "/opt/bin" {
		t.Fatalf("expected /opt/bin, got %s", dirAbs)
	}

	// 3. Default empty
	rootsEmpty := ManifestInstallRoots{}
	dirEmpty := resolveInstallDir(rootsEmpty, getenv)
	if runtime.GOOS != "windows" {
		expected := filepath.Join(fakeHome, ".local", "bin")
		if dirEmpty != expected {
			t.Fatalf("expected %s, got %s", expected, dirEmpty)
		}
	}
}

func TestCleanStaleBackups_ExpiredFiles(t *testing.T) {
	dir := t.TempDir()
	staleFile := filepath.Join(dir, "agent.old.12345.exe")
	if err := os.WriteFile(staleFile, []byte("old binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-10 * time.Minute)
	_ = os.Chtimes(staleFile, past, past)

	freshFile := filepath.Join(dir, "agent.old.fresh.exe")
	if err := os.WriteFile(freshFile, []byte("fresh binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	cleanStaleBackups(dir)

	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Fatal("expected stale file to be deleted")
	}
	if _, err := os.Stat(freshFile); err != nil {
		t.Fatal("expected fresh file to remain")
	}
}

func TestUpdateTool_ChecksumMismatchRejection(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	_ = os.MkdirAll(binDir, 0o755)

	archiveData := createTarGz(t, "my-tool", []byte("#!/bin/sh\necho ok\n"))
	archiveName := fmt.Sprintf("my-tool-%s-%s.tar.gz", runtime.GOOS, runtime.GOARCH)

	// Checksums file with an intentionally wrong hash
	badChecksums := fmt.Sprintf("0000000000000000000000000000000000000000000000000000000000000000  %s\n", archiveName)

	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			path := r.URL.Path
			switch {
			case strings.HasSuffix(path, "/releases/latest"):
				rel := gitHubRelease{
					TagName: "1.0.0",
					Assets: []gitHubAsset{
						{Name: archiveName, BrowserDownloadURL: "https://example.com/download/" + archiveName},
						{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/download/checksums.txt"},
					},
				}
				b, _ := json.Marshal(rel)
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(b)),
					Header:     make(http.Header),
				}, nil
			case strings.HasSuffix(path, archiveName):
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(archiveData)),
					Header:     make(http.Header),
				}, nil
			case strings.HasSuffix(path, "checksums.txt"):
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(badChecksums)),
					Header:     make(http.Header),
				}, nil
			default:
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Status:     "404 Not Found",
					Body:       io.NopCloser(strings.NewReader("not found")),
					Header:     make(http.Header),
				}, nil
			}
		}),
	}

	tool := ManifestToolItem{
		Name:          "my-tool",
		Repository:    "viovy/my-tool",
		BinaryName:    "my-tool",
		Tier:          "baseline",
		TargetVersion: "latest",
	}

	var stdout, stderr bytes.Buffer
	updated, err := updateSingleTool(context.Background(), client, tool, binDir, "", true, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch, got updated=%v", updated)
	}
	if !strings.Contains(err.Error(), "integrity verification failed") {
		t.Fatalf("expected integrity verification error, got: %v", err)
	}

	// Verify the binary was NOT installed
	binPath := filepath.Join(binDir, "my-tool")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	if _, statErr := os.Stat(binPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected binary not to be installed after checksum failure, but found: %s", binPath)
	}
}

func TestUpdateSingleTool_MissingChecksumsAssetFailsClosed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	_ = os.MkdirAll(binDir, 0755)

	tarData := createTarGz(t, "my-tool", []byte("#!/bin/sh\necho hello\n"))
	assetName := fmt.Sprintf("my-tool-%s-%s.tar.gz", runtime.GOOS, runtime.GOARCH)

	// Mock release has only the binary tarball, NO checksums asset
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/releases/latest") {
				rel := gitHubRelease{
					TagName: "0.2.0",
					Assets: []gitHubAsset{
						{
							Name:               assetName,
							BrowserDownloadURL: "https://github.com/viovy/my-tool/releases/download/0.2.0/" + assetName,
						},
					},
				}
				data, _ := json.Marshal(rel)
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(data)),
					Header:     make(http.Header),
				}, nil
			}
			if strings.HasSuffix(req.URL.Path, ".tar.gz") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(tarData)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("404")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	tool := ManifestToolItem{
		Name:          "my-tool",
		Repository:    "viovy/my-tool",
		BinaryName:    "my-tool",
		Tier:          "baseline",
		TargetVersion: "latest",
	}

	// 1. Unattended mode: fails closed when checksums asset missing (G2)
	t.Setenv("IAZIO_ALLOW_UNVERIFIED_UPDATES", "")
	var stdout, stderr bytes.Buffer
	updated, err := updateSingleTool(context.Background(), client, tool, binDir, "", true, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error when checksums asset is missing in unattended mode, got updated=%v", updated)
	}
	if !strings.Contains(err.Error(), "no checksums asset found") {
		t.Fatalf("expected error to mention 'no checksums asset found', got: %v", err)
	}

	// 2. Explicit operator override allows installation (G2)
	t.Setenv("IAZIO_ALLOW_UNVERIFIED_UPDATES", "1")
	stdout.Reset()
	stderr.Reset()
	updated, err = updateSingleTool(context.Background(), client, tool, binDir, "", true, &stdout, &stderr)
	if err != nil {
		t.Fatalf("expected update to succeed with IAZIO_ALLOW_UNVERIFIED_UPDATES=1 override, got: %v", err)
	}
	if !updated {
		t.Fatalf("expected tool to be updated with override")
	}
}

func TestUpdate_SingleToolGuard(t *testing.T) {
	// F2, F7: TargetVersion can only be applied when exactly one tool is targeted
	optsSuite := UpdateOptions{
		Suite:         true,
		TargetVersion: "0.0.492",
	}
	res, err := performUpdateWithResult(context.Background(), optsSuite, nil, nil, nil)
	if err == nil {
		t.Fatalf("expected error when TargetVersion is used with Suite=true (multiple targets), got res=%+v", res)
	}
	if !strings.Contains(err.Error(), "can only be specified when exactly one tool is targeted") {
		t.Fatalf("expected single-tool guard error message, got %v", err)
	}

	optsMulti := UpdateOptions{
		TargetTools:   []string{"autopilot", "iazio-agent"},
		TargetVersion: "0.0.492",
	}
	res2, err2 := performUpdateWithResult(context.Background(), optsMulti, nil, nil, nil)
	if err2 == nil {
		t.Fatalf("expected error when TargetVersion is used with multiple TargetTools, got res=%+v", res2)
	}
	if !strings.Contains(err2.Error(), "can only be specified when exactly one tool is targeted") {
		t.Fatalf("expected single-tool guard error message, got %v", err2)
	}
}

func TestUpdate_CLIFlagParsing_Version(t *testing.T) {
	// R6: Test real CLI flag parsing for --version and --version= using parseUpdateFlags
	opts1, err1 := parseUpdateFlags([]string{"--version", "0.0.492", "--tool", "autopilot"})
	if err1 != nil {
		t.Fatalf("unexpected parse error: %v", err1)
	}
	if opts1.TargetVersion != "0.0.492" || len(opts1.TargetTools) != 1 || opts1.TargetTools[0] != "autopilot" {
		t.Fatalf("unexpected parsed opts: %+v", opts1)
	}

	opts2, err2 := parseUpdateFlags([]string{"--version=0.0.493"})
	if err2 != nil {
		t.Fatalf("unexpected parse error: %v", err2)
	}
	if opts2.TargetVersion != "0.0.493" {
		t.Fatalf("expected TargetVersion 0.0.493, got %s", opts2.TargetVersion)
	}
}

func TestCleanTargetVersion(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"autopilot-fleet version dev", ""},
		{"autopilot-fleet 0.0.492", "0.0.492"},
		{"v0.0.492", "0.0.492"},
		{"0.0.492", "0.0.492"},
		{"latest", ""},
		{"dev", ""},
		{"", ""},
		{"autopilot-fleet version 1.2.3-beta.1", "1.2.3-beta.1"},
	}

	for _, c := range cases {
		got := cleanTargetVersion(c.input)
		if got != c.expected {
			t.Errorf("cleanTargetVersion(%q) = %q; want %q", c.input, got, c.expected)
		}
	}
}

func TestFindMatchingAsset_MacOS_ArchitectureAliases(t *testing.T) {
	rel := &gitHubRelease{
		TagName: "v0.0.492",
		Assets: []gitHubAsset{
			{Name: "autopilot-fleet_0.0.492_macos_aarch64.tar.gz"},
			{Name: "autopilot-fleet_0.0.492_linux_x86_64.tar.gz"},
		},
	}

	// 1. macOS on arm64 matching macos + aarch64
	a1, err := findMatchingAsset(rel, "autopilot-fleet", "darwin", "arm64")
	if err != nil {
		t.Fatalf("expected to match darwin/arm64 to macos/aarch64 asset, got err: %v", err)
	}
	if a1.Name != "autopilot-fleet_0.0.492_macos_aarch64.tar.gz" {
		t.Errorf("matched wrong asset: %s", a1.Name)
	}

	// 2. Linux on amd64 matching linux + x86_64
	a2, err := findMatchingAsset(rel, "autopilot-fleet", "linux", "amd64")
	if err != nil {
		t.Fatalf("expected to match linux/amd64 to linux/x86_64 asset, got err: %v", err)
	}
	if a2.Name != "autopilot-fleet_0.0.492_linux_x86_64.tar.gz" {
		t.Errorf("matched wrong asset: %s", a2.Name)
	}
}

func TestUpdateSingleTool_ShadowingBinarySync(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempDir := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(tempDir); err == nil {
		t.Cleanup(func() { _ = os.Chdir(origWd) })
	}
	binDir := filepath.Join(tempDir, "bin")
	shadowDir := filepath.Join(tempDir, "shadow")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(shadowDir, 0755); err != nil {
		t.Fatal(err)
	}

	toolName := "autopilot-fleet"
	shadowBin := filepath.Join(shadowDir, toolName)
	if err := os.WriteFile(shadowBin, []byte("#!/bin/sh\necho old-dev\n"), 0755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(shadowBin+".version", []byte("autopilot-fleet version dev"), 0644)

	// Prepend shadowDir to PATH so LookPath finds it
	origPath := os.Getenv("PATH")
	t.Setenv("PATH", shadowDir+string(os.PathListSeparator)+origPath)

	payload := []byte("#!/bin/sh\necho v0.0.492\n")
	tarData := createTarGz(t, toolName, payload)
	sum := sha256.Sum256(tarData)
	hashStr := hex.EncodeToString(sum[:])
	assetName := fmt.Sprintf("%s_0.0.492_%s_%s.tar.gz", toolName, runtime.GOOS, runtime.GOARCH)
	checksumsData := fmt.Sprintf("%s  %s\n", hashStr, assetName)

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/releases/latest") || strings.HasSuffix(req.URL.Path, "/releases/tags/0.0.492") {
				rel := gitHubRelease{
					TagName: "0.0.492",
					Assets: []gitHubAsset{
						{
							Name:               assetName,
							BrowserDownloadURL: fmt.Sprintf("https://github.com/viovy/%s/releases/download/0.0.492/%s", toolName, assetName),
						},
						{
							Name:               "checksums.txt",
							BrowserDownloadURL: fmt.Sprintf("https://github.com/viovy/%s/releases/download/0.0.492/checksums.txt", toolName),
						},
					},
				}
				data, _ := json.Marshal(rel)
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(data)),
					Header:     make(http.Header),
				}, nil
			}
			if strings.HasSuffix(req.URL.Path, ".tar.gz") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(bytes.NewReader(tarData)),
					Header:     make(http.Header),
				}, nil
			}
			if strings.HasSuffix(req.URL.Path, "checksums.txt") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(checksumsData)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("404")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	tool := ManifestToolItem{
		Name:          toolName,
		Repository:    "viovy/" + toolName,
		BinaryName:    toolName,
		Tier:          "baseline",
		TargetVersion: "autopilot-fleet version dev",
	}

	var stdout, stderr bytes.Buffer
	updated, err := updateSingleTool(context.Background(), client, tool, binDir, "", true, &stdout, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !updated {
		t.Fatalf("expected tool to be updated")
	}

	// Verify target binDir was written
	destBin := filepath.Join(binDir, toolName)
	destData, err := os.ReadFile(destBin)
	if err != nil {
		t.Fatalf("failed to read dest binary: %v", err)
	}
	if string(destData) != string(payload) {
		t.Fatalf("dest binary content mismatch")
	}

	// Verify shadowing binary was not overwritten (prevent resurrecting shadow duplicates per AC4/H2)
	shadowData, err := os.ReadFile(shadowBin)
	if err != nil {
		t.Fatalf("failed to read shadow binary: %v", err)
	}
	if string(shadowData) == string(payload) {
		t.Fatalf("shadow binary should not be overwritten per AC4/H2")
	}
}

func TestCleanTargetVersion_Table(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"latest", ""},
		{"LATEST", ""},
		{"autopilot-fleet version dev", ""},
		{"v1.2.3", "1.2.3"},
		{"1.2.3", "1.2.3"},
		{"v2.0.0-beta.1", "2.0.0-beta.1"},
		{"tool-name-v3.4.5", "3.4.5"},
		{"custom_tag", "custom_tag"},
	}
	for _, tc := range cases {
		got := cleanTargetVersion(tc.input)
		if got != tc.want {
			t.Errorf("cleanTargetVersion(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestResolveInstallDir_Coverage(t *testing.T) {
	getenv := func(key string) string {
		switch key {
		case "HOME":
			return "/home/testuser"
		case "USERPROFILE":
			return "C:\\Users\\testuser"
		case "HOMEDRIVE":
			return "C:"
		case "HOMEPATH":
			return "\\Users\\testuser"
		default:
			return ""
		}
	}

	rootsUnixTilde := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{Primary: "~/.local/bin"},
	}
	if dir := resolveInstallDir(rootsUnixTilde, getenv); dir != "" {
		if runtime.GOOS != "windows" && dir != "/home/testuser/.local/bin" {
			t.Errorf("expected /home/testuser/.local/bin, got %s", dir)
		}
	}

	rootsUnixDirect := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{Primary: "/opt/custom/bin"},
	}
	if dir := resolveInstallDir(rootsUnixDirect, getenv); dir != "" {
		if runtime.GOOS != "windows" && dir != "/opt/custom/bin" {
			t.Errorf("expected /opt/custom/bin, got %s", dir)
		}
	}

	rootsUnixEmpty := ManifestInstallRoots{}
	if dir := resolveInstallDir(rootsUnixEmpty, getenv); dir != "" {
		if runtime.GOOS != "windows" && dir != "/home/testuser/.local/bin" {
			t.Errorf("expected /home/testuser/.local/bin, got %s", dir)
		}
	}
}

func TestInstallExecutableSafeDirect_Unit(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "testtool")
	data := []byte("#!/bin/sh\necho ok\n")

	// Direct install
	if err := installExecutableSafeDirect(binPath, data); err != nil {
		t.Fatalf("installExecutableSafeDirect failed: %v", err)
	}

	read, err := os.ReadFile(binPath)
	if err != nil || string(read) != string(data) {
		t.Fatalf("unexpected content after install: %v", err)
	}

	// Overwrite install
	newData := []byte("#!/bin/sh\necho updated\n")
	if err := installExecutableSafeDirect(binPath, newData); err != nil {
		t.Fatalf("installExecutableSafeDirect overwrite failed: %v", err)
	}

	read2, err := os.ReadFile(binPath)
	if err != nil || string(read2) != string(newData) {
		t.Fatalf("unexpected content after overwrite: %v", err)
	}
}

func TestInstallExecutableSafe_MultiDirSync(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	altDir1 := filepath.Join(tmpHome, ".local", "bin")
	altDir2 := filepath.Join(tmpHome, ".iazio", "bin")
	altDir3 := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "bin")

	if err := os.MkdirAll(altDir1, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(altDir2, 0755); err != nil {
		t.Fatal(err)
	}

	destPath := filepath.Join(altDir3, "my-tool")
	payload := []byte("binary-payload-multi-dir")

	if err := installExecutableSafe(destPath, payload); err != nil {
		t.Fatalf("installExecutableSafe failed: %v", err)
	}

	// Verify target destPath has the binary and alternate dirs do not per AC4/H2
	c, err := os.ReadFile(destPath)
	if err != nil || string(c) != string(payload) {
		t.Fatalf("expected binary at destPath %s", destPath)
	}
	for _, dir := range []string{altDir1, altDir2} {
		p := filepath.Join(dir, "my-tool")
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("alternate directory %s should not have duplicate binary", dir)
		}
	}
}

func TestFindToolSourceRepo(t *testing.T) {
	tmpDir := t.TempDir()
	envMap := map[string]string{
		"IAZIO_FLEET_ROOT": tmpDir,
	}
	getenv := func(k string) string { return envMap[k] }

	subrepo := filepath.Join(tmpDir, "custom-tool")
	if err := os.MkdirAll(subrepo, 0755); err != nil {
		t.Fatal(err)
	}
	// Without .git, shouldn't match
	if found := findToolSourceRepo("custom-tool", getenv); found != "" {
		t.Errorf("expected empty before .git, got %s", found)
	}

	if err := os.MkdirAll(filepath.Join(subrepo, ".git"), 0755); err != nil {
		t.Fatal(err)
	}

	found := findToolSourceRepo("custom-tool", getenv)
	if found != subrepo {
		t.Errorf("expected %s, got %s", subrepo, found)
	}
}

func TestRunCleanupBinaries_DryRun(t *testing.T) {
	tmpDir := t.TempDir()
	canonDir := filepath.Join(tmpDir, "canon")
	_ = os.MkdirAll(canonDir, 0755)

	envMap := map[string]string{
		"PATH":              tmpDir,
		"HOME":              tmpDir,
		"CANONICAL_BIN_DIR": canonDir,
	}
	getenv := func(k string) string { return envMap[k] }

	// Create dev binary
	devBin := filepath.Join(tmpDir, "iazio-agent-dev")
	if err := os.WriteFile(devBin, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	// Run cleanup with dry-run
	err := runCleanupBinaries(context.Background(), []string{"--dry-run"}, &stdout, &stderr, getenv)
	if err != nil {
		t.Fatalf("runCleanupBinaries dry run failed: %v", err)
	}

	// File should still exist
	if _, err := os.Stat(devBin); err != nil {
		t.Errorf("expected dev bin to still exist after dry run: %v", err)
	}

	// Run cleanup without dry-run
	err = runCleanupBinaries(context.Background(), []string{}, &stdout, &stderr, getenv)
	if err != nil {
		t.Fatalf("runCleanupBinaries live failed: %v", err)
	}

	// File should now be gone
	if _, err := os.Stat(devBin); !os.IsNotExist(err) {
		t.Errorf("expected dev bin to be deleted: %v", err)
	}
}

func TestBuildToolFromSourceRepo_Lifecycle(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not installed")
	}

	tmpDir := t.TempDir()
	srcRepo := filepath.Join(tmpDir, "repo")
	if err := os.MkdirAll(srcRepo, 0755); err != nil {
		t.Fatal(err)
	}

	// Init git repo
	runGit := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", srcRepo}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.local",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.local",
			"GIT_TERMINAL_PROMPT=0",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v: %s", args, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init")
	runGit("config", "user.name", "Test")
	runGit("config", "user.email", "test@test.local")

	goModContent := "module customtesttool\n\ngo 1.22\n"
	if err := os.WriteFile(filepath.Join(srcRepo, "go.mod"), []byte(goModContent), 0644); err != nil {
		t.Fatal(err)
	}
	mainGoContent := "package main\nimport \"fmt\"\nfunc main() { fmt.Println(\"v1\") }\n"
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte(mainGoContent), 0644); err != nil {
		t.Fatal(err)
	}

	runGit("add", ".")
	runGit("commit", "-m", "initial commit")
	headSHA := runGit("rev-parse", "HEAD")

	destBinDir := filepath.Join(tmpDir, "bin")
	_ = os.MkdirAll(destBinDir, 0755)
	destPath := filepath.Join(destBinDir, "customtesttool")

	tool := ManifestToolItem{
		Name:       "customtesttool",
		BinaryName: "customtesttool",
	}

	ctx := context.Background()

	// Case A: Clean repo, installed matches HEAD SHA => should skip update
	_ = os.WriteFile(destPath, []byte("dummy-bin"), 0755)
	_ = os.WriteFile(destPath+".version", []byte(headSHA), 0644)

	var outA bytes.Buffer
	updatedA, err := buildToolFromSourceRepo(ctx, tool, srcRepo, destPath, false, &outA, &outA)
	if err != nil {
		t.Fatalf("Case A failed: %v", err)
	}
	if updatedA {
		t.Errorf("expected Case A not to update, got true")
	}
	if !strings.Contains(outA.String(), "up-to-date") {
		t.Errorf("expected output to mention up-to-date, got: %s", outA.String())
	}

	// Case B: Repo has uncommitted tracked modification => eager update should skip
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte("package main\nfunc main() {}\n// dirty edit\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// change destination version so SHA mismatch would otherwise trigger update
	_ = os.WriteFile(destPath+".version", []byte("oldsha"), 0644)

	var outB bytes.Buffer
	updatedB, err := buildToolFromSourceRepo(ctx, tool, srcRepo, destPath, false, &outB, &outB)
	if err != nil {
		t.Fatalf("Case B failed: %v", err)
	}
	if updatedB {
		t.Errorf("expected Case B to skip update due to dirty repo, got updated=true")
	}
	if !strings.Contains(outB.String(), "uncommitted tracked changes") {
		t.Errorf("expected output to mention uncommitted tracked changes, got: %s", outB.String())
	}

	// Case C: Force update => should bypass dirty repo and install fresh binary
	var outC bytes.Buffer
	updatedC, err := buildToolFromSourceRepo(ctx, tool, srcRepo, destPath, true, &outC, &outC)
	if err != nil {
		t.Fatalf("Case C force update failed: %v: %s", err, outC.String())
	}
	if !updatedC {
		t.Errorf("expected Case C to update successfully with force=true")
	}

	// Verify installed binary and version sidecar (7-char short SHA)
	verSidecar, err := os.ReadFile(destPath + ".version")
	if err != nil {
		t.Fatalf("missing .version sidecar: %v", err)
	}
	wantShort := headSHA
	if len(wantShort) > 7 {
		wantShort = wantShort[:7]
	}
	if string(verSidecar) != wantShort {
		t.Errorf("expected .version to contain %s, got %s", wantShort, string(verSidecar))
	}
}

func TestFindToolSourceRepo_AutopilotPluginsAndReverseMetaRepo(t *testing.T) {
	home := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(home); err == nil {
		t.Cleanup(func() { _ = os.Chdir(origWd) })
	}
	mr03 := filepath.Join(home, "work", "meta-repo-03", "autopilot-plugins-internal")
	mr04 := filepath.Join(home, "work", "meta-repo-04", "autopilot-plugins-internal")
	if err := os.MkdirAll(filepath.Join(mr03, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mr04, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}

	getenv := func(k string) string {
		if k == "HOME" {
			return home
		}
		return ""
	}

	for _, tool := range []string{"autopilot-fleet", "autopilot-story", "autopilot-backlog"} {
		got := findToolSourceRepo(tool, getenv)
		if got != mr04 {
			t.Fatalf("expected %s to resolve to reverse-sorted %s, got %s", tool, mr04, got)
		}
	}
}

func TestPerformUpdateWithResult_ForcePartialFailureVsAllFailed(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	origWd, _ := os.Getwd()
	if err := os.Chdir(home); err == nil {
		t.Cleanup(func() { _ = os.Chdir(origWd) })
	}
	binDir := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	tarContent := createTarGz(t, "primary-core-tool", []byte("#!/bin/sh\necho primary-core-tool\n"))
	sum := sha256.Sum256(tarContent)
	assetName := fmt.Sprintf("primary-core-tool_1.2.3_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	sumText := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/fleet/manifest") {
			manifest := ManifestResponse{
				Profile: "workstation",
				Tools: []ManifestToolItem{
					{Name: "primary-core-tool", Repository: "viovy/primary-core-tool", BinaryName: "primary-core-tool"},
					{Name: "secondary-broken-tool", Repository: "viovy/secondary-broken-tool", BinaryName: "secondary-broken-tool"},
				},
			}
			_ = json.NewEncoder(w).Encode(manifest)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	origTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = origTransport }()
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasPrefix(r.URL.String(), srv.URL) {
			return origTransport.RoundTrip(r)
		}
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/repos/viovy/primary-core-tool/releases/latest"):
			rel := gitHubRelease{
				TagName: "v1.2.3",
				Assets: []gitHubAsset{
					{Name: assetName, BrowserDownloadURL: "https://example.com/" + assetName},
					{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
				},
			}
			b, _ := json.Marshal(rel)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(b)), Header: make(http.Header)}, nil
		case strings.HasSuffix(path, "checksums.txt"):
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte(sumText))), Header: make(http.Header)}, nil
		case strings.HasSuffix(path, assetName):
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(tarContent)), Header: make(http.Header)}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("not found")),
				Header:     make(http.Header),
			}, nil
		}
	})

	// 1. Suite forced update where primary-core-tool updates and secondary-broken-tool fails -> returns nil error with FailedTools populated
	var stdout, stderr bytes.Buffer
	res, err := performUpdateWithResult(context.Background(), UpdateOptions{
		Suite:   true,
		Force:   true,
		API:     srv.URL,
		Profile: "workstation",
		BinDir:  binDir,
	}, &stdout, &stderr, func(k string) string {
		if k == "HOME" {
			return home
		}
		return ""
	})
	if err != nil {
		t.Fatalf("expected nil error on partial forced suite update, got: %v", err)
	}
	if len(res.UpdatedTools) != 1 || res.UpdatedTools[0] != "primary-core-tool" {
		t.Fatalf("expected primary-core-tool in UpdatedTools, got: %+v", res.UpdatedTools)
	}
	if len(res.FailedTools) != 1 || res.FailedTools[0] != "secondary-broken-tool" {
		t.Fatalf("expected secondary-broken-tool in FailedTools, got: %+v", res.FailedTools)
	}

	// 2. Single-tool forced update that fails -> returns non-nil error
	_, errSingle := performUpdateWithResult(context.Background(), UpdateOptions{
		Force:       true,
		API:         srv.URL,
		Profile:     "workstation",
		BinDir:      binDir,
		TargetTools: []string{"secondary-broken-tool"},
	}, &stdout, &stderr, func(k string) string {
		if k == "HOME" {
			return home
		}
		return ""
	})
	if errSingle == nil {
		t.Fatal("expected non-nil error when single targeted forced tool update fails")
	}
}

func TestFindMatchingAsset_PrefixDisambiguationAndBundle(t *testing.T) {
	rel := &gitHubRelease{
		TagName: "v0.0.105",
		Assets: []gitHubAsset{
			{Name: "autopilot-fleet-exec_0.0.105_linux_amd64.tar.gz"},
			{Name: "autopilot-fleet-health_0.0.105_linux_amd64.tar.gz"},
			{Name: "autopilot-fleet_0.0.105_linux_amd64.tar.gz"},
			{Name: "autopilot-plugins-internal_0.0.105_linux_amd64.tar.gz"},
			{Name: "checksums.txt"},
		},
	}

	asset, err := findMatchingAsset(rel, "autopilot-fleet", "linux", "amd64")
	if err != nil {
		t.Fatalf("expected asset for autopilot-fleet, got err: %v", err)
	}
	if asset.Name != "autopilot-fleet_0.0.105_linux_amd64.tar.gz" {
		t.Fatalf("expected autopilot-fleet_0.0.105_linux_amd64.tar.gz, got %s", asset.Name)
	}

	bundleAsset, err := findMatchingAsset(rel, "autopilot-backlog", "linux", "amd64")
	if err != nil {
		t.Fatalf("expected bundle fallback for autopilot-backlog, got err: %v", err)
	}
	if bundleAsset.Name != "autopilot-plugins-internal_0.0.105_linux_amd64.tar.gz" {
		t.Fatalf("expected bundle autopilot-plugins-internal_0.0.105_linux_amd64.tar.gz, got %s", bundleAsset.Name)
	}
}

func TestFindToolSourceRepo_PluginMapping(t *testing.T) {
	tmpRoot := t.TempDir()
	pluginsRepo := filepath.Join(tmpRoot, "autopilot-plugins-internal")
	if err := os.MkdirAll(filepath.Join(pluginsRepo, ".git"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(pluginsRepo, "cmd", "autopilot-fleet"), 0755); err != nil {
		t.Fatal(err)
	}

	getenv := func(k string) string {
		if k == "META_REPO_ROOT" {
			return tmpRoot
		}
		return ""
	}

	got := findToolSourceRepo("autopilot-fleet", getenv, "viovy/autopilot-plugins-internal")
	if got != pluginsRepo {
		t.Fatalf("expected %s, got %s", pluginsRepo, got)
	}
}

func TestBuildToolFromSourceRepo_PrunesStaleAndCancelledBuildWorktrees(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not installed")
	}

	tmpDir := t.TempDir()
	srcRepo := filepath.Join(tmpDir, "repo")
	if err := os.MkdirAll(srcRepo, 0755); err != nil {
		t.Fatal(err)
	}

	runGit := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", srcRepo}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.local",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.local",
			"GIT_TERMINAL_PROMPT=0",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v: %s", args, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init")
	runGit("config", "user.name", "Test")
	runGit("config", "user.email", "test@test.local")

	if err := os.WriteFile(filepath.Join(srcRepo, "go.mod"), []byte("module stalewttest\n\ngo 1.22\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte("package main\nfunc main() {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	runGit("add", ".")
	runGit("commit", "-m", "initial commit")

	// 1. Create a lingering detached worktree in os.TempDir() simulating an interrupted prior build
	staleWT := filepath.Join(os.TempDir(), fmt.Sprintf("iazio-build-stalewttest-%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_ = exec.Command("git", "-C", srcRepo, "worktree", "remove", "--force", staleWT).Run()
		_ = os.RemoveAll(staleWT)
	})
	runGit("worktree", "add", "--force", "--detach", staleWT, "HEAD")

	// Dirty srcRepo so buildToolFromSourceRepo uses the isolated worktree path
	if err := os.WriteFile(filepath.Join(srcRepo, "main.go"), []byte("package main\nfunc main() {}\n// dirty\n"), 0644); err != nil {
		t.Fatal(err)
	}

	destPath := filepath.Join(tmpDir, "bin", "stalewttest")
	_ = os.MkdirAll(filepath.Dir(destPath), 0755)
	tool := ManifestToolItem{Name: "stalewttest", BinaryName: "stalewttest"}

	// 2. Run with an already-cancelled context: must clean up both the pre-existing stale worktree AND its own tempDir
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	var outBuf bytes.Buffer
	_, _ = buildToolFromSourceRepo(cancelledCtx, tool, srcRepo, destPath, true, &outBuf, &outBuf)

	if _, err := os.Stat(staleWT); !os.IsNotExist(err) {
		t.Fatalf("expected pre-existing stale worktree dir %s to be removed, stat err: %v", staleWT, err)
	}

	wtList := runGit("worktree", "list", "--porcelain")
	wtCount := 0
	for _, line := range strings.Split(wtList, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "worktree ") {
			wtCount++
		}
	}
	if wtCount != 1 {
		t.Fatalf("expected exactly 1 worktree (primary checkout) after cancelled build cleanup, got %d:\n%s", wtCount, wtList)
	}
}

