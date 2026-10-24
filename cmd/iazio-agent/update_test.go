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
			Primary:  "~/.custom/bin",
			Fallback: "~/.iazio/bin",
		},
		Windows: ManifestInstallRootPaths{
			Primary:  "%USERPROFILE%/custom/bin",
			Fallback: "%USERPROFILE%/.iazio/bin",
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

		// Fallback when empty primary
		rootsEmpty := ManifestInstallRoots{
			Unix: ManifestInstallRootPaths{Primary: ""},
		}
		dirFallback := resolveInstallDir(rootsEmpty, func(k string) string {
			if k == "HOME" {
				return "/home/test"
			}
			return ""
		})
		if dirFallback != "/home/test/.local/bin" {
			t.Fatalf("expected fallback /home/test/.local/bin, got %s", dirFallback)
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
	ctx := context.Background()
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "iazio-agent")
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
				Assets:  []gitHubAsset{{Name: "iazio-agent_1.0.0_darwin_arm64.tar.gz"}},
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
		Name:       "iazio-agent",
		Repository: "viovy/iazio-agent",
		BinaryName: "iazio-agent",
	}

	updated, err := updateSingleTool(ctx, client, tool, binDir, "", false, &stdout, &stderr)
	if err != nil || updated {
		t.Fatalf("expected not updated, got updated=%v err=%v", updated, err)
	}
	if !strings.Contains(stdout.String(), "up-to-date") {
		t.Fatalf("expected stdout to mention up-to-date, got %s", stdout.String())
	}

	// 2. Tool out of date -> installs new version
	tarContent := createTarGz(t, "iazio-agent", []byte("#!/bin/sh\necho updated\n"))
	sum := sha256.Sum256(tarContent)
	sumText := fmt.Sprintf("%s  iazio-agent_2.0.0_%s_%s.tar.gz\n", hex.EncodeToString(sum[:]), runtime.GOOS, runtime.GOARCH)

	client2 := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			path := r.URL.Path
			switch {
			case strings.HasSuffix(path, "/releases/latest"):
				rel := gitHubRelease{
					TagName: "v2.0.0",
					Assets: []gitHubAsset{
						{
							Name:               fmt.Sprintf("iazio-agent_2.0.0_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH),
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
	// Install to binDir2; binDir1 exists as a directory so it initializes altPath
	if err := installExecutableSafe(target2, data); err != nil {
		t.Fatalf("installExecutableSafe reverse failed: %v", err)
	}

	if c, _ := os.ReadFile(target2); string(c) != string(data) {
		t.Fatalf("target2 mismatch: %q", string(c))
	}
	if c, _ := os.ReadFile(target1); string(c) != string(data) {
		t.Fatalf("target1 was not initialized: %q", string(c))
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
	// Install to binDir1; binDir2 file exists so it synchronizes altPath
	if err := installExecutableSafe(target1, data); err != nil {
		t.Fatalf("installExecutableSafe failed: %v", err)
	}

	if c, _ := os.ReadFile(target2); string(c) != string(data) {
		t.Fatalf("target2 was not updated: %q", string(c))
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
		Name:       "iazio-mcp",
		Repository: "viovy/iazio-mcp",
		BinaryName: "iazio-mcp",
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

	// 3. empty primary, uses fallback
	roots = ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Fallback: "~/fallback/bin",
		},
	}
	got = resolveInstallDir(roots, getenv)
	want = filepath.Join(home, "fallback/bin")
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}

	// 4. empty primary and empty fallback, uses default ~/.local/bin
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

	opts := UpdateOptions{
		Suite:   false,
		API:     srv.URL,
		Profile: "generic",
		BinDir:  t.TempDir(),
	}
	var stdout, stderr bytes.Buffer
	_, _ = performUpdateWithResult(ctx, opts, &stdout, &stderr, func(string) string { return "" })
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

	// 3. Fallback path
	rootsFallback := ManifestInstallRoots{
		Unix: ManifestInstallRootPaths{
			Fallback: "/opt/fallback/bin",
		},
	}
	dirFallback := resolveInstallDir(rootsFallback, getenv)
	if runtime.GOOS != "windows" && dirFallback != "/opt/fallback/bin" {
		t.Fatalf("expected /opt/fallback/bin, got %s", dirFallback)
	}

	// 4. Default empty fallback
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
	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	_ = os.MkdirAll(binDir, 0755)

	tarData := createTarGz(t, "my-tool", []byte("#!/bin/sh\necho hello\n"))

	// Mock release has only the binary tarball, NO checksums asset
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/releases/latest") {
				rel := gitHubRelease{
					TagName: "0.2.0",
					Assets: []gitHubAsset{
						{
							Name:               "my-tool-linux-amd64.tar.gz",
							BrowserDownloadURL: "https://github.com/viovy/my-tool/releases/download/0.2.0/my-tool-linux-amd64.tar.gz",
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
	// F7: Test CLI flag parsing for --version and --version=
	var opts1 UpdateOptions
	args1 := []string{"--version", "0.0.492", "--tool", "autopilot"}
	for i := 0; i < len(args1); i++ {
		switch {
		case args1[i] == "--version" && i+1 < len(args1):
			i++
			opts1.TargetVersion = args1[i]
		case strings.HasPrefix(args1[i], "--version="):
			opts1.TargetVersion = strings.TrimPrefix(args1[i], "--version=")
		case args1[i] == "--tool" && i+1 < len(args1):
			i++
			opts1.TargetTools = append(opts1.TargetTools, args1[i])
		}
	}
	if opts1.TargetVersion != "0.0.492" || len(opts1.TargetTools) != 1 || opts1.TargetTools[0] != "autopilot" {
		t.Fatalf("unexpected parsed opts: %+v", opts1)
	}

	var opts2 UpdateOptions
	args2 := []string{"--version=0.0.493"}
	for i := 0; i < len(args2); i++ {
		switch {
		case strings.HasPrefix(args2[i], "--version="):
			opts2.TargetVersion = strings.TrimPrefix(args2[i], "--version=")
		}
	}
	if opts2.TargetVersion != "0.0.493" {
		t.Fatalf("expected TargetVersion 0.0.493, got %s", opts2.TargetVersion)
	}
}
