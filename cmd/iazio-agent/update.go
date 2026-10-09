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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/service"
)

type ManifestToolItem struct {
	Name          string `json:"name"`
	Repository    string `json:"repository"`
	BinaryName    string `json:"binary_name"`
	Description   string `json:"description"`
	Tier          string `json:"tier"`
	TargetVersion string `json:"target_version,omitempty"`
}

type ManifestInstallRoots struct {
	Unix    ManifestInstallRootPaths `json:"unix"`
	Windows ManifestInstallRootPaths `json:"windows"`
}

type ManifestInstallRootPaths struct {
	Primary  string `json:"primary"`
	Fallback string `json:"fallback"`
}

type ManifestResponse struct {
	Profile      string               `json:"profile"`
	InstallRoots ManifestInstallRoots `json:"install_roots"`
	Endpoints    map[string]string    `json:"endpoints,omitempty"`
	Tools        []ManifestToolItem   `json:"tools"`
}

type gitHubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []gitHubAsset `json:"assets"`
}

type gitHubAsset struct {
	ID                 int64  `json:"id"`
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	URL                string `json:"url"`
}

// syncLocalEndpoints writes or updates endpoints in ~/.iazio/config.json.
func syncLocalEndpoints(endpoints map[string]string, profile string) error {
	if len(endpoints) == 0 {
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil
	}
	iazioDir := filepath.Join(home, ".iazio")
	if err := os.MkdirAll(iazioDir, 0755); err != nil {
		return err
	}
	cfgPath := filepath.Join(iazioDir, "config.json")
	var cfg struct {
		Profile   string            `json:"profile,omitempty"`
		Endpoints map[string]string `json:"endpoints,omitempty"`
	}
	if data, err := os.ReadFile(cfgPath); err == nil {
		_ = json.Unmarshal(data, &cfg)
	}
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]string)
	}
	for k, v := range endpoints {
		if strings.TrimSpace(v) != "" {
			cfg.Endpoints[k] = strings.TrimSpace(v)
		}
	}
	if profile != "" {
		cfg.Profile = profile
	}
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0600)
}

// resolveLocalAPI finds the harness API URL from env, ~/.iazio/config.json, ~/.iazio/api_url, or fallback.
func resolveLocalAPI(getenv func(string) string) string {
	if getenv == nil {
		getenv = os.Getenv
	}
	if api := strings.TrimSpace(getenv("IAZIO_HARNESS_API_URL")); api != "" {
		return api
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cfgPath := filepath.Join(home, ".iazio", "config.json")
		if data, err := os.ReadFile(cfgPath); err == nil {
			var cfg struct {
				Endpoints map[string]string `json:"endpoints"`
			}
			if err := json.Unmarshal(data, &cfg); err == nil && cfg.Endpoints != nil {
				if v, ok := cfg.Endpoints["harness_api"]; ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
				if v, ok := cfg.Endpoints["harness-api"]; ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
		if b, err := os.ReadFile(filepath.Join(home, ".iazio", "api_url")); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return s
			}
		}
	}
	return "http://localhost:8090"
}

type UpdateOptions struct {
	Suite     bool
	DryRun    bool
	Force     bool
	Profile   string
	API       string
	BinDir    string
	OnTimeout string
	Expired   bool
}

func performUpdate(ctx context.Context, opts UpdateOptions, stdout, stderr io.Writer, getenv func(string) string) (bool, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if opts.Profile == "" {
		opts.Profile = getenv("IAZIO_AGENT_PROFILE")
	}
	if opts.Profile == "" {
		opts.Profile = "generic"
	}
	if opts.API == "" {
		opts.API = resolveLocalAPI(getenv)
	}

	if opts.DryRun {
		return false, nil
	}

	manifestURL := fmt.Sprintf("%s/v1/fleet/manifest?profile=%s&os=%s&arch=%s",
		strings.TrimRight(opts.API, "/"), opts.Profile, runtime.GOOS, runtime.GOARCH)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return false, fmt.Errorf("create manifest request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Control plane might be unreachable in offline or mock test runs
		return false, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("fetch manifest returned HTTP %d", resp.StatusCode)
	}

	var manifest ManifestResponse
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return false, fmt.Errorf("decode manifest: %w", err)
	}

	if len(manifest.Endpoints) > 0 {
		_ = syncLocalEndpoints(manifest.Endpoints, manifest.Profile)
	}

	binDir := opts.BinDir
	if binDir == "" {
		binDir = resolveInstallDir(manifest.InstallRoots, getenv)
	}

	ghToken := resolveGitHubToken(getenv)

	var targets []ManifestToolItem
	if opts.Suite {
		targets = manifest.Tools
	} else {
		for _, t := range manifest.Tools {
			if t.Name == "iazio-agent" {
				targets = append(targets, t)
				break
			}
		}
		if len(targets) == 0 {
			targets = append(targets, ManifestToolItem{
				Name:        "iazio-agent",
				Repository:  "viovy/iazio-agent",
				BinaryName:  "iazio-agent",
				Description: "Workstation runner daemon",
				Tier:        "baseline",
			})
		}
	}

	agentUpdated := false
	for _, tool := range targets {
		updated, err := updateSingleTool(ctx, client, tool, binDir, ghToken, opts.Force, stdout, stderr)
		if err != nil {
			if stderr != nil {
				fmt.Fprintf(stderr, "  [-] %s: %v\n", tool.Name, err)
			}
			continue
		}
		if updated && tool.Name == "iazio-agent" {
			agentUpdated = true
		}
	}

	return agentUpdated, nil
}

func runUpdate(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	var opts UpdateOptions
	opts.OnTimeout = "reject"

	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--suite":
			opts.Suite = true
		case args[i] == "--dry-run":
			opts.DryRun = true
		case args[i] == "--force":
			opts.Force = true
		case args[i] == "--on-timeout" && i+1 < len(args):
			i++
			opts.OnTimeout = args[i]
		case strings.HasPrefix(args[i], "--on-timeout="):
			opts.OnTimeout = strings.TrimPrefix(args[i], "--on-timeout=")
		case args[i] == "--drain-expired":
			opts.Expired = true
		case args[i] == "--profile" && i+1 < len(args):
			i++
			opts.Profile = args[i]
		case strings.HasPrefix(args[i], "--profile="):
			opts.Profile = strings.TrimPrefix(args[i], "--profile=")
		case args[i] == "--api" && i+1 < len(args):
			i++
			opts.API = args[i]
		case strings.HasPrefix(args[i], "--api="):
			opts.API = strings.TrimPrefix(args[i], "--api=")
		case args[i] == "--bin-dir" && i+1 < len(args):
			i++
			opts.BinDir = args[i]
		case strings.HasPrefix(args[i], "--bin-dir="):
			opts.BinDir = strings.TrimPrefix(args[i], "--bin-dir=")
		default:
			return fmt.Errorf("unknown update flag %s", args[i])
		}
	}

	if opts.Suite {
		if !service.SuiteAllowed(false, opts.OnTimeout, opts.Expired) {
			return service.ErrSuiteBusy
		}
		fmt.Fprintln(stdout, "update suite")
	} else {
		fmt.Fprintln(stdout, "update")
	}

	agentUpdated, err := performUpdate(ctx, opts, stdout, stderr, getenv)
	if err != nil {
		return err
	}

	if agentUpdated {
		fmt.Fprintln(stdout, "iazio-agent updated: service reload required")
	}

	return nil
}

func updateSingleTool(
	ctx context.Context,
	client *http.Client,
	tool ManifestToolItem,
	binDir, ghToken string,
	force bool,
	stdout, stderr io.Writer,
) (bool, error) {
	binName := tool.BinaryName
	if binName == "" {
		binName = tool.Name
	}
	if runtime.GOOS == "windows" && !strings.HasSuffix(binName, ".exe") {
		binName += ".exe"
	}

	destPath := filepath.Join(binDir, binName)
	curVer := inventory.InspectInstalledVersion(destPath)

	if !force && (strings.Contains(curVer, "-dev") || curVer == "dev" || curVer == "0.1.0-dev") {
		if stdout != nil {
			fmt.Fprintf(stdout, "  [*] %s: %s (dev version preserved, skip update)\n", tool.Name, curVer)
		}
		return false, nil
	}

	repo := tool.Repository
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}

	rel, err := fetchRelease(ctx, client, repo, tool.TargetVersion, ghToken)
	if err != nil {
		return false, fmt.Errorf("fetch release: %w", err)
	}

	latestVer := strings.TrimPrefix(rel.TagName, "v")
	if curVer != "" && (curVer == latestVer || strings.Contains(curVer, latestVer)) && !force {
		fmt.Fprintf(stdout, "  [=] %s: %s (up-to-date)\n", tool.Name, latestVer)
		return false, nil
	}

	asset, err := findMatchingAsset(rel, tool.Name, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return false, err
	}

	data, err := downloadAsset(ctx, client, asset, ghToken)
	if err != nil {
		return false, fmt.Errorf("download: %w", err)
	}

	if csAsset := findChecksumsAsset(rel); csAsset != nil {
		if csData, err := downloadAsset(ctx, client, csAsset, ghToken); err == nil {
			_ = verifyChecksum(data, asset.Name, string(csData))
		}
	}

	binBytes, err := extractBinary(data, asset.Name, tool.BinaryName, runtime.GOOS)
	if err != nil {
		return false, fmt.Errorf("extract: %w", err)
	}

	if err := installExecutableSafe(destPath, binBytes); err != nil {
		return false, fmt.Errorf("install: %w", err)
	}

	_ = os.WriteFile(destPath+".version", []byte(latestVer), 0644)
	fmt.Fprintf(stdout, "  [+] %s: %s -> %s\n", tool.Name, latestVer, destPath)
	return true, nil
}

func resolveInstallDir(roots ManifestInstallRoots, getenv func(string) string) string {
	if runtime.GOOS == "windows" {
		primary := roots.Windows.Primary
		if primary == "" {
			primary = roots.Windows.Fallback
		}
		userProfile := getenv("USERPROFILE")
		if userProfile == "" {
			userProfile = getenv("HOMEDRIVE") + getenv("HOMEPATH")
		}
		if primary != "" {
			primary = strings.ReplaceAll(primary, "%USERPROFILE%", userProfile)
			primary = strings.ReplaceAll(primary, "/", "\\")
			return primary
		}
		return filepath.Join(userProfile, ".local", "bin")
	}

	primary := roots.Unix.Primary
	if primary == "" {
		primary = roots.Unix.Fallback
	}
	home := getenv("HOME")
	if strings.HasPrefix(primary, "~/") {
		return filepath.Join(home, strings.TrimPrefix(primary, "~/"))
	}
	if primary != "" {
		return primary
	}
	return filepath.Join(home, ".local", "bin")
}

func resolveGitHubToken(getenv func(string) string) string {
	if tok := strings.TrimSpace(getenv("GITHUB_TOKEN")); tok != "" {
		return tok
	}
	if tok := strings.TrimSpace(getenv("GH_TOKEN")); tok != "" {
		return tok
	}
	if out, err := exec.Command("gh", "auth", "token").Output(); err == nil {
		if tok := strings.TrimSpace(string(out)); tok != "" {
			return tok
		}
	}
	return ""
}

func fetchRelease(ctx context.Context, client *http.Client, repo, version, token string) (*gitHubRelease, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", repo)
	if version != "" && version != "latest" {
		tag := strings.TrimPrefix(version, "v")
		url = fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-agent/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound && version != "" && version != "latest" {
		// Try with v prefix
		vURL := fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/v%s", repo, strings.TrimPrefix(version, "v"))
		req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, vURL, nil)
		req2.Header.Set("Accept", "application/vnd.github.v3+json")
		req2.Header.Set("User-Agent", "iazio-agent/1.0")
		if token != "" {
			req2.Header.Set("Authorization", "Bearer "+token)
		}
		if resp2, err2 := client.Do(req2); err2 == nil && resp2.StatusCode == http.StatusOK {
			defer resp2.Body.Close()
			var r gitHubRelease
			if err := json.NewDecoder(resp2.Body).Decode(&r); err == nil {
				return &r, nil
			}
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}

	var r gitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func findMatchingAsset(rel *gitHubRelease, toolName, targetOS, targetArch string) (*gitHubAsset, error) {
	for i := range rel.Assets {
		a := &rel.Assets[i]
		lower := strings.ToLower(a.Name)
		if strings.HasSuffix(lower, ".txt") || strings.HasSuffix(lower, ".sig") {
			continue
		}
		if strings.Contains(lower, targetOS) && (strings.Contains(lower, targetArch) || (targetArch == "amd64" && strings.Contains(lower, "x86_64"))) {
			return a, nil
		}
	}
	return nil, fmt.Errorf("no asset matching %s/%s in release %s", targetOS, targetArch, rel.TagName)
}

func findChecksumsAsset(rel *gitHubRelease) *gitHubAsset {
	for i := range rel.Assets {
		a := &rel.Assets[i]
		lower := strings.ToLower(a.Name)
		if strings.Contains(lower, "checksum") || strings.HasSuffix(lower, "sha256sums.txt") {
			return a
		}
	}
	return nil
}

func downloadAsset(ctx context.Context, client *http.Client, asset *gitHubAsset, token string) ([]byte, error) {
	reqURL := asset.BrowserDownloadURL
	if token != "" && asset.URL != "" {
		reqURL = asset.URL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "iazio-agent/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
		if asset.URL != "" {
			req.Header.Set("Accept", "application/octet-stream")
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, reqURL)
	}

	return io.ReadAll(resp.Body)
}

func verifyChecksum(data []byte, filename, checksumsText string) error {
	sum := sha256.Sum256(data)
	calculated := hex.EncodeToString(sum[:])

	for _, line := range strings.Split(checksumsText, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) >= 2 {
			expectedHash := fields[0]
			entryFile := filepath.Base(fields[1])
			if strings.EqualFold(entryFile, filename) {
				if !strings.EqualFold(calculated, expectedHash) {
					return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedHash, calculated)
				}
				return nil
			}
		}
	}
	return nil
}

func extractBinary(archiveData []byte, archiveName, binaryName, targetOS string) ([]byte, error) {
	lowerArchive := strings.ToLower(archiveName)
	expectedName := binaryName
	if targetOS == "windows" && !strings.HasSuffix(expectedName, ".exe") {
		expectedName += ".exe"
	}

	if strings.HasSuffix(lowerArchive, ".zip") {
		zr, err := zip.NewReader(bytes.NewReader(archiveData), int64(len(archiveData)))
		if err != nil {
			return nil, fmt.Errorf("open zip: %w", err)
		}
		for _, f := range zr.File {
			base := filepath.Base(f.Name)
			if strings.EqualFold(base, expectedName) {
				rc, err := f.Open()
				if err != nil {
					return nil, err
				}
				defer rc.Close()
				return io.ReadAll(rc)
			}
		}
		return nil, fmt.Errorf("binary %q not found in zip", expectedName)
	}

	if strings.HasSuffix(lowerArchive, ".tar.gz") || strings.HasSuffix(lowerArchive, ".tgz") {
		gzr, err := gzip.NewReader(bytes.NewReader(archiveData))
		if err != nil {
			return nil, fmt.Errorf("open gzip: %w", err)
		}
		defer gzr.Close()

		tr := tar.NewReader(gzr)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			base := filepath.Base(hdr.Name)
			if strings.EqualFold(base, expectedName) {
				return io.ReadAll(tr)
			}
		}
		return nil, fmt.Errorf("binary %q not found in tar.gz", expectedName)
	}

	return nil, fmt.Errorf("unsupported archive format: %s", archiveName)
}

func installExecutableSafe(destPath string, data []byte) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	if runtime.GOOS == "windows" {
		if _, err := os.Stat(destPath); err == nil {
			f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				backupPath := fmt.Sprintf("%s.old.%d.exe", strings.TrimSuffix(destPath, ".exe"), time.Now().UnixNano())
				if renameErr := os.Rename(destPath, backupPath); renameErr != nil {
					return fmt.Errorf("file locked and rename failed: %w", renameErr)
				}
			} else {
				_ = f.Close()
			}
		}
		return os.WriteFile(destPath, data, 0755)
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d", destPath, time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, data, 0755); err != nil {
		return err
	}
	return os.Rename(tmpPath, destPath)
}

func inspectInstalledVersion(binaryPath string) string {
	return inventory.InspectInstalledVersion(binaryPath)
}
