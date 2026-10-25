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
	"regexp"
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
	return service.DefaultFleetAPI
}

func defaultBaselineManifest(profile string) ManifestResponse {
	return ManifestResponse{
		Profile: profile,
		InstallRoots: ManifestInstallRoots{
			Unix: ManifestInstallRootPaths{
				Primary:  "~/.local/bin",
				Fallback: "~/.iazio/bin",
			},
			Windows: ManifestInstallRootPaths{
				Primary:  "%USERPROFILE%/bin",
				Fallback: "%USERPROFILE%/.iazio/bin",
			},
		},
		Endpoints: map[string]string{
			"harness_api": resolveLocalAPI(nil),
			"iazio_api":   "",
		},
		Tools: []ManifestToolItem{
			{Name: "iazio-agent", Repository: "viovy/iazio-agent", BinaryName: "iazio-agent", Description: "Workstation runner daemon", Tier: "baseline"},
			{Name: "iazio-harness", Repository: "viovy/iazio-harness", BinaryName: "iazio-harness", Description: "Prompt execution harness", Tier: "baseline"},
			{Name: "iazio-harvester", Repository: "viovy/iazio-harvester", BinaryName: "iazio-harvester", Description: "Conversation harvester", Tier: "baseline"},
			{Name: "autopilot", Repository: "viovy/autopilot", BinaryName: "autopilot", Description: "Autopilot CLI", Tier: "baseline"},
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", BinaryName: "autopilot-fleet", Description: "Fleet orchestration plugin", Tier: "baseline"},
			{Name: "autopilot-story", Repository: "viovy/autopilot-plugins-internal", BinaryName: "autopilot-story", Description: "Story lifecycle plugin", Tier: "baseline"},
			{Name: "autopilot-backlog", Repository: "viovy/autopilot-plugins-internal", BinaryName: "autopilot-backlog", Description: "Backlog management plugin", Tier: "baseline"},
			{Name: "iazio-mcp", Repository: "viovy/iazio-mcp", BinaryName: "iazio-mcp", Description: "MCP protocol tool bridge", Tier: "baseline"},
		},
	}
}

type UpdateOptions struct {
	Suite         bool
	DryRun        bool
	Force         bool
	Profile       string
	API           string
	BinDir        string
	OnTimeout     string
	Expired       bool
	TargetTools   []string
	TargetVersion string
}

type UpdateResult struct {
	AgentUpdated bool     `json:"agent_updated"`
	UpdatedTools []string `json:"updated_tools"`
	FailedTools  []string `json:"failed_tools,omitempty"`
	Errors       []error  `json:"errors,omitempty"`
}

func performUpdate(ctx context.Context, opts UpdateOptions, stdout, stderr io.Writer, getenv func(string) string) (bool, error) {
	res, err := performUpdateWithResult(ctx, opts, stdout, stderr, getenv)
	return res.AgentUpdated, err
}

func performUpdateWithResult(ctx context.Context, opts UpdateOptions, stdout, stderr io.Writer, getenv func(string) string) (UpdateResult, error) {
	var res UpdateResult

	// Single-tool guard (F2, F7): TargetVersion can only be applied when exactly one tool is targeted
	if opts.TargetVersion != "" {
		if opts.Suite {
			return res, fmt.Errorf("target_version %q can only be specified when exactly one tool is targeted, got suite with multiple targets", opts.TargetVersion)
		}
		if len(opts.TargetTools) > 1 {
			return res, fmt.Errorf("target_version %q can only be specified when exactly one tool is targeted, got %d targets", opts.TargetVersion, len(opts.TargetTools))
		}
	}

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
		return res, nil
	}

	manifestURL := fmt.Sprintf("%s/v1/fleet/manifest?profile=%s&os=%s&arch=%s",
		strings.TrimRight(opts.API, "/"), opts.Profile, runtime.GOOS, runtime.GOARCH)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return res, fmt.Errorf("create manifest request: %w", err)
	}

	var manifest ManifestResponse
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Control plane might be unreachable in offline or mock test runs; fallback to baseline
		manifest = defaultBaselineManifest(opts.Profile)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			// Fallback to baseline default profile if control plane hasn't registered profile yet
			manifest = defaultBaselineManifest(opts.Profile)
		} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return res, fmt.Errorf("fetch manifest returned HTTP %d", resp.StatusCode)
		} else {
			if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
				return res, fmt.Errorf("decode manifest: %w", err)
			}
		}
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
	if len(opts.TargetTools) > 0 {
		targetMap := make(map[string]bool)
		for _, name := range opts.TargetTools {
			targetMap[strings.ToLower(strings.TrimSpace(name))] = true
		}
		for _, t := range manifest.Tools {
			if targetMap[strings.ToLower(t.Name)] || targetMap[strings.ToLower(t.BinaryName)] {
				targets = append(targets, t)
			}
		}
		if len(targets) == 0 {
			for _, name := range opts.TargetTools {
				cleanName := strings.TrimSpace(name)
				if cleanName == "" {
					continue
				}
				targets = append(targets, ManifestToolItem{
					Name:        cleanName,
					Repository:  "viovy/" + cleanName,
					BinaryName:  cleanName,
					Description: cleanName,
					Tier:        "baseline",
				})
			}
		}
	} else if opts.Suite {
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

	// Single-tool guard (F2): TargetVersion can only be applied when exactly one tool is targeted
	if opts.TargetVersion != "" && len(targets) != 1 {
		return res, fmt.Errorf("target_version %q can only be specified when exactly one tool is targeted, got %d targets", opts.TargetVersion, len(targets))
	}

	for _, tool := range targets {
		if opts.TargetVersion != "" {
			tool.TargetVersion = opts.TargetVersion
		}
		updated, err := updateSingleTool(ctx, client, tool, binDir, ghToken, opts.Force, stdout, stderr, getenv)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("%s: %w", tool.Name, err))
			res.FailedTools = append(res.FailedTools, tool.Name)
			if stderr != nil {
				fmt.Fprintf(stderr, "  [-] %s: %v\n", tool.Name, err)
			}
			continue
		}
		if updated {
			res.UpdatedTools = append(res.UpdatedTools, tool.Name)
			if tool.Name == "iazio-agent" {
				res.AgentUpdated = true
			}
		}
	}

	if len(res.Errors) > 0 && opts.Force {
		return res, fmt.Errorf("forced tool update failed: %w", res.Errors[0])
	}

	return res, nil
}

func parseUpdateFlags(args []string) (UpdateOptions, error) {
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
		case args[i] == "--tool" && i+1 < len(args):
			i++
			opts.TargetTools = append(opts.TargetTools, args[i])
		case strings.HasPrefix(args[i], "--tool="):
			opts.TargetTools = append(opts.TargetTools, strings.TrimPrefix(args[i], "--tool="))
		case args[i] == "--version" && i+1 < len(args):
			i++
			opts.TargetVersion = args[i]
		case strings.HasPrefix(args[i], "--version="):
			opts.TargetVersion = strings.TrimPrefix(args[i], "--version=")
		case args[i] == "--bin-dir" && i+1 < len(args):
			i++
			opts.BinDir = args[i]
		case strings.HasPrefix(args[i], "--bin-dir="):
			opts.BinDir = strings.TrimPrefix(args[i], "--bin-dir=")
		default:
			return opts, fmt.Errorf("unknown update flag %s", args[i])
		}
	}
	return opts, nil
}

func runUpdate(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	opts, err := parseUpdateFlags(args)
	if err != nil {
		return err
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
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			binPath := filepath.Join(home, ".iazio", "bin", "iazio-agent")
			if runtime.GOOS == "windows" {
				binPath += ".exe"
			}
			_ = service.ReconcileServiceConfiguration(home, binPath, nil)
		}
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
	getenv ...func(string) string,
) (bool, error) {
	envFn := os.Getenv
	if len(getenv) > 0 && getenv[0] != nil {
		envFn = getenv[0]
	}

	binName := tool.BinaryName
	if binName == "" {
		binName = tool.Name
	}
	if runtime.GOOS == "windows" && !strings.HasSuffix(binName, ".exe") {
		binName += ".exe"
	}

	destPath := filepath.Join(binDir, binName)
	var existingShadowPath string
	if lookPath, err := exec.LookPath(binName); err == nil && lookPath != "" {
		if absLook, err := filepath.Abs(lookPath); err == nil {
			if absDest, err := filepath.Abs(destPath); err == nil && absLook != absDest {
				existingShadowPath = absLook
			}
		}
	}
	curVer := inventory.InspectInstalledVersion(destPath)
	if curVer == "" && existingShadowPath != "" {
		curVer = inventory.InspectInstalledVersion(existingShadowPath)
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
		csData, csErr := downloadAsset(ctx, client, csAsset, ghToken)
		if csErr != nil {
			return false, fmt.Errorf("download checksums (%s): %w", csAsset.Name, csErr)
		}
		if vErr := verifyChecksum(data, asset.Name, string(csData)); vErr != nil {
			if force && envFn("IAZIO_ALLOW_UNVERIFIED_UPDATES") == "1" {
				fmt.Fprintf(stdout, "  [!] %s: checksum verification failed (%v), skipping hash verification due to --force and IAZIO_ALLOW_UNVERIFIED_UPDATES=1\n", tool.Name, vErr)
			} else {
				return false, fmt.Errorf("integrity verification failed for %s: %w", asset.Name, vErr)
			}
		}
	} else {
		if !(force && envFn("IAZIO_ALLOW_UNVERIFIED_UPDATES") == "1") {
			return false, fmt.Errorf("integrity verification failed: no checksums asset found in release %s for %s (fail-closed, set IAZIO_ALLOW_UNVERIFIED_UPDATES=1 with --force to override)", latestVer, tool.Name)
		}
		fmt.Fprintf(stdout, "  [!] %s: no checksums file found in release %s, skipping hash verification due to --force and IAZIO_ALLOW_UNVERIFIED_UPDATES=1\n", tool.Name, latestVer)
	}

	binBytes, err := extractBinary(data, asset.Name, tool.BinaryName, runtime.GOOS)
	if err != nil {
		return false, fmt.Errorf("extract: %w", err)
	}

	if err := installExecutableSafe(destPath, binBytes); err != nil {
		return false, fmt.Errorf("install: %w", err)
	}

	_ = os.WriteFile(destPath+".version", []byte(latestVer), 0644)
	inventory.InvalidateVersionCache(destPath)

	if existingShadowPath != "" {
		if err := installExecutableSafe(existingShadowPath, binBytes); err == nil {
			_ = os.WriteFile(existingShadowPath+".version", []byte(latestVer), 0644)
			inventory.InvalidateVersionCache(existingShadowPath)
			fmt.Fprintf(stdout, "  [+] %s: synchronized active PATH binary at %s\n", tool.Name, existingShadowPath)
		} else if stderr != nil {
			fmt.Fprintf(stderr, "  [!] %s: warning: failed to update shadowing binary at %s: %v\n", tool.Name, existingShadowPath, err)
		}
	}

	if home := os.Getenv("HOME"); home != "" {
		for _, altDir := range []string{
			filepath.Join(home, ".local", "bin"),
			filepath.Join(home, ".iazio", "bin"),
			filepath.Join(home, ".gemini", "antigravity-cli", "bin"),
		} {
			altPath := filepath.Join(altDir, filepath.Base(destPath))
			if _, err := os.Stat(altPath); err == nil {
				_ = installExecutableSafe(altPath, binBytes)
				_ = os.WriteFile(altPath+".version", []byte(latestVer), 0644)
				inventory.InvalidateVersionCache(altPath)
			}
		}
	}

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

func cleanTargetVersion(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || strings.EqualFold(v, "latest") {
		return ""
	}
	if strings.Contains(strings.ToLower(v), "dev") {
		return ""
	}
	re := regexp.MustCompile(`v?([0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?(?:\+[a-zA-Z0-9.]+)?)|([0-9]+\.[0-9]+\.[0-9]+)`)
	if match := re.FindString(v); match != "" {
		return strings.TrimPrefix(match, "v")
	}
	return strings.TrimPrefix(v, "v")
}

func fetchRelease(ctx context.Context, client *http.Client, repo, version, token string) (*gitHubRelease, error) {
	tag := cleanTargetVersion(version)
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", repo)
	if tag != "" {
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

	if resp.StatusCode == http.StatusNotFound && tag != "" {
		// Try with v prefix
		vURL := fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/v%s", repo, strings.TrimPrefix(tag, "v"))
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

	if resp.StatusCode == http.StatusNotFound && tag == "" {
		listURL := fmt.Sprintf("https://api.github.com/repos/%s/releases", repo)
		reqList, _ := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
		if reqList != nil {
			reqList.Header.Set("Accept", "application/vnd.github.v3+json")
			reqList.Header.Set("User-Agent", "iazio-agent/1.0")
			if token != "" {
				reqList.Header.Set("Authorization", "Bearer "+token)
			}
			if respList, errList := client.Do(reqList); errList == nil {
				defer respList.Body.Close()
				if respList.StatusCode == http.StatusOK {
					var list []gitHubRelease
					if err := json.NewDecoder(respList.Body).Decode(&list); err == nil && len(list) > 0 {
						return &list[0], nil
					}
				}
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
	normTool := strings.ToLower(strings.TrimSpace(toolName))

	isPlatformMatch := func(lower string) bool {
		if strings.HasSuffix(lower, ".txt") || strings.HasSuffix(lower, ".sig") {
			return false
		}
		osMatch := strings.Contains(lower, targetOS) || (targetOS == "darwin" && (strings.Contains(lower, "macos") || strings.Contains(lower, "osx") || strings.Contains(lower, "apple")))
		archMatch := strings.Contains(lower, targetArch) || (targetArch == "amd64" && strings.Contains(lower, "x86_64")) || (targetArch == "arm64" && strings.Contains(lower, "aarch64"))
		return osMatch && archMatch
	}

	// 1. Exact component prefix: <toolName>_<version>... or <toolName>-<version>...
	if normTool != "" {
		for i := range rel.Assets {
			a := &rel.Assets[i]
			lower := strings.ToLower(a.Name)
			if !isPlatformMatch(lower) {
				continue
			}
			if strings.HasPrefix(lower, normTool+"_") || strings.HasPrefix(lower, normTool+"-") || strings.HasPrefix(lower, normTool+".") {
				return a, nil
			}
		}

		// 2. Contains toolName
		for i := range rel.Assets {
			a := &rel.Assets[i]
			lower := strings.ToLower(a.Name)
			if !isPlatformMatch(lower) {
				continue
			}
			if strings.Contains(lower, normTool) {
				return a, nil
			}
		}
	}

	// 3. Multi-binary bundle or suite archive fallback (e.g. contains "build-release" or "bundle" or "plugins")
	for i := range rel.Assets {
		a := &rel.Assets[i]
		lower := strings.ToLower(a.Name)
		if !isPlatformMatch(lower) {
			continue
		}
		if strings.Contains(lower, "build-release") || strings.Contains(lower, "bundle") || strings.Contains(lower, "plugins") {
			return a, nil
		}
	}

	// 4. Default: first platform matching asset
	for i := range rel.Assets {
		a := &rel.Assets[i]
		lower := strings.ToLower(a.Name)
		if isPlatformMatch(lower) {
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
	return fmt.Errorf("no checksum entry found for %s in checksums file", filename)
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

func inspectInstalledVersion(binaryPath string) string {
	return inventory.InspectInstalledVersion(binaryPath)
}
