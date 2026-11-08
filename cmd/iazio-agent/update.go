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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/auth"
	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/procutil"
	"github.com/viovy/iazio-agent/internal/service"
)

type ManifestToolItem struct {
	Name              string `json:"name"`
	Repository        string `json:"repository"`
	BinaryName        string `json:"binary_name"`
	Description       string `json:"description"`
	Tier              string `json:"tier"`
	TargetVersion     string `json:"target_version,omitempty"`
	AuthStatusEnabled bool   `json:"auth_status_enabled,omitempty"`
}

type ManifestInstallRoots struct {
	Unix    ManifestInstallRootPaths `json:"unix"`
	Windows ManifestInstallRootPaths `json:"windows"`
}

type ManifestInstallRootPaths struct {
	Primary string `json:"primary"`
}

type ManifestResponse struct {
	Profile      string               `json:"profile"`
	InstallRoots ManifestInstallRoots `json:"install_roots"`
	Endpoints    map[string]string    `json:"endpoints,omitempty"`
	Tools        []ManifestToolItem   `json:"tools"`
}

func fetchManifestAuthEnabledTools(ctx context.Context, apiURL, profile, token string) (map[string]bool, error) {
	cleanAPI := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if cleanAPI == "" {
		return nil, nil
	}
	prof := strings.TrimSpace(profile)
	if prof == "" {
		prof = "generic"
	}
	manifestURL := fmt.Sprintf("%s/v1/fleet/manifest?profile=%s&os=%s&arch=%s",
		cleanAPI, prof, runtime.GOOS, runtime.GOARCH)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	}
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("manifest HTTP %d", resp.StatusCode)
	}
	var manifest ManifestResponse
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, err
	}
	enabled := make(map[string]bool)
	for _, t := range manifest.Tools {
		if t.AuthStatusEnabled {
			if strings.TrimSpace(t.Name) != "" {
				enabled[strings.ToLower(strings.TrimSpace(t.Name))] = true
			}
			if strings.TrimSpace(t.BinaryName) != "" {
				bin := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(t.BinaryName)), ".exe")
				enabled[bin] = true
			}
		}
	}
	return enabled, nil
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

func defaultRepositoryForTool(name string) string {
	clean := strings.ToLower(strings.TrimSpace(name))
	if clean == "autopilot" {
		return "viovy/autopilot"
	}
	if strings.HasPrefix(clean, "autopilot-") {
		return "viovy/autopilot-plugins-internal"
	}
	return "viovy/" + strings.TrimSpace(name)
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
	home, _ := os.UserHomeDir()
	if h := os.Getenv("HOME"); h != "" && runtime.GOOS == "windows" {
		home = h
	}
	if p := auth.ConfigPathFrom(home, getenv); p != "" {
		if data, err := os.ReadFile(p); err == nil {
			var cfg struct {
				APIBaseURL string `json:"api_base_url"`
			}
			if err := json.Unmarshal(data, &cfg); err == nil && strings.TrimSpace(cfg.APIBaseURL) != "" {
				return strings.TrimSpace(cfg.APIBaseURL)
			}
		}
	}
	if home != "" {
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
				Primary: "~/.local/bin",
			},
			Windows: ManifestInstallRootPaths{
				Primary: "%USERPROFILE%/bin",
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
	if ctx.Value(downloadAssetCacheKey{}) == nil {
		ctx = context.WithValue(ctx, downloadAssetCacheKey{}, &assetCache{items: make(map[string][]byte)})
	}
	var res UpdateResult

	// Single-tool guard (F2, F7): TargetVersion can only be applied when exactly one tool is targeted
	if opts.TargetVersion != "" {
		if opts.Suite && len(opts.TargetTools) != 1 {
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

	manifestURL := fmt.Sprintf("%s/v1/fleet/manifest?profile=%s&os=%s&arch=%s",
		strings.TrimRight(opts.API, "/"), opts.Profile, runtime.GOOS, runtime.GOARCH)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return res, fmt.Errorf("create manifest request: %w", err)
	}
	if cpToken := strings.TrimSpace(resolveControlPlaneToken(getenv)); cpToken != "" {
		req.Header.Set("Authorization", "Bearer "+cpToken)
	}

	var manifest ManifestResponse
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		// Control plane might be unreachable in offline or mock test runs; fallback to baseline
		manifest = defaultBaselineManifest(opts.Profile)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound || (opts.DryRun && resp.StatusCode >= 500) {
			// Fallback to baseline default profile if control plane hasn't registered profile yet or is unreachable in dry-run
			manifest = defaultBaselineManifest(opts.Profile)
		} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return res, fmt.Errorf("fetch manifest returned HTTP %d", resp.StatusCode)
		} else {
			if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
				return res, fmt.Errorf("decode manifest: %w", err)
			}
		}
	}

	if !opts.DryRun && len(manifest.Endpoints) > 0 {
		_ = syncLocalEndpoints(manifest.Endpoints, manifest.Profile)
	}

	binDir := opts.BinDir
	if binDir == "" {
		binDir = resolveInstallDir(manifest.InstallRoots, getenv)
	}

	ghToken := resolveGitHubToken(getenv)

	var targets []ManifestToolItem
	if len(opts.TargetTools) > 0 {
		hasWildcard := false
		for _, name := range opts.TargetTools {
			trimmed := strings.TrimSpace(name)
			if trimmed == "*" || strings.EqualFold(trimmed, "all") {
				hasWildcard = true
				break
			}
		}
		if hasWildcard {
			opts.Suite = true
			opts.TargetTools = nil
		}
	}
	if len(opts.TargetTools) > 0 {
		targetMap := make(map[string]bool)
		for _, name := range opts.TargetTools {
			targetMap[strings.ToLower(strings.TrimSpace(name))] = true
		}
		matchedTargets := make(map[string]bool)
		for _, t := range manifest.Tools {
			nLower := strings.ToLower(t.Name)
			bLower := strings.ToLower(t.BinaryName)
			if targetMap[nLower] || targetMap[bLower] {
				targets = append(targets, t)
				matchedTargets[nLower] = true
				if bLower != "" {
					matchedTargets[bLower] = true
				}
			}
		}
		for _, name := range opts.TargetTools {
			cleanName := strings.TrimSpace(name)
			if cleanName == "" {
				continue
			}
			lowerName := strings.ToLower(cleanName)
			if matchedTargets[lowerName] {
				continue
			}
			matchedTargets[lowerName] = true
			targets = append(targets, ManifestToolItem{
				Name:        cleanName,
				Repository:  defaultRepositoryForTool(cleanName),
				BinaryName:  cleanName,
				Description: cleanName,
				Tier:        "baseline",
			})
		}
	} else if opts.Suite {
		targets = manifest.Tools
		if len(targets) == 0 {
			for _, name := range inventory.ManagedTools {
				targets = append(targets, ManifestToolItem{
					Name:        name,
					Repository:  defaultRepositoryForTool(name),
					BinaryName:  name,
					Description: name,
					Tier:        "baseline",
				})
			}
		}
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

	// Sanitize and deduplicate targets (skip deprecated unprefixed git-flow when autopilot-git-flow is present)
	hasCanonicalGitFlow := false
	for _, t := range manifest.Tools {
		if strings.EqualFold(strings.TrimSpace(t.Name), "autopilot-git-flow") ||
			strings.EqualFold(strings.TrimSpace(t.BinaryName), "autopilot-git-flow") {
			hasCanonicalGitFlow = true
			break
		}
	}
	explicitGitFlow := false
	for _, name := range opts.TargetTools {
		if strings.EqualFold(strings.TrimSpace(name), "git-flow") {
			explicitGitFlow = true
			break
		}
	}
	seenTargets := make(map[string]bool, len(targets))
	dedupedTargets := make([]ManifestToolItem, 0, len(targets))
	for _, t := range targets {
		key := strings.ToLower(strings.TrimSpace(t.Name))
		if key == "" {
			key = strings.ToLower(strings.TrimSpace(t.BinaryName))
		}
		if key == "" || seenTargets[key] {
			continue
		}
		if key == "git-flow" && hasCanonicalGitFlow && !explicitGitFlow {
			continue
		}
		seenTargets[key] = true
		dedupedTargets = append(dedupedTargets, t)
	}
	targets = dedupedTargets

	ctx = withReleaseCache(ctx)

	// Always update iazio-agent last so self-update never interrupts sibling tool updates
	sort.SliceStable(targets, func(i, j int) bool {
		return targets[i].Name != "iazio-agent" && targets[j].Name == "iazio-agent"
	})

	for _, tool := range targets {
		if opts.TargetVersion != "" {
			tool.TargetVersion = opts.TargetVersion
		}
		if opts.DryRun {
			_, err := evalDryRunTool(ctx, client, tool, binDir, ghToken, opts.Force, stdout, stderr, getenv)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Errorf("%s: %w", tool.Name, err))
				res.FailedTools = append(res.FailedTools, tool.Name)
				if stderr != nil {
					fmt.Fprintf(stderr, "  [-] %s: %v\n", tool.Name, err)
				}
			}
			continue
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
		if len(targets) == 1 || (len(res.UpdatedTools) == 0 && len(res.Errors) == len(targets)) {
			return res, fmt.Errorf("forced tool update failed: %w", res.Errors[0])
		}
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
			if !strings.HasPrefix(args[i], "-") {
				opts.TargetTools = append(opts.TargetTools, args[i])
				continue
			}
			return opts, fmt.Errorf("unknown update flag %s", args[i])
		}
	}
	return opts, nil
}

func runCleanupBinariesWithReport(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) (*inventory.CleanupReport, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	jsonOutput := false
	dryRun := false
	for _, a := range args {
		if a == "--json" || a == "-json" {
			jsonOutput = true
		}
		if a == "--dry-run" || a == "-n" {
			dryRun = true
		}
	}

	home := getenv("HOME")
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	canonicalBinDir := filepath.Join(home, ".local", "bin")
	if canon := getenv("CANONICAL_BIN_DIR"); canon != "" {
		canonicalBinDir = canon
	} else if runtime.GOOS == "windows" {
		canonicalBinDir = filepath.Join(home, "bin")
	}

	searchDirs := inventory.DefaultDirs(getenv("PATH"), home, runtime.GOOS)
	candidates := inventory.ScanDevAndShadowBinaries(searchDirs, canonicalBinDir)

	if dryRun {
		if jsonOutput {
			data, _ := json.MarshalIndent(candidates, "", "  ")
			if stdout != nil {
				fmt.Fprintln(stdout, string(data))
			}
			return nil, nil
		}
		if stdout != nil {
			fmt.Fprintf(stdout, "Dev binary dry-run scan on %s: %d candidates found\n", runtime.GOOS, len(candidates))
			for _, c := range candidates {
				fmt.Fprintf(stdout, "  - %s (%s)\n", c.Path, c.VariantType)
			}
		}
		return nil, nil
	}

	rep, err := inventory.CleanupDevBinaries(ctx, candidates, canonicalBinDir)
	report := &rep
	if err != nil {
		return report, err
	}

	if jsonOutput {
		data, _ := json.MarshalIndent(report, "", "  ")
		if stdout != nil {
			fmt.Fprintln(stdout, string(data))
		}
		return report, nil
	}

	if stdout != nil {
		fmt.Fprintf(stdout, "Dev binary cleanup complete on %s:\n", runtime.GOOS)
		fmt.Fprintf(stdout, "  Deleted: %d\n", len(report.DeletedPaths))
		for _, p := range report.DeletedPaths {
			fmt.Fprintf(stdout, "    - %s\n", p)
		}
		if len(report.KilledPIDs) > 0 {
			fmt.Fprintf(stdout, "  Killed PIDs: %v\n", report.KilledPIDs)
		}
		if len(report.FailedPaths) > 0 {
			fmt.Fprintf(stdout, "  Failed: %d\n", len(report.FailedPaths))
			for _, f := range report.FailedPaths {
				fmt.Fprintf(stdout, "    - %s\n", f)
			}
		}
		if len(report.RootRequiredPaths) > 0 {
			fmt.Fprintf(stdout, "  Root required (run with sudo): %d\n", len(report.RootRequiredPaths))
			for _, r := range report.RootRequiredPaths {
				fmt.Fprintf(stdout, "    - %s\n", r)
			}
		}
	}
	return report, nil
}

func runCleanupBinaries(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	_, err := runCleanupBinariesWithReport(ctx, args, stdout, stderr, getenv)
	return err
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
			binPath := filepath.Join(home, ".local", "bin", "iazio-agent")
			if runtime.GOOS == "windows" {
				binPath = filepath.Join(home, "bin", "iazio-agent.exe")
			}
			_ = service.ReconcileServiceConfiguration(home, binPath, nil)
		}
	}

	return nil
}

func evalDryRunTool(
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

	sourceRepo := findToolSourceRepo(tool.Name, envFn, tool.Repository)
	if sourceRepo != "" {
		if hasStoryChanges, reason := shouldBuildFromSourceDuringStory(ctx, sourceRepo, envFn); hasStoryChanges {
			if stdout != nil {
				fmt.Fprintf(stdout, "  [*] %s: %s (dry-run: active story code changes detected in %s [%s], source build Path A available)\n", tool.Name, curVer, filepath.Base(sourceRepo), reason)
			}
			return true, nil
		}
	}

	repo := tool.Repository
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}

	rel, err := fetchRelease(ctx, client, repo, tool.TargetVersion, ghToken)
	if err != nil {
		if stdout != nil {
			fmt.Fprintf(stdout, "  [?] %s: unable to fetch release (%v)\n", tool.Name, err)
		}
		return false, nil
	}

	latestVer := strings.TrimPrefix(rel.TagName, "v")
	upToDate, isOverride := evaluateReleaseAgainstInstalled(curVer, latestVer)
	if upToDate && !force {
		if stdout != nil {
			fmt.Fprintf(stdout, "  [=] %s: %s (dry-run: up-to-date)\n", tool.Name, latestVer)
		}
		return false, nil
	}

	if isOverride && stdout != nil {
		fmt.Fprintf(stdout, "  [+] %s: %s -> %s (dry-run: official GitHub Release can override local source build) [Path B]\n", tool.Name, curVer, latestVer)
		return true, nil
	}

	if stdout != nil {
		fmt.Fprintf(stdout, "  [+] %s: %s -> %s (dry-run: update available)\n", tool.Name, curVer, latestVer)
	}
	return true, nil
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

	sourceRepo := findToolSourceRepo(tool.Name, envFn, tool.Repository)
	if sourceRepo != "" {
		if hasStoryChanges, reason := shouldBuildFromSourceDuringStory(ctx, sourceRepo, envFn); hasStoryChanges {
			if stdout != nil {
				fmt.Fprintf(stdout, "  [*] %s: active story code changes detected in %s (%s), compiling local source build (Path A)\n", tool.Name, filepath.Base(sourceRepo), reason)
			}
			updated, srcErr := buildToolFromSourceRepo(ctx, tool, sourceRepo, destPath, true, stdout, stderr)
			if srcErr == nil {
				return updated, nil
			}
			if stderr != nil {
				fmt.Fprintf(stderr, "  [!] %s: source repo build encountered error (%v), attempting release asset fetch (Path B)\n", tool.Name, srcErr)
			}
		}
	}

	repo := tool.Repository
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}

	allowSourceFallback := envFn("IAZIO_AGENT_NONINTERACTIVE") != "1"

	rel, err := fetchRelease(ctx, client, repo, tool.TargetVersion, ghToken)
	if err != nil {
		if sourceRepo != "" && allowSourceFallback {
			if stderr != nil {
				fmt.Fprintf(stderr, "  [!] %s: fetch release failed (%v), falling back to source repo build (Path A)\n", tool.Name, err)
			}
			return buildToolFromSourceRepo(ctx, tool, sourceRepo, destPath, force, stdout, stderr)
		}
		return false, fmt.Errorf("fetch release: %w", err)
	}

	latestVer := strings.TrimPrefix(rel.TagName, "v")
	upToDate, isOverride := evaluateReleaseAgainstInstalled(curVer, latestVer)
	if upToDate && !force {
		fmt.Fprintf(stdout, "  [=] %s: %s (up-to-date)\n", tool.Name, latestVer)
		return false, nil
	}

	if isOverride && stdout != nil {
		fmt.Fprintf(stdout, "  [+] %s: overriding local source build (%s) with official GitHub Release (%s) [Path B]\n", tool.Name, curVer, latestVer)
	}

	asset, err := findMatchingAsset(rel, tool.Name, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		if sourceRepo != "" && allowSourceFallback {
			if stderr != nil {
				fmt.Fprintf(stderr, "  [!] %s: matching release asset not found (%v), falling back to source repo build (Path A)\n", tool.Name, err)
			}
			return buildToolFromSourceRepo(ctx, tool, sourceRepo, destPath, force, stdout, stderr)
		}
		return false, err
	}

	data, err := downloadAsset(ctx, client, asset, ghToken)
	if err != nil {
		return false, fmt.Errorf("download: %w", err)
	}

	csAsset := findChecksumsAsset(rel)
	if csAsset == nil {
		return false, fmt.Errorf("integrity verification failed: no checksums asset found in release %s for %s (fail-closed)", latestVer, tool.Name)
	}
	csData, csErr := downloadAsset(ctx, client, csAsset, ghToken)
	if csErr != nil {
		return false, fmt.Errorf("download checksums (%s): %w", csAsset.Name, csErr)
	}
	if vErr := verifyChecksum(data, asset.Name, string(csData)); vErr != nil {
		return false, fmt.Errorf("integrity verification failed for %s: %w", asset.Name, vErr)
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

	if tool.Name == "iazio-harvester" {
		_ = service.RestartToolService(tool.Name)
	}

	fmt.Fprintf(stdout, "  [+] %s: %s -> %s\n", tool.Name, latestVer, destPath)
	return true, nil
}

func resolveInstallDir(roots ManifestInstallRoots, getenv func(string) string) string {
	if runtime.GOOS == "windows" {
		primary := roots.Windows.Primary
		userProfile := getenv("USERPROFILE")
		if userProfile == "" {
			userProfile = getenv("HOMEDRIVE") + getenv("HOMEPATH")
		}
		if primary == "" && roots.Unix.Primary != "" && userProfile == "" {
			home := getenv("HOME")
			if strings.HasPrefix(roots.Unix.Primary, "~/") {
				return filepath.Join(home, strings.TrimPrefix(roots.Unix.Primary, "~/"))
			}
			return roots.Unix.Primary
		}
		if userProfile == "" {
			if home := getenv("HOME"); home != "" {
				return filepath.Join(home, ".local", "bin")
			}
		}
		if primary != "" {
			primary = strings.ReplaceAll(primary, "%USERPROFILE%", userProfile)
			primary = strings.ReplaceAll(primary, "/", "\\")
			return primary
		}
		return filepath.Join(userProfile, "bin")
	}

	primary := roots.Unix.Primary
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
	home := strings.TrimSpace(getenv("HOME"))
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	ghCandidates := []string{"gh"}
	if home != "" {
		ghCandidates = append(ghCandidates, filepath.Join(home, "brew", "bin", "gh"))
	}
	ghCandidates = append(ghCandidates, "/opt/homebrew/bin/gh", "/usr/local/bin/gh", "/usr/bin/gh")
	seen := make(map[string]bool, len(ghCandidates))
	for _, bin := range ghCandidates {
		if bin == "" || seen[bin] {
			continue
		}
		seen[bin] = true
		if out, err := procutil.Command(bin, "auth", "token").Output(); err == nil {
			if tok := strings.TrimSpace(string(out)); tok != "" {
				return tok
			}
		}
	}
	return ""
}

type releaseCacheKey struct{}

type releaseCacheStore struct {
	mu    sync.Mutex
	items map[string]*gitHubRelease
}

func withReleaseCache(ctx context.Context) context.Context {
	if ctx.Value(releaseCacheKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, releaseCacheKey{}, &releaseCacheStore{
		items: make(map[string]*gitHubRelease),
	})
}

var targetVersionRegex = regexp.MustCompile(`v?([0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?(?:\+[a-zA-Z0-9.]+)?)|([0-9]+\.[0-9]+\.[0-9]+)`)

func cleanTargetVersion(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || strings.EqualFold(v, "latest") {
		return ""
	}
	if strings.Contains(strings.ToLower(v), "dev") {
		return ""
	}
	if match := targetVersionRegex.FindString(v); match != "" {
		return strings.TrimPrefix(match, "v")
	}
	return strings.TrimPrefix(v, "v")
}

func fetchRelease(ctx context.Context, client *http.Client, repo, version, token string) (*gitHubRelease, error) {
	tag := cleanTargetVersion(version)
	cacheKey := repo + "@" + tag
	var cache *releaseCacheStore
	if v := ctx.Value(releaseCacheKey{}); v != nil {
		cache, _ = v.(*releaseCacheStore)
		if cache != nil {
			cache.mu.Lock()
			if cached, ok := cache.items[cacheKey]; ok && cached != nil {
				cache.mu.Unlock()
				return cached, nil
			}
			cache.mu.Unlock()
		}
	}
	storeCached := func(r *gitHubRelease) *gitHubRelease {
		if cache != nil && r != nil {
			cache.mu.Lock()
			cache.items[cacheKey] = r
			cache.mu.Unlock()
		}
		return r
	}

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
		if req2 != nil {
			req2.Header.Set("Accept", "application/vnd.github.v3+json")
			req2.Header.Set("User-Agent", "iazio-agent/1.0")
			if token != "" {
				req2.Header.Set("Authorization", "Bearer "+token)
			}
			if resp2, err2 := client.Do(req2); err2 == nil {
				defer resp2.Body.Close()
				if resp2.StatusCode == http.StatusOK {
					var r gitHubRelease
					if err := json.NewDecoder(resp2.Body).Decode(&r); err == nil {
						return storeCached(&r), nil
					}
				}
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
						return storeCached(&list[0]), nil
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
	return storeCached(&r), nil
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

	// 1. Exact component prefix: <toolName>_<version>... or <toolName>.<ext>
	if normTool != "" {
		for i := range rel.Assets {
			a := &rel.Assets[i]
			lower := strings.ToLower(a.Name)
			if !isPlatformMatch(lower) {
				continue
			}
			if strings.HasPrefix(lower, normTool+"_") || strings.HasPrefix(lower, normTool+".") {
				return a, nil
			}
		}

		// 2. Hyphen-delimited version/OS suffix: <toolName>-<version> or <toolName>-<os>
		// Ensures "autopilot-fleet" does NOT match "autopilot-fleet-exec_..."
		for i := range rel.Assets {
			a := &rel.Assets[i]
			lower := strings.ToLower(a.Name)
			if !isPlatformMatch(lower) {
				continue
			}
			if strings.HasPrefix(lower, normTool+"-") {
				rest := strings.TrimPrefix(lower, normTool+"-")
				isVer := len(rest) > 0 && ((rest[0] >= '0' && rest[0] <= '9') || (rest[0] == 'v' && len(rest) > 1 && rest[1] >= '0' && rest[1] <= '9'))
				if isVer || strings.HasPrefix(rest, targetOS) {
					return a, nil
				}
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
		if strings.Contains(lower, "build-release") || strings.Contains(lower, "bundle") || strings.Contains(lower, "plugins") || strings.HasPrefix(lower, "autopilot-plugins-internal_") {
			return a, nil
		}
	}

	// 4. Fallback for single-tool releases (<= 6 assets)
	if len(rel.Assets) <= 6 {
		if normTool != "" {
			for i := range rel.Assets {
				a := &rel.Assets[i]
				lower := strings.ToLower(a.Name)
				if isPlatformMatch(lower) && strings.Contains(lower, normTool) {
					return a, nil
				}
			}
		}
		for i := range rel.Assets {
			a := &rel.Assets[i]
			lower := strings.ToLower(a.Name)
			if isPlatformMatch(lower) {
				return a, nil
			}
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

type downloadAssetCacheKey struct{}

type assetCache struct {
	mu    sync.Mutex
	items map[string][]byte
}

func isArchiveAsset(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".zip") || strings.HasSuffix(lower, ".tgz")
}

func downloadAsset(ctx context.Context, client *http.Client, asset *gitHubAsset, token string) ([]byte, error) {
	reqURL := asset.BrowserDownloadURL
	if token != "" && asset.URL != "" {
		reqURL = asset.URL
	}

	var cache *assetCache
	if c, ok := ctx.Value(downloadAssetCacheKey{}).(*assetCache); ok && c != nil {
		cache = c
		if isArchiveAsset(asset.Name) {
			cache.mu.Lock()
			if cached, ok := cache.items[reqURL]; ok {
				cache.mu.Unlock()
				return cached, nil
			}
			cache.mu.Unlock()
		}
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

	limit := maxExtractedBinarySize
	if limit <= 0 {
		limit = defaultMaxExtractedBinarySize
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("downloaded asset %s exceeds maximum size of %d bytes", asset.Name, limit)
	}

	if cache != nil && isArchiveAsset(asset.Name) {
		cache.mu.Lock()
		cache.items[reqURL] = data
		cache.mu.Unlock()
	}

	return data, nil
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

const defaultMaxExtractedBinarySize int64 = 256 << 20 // 256 MiB

var maxExtractedBinarySize int64 = defaultMaxExtractedBinarySize

func readBoundedArchiveEntry(r io.Reader) ([]byte, error) {
	limit := maxExtractedBinarySize
	if limit <= 0 {
		limit = defaultMaxExtractedBinarySize
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("extracted binary exceeds maximum size of %d bytes", limit)
	}
	return data, nil
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
			if strings.EqualFold(base, expectedName) || (targetOS == "windows" && strings.EqualFold(base, strings.TrimSuffix(expectedName, ".exe"))) {
				rc, err := f.Open()
				if err != nil {
					return nil, err
				}
				defer rc.Close()
				return readBoundedArchiveEntry(rc)
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
			if strings.EqualFold(base, expectedName) || (targetOS == "windows" && strings.EqualFold(base, strings.TrimSuffix(expectedName, ".exe"))) {
				return readBoundedArchiveEntry(tr)
			}
		}
		return nil, fmt.Errorf("binary %q not found in tar.gz", expectedName)
	}

	return nil, fmt.Errorf("unsupported archive format: %s", archiveName)
}

func inspectInstalledVersion(binaryPath string) string {
	return inventory.InspectInstalledVersion(binaryPath)
}

func findToolSourceRepo(toolName string, getenv func(string) string, repoHint ...string) string {
	if getenv == nil {
		getenv = os.Getenv
	}
	cleanTool := strings.ToLower(strings.TrimSpace(toolName))
	cleanTool = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(cleanTool, ".exe"), ".cmd"), ".bat")
	if cleanTool == "" {
		return ""
	}

	candidateRepoNames := []string{cleanTool}
	if len(repoHint) > 0 && strings.TrimSpace(repoHint[0]) != "" {
		rh := strings.ToLower(strings.TrimSpace(repoHint[0]))
		if idx := strings.LastIndex(rh, "/"); idx != -1 {
			rh = rh[idx+1:]
		}
		switch rh {
		case "autopilot-plugins":
			rh = "autopilot-plugins-internal"
		case "autopilot-core":
			rh = "autopilot"
		}
		if rh != "" && rh != cleanTool {
			candidateRepoNames = append(candidateRepoNames, rh)
		}
	}
	if strings.HasPrefix(cleanTool, "autopilot-") {
		hasPluginsCandidate := false
		for _, c := range candidateRepoNames {
			if c == "autopilot-plugins-internal" {
				hasPluginsCandidate = true
				break
			}
		}
		if !hasPluginsCandidate {
			candidateRepoNames = append(candidateRepoNames, "autopilot-plugins-internal")
		}
	}

	isGitRepoWithTool := func(dir string) bool {
		fi, err := os.Stat(filepath.Join(dir, ".git"))
		if err != nil || (!fi.IsDir() && !fi.Mode().IsRegular()) {
			return false
		}
		if strings.EqualFold(filepath.Base(dir), cleanTool) {
			return true
		}
		if st, err := os.Stat(filepath.Join(dir, "cmd", cleanTool)); err == nil && st.IsDir() {
			return true
		}
		if strings.EqualFold(filepath.Base(dir), "autopilot-plugins-internal") && strings.HasPrefix(cleanTool, "autopilot-") {
			if _, err := os.Stat(filepath.Join(dir, "cmd")); os.IsNotExist(err) {
				return true
			}
		}
		return false
	}

	// 1. Check explicit environment overrides
	for _, envKey := range []string{"IAZIO_FLEET_ROOT", "META_REPO_ROOT", "AUTOPILOT_ROOT"} {
		if root := strings.TrimSpace(getenv(envKey)); root != "" {
			for _, repoDirName := range candidateRepoNames {
				cand := filepath.Join(root, repoDirName)
				if isGitRepoWithTool(cand) {
					return cand
				}
				if strings.EqualFold(filepath.Base(root), repoDirName) && isGitRepoWithTool(root) {
					return root
				}
			}
		}
	}

	// 2. Relative to working directory and parent directories (in unit tests, only when cwd is inside TempDir)
	if cwd, err := os.Getwd(); err == nil && cwd != "" && (!isTestBinary() || isPathWithinTempDir(cwd)) {
		curr := cwd
		for i := 0; i < 4; i++ {
			for _, repoDirName := range candidateRepoNames {
				cand := filepath.Join(curr, repoDirName)
				if isGitRepoWithTool(cand) {
					return cand
				}
				if strings.EqualFold(filepath.Base(curr), repoDirName) && isGitRepoWithTool(curr) {
					return curr
				}
			}
			parent := filepath.Dir(curr)
			if parent == curr {
				break
			}
			curr = parent
		}
	}

	// 3. Common user directory locations (in unit tests, never fall back to host UserHomeDir)
	home := strings.TrimSpace(getenv("HOME"))
	if home == "" && !isTestBinary() {
		home, _ = os.UserHomeDir()
	}
	if home != "" && (!isTestBinary() || isPathWithinTempDir(home)) {
		metaRepos, _ := filepath.Glob(filepath.Join(home, "work", "meta-repo*"))
		sort.Sort(sort.Reverse(sort.StringSlice(metaRepos)))

		for _, repoDirName := range candidateRepoNames {
			for _, mr := range metaRepos {
				cand := filepath.Join(mr, repoDirName)
				if isGitRepoWithTool(cand) {
					return cand
				}
			}
			homeCandidates := []string{
				filepath.Join(home, "work", "meta"+"-repo-02", repoDirName),
				filepath.Join(home, "work", "meta"+"-repo", repoDirName),
				filepath.Join(home, "work", repoDirName),
				filepath.Join(home, "src", repoDirName),
				filepath.Join(home, "projects", repoDirName),
				filepath.Join(home, repoDirName),
			}
			if runtime.GOOS == "windows" && !isTestBinary() {
				homeCandidates = append(homeCandidates,
					filepath.Join(`C:\work`, "meta"+"-repo-02", repoDirName),
					filepath.Join(`C:\work`, "meta"+"-repo", repoDirName),
					filepath.Join(`C:\work`, repoDirName),
				)
			}
			for _, cand := range homeCandidates {
				if isGitRepoWithTool(cand) {
					return cand
				}
			}
		}
	}

	return ""
}

func isPathWithinTempDir(p string) bool {
	if p == "" {
		return false
	}
	cleanP := filepath.Clean(p)
	if evalP, err := filepath.EvalSymlinks(cleanP); err == nil {
		cleanP = evalP
	}
	tmpBase := filepath.Clean(os.TempDir())
	if evalTmp, err := filepath.EvalSymlinks(tmpBase); err == nil {
		tmpBase = evalTmp
	}
	if strings.HasPrefix(cleanP, tmpBase+string(filepath.Separator)) || cleanP == tmpBase {
		return true
	}
	slash := filepath.ToSlash(cleanP)
	return strings.HasPrefix(slash, "/tmp/") || strings.HasPrefix(slash, "/private/tmp/") ||
		strings.HasPrefix(slash, "/var/folders/") || strings.HasPrefix(slash, "/private/var/folders/")
}

// pruneStaleBuildWorktrees evicts any leftover ephemeral iazio-build-* git worktrees and temp directories
// from previous interrupted or timed-out source builds before starting a new source build.
func pruneStaleBuildWorktrees(sourceRepo, toolName string) {
	if sourceRepo == "" {
		return
	}
	cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cleanSource := filepath.Clean(sourceRepo)
	if evalSource, err := filepath.EvalSymlinks(cleanSource); err == nil {
		cleanSource = evalSource
	}

	listCmd := procutil.CommandContext(cleanCtx, "git", "-C", sourceRepo, "worktree", "list", "--porcelain")
	listCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := listCmd.Output(); err == nil {
		for idx, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "worktree ") {
				continue
			}
			wtPath := strings.TrimSpace(strings.TrimPrefix(line, "worktree "))
			if wtPath == "" || idx == 0 {
				continue
			}
			cleanWT := filepath.Clean(wtPath)
			if evalWT, err := filepath.EvalSymlinks(cleanWT); err == nil {
				cleanWT = evalWT
			}
			if cleanWT == cleanSource {
				continue
			}
			base := filepath.Base(cleanWT)
			if strings.HasPrefix(base, "iazio-build-") {
				rmCmd := procutil.CommandContext(cleanCtx, "git", "-C", sourceRepo, "worktree", "remove", "--force", wtPath)
				rmCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
				_ = rmCmd.Run()
				_ = os.RemoveAll(wtPath)
			}
		}
	}

	tmpDir := os.TempDir()
	if entries, err := os.ReadDir(tmpDir); err == nil {
		toolBuildPrefix := fmt.Sprintf("iazio-build-%s-", toolName)
		toolStagePrefix := fmt.Sprintf("staging-%s-", toolName)
		cutoff := time.Now().Add(-15 * time.Minute)
		for _, entry := range entries {
			name := entry.Name()
			isToolMatch := toolName != "" && (strings.HasPrefix(name, toolBuildPrefix) || strings.HasPrefix(name, toolStagePrefix))
			isAnyBuild := strings.HasPrefix(name, "iazio-build-") || strings.HasPrefix(name, "staging-")
			if !isToolMatch && !isAnyBuild {
				continue
			}
			fullPath := filepath.Join(tmpDir, name)
			if isToolMatch {
				_ = os.RemoveAll(fullPath)
				continue
			}
			if info, statErr := entry.Info(); statErr == nil && info.ModTime().Before(cutoff) {
				_ = os.RemoveAll(fullPath)
			}
		}
	}

	pruneCmd := procutil.CommandContext(cleanCtx, "git", "-C", sourceRepo, "worktree", "prune")
	pruneCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	_ = pruneCmd.Run()
}

// isSourceBuildVersion reports whether a version string denotes a local source build
// (e.g. containing build metadata '+<sha>', 'dev', a raw git commit SHA, or '-dirty').
func isSourceBuildVersion(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" || v == "installed" || v == "unknown" || v == "none" {
		return true
	}
	if strings.Contains(v, "+") {
		return true
	}
	lv := strings.ToLower(v)
	if strings.HasPrefix(lv, "dev") || strings.Contains(lv, "-dev") || strings.Contains(lv, "-dirty") {
		return true
	}
	// Check if purely a hex commit SHA (7 to 40 characters)
	if len(v) >= 7 && len(v) <= 40 {
		isHex := true
		for _, r := range v {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				isHex = false
				break
			}
		}
		if isHex {
			return true
		}
	}
	return false
}

// parseTripleSemVer parses major, minor, patch from a semantic version string,
// ignoring build metadata (+...) and pre-release (-...) extensions.
func parseTripleSemVer(v string) (int, int, int, bool) {
	c := cleanTargetVersion(v)
	if c == "" {
		return 0, 0, 0, false
	}
	if idx := strings.IndexAny(c, "-+"); idx >= 0 {
		c = c[:idx]
	}
	parts := strings.Split(c, ".")
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	var maj, min, pat int
	if _, err := fmt.Sscanf(parts[0]+"."+parts[1]+"."+parts[2], "%d.%d.%d", &maj, &min, &pat); err != nil {
		return 0, 0, 0, false
	}
	return maj, min, pat, true
}

// evaluateReleaseAgainstInstalled evaluates whether an official GitHub release (latestVer)
// should be installed over the currently installed version (curVer).
// Returns isUpToDate (true if no install needed) and isOverride (true if latestVer overrides a source build).
func evaluateReleaseAgainstInstalled(curVer, latestVer string) (bool, bool) {
	cleanCur := strings.TrimSpace(strings.TrimPrefix(curVer, "v"))
	cleanLatest := strings.TrimSpace(strings.TrimPrefix(latestVer, "v"))

	if cleanCur == "" || cleanCur == "installed" || cleanCur == "unknown" || cleanCur == "none" {
		return false, false
	}

	curIsSource := isSourceBuildVersion(cleanCur)
	cMaj, cMin, cPat, okCur := parseTripleSemVer(cleanCur)
	lMaj, lMin, lPat, okLat := parseTripleSemVer(cleanLatest)

	if !okCur || !okLat {
		if cleanCur == cleanLatest && !curIsSource {
			return true, false
		}
		if curIsSource {
			return false, true // override non-semver source build with official release
		}
		return cleanCur == cleanLatest, false
	}

	curIsNewer := false
	equalBase := false
	latestIsNewer := false

	if cMaj != lMaj {
		curIsNewer = cMaj > lMaj
		latestIsNewer = lMaj > cMaj
	} else if cMin != lMin {
		curIsNewer = cMin > lMin
		latestIsNewer = lMin > cMin
	} else if cPat != lPat {
		curIsNewer = cPat > lPat
		latestIsNewer = lPat > cPat
	} else {
		equalBase = true
	}

	if curIsSource {
		// If installed binary is a source build (Path A), an official GitHub Release (Path B)
		// overrides it whenever the release version is greater than or equal to the base semver.
		if equalBase || latestIsNewer {
			return false, true
		}
		// If the source build is on a higher version branch (e.g. 0.0.572+xyz vs release 0.0.571), do not downgrade.
		return true, false
	}

	if equalBase {
		return true, false
	}
	if curIsNewer {
		return true, false
	}
	return false, false
}

// shouldBuildFromSourceDuringStory checks whether a tool's local source repository
// has active code modifications indicating ongoing story implementation.
func shouldBuildFromSourceDuringStory(ctx context.Context, sourceRepo string, envFn func(string) string) (bool, string) {
	if envFn == nil {
		envFn = os.Getenv
	}
	if envFn("IAZIO_AGENT_NONINTERACTIVE") == "1" {
		return false, "background service daemon prefers official release (Path B)"
	}
	if envFn("IAZIO_PREFER_RELEASE_DOWNLOAD") == "1" || envFn("AUTOPILOT_PREFER_RELEASE_DOWNLOAD") == "1" {
		return false, "release download preferred via environment"
	}
	if envFn("IAZIO_FORCE_SOURCE_BUILD") == "1" || envFn("AUTOPILOT_FORCE_SOURCE_BUILD") == "1" {
		return true, "forced via environment"
	}
	if sourceRepo == "" {
		return false, "no local source repo"
	}

	// 1. Check if git status has uncommitted tracked modifications in the tool repository
	statusCtx, statusCancel := context.WithTimeout(ctx, 5*time.Second)
	defer statusCancel()
	statusCmd := procutil.CommandContext(statusCtx, "git", "-C", sourceRepo, "status", "--porcelain", "-uno")
	statusCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := statusCmd.Output(); err == nil && len(strings.TrimSpace(string(out))) > 0 {
		return true, "uncommitted tracked code modifications in submodule"
	}

	// 2. Check if the current branch is an active feature/story branch
	branchCtx, branchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer branchCancel()
	branchCmd := procutil.CommandContext(branchCtx, "git", "-C", sourceRepo, "rev-parse", "--abbrev-ref", "HEAD")
	branchCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if bOut, bErr := branchCmd.Output(); bErr == nil {
		branch := strings.TrimSpace(string(bOut))
		if branch != "" && branch != "HEAD" && branch != "main" && branch != "master" {
			return true, fmt.Sprintf("active story branch (%s)", branch)
		}
	}

	// 3. Check if local commits exist ahead of remote
	revCtx, revCancel := context.WithTimeout(ctx, 5*time.Second)
	defer revCancel()
	revCmd := procutil.CommandContext(revCtx, "git", "-C", sourceRepo, "rev-list", "--count", "origin/main..HEAD")
	revCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if rOut, rErr := revCmd.Output(); rErr == nil {
		countStr := strings.TrimSpace(string(rOut))
		var count int
		if _, err := fmt.Sscanf(countStr, "%d", &count); err == nil && count > 0 {
			return true, fmt.Sprintf("submodule has %d unpushed commit(s) ahead of origin/main", count)
		}
	}

	return false, "submodule is clean and on default branch"
}

// isInstalledVersionNewerOrEqual returns true when curVer is a valid semver that is >= latestVer.
func isInstalledVersionNewerOrEqual(curVer, latestVer string) bool {
	upToDate, _ := evaluateReleaseAgainstInstalled(curVer, latestVer)
	return upToDate
}

func buildToolFromSourceRepo(
	ctx context.Context,
	tool ManifestToolItem,
	sourceRepo, destPath string,
	force bool,
	stdout, stderr io.Writer,
) (bool, error) {
	toolName := tool.BinaryName
	if toolName == "" {
		toolName = tool.Name
	}
	cleanToolName := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(toolName, ".exe"), ".EXE"), ".cmd")
	if cleanToolName == "" {
		cleanToolName = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(tool.Name, ".exe"), ".EXE"), ".cmd")
	}

	// 0. Sweep any orphaned ephemeral iazio-build-* worktrees or temp directories from prior interrupted runs
	pruneStaleBuildWorktrees(sourceRepo, cleanToolName)

	// 1. Run git fetch origin --prune --quiet (30s timeout, non-interactive)
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer fetchCancel()
	fetchCmd := procutil.CommandContext(fetchCtx, "git", "-C", sourceRepo, "fetch", "origin", "--prune", "--quiet")
	fetchCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if fetchErr := fetchCmd.Run(); fetchErr != nil && stderr != nil {
		fmt.Fprintf(stderr, "  [!] %s: git fetch warning: %v\n", tool.Name, fetchErr)
	}

	// 2. Resolve remote commit SHA (honoring explicit TargetVersion when pinned)
	var remoteSHA string
	pinnedTag := cleanTargetVersion(tool.TargetVersion)
	var candidateRefs []string
	if pinnedTag != "" {
		candidateRefs = []string{
			"refs/tags/" + pinnedTag,
			"refs/tags/v" + pinnedTag,
			pinnedTag,
			"v" + pinnedTag,
			"origin/" + pinnedTag,
		}
	} else {
		candidateRefs = []string{"origin/main", "origin/master"}
	}
	for _, ref := range candidateRefs {
		revCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "rev-parse", ref+"^{commit}")
		revCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		out, err := revCmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			remoteSHA = strings.TrimSpace(string(out))
			break
		}
	}
	if remoteSHA == "" && pinnedTag != "" {
		return false, fmt.Errorf("pinned target version %q not found in local source repo %s; falling back to release asset", tool.TargetVersion, sourceRepo)
	}
	if remoteSHA == "" {
		revCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "rev-parse", "HEAD")
		revCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if out, err := revCmd.Output(); err == nil && len(strings.TrimSpace(string(out))) > 0 {
			remoteSHA = strings.TrimSpace(string(out))
		}
	}
	if remoteSHA == "" {
		return false, fmt.Errorf("unable to resolve remote commit SHA for %s in %s", tool.Name, sourceRepo)
	}

	// Resolve semver tag if available in sourceRepo (or from pinnedTag)
	semVer := pinnedTag
	if semVer == "" {
		tagCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "describe", "--tags", "--abbrev=0", remoteSHA)
		tagCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if out, err := tagCmd.Output(); err == nil {
			candTag := cleanTargetVersion(strings.TrimSpace(string(out)))
			if candTag != "" {
				semVer = candTag
			}
		}
	}

	// 3. Resolve installed commit SHA from canonical binary
	installedSHA := inventory.InspectToolCommitSHA(destPath)

	// 4. Check ONLY specific tool repository tracked cleanliness
	statusCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "status", "--porcelain", "--untracked-files=no")
	statusCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	statusOut, _ := statusCmd.Output()
	repoIsClean := len(strings.TrimSpace(string(statusOut))) == 0

	// If local checkout is clean and unpinned, fast-forward local main/master to remoteSHA so local checkout stays current
	if repoIsClean && pinnedTag == "" {
		branchCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "rev-parse", "--abbrev-ref", "HEAD")
		branchCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if bOut, bErr := branchCmd.Output(); bErr == nil {
			br := strings.TrimSpace(string(bOut))
			if br == "main" || br == "master" {
				ffCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "merge", "--ff-only", remoteSHA)
				ffCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
				_ = ffCmd.Run()
			}
		}
	}

	// 5. Evaluate eager update condition
	if !force {
		if !repoIsClean {
			if stdout != nil {
				fmt.Fprintf(stdout, "  [!] %s: tool repository has uncommitted tracked changes, skipping autonomous eager update\n", tool.Name)
			}
			return false, nil
		}
		if installedSHA != "" && (installedSHA == remoteSHA || strings.HasPrefix(remoteSHA, installedSHA) || strings.HasPrefix(installedSHA, remoteSHA)) {
			short := remoteSHA
			if len(short) > 7 {
				short = short[:7]
			}
			if semVer != "" {
				expectedSidecar := semVer + "+" + short
				sidecarBytes, _ := os.ReadFile(destPath + ".version")
				if strings.TrimSpace(string(sidecarBytes)) != expectedSidecar {
					_ = os.WriteFile(destPath+".version", []byte(expectedSidecar), 0644)
					inventory.InvalidateVersionCache(destPath)
				}
			}
			if stdout != nil {
				fmt.Fprintf(stdout, "  [=] %s: %s (up-to-date commit %s)\n", tool.Name, short, short)
			}
			return false, nil
		}
	}

	// Check whether sourceRepo HEAD equals remoteSHA
	var localHeadSHA string
	if headOut, err := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "rev-parse", "HEAD").Output(); err == nil {
		localHeadSHA = strings.TrimSpace(string(headOut))
	}

	// 6. Build from remoteSHA (using isolated ephemeral worktree if force, dirty, pinned, or local HEAD != remoteSHA)
	buildDir := sourceRepo

	if force || !repoIsClean || pinnedTag != "" || localHeadSHA != remoteSHA {
		tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("iazio-build-%s-%d", cleanToolName, time.Now().UnixNano()))
		removeTempWorktree := func() {
			cleanCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			rmCmd := procutil.CommandContext(cleanCtx, "git", "-C", sourceRepo, "worktree", "remove", "--force", tempDir)
			rmCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			_ = rmCmd.Run()
			_ = os.RemoveAll(tempDir)
			pruneCmd := procutil.CommandContext(cleanCtx, "git", "-C", sourceRepo, "worktree", "prune")
			pruneCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			_ = pruneCmd.Run()
		}
		// Register unconditional cleanup before provisioning so cancelled/timed-out git worktree add or git archive never leaks tempDir or .git/worktrees entries
		defer removeTempWorktree()

		var addWorktreeSucceeded bool
		worktreeRefs := []string{remoteSHA, "origin/main", "origin/master", "HEAD"}
		if pinnedTag != "" {
			worktreeRefs = []string{remoteSHA}
		}
		for _, targetRef := range worktreeRefs {
			addCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "worktree", "add", "--force", "--detach", tempDir, targetRef)
			addCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			if err := addCmd.Run(); err == nil {
				addWorktreeSucceeded = true
				break
			}
			// Clean any partial worktree/directory created before retrying next ref or archive fallback
			removeTempWorktree()
		}
		if addWorktreeSucceeded {
			buildDir = tempDir
		} else {
			// Fallback: extract archive from remoteSHA to ensure clean ephemeral build
			if mkErr := os.MkdirAll(tempDir, 0755); mkErr == nil {
				archiveCmd := procutil.CommandContext(ctx, "git", "-C", sourceRepo, "archive", remoteSHA)
				archiveCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
				tarCmd := procutil.CommandContext(ctx, "tar", "-x", "-C", tempDir)
				if pipe, pErr := archiveCmd.StdoutPipe(); pErr == nil {
					tarCmd.Stdin = pipe
					if err := tarCmd.Start(); err == nil && archiveCmd.Start() == nil {
						_ = archiveCmd.Wait()
						if err := tarCmd.Wait(); err == nil {
							addWorktreeSucceeded = true
							buildDir = tempDir
						}
					}
				}
			}
			if !addWorktreeSucceeded {
				removeTempWorktree()
				if !repoIsClean {
					return false, fmt.Errorf("failed to create isolated worktree or archive from %s for %s; refusing to build in dirty working tree", remoteSHA, tool.Name)
				}
			}
		}
	}

	stagingBin := filepath.Join(os.TempDir(), fmt.Sprintf("staging-%s-%d", cleanToolName, time.Now().UnixNano()))
	if runtime.GOOS == "windows" {
		stagingBin += ".exe"
	}
	defer func() { _ = os.Remove(stagingBin) }()

	verStr := "0.0.1"
	if semVer != "" {
		verStr = semVer
	} else if out, err := procutil.CommandContext(ctx, "git", "-C", buildDir, "describe", "--tags", "--always").Output(); err == nil {
		verStr = strings.TrimSpace(string(out))
	}
	shortSHA := remoteSHA
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	nowUTC := time.Now().UTC().Format(time.RFC3339)
	ldflags := fmt.Sprintf("-s -w -X main.version=%s -X github.com/viovy/autopilot-plugins-internal/pkg/pluginsdk.Version=%s -X main.commit=%s -X main.buildCommitSHA=%s -X main.buildTimeUTC=%s -X main.buildDirty=false", verStr, verStr, shortSHA, shortSHA, nowUTC)

	cmdPath := ""
	candidates := []string{
		cleanToolName,
		strings.TrimPrefix(cleanToolName, "autopilot-"),
		"autopilot-" + cleanToolName,
	}
	for _, c := range candidates {
		if fi, err := os.Stat(filepath.Join(buildDir, "cmd", c)); err == nil && fi.IsDir() {
			cmdPath = "./cmd/" + c
			break
		}
	}
	if cmdPath == "" {
		hasRootGo := false
		if entries, err := os.ReadDir(buildDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
					hasRootGo = true
					break
				}
			}
		}
		if hasRootGo {
			cmdPath = "."
		} else {
			return false, fmt.Errorf("compile %s: entrypoint package not found under cmd/ in %s", tool.Name, buildDir)
		}
	}

	buildCmd := procutil.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-ldflags="+ldflags, "-o", stagingBin, cmdPath)
	buildCmd.Dir = buildDir
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		return false, fmt.Errorf("compile %s: %w: %s", tool.Name, err, string(out))
	}

	binBytes, err := os.ReadFile(stagingBin)
	if err != nil {
		return false, fmt.Errorf("read compiled binary: %w", err)
	}

	if err := installExecutableSafe(destPath, binBytes); err != nil {
		return false, fmt.Errorf("install %s: %w", tool.Name, err)
	}

	sidecarContent := shortSHA
	if semVer != "" {
		sidecarContent = semVer + "+" + shortSHA
	}
	_ = os.WriteFile(destPath+".version", []byte(sidecarContent), 0644)
	inventory.InvalidateVersionCache(destPath)

	// Restart companion daemons immediately; iazio-agent restarts itself after completing the update pass and reporting job completion
	if tool.Name == "iazio-harvester" {
		_ = service.RestartToolService(tool.Name)
	}

	// Dev binary cleanup (only when installing into a real canonical ~/.local/bin directory outside unit tests)
	destDirClean := filepath.Clean(filepath.Dir(destPath))
	if !isTestBinary() && filepath.Base(destDirClean) == "bin" && filepath.Base(filepath.Dir(destDirClean)) == ".local" {
		home := filepath.Dir(filepath.Dir(destDirClean))
		searchDirs := []string{destDirClean, filepath.Join(home, ".iazio", "bin")}
		if sourceRepo != "" {
			searchDirs = append(searchDirs, sourceRepo, filepath.Dir(sourceRepo))
		}
		candidates := inventory.ScanDevAndShadowBinaries(searchDirs, destDirClean)
		report, _ := inventory.CleanupDevBinaries(ctx, candidates, destDirClean)
		if len(report.DeletedPaths) > 0 && stdout != nil {
			fmt.Fprintf(stdout, "  [*] %s: cleaned up %d dev/shadow binaries\n", tool.Name, len(report.DeletedPaths))
		}
	}

	short := remoteSHA
	if len(short) > 7 {
		short = short[:7]
	}
	if stdout != nil {
		fmt.Fprintf(stdout, "  [+] %s: successfully updated to commit %s (%s)\n", tool.Name, short, destPath)
	}
	return true, nil
}
