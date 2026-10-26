package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ManifestTool describes a managed tool in the fleet manifest.
type ManifestTool struct {
	Name                   string `json:"name"`
	BinaryName             string `json:"binary_name"`
	TargetVersion          string `json:"target_version"`
	LatestAvailableVersion string `json:"latest_available_version"`
	Tier                   string `json:"tier"`
}

// ManifestRoots describes installation directories in the fleet manifest.
type ManifestRoots struct {
	Unix struct {
		Primary  string `json:"primary"`
		Fallback string `json:"fallback"`
	} `json:"unix"`
	Windows struct {
		Primary  string `json:"primary"`
		Fallback string `json:"fallback"`
	} `json:"windows"`
}

// FleetManifest is the manifest payload fetched from the fleet endpoint.
type FleetManifest struct {
	Tools        []ManifestTool `json:"tools"`
	InstallRoots ManifestRoots  `json:"install_roots"`
}

// DriftFetcher abstracts manifest retrieval for drift inspection.
type DriftFetcher interface {
	FetchManifest(ctx context.Context) (*FleetManifest, error)
}

// HTTPDriftFetcher implements DriftFetcher by querying the HTTP control plane endpoint.
type HTTPDriftFetcher struct {
	Client  *http.Client
	API     string
	Profile string
	OS      string
	Arch    string

	mu        sync.Mutex
	cached    *FleetManifest
	expiresAt time.Time
}

// FetchManifest retrieves the latest manifest from the API endpoint.
// It caches results for 10 seconds to avoid redundant round-trips within the same pass (S5).
func (h *HTTPDriftFetcher) FetchManifest(ctx context.Context) (*FleetManifest, error) {
	h.mu.Lock()
	if h.cached != nil && time.Now().Before(h.expiresAt) {
		m := h.cached
		h.mu.Unlock()
		return m, nil
	}
	h.mu.Unlock()

	if h.API == "" {
		return nil, fmt.Errorf("control plane API URL is empty")
	}
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	targetOS := h.OS
	if targetOS == "" {
		targetOS = runtime.GOOS
	}
	targetArch := h.Arch
	if targetArch == "" {
		targetArch = runtime.GOARCH
	}
	profile := h.Profile
	if profile == "" {
		profile = "generic"
	}

	manifestURL := fmt.Sprintf("%s/v1/fleet/manifest?profile=%s&os=%s&arch=%s",
		strings.TrimRight(h.API, "/"), profile, targetOS, targetArch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create manifest request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute manifest request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest request returned status %d", resp.StatusCode)
	}

	var m FleetManifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest JSON: %w", err)
	}

	h.mu.Lock()
	h.cached = &m
	h.expiresAt = time.Now().Add(10 * time.Second)
	h.mu.Unlock()

	return &m, nil
}

// NormalizeVersion strips leading 'v' / 'V' and trims whitespace for strict SemVer comparison (F4).
func NormalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	v = strings.TrimPrefix(v, "V")
	return strings.TrimSpace(v)
}

// EvaluateDrift checks whether any of targetTools (or all manifest tools if targetTools is empty)
// have drifted from their expected versions.
func EvaluateDrift(
	manifest *FleetManifest,
	inspectVersion func(path string) string,
	installDir string,
	targetTools []string,
	targetOS string,
) (bool, error) {
	if manifest == nil {
		return false, fmt.Errorf("nil manifest provided")
	}
	if inspectVersion == nil {
		return false, fmt.Errorf("nil inspectVersion function provided")
	}

	targetMap := make(map[string]bool)
	for _, name := range targetTools {
		targetMap[strings.ToLower(strings.TrimSpace(name))] = true
	}

	for _, tool := range manifest.Tools {
		if len(targetMap) > 0 && !targetMap[strings.ToLower(tool.Name)] && !targetMap[strings.ToLower(tool.BinaryName)] {
			continue
		}

		binName := tool.BinaryName
		if binName == "" {
			binName = tool.Name
		}
		if targetOS == "windows" && !strings.HasSuffix(strings.ToLower(binName), ".exe") {
			binName += ".exe"
		}

		destPath := filepath.Join(installDir, binName)
		curVer := inspectVersion(destPath)
		// Missing binary represents immediate drift
		if curVer == "" {
			return true, nil
		}

		targetVer := tool.TargetVersion
		// If target version is empty or "latest", resolve from LatestAvailableVersion (F3)
		if targetVer == "" || targetVer == "latest" {
			if tool.LatestAvailableVersion != "" {
				targetVer = tool.LatestAvailableVersion
			} else {
				// Target is unpinned latest and upstream hasn't published a concrete string: treat as drift-unknown -> update
				return true, nil
			}
		}

		cleanCur := NormalizeVersion(curVer)
		cleanTgt := NormalizeVersion(targetVer)

		// Semver "dev" trap: unversioned or dev builds must always be considered drifted
		// so that autonomous eager update inspects remote repository/releases and doesn't get stuck.
		if strings.EqualFold(cleanCur, "dev") || strings.EqualFold(cleanTgt, "dev") ||
			strings.EqualFold(cleanCur, "0.0.0-dev") || strings.EqualFold(cleanTgt, "0.0.0-dev") {
			return true, nil
		}

		// Strict normalized equality comparison (F4)
		if cleanCur != cleanTgt {
			return true, nil
		}
	}

	return false, nil
}

// CheckDrift inspects drift against a DriftFetcher, returning (false, err) on fetch/parse failure
// to enable the caller to fail-open (F3).
func CheckDrift(
	ctx context.Context,
	fetcher DriftFetcher,
	inspectVersion func(path string) string,
	installDir string,
	targetTools []string,
	targetOS string,
) (bool, error) {
	if fetcher == nil {
		return false, nil
	}
	manifest, err := fetcher.FetchManifest(ctx)
	if err != nil {
		return false, err
	}
	return EvaluateDrift(manifest, inspectVersion, installDir, targetTools, targetOS)
}
