package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/preflight"
)

// ProbeResult holds the health and readiness status of the agent runtime.
type ProbeResult struct {
	Status             string            `json:"status"`
	Version            string            `json:"version"`
	HostID             string            `json:"host_id"`
	OS                 string            `json:"os"`
	Arch               string            `json:"arch"`
	DiskFreeBytes      uint64            `json:"disk_free_bytes"`
	DiskMinRequired    uint64            `json:"disk_min_required_bytes"`
	DiskHealthy        bool              `json:"disk_healthy"`
	ToolsCount         int               `json:"tools_count"`
	ToolsSummary       map[string]string `json:"tools_summary,omitempty"`
	ControlPlaneURL    string            `json:"control_plane_url,omitempty"`
	ControlPlaneStatus string            `json:"control_plane_status,omitempty"`
	Error              string            `json:"error,omitempty"`
}

func runProbe(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	var jsonOutput bool
	var checkAPI bool
	var apiOverride string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json" || arg == "-json":
			jsonOutput = true
		case arg == "--check-api":
			checkAPI = true
		case arg == "--api" && i+1 < len(args):
			i++
			apiOverride = args[i]
		case strings.HasPrefix(arg, "--api="):
			apiOverride = strings.TrimPrefix(arg, "--api=")
		case arg == "--help" || arg == "-h" || arg == "help":
			fmt.Fprintln(stdout, "usage: iazio-agent probe [--json] [--check-api] [--api URL]")
			return nil
		}
	}

	hostID := resolveHostID(getenv, os.Hostname, nil)
	ver := formatVersion()

	// 1. Disk free space check
	home, _ := os.UserHomeDir()
	checkPath := "."
	if home != "" {
		checkPath = home
	}
	freeBytes, err := preflight.FreeSpace(checkPath)
	minReq := preflight.RequiredMinFreeBytes()
	diskHealthy := err == nil && freeBytes >= minReq

	// 2. Tools inventory check
	dirs := inventory.DefaultDirs(getenv("PATH"), home, runtime.GOOS)
	found := inventory.Scan(dirs)
	classified, _ := inventory.Classify(found, nil)
	toolsSummary := make(map[string]string, len(classified))
	for _, t := range classified {
		toolsSummary[t.Name] = t.Version
	}

	// 3. Optional control plane check
	api := apiOverride
	if api == "" {
		if v := strings.TrimSpace(getenv("IAZIO_AGENT_API")); v != "" {
			api = v
		} else if v := strings.TrimSpace(getenv("IAZIO_HARNESS_API_URL")); v != "" {
			api = v
		} else {
			api = resolveLocalAPI(getenv)
		}
	}

	var apiStatus string
	if checkAPI || api != "" {
		apiStatus = "UNKNOWN"
		if api != "" {
			client := &http.Client{Timeout: 3 * time.Second}
			probeURL := strings.TrimRight(api, "/") + "/healthz"
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
			if reqErr == nil {
				resp, respErr := client.Do(req)
				if respErr == nil {
					_ = resp.Body.Close()
					if resp.StatusCode >= 200 && resp.StatusCode < 300 {
						apiStatus = "OK"
					} else {
						apiStatus = fmt.Sprintf("HTTP_%d", resp.StatusCode)
					}
				} else {
					apiStatus = "UNREACHABLE"
				}
			}
		}
	}

	// Aggregate status
	status := "HEALTHY"
	var errStr string
	if !diskHealthy {
		status = "DEGRADED"
		if err != nil {
			errStr = fmt.Sprintf("disk check failed: %v", err)
		} else {
			errStr = fmt.Sprintf("disk free (%d) below required floor (%d)", freeBytes, minReq)
		}
	}
	if checkAPI && apiStatus != "OK" {
		status = "UNHEALTHY"
		if errStr != "" {
			errStr += "; "
		}
		errStr += fmt.Sprintf("control plane status: %s", apiStatus)
	}

	res := ProbeResult{
		Status:             status,
		Version:            ver,
		HostID:             hostID,
		OS:                 runtime.GOOS,
		Arch:               runtime.GOARCH,
		DiskFreeBytes:      freeBytes,
		DiskMinRequired:    minReq,
		DiskHealthy:        diskHealthy,
		ToolsCount:         len(classified),
		ToolsSummary:       toolsSummary,
		ControlPlaneURL:    api,
		ControlPlaneStatus: apiStatus,
		Error:              errStr,
	}

	if jsonOutput {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}

	fmt.Fprintf(stdout, "%s: host=%s version=%s os=%s/%s disk_free_gb=%.2f tools=%d\n",
		res.Status, res.HostID, res.Version, res.OS, res.Arch,
		float64(res.DiskFreeBytes)/(1024*1024*1024), res.ToolsCount)
	if res.ControlPlaneStatus != "" {
		fmt.Fprintf(stdout, "control_plane=%s status=%s\n", res.ControlPlaneURL, res.ControlPlaneStatus)
	}
	if errStr != "" && stderr != nil {
		fmt.Fprintf(stderr, "warning: %s\n", errStr)
	}

	if status == "UNHEALTHY" {
		return fmt.Errorf("probe failed: %s", errStr)
	}
	return nil
}
