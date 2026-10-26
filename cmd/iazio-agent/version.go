package main

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"time"
)

// version is the semantic version stamped at link time.
var version = "dev"

// commit is the source revision stamped at link time.
var commit = "unknown"

// branch is the source branch stamped at link time.
var branch = "unknown"

// Build metadata stamped at link time via -ldflags.
var (
	buildCommitSHA = ""
	buildTimeUTC   = ""
	buildDirty     = ""
)

func resolveCommit() string {
	if c := strings.TrimSpace(buildCommitSHA); c != "" && c != "unknown" {
		return c
	}
	if c := strings.TrimSpace(commit); c != "" && c != "unknown" {
		return c
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && strings.TrimSpace(s.Value) != "" {
				return s.Value
			}
		}
	}
	return "unknown"
}

func resolveBuildTime() string {
	if t := strings.TrimSpace(buildTimeUTC); t != "" {
		return t
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.time" && strings.TrimSpace(s.Value) != "" {
				return s.Value
			}
		}
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func resolveDirty() bool {
	if d := strings.TrimSpace(buildDirty); d != "" {
		return d == "true" || d == "1"
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.modified" {
				return s.Value == "true" || s.Value == "1"
			}
		}
	}
	return false
}

// VersionInfo models structured binary version telemetry.
type VersionInfo struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	CommitSHA string `json:"commit_sha"`
	BuildTime string `json:"build_time,omitempty"`
	Dirty     bool   `json:"dirty"`
}

func formatVersionJSON() string {
	info := VersionInfo{
		Name:      "iazio-agent",
		Version:   version,
		CommitSHA: resolveCommit(),
		BuildTime: resolveBuildTime(),
		Dirty:     resolveDirty(),
	}
	data, _ := json.Marshal(info)
	return string(data)
}

func formatVersion() string {
	if sha, ok := shortCommit(resolveCommit()); ok {
		return "iazio-agent " + version + "+" + sha
	}
	return "iazio-agent " + version
}

func shortCommit(value string) (string, bool) {
	c := strings.TrimSpace(value)
	if c == "" || c == "unknown" {
		return "", false
	}
	if len(c) > 7 {
		c = c[:7]
	}
	return c, true
}

func branchName() string {
	if branch == "" {
		return "unknown"
	}
	return branch
}
