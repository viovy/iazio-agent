package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
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

func TestExecuteCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, []string{"version"}, &stdout, &stderr, func(string) string { return "" }); err != nil || !strings.Contains(stdout.String(), "iazio-agent ") {
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
