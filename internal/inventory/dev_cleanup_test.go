package inventory

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestScanDevAndShadowBinaries(t *testing.T) {
	tmpDir := t.TempDir()

	canonicalDir := filepath.Join(tmpDir, "canonical")
	shadowDir := filepath.Join(tmpDir, "shadow")
	repoDir := filepath.Join(tmpDir, "sample-repo", "iazio-agent")

	if err := os.MkdirAll(canonicalDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(shadowDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		t.Fatal(err)
	}

	createExecutable := func(path string) {
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho ok\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}

	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}

	// 1. Canonical binary (must NOT be marked as dev/shadow)
	createExecutable(filepath.Join(canonicalDir, "iazio-agent"+ext))

	// 2. Dev naming variants in canonicalDir
	createExecutable(filepath.Join(canonicalDir, "iazio-agent-dev"+ext))
	createExecutable(filepath.Join(canonicalDir, "iazio-agent.old"+ext))
	createExecutable(filepath.Join(canonicalDir, "iazio-agent.bak"+ext))
	createExecutable(filepath.Join(canonicalDir, "iazio-harvester-darwin-arm64"+ext))
	createExecutable(filepath.Join(canonicalDir, ".iazio-agent.new.12345"))

	// 3. Shadow duplicate in shadowDir
	createExecutable(filepath.Join(shadowDir, "iazio-agent"+ext))
	createExecutable(filepath.Join(shadowDir, "iazio-mcp-old"+ext))

	// 4. Stale build in repository
	createExecutable(filepath.Join(repoDir, "iazio-agent"+ext))

	searchDirs := []string{canonicalDir, shadowDir, repoDir}
	candidates := ScanDevAndShadowBinaries(searchDirs, canonicalDir)

	candidatePaths := make(map[string]DevBinaryCandidate)
	for _, c := range candidates {
		candidatePaths[c.Path] = c
	}

	// Canonical binary must NOT be in candidates
	canonicalPath := filepath.Join(canonicalDir, "iazio-agent"+ext)
	if _, found := candidatePaths[canonicalPath]; found {
		t.Fatalf("canonical binary %s was falsely flagged as candidate", canonicalPath)
	}

	// Expected candidates
	expected := []string{
		filepath.Join(canonicalDir, "iazio-agent-dev"+ext),
		filepath.Join(canonicalDir, "iazio-agent.old"+ext),
		filepath.Join(canonicalDir, "iazio-agent.bak"+ext),
		filepath.Join(canonicalDir, "iazio-harvester-darwin-arm64"+ext),
		filepath.Join(canonicalDir, ".iazio-agent.new.12345"),
		filepath.Join(shadowDir, "iazio-agent"+ext),
		filepath.Join(shadowDir, "iazio-mcp-old"+ext),
		filepath.Join(repoDir, "iazio-agent"+ext),
	}

	for _, exp := range expected {
		if _, found := candidatePaths[exp]; !found {
			t.Errorf("expected candidate %s not found in results: %+v", exp, candidatePaths)
		}
	}
}

func TestCleanupDevBinaries(t *testing.T) {
	tmpDir := t.TempDir()
	canonicalDir := filepath.Join(tmpDir, "bin")
	_ = os.MkdirAll(canonicalDir, 0755)

	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}

	canonicalPath := filepath.Join(canonicalDir, "iazio-agent"+ext)
	devPath1 := filepath.Join(canonicalDir, "iazio-agent-dev"+ext)
	devPath2 := filepath.Join(canonicalDir, "iazio-agent.old"+ext)

	_ = os.WriteFile(canonicalPath, []byte("canonical"), 0755)
	_ = os.WriteFile(devPath1, []byte("dev1"), 0755)
	_ = os.WriteFile(devPath2, []byte("dev2"), 0755)
	_ = os.WriteFile(devPath1+".version", []byte("dev"), 0644)

	candidates := []DevBinaryCandidate{
		{Path: devPath1, ToolName: "iazio-agent", VariantType: "dev_suffix"},
		{Path: devPath2, ToolName: "iazio-agent", VariantType: "staging_extension"},
	}

	report, err := CleanupDevBinaries(context.Background(), candidates, canonicalDir)
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	if len(report.DeletedPaths) != 2 {
		t.Errorf("expected 2 deleted paths, got %d: %+v", len(report.DeletedPaths), report.DeletedPaths)
	}

	// Canonical binary must still exist
	if _, err := os.Stat(canonicalPath); err != nil {
		t.Errorf("canonical binary was unexpectedly deleted: %v", err)
	}

	// Dev binaries must be removed
	if _, err := os.Stat(devPath1); !os.IsNotExist(err) {
		t.Errorf("devPath1 was not deleted")
	}
	if _, err := os.Stat(devPath2); !os.IsNotExist(err) {
		t.Errorf("devPath2 was not deleted")
	}
	if _, err := os.Stat(devPath1 + ".version"); !os.IsNotExist(err) {
		t.Errorf("version sidecar was not deleted")
	}
}

func TestInspectToolCommitSHA_Fallback(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "test-tool")
	_ = os.WriteFile(binPath, []byte("binary-content"), 0755)

	sha := InspectToolCommitSHA(binPath)
	if sha == "" {
		t.Errorf("expected non-empty commit sha or fingerprint fallback, got empty")
	}
}
