// Package harness provides invokers for delegating tasks to the local iazio-harness runtime.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// RepoManifest describes a submodule or superproject repository returned by codebase repos.
type RepoManifest struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	DefaultBranch string `json:"default_branch"`
	GitTreeSHA    string `json:"git_tree_sha"`
	Description   string `json:"description,omitempty"`
}

// CodeChunk describes a source code chunk emitted by codebase export.
type CodeChunk struct {
	RepoName    string `json:"repo_name,omitempty"`
	FilePath    string `json:"file_path"`
	StartLine   int    `json:"start_line"`
	EndLine     int    `json:"end_line"`
	Symbol      string `json:"symbol,omitempty"`
	Content     string `json:"content"`
	FileBlobSHA string `json:"file_blob_sha"`
	GitTreeSHA  string `json:"git_tree_sha"`
}

// ExportRequest specifies parameters for incremental codebase chunk export.
type ExportRequest struct {
	RepoName    string `json:"repo_name"`
	BaseTreeSHA string `json:"base_tree_sha,omitempty"`
	MaxChunks   int    `json:"max_chunks,omitempty"`
}

// ExportResponse contains the export chunks and deleted files metadata.
type ExportResponse struct {
	RepoName     string      `json:"repo_name"`
	BaseTreeSHA  string      `json:"base_tree_sha"`
	GitTreeSHA   string      `json:"git_tree_sha"`
	UpToDate     bool        `json:"up_to_date"`
	DeletedFiles []string    `json:"deleted_files"`
	Chunks       []CodeChunk `json:"chunks"`
}

// CodebaseInvoker defines the contract for invoking iazio-harness codebase operations.
// iazio-agent must not directly run git commands or parse .gitmodules.
type CodebaseInvoker interface {
	ListLocalRepos(ctx context.Context) ([]RepoManifest, error)
	ExportRepoDelta(ctx context.Context, req ExportRequest) (*ExportResponse, error)
}

// CLICodebaseInvoker delegates codebase discovery and delta exports to iazio-harness CLI.
type CLICodebaseInvoker struct {
	HarnessBinary string
	WorkDir       string
}

// NewCodebaseInvoker creates a CLICodebaseInvoker with binary path resolution.
func NewCodebaseInvoker(binaryPath, workDir string) *CLICodebaseInvoker {
	if binaryPath == "" {
		if env := os.Getenv("IAZIO_HARNESS_BIN"); env != "" {
			binaryPath = env
		} else {
			binaryPath = "iazio-harness"
		}
	}
	return &CLICodebaseInvoker{
		HarnessBinary: binaryPath,
		WorkDir:       workDir,
	}
}

// ListLocalRepos runs `iazio-harness codebase repos --json` and parses the output into []RepoManifest.
func (c *CLICodebaseInvoker) ListLocalRepos(ctx context.Context) ([]RepoManifest, error) {
	cmd := exec.CommandContext(ctx, c.HarnessBinary, "codebase", "repos", "--json")
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("iazio-harness codebase repos failed: %w (stderr: %s)", err, strings.TrimSpace(stderrBuf.String()))
	}

	var raw struct {
		Repos []RepoManifest `json:"repos"`
	}
	if err := json.Unmarshal(stdoutBuf.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse iazio-harness codebase repos output: %w", err)
	}

	return raw.Repos, nil
}

// ExportRepoDelta runs `iazio-harness codebase export --json` piping the request over stdin and parsing the response.
func (c *CLICodebaseInvoker) ExportRepoDelta(ctx context.Context, req ExportRequest) (*ExportResponse, error) {
	if req.RepoName == "" {
		return nil, errors.New("repo_name must not be empty for export")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal export request: %w", err)
	}

	cmd := exec.CommandContext(ctx, c.HarnessBinary, "codebase", "export", "--json")
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}
	cmd.Stdin = bytes.NewReader(payload)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("iazio-harness codebase export failed: %w (stderr: %s)", err, strings.TrimSpace(stderrBuf.String()))
	}

	var resp ExportResponse
	if err := json.Unmarshal(stdoutBuf.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse iazio-harness codebase export output: %w", err)
	}

	return &resp, nil
}
