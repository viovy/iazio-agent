package harness

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCodebaseInvoker_Interface(t *testing.T) {
	var _ CodebaseInvoker = (*CLICodebaseInvoker)(nil)
	var _ CodebaseInvoker = (*CLIInvoker)(nil)
}

func createFakeBinary(t *testing.T, scriptContent string) string {
	t.Helper()
	dir := t.TempDir()
	binPath := filepath.Join(dir, "fake-harness")
	if runtime.GOOS == "windows" {
		binPath += ".bat"
		scriptContent = "@echo off\r\n" + scriptContent
	} else {
		scriptContent = "#!/bin/sh\n" + scriptContent
	}

	if err := os.WriteFile(binPath, []byte(scriptContent), 0755); err != nil {
		t.Fatalf("failed to write fake binary: %v", err)
	}
	return binPath
}

func TestCodebaseInvoker_ListLocalRepos_Success(t *testing.T) {
	script := `
if [ "$1" = "codebase" ] && [ "$2" = "repos" ] && [ "$3" = "--json" ]; then
  cat <<'EOF'
{
  "repos": [
    {
      "name": "iazio-mcp",
      "path": "iazio-mcp",
      "default_branch": "main",
      "git_tree_sha": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
      "description": "MCP server bridging iazio-api and iazio-harness-api"
    },
    {
      "name": "iazio",
      "path": ".",
      "default_branch": "main",
      "git_tree_sha": "9f8e7d6c5b4a3210123456789abcdef012345678",
      "description": "Superproject"
    }
  ]
}
EOF
  exit 0
fi
echo "unknown args" >&2
exit 1
`
	bin := createFakeBinary(t, script)
	invoker := NewCodebaseInvoker(bin, "")

	repos, err := invoker.ListLocalRepos(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repos) != 2 {
		t.Fatalf("expected 2 repos, got %d", len(repos))
	}
	if repos[0].Name != "iazio-mcp" || repos[0].GitTreeSHA != "4b825dc642cb6eb9a060e54bf8d69288fbee4904" {
		t.Errorf("unexpected repo[0]: %+v", repos[0])
	}
	if repos[1].Name != "iazio" || repos[1].Path != "." {
		t.Errorf("unexpected repo[1]: %+v", repos[1])
	}
}

func TestCodebaseInvoker_ListLocalRepos_Failure(t *testing.T) {
	script := `
echo "fatal: git repository not found" >&2
exit 128
`
	bin := createFakeBinary(t, script)
	invoker := NewCodebaseInvoker(bin, "")

	_, err := invoker.ListLocalRepos(context.Background())
	if err == nil {
		t.Fatal("expected error on failed command, got nil")
	}
}

func TestCodebaseInvoker_ExportRepoDelta_Success(t *testing.T) {
	script := `
if [ "$1" = "codebase" ] && [ "$2" = "export" ] && [ "$3" = "--json" ]; then
  cat <<'EOF'
{
  "repo_name": "iazio-mcp",
  "base_tree_sha": "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b",
  "git_tree_sha": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
  "up_to_date": false,
  "deleted_files": ["internal/mcp/legacy_tools.go"],
  "chunks": [
    {
      "file_path": "internal/mcp/tools_codebase.go",
      "start_line": 1,
      "end_line": 64,
      "symbol": "registerCodebaseTools",
      "content": "func registerCodebaseTools() {}",
      "file_blob_sha": "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
      "git_tree_sha": "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
    }
  ]
}
EOF
  exit 0
fi
echo "unknown args" >&2
exit 1
`
	bin := createFakeBinary(t, script)
	invoker := NewCodebaseInvoker(bin, "")

	resp, err := invoker.ExportRepoDelta(context.Background(), ExportRequest{
		RepoName:    "iazio-mcp",
		BaseTreeSHA: "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b",
		MaxChunks:   200,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RepoName != "iazio-mcp" {
		t.Errorf("expected repo_name iazio-mcp, got %s", resp.RepoName)
	}
	if len(resp.DeletedFiles) != 1 || resp.DeletedFiles[0] != "internal/mcp/legacy_tools.go" {
		t.Errorf("unexpected deleted_files: %v", resp.DeletedFiles)
	}
	if len(resp.Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(resp.Chunks))
	}
	chunk := resp.Chunks[0]
	if chunk.FilePath != "internal/mcp/tools_codebase.go" || chunk.Symbol != "registerCodebaseTools" {
		t.Errorf("unexpected chunk: %+v", chunk)
	}
}

func TestCodebaseInvoker_ExportRepoDelta_Validation(t *testing.T) {
	invoker := NewCodebaseInvoker("iazio-harness", "")
	_, err := invoker.ExportRepoDelta(context.Background(), ExportRequest{
		RepoName: "",
	})
	if err == nil {
		t.Fatal("expected error for empty repo_name, got nil")
	}
}

func TestCodebaseInvoker_ExportRepoDelta_CommandError(t *testing.T) {
	script := `
echo "fatal: base tree does not exist" >&2
exit 1
`
	bin := createFakeBinary(t, script)
	invoker := NewCodebaseInvoker(bin, "")

	_, err := invoker.ExportRepoDelta(context.Background(), ExportRequest{
		RepoName: "iazio-mcp",
	})
	if err == nil {
		t.Fatal("expected error on command failure, got nil")
	}
}
