package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHelperProcess is used as a mock subprocess for iazio-harness CLI tests.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	defer os.Exit(0)

	args := os.Args
	for len(args) > 0 {
		if args[0] == "--" {
			args = args[1:]
			break
		}
		args = args[1:]
	}

	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "no arguments passed to helper")
		os.Exit(2)
	}

	cmd := args[0]
	switch cmd {
	case "env":
		fmt.Print(os.Getenv("IAZIO_FLEET_ROOT"))
		return
	case "story":
		if len(args) >= 3 && args[1] == "create" && args[2] == "--json" {
			// Read stdin
			var payload map[string]any
			if err := json.NewDecoder(os.Stdin).Decode(&payload); err != nil {
				fmt.Fprintf(os.Stderr, "invalid stdin JSON: %v\n", err)
				os.Exit(1)
			}
			sub, _ := payload["submodule"].(string)
			if sub == "fail-repo" {
				fmt.Fprintln(os.Stderr, "validation error: fail-repo disallowed")
				os.Exit(1)
			}
			res := StoryExecutionResult{
				StoryID:      "STORY-MOCK-001",
				Submodule:    sub,
				StoryPath:    "docs/stories/2026-10-19-test.story.md",
				IndexUpdated: true,
			}
			_ = json.NewEncoder(os.Stdout).Encode(res)
			return
		}
		os.Exit(2)
	case "prompt":
		if len(args) >= 3 && args[1] == "run" && args[2] == "--json" {
			var payload map[string]any
			_ = json.NewDecoder(os.Stdin).Decode(&payload)
			_ = json.NewEncoder(os.Stdout).Encode(PromptExecutionResult{
				Status:  "completed",
				Summary: "done",
				Output:  "done",
			})
			return
		}
		os.Exit(2)
	case "run":
		if len(args) >= 2 && args[1] == "--json" {
			var payload map[string]any
			_ = json.NewDecoder(os.Stdin).Decode(&payload)
			_ = json.NewEncoder(os.Stdout).Encode(PromptExecutionResult{
				Status:  "completed",
				Summary: "done",
				Output:  "done",
			})
			return
		}
		os.Exit(2)
	case "codebase":
		if len(args) >= 3 && args[1] == "repos" && args[2] == "--json" {
			_ = json.NewEncoder(os.Stdout).Encode(CodebaseReposResult{
				Repos: []CodebaseRepoInfo{
					{
						Name:          "iazio-mcp",
						Path:          "iazio-mcp",
						DefaultBranch: "main",
						GitTreeSHA:    "tree-123",
						Description:   "mock mcp",
					},
				},
			})
			return
		}
		if len(args) >= 3 && args[1] == "export" && args[2] == "--json" {
			var payload map[string]any
			if err := json.NewDecoder(os.Stdin).Decode(&payload); err != nil {
				fmt.Fprintf(os.Stderr, "invalid stdin JSON: %v\n", err)
				os.Exit(1)
			}
			repo, _ := payload["repo_name"].(string)
			_ = json.NewEncoder(os.Stdout).Encode(CodebaseExportResult{
				RepoName:     repo,
				GitTreeSHA:   "tree-456",
				UpToDate:     true,
				DeletedFiles: []string{},
				Chunks:       []CodebaseChunk{},
			})
			return
		}
		os.Exit(2)
	default:
		os.Exit(2)
	}
}

func makeFakeInvoker(t *testing.T) *CLIInvoker {
	t.Helper()
	invoker := NewCLIInvoker(os.Args[0], "")
	return invoker
}

func TestCLIInvoker_CreateStory_Success(t *testing.T) {
	inv := makeFakeInvoker(t)
	// Point to this test binary with GO_WANT_HELPER_PROCESS=1
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")

	// Wrap command execution
	origBinary := inv.HarnessBinary
	inv.HarnessBinary = os.Args[0]
	// Inject fake command arguments into custom runCommand or using args
	// Since invoker calls exec.CommandContext(ctx, c.HarnessBinary, args...),
	// if HarnessBinary is os.Args[0], it will run TestHelperProcess!
	// But we need the helper flag. Let's create a temporary executable shell script on unix.
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin
	defer func() { inv.HarnessBinary = origBinary }()

	payload := json.RawMessage(`{"submodule":"iazio-mcp","title":"Test Story"}`)
	res, err := inv.CreateStory(context.Background(), payload)
	if err != nil {
		t.Fatalf("unexpected CreateStory error: %v", err)
	}

	if res.StoryID != "STORY-MOCK-001" {
		t.Errorf("expected story_id STORY-MOCK-001, got %s", res.StoryID)
	}
	if res.Submodule != "iazio-mcp" {
		t.Errorf("expected submodule iazio-mcp, got %s", res.Submodule)
	}
	if !res.IndexUpdated {
		t.Error("expected index_updated=true")
	}
}

func TestCLIInvoker_CreateStory_Failure(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	payload := json.RawMessage(`{"submodule":"fail-repo","title":"Test Story"}`)
	_, err := inv.CreateStory(context.Background(), payload)
	if err == nil {
		t.Fatal("expected CreateStory error for fail-repo, got nil")
	}
	if !strings.Contains(err.Error(), "validation error: fail-repo disallowed") {
		t.Errorf("expected error to contain stderr details, got %v", err)
	}
}

func TestCLIInvoker_Timeout(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness-sleep"
	script := "#!/bin/sh\nsleep 1\n"
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	payload := json.RawMessage(`{"submodule":"iazio-mcp"}`)
	_, err := inv.CreateStory(ctx, payload)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestCLIInvoker_ExecutePrompt_Success(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	payload := json.RawMessage(`{"repo":"iazio-mcp","prompt":"Refactor test code"}`)
	res, err := inv.ExecutePrompt(context.Background(), payload)
	if err != nil {
		t.Fatalf("unexpected ExecutePrompt error: %v", err)
	}

	if res.Status != "completed" {
		t.Errorf("expected status completed, got %s", res.Status)
	}
	if res.Summary != "done" {
		t.Errorf("expected summary done, got %s", res.Summary)
	}
	if res.Output != "done" {
		t.Errorf("expected output done, got %s", res.Output)
	}
}

func TestCLIInvoker_ListRepos_Success(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	res, err := inv.ListRepos(context.Background())
	if err != nil {
		t.Fatalf("unexpected ListRepos error: %v", err)
	}

	if len(res.Repos) != 1 {
		t.Fatalf("expected 1 repo, got %d", len(res.Repos))
	}
	if res.Repos[0].Name != "iazio-mcp" {
		t.Errorf("expected repo name iazio-mcp, got %s", res.Repos[0].Name)
	}
	if res.Repos[0].GitTreeSHA != "tree-123" {
		t.Errorf("expected git_tree_sha tree-123, got %s", res.Repos[0].GitTreeSHA)
	}
}

func TestCLIInvoker_ExportCodebase_Success(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	payload := json.RawMessage(`{"repo_name":"iazio-mcp","base_tree_sha":"tree-000"}`)
	res, err := inv.ExportCodebase(context.Background(), payload)
	if err != nil {
		t.Fatalf("unexpected ExportCodebase error: %v", err)
	}

	if res.RepoName != "iazio-mcp" {
		t.Errorf("expected repo_name iazio-mcp, got %s", res.RepoName)
	}
	if !res.UpToDate {
		t.Error("expected up_to_date=true")
	}
}

func TestCLIInvoker_ListLocalRepos_And_ExportRepoDelta(t *testing.T) {
	inv := makeFakeInvoker(t)
	tmpDir := t.TempDir()
	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}
	inv.HarnessBinary = fakeBin

	repos, err := inv.ListLocalRepos(context.Background())
	if err != nil {
		t.Fatalf("unexpected ListLocalRepos error: %v", err)
	}
	if len(repos) != 1 || repos[0].Name != "iazio-mcp" {
		t.Fatalf("unexpected repos: %+v", repos)
	}

	exp, err := inv.ExportRepoDelta(context.Background(), ExportRequest{
		RepoName:    "iazio-mcp",
		BaseTreeSHA: "tree-000",
	})
	if err != nil {
		t.Fatalf("unexpected ExportRepoDelta error: %v", err)
	}
	if exp.RepoName != "iazio-mcp" || !exp.UpToDate {
		t.Fatalf("unexpected export delta: %+v", exp)
	}
}

func TestNewInvokers_EnvDefaults(t *testing.T) {
	t.Setenv("IAZIO_HARNESS_BIN", "/custom/bin/iazio-harness")
	inv := NewCLIInvoker("", "/tmp/work")
	if inv.HarnessBinary != "/custom/bin/iazio-harness" {
		t.Fatalf("expected env binary, got %s", inv.HarnessBinary)
	}

	t.Setenv("IAZIO_HARNESS_BIN", "")
	invDef := NewCLIInvoker("", "")
	if invDef.HarnessBinary != "iazio-harness" {
		t.Fatalf("expected default binary iazio-harness, got %s", invDef.HarnessBinary)
	}

	t.Setenv("IAZIO_HARNESS_BIN", "/custom/bin/harness")
	ci := NewCodebaseInvoker("", "")
	if ci.HarnessBinary != "/custom/bin/harness" {
		t.Fatalf("expected env binary for codebase invoker, got %s", ci.HarnessBinary)
	}
}

func TestCLIInvoker_PayloadValidationAndPlainTextFallback(t *testing.T) {
	ctx := context.Background()
	inv := &CLIInvoker{HarnessBinary: "/bin/echo"}

	// 1. Empty payload checks
	if _, err := inv.CreateStory(ctx, nil); err == nil || !strings.Contains(err.Error(), "empty payload") {
		t.Fatalf("expected empty payload error for CreateStory: %v", err)
	}
	if _, err := inv.ExecutePrompt(ctx, []byte{}); err == nil || !strings.Contains(err.Error(), "empty payload") {
		t.Fatalf("expected empty payload error for ExecutePrompt: %v", err)
	}
	if _, err := inv.ExportCodebase(ctx, nil); err == nil || !strings.Contains(err.Error(), "empty payload") {
		t.Fatalf("expected empty payload error for ExportCodebase: %v", err)
	}

	// 2. ExecutePrompt plain text fallback (echo outputs non-JSON plain text)
	res, err := inv.ExecutePrompt(ctx, json.RawMessage(`{"prompt":"hi"}`))
	if err != nil {
		t.Fatalf("unexpected error on plain text fallback: %v", err)
	}
	if res.Status != "completed" {
		t.Fatalf("expected status completed, got: %s", res.Status)
	}

	// 3. Binary execution failure
	badInv := &CLIInvoker{HarnessBinary: "/nonexistent/path/for/harness"}
	if _, err := badInv.CreateStory(ctx, json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected error on bad binary for CreateStory")
	}
	if _, err := badInv.ExecutePrompt(ctx, json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected error on bad binary for ExecutePrompt")
	}
	if _, err := badInv.ListRepos(ctx); err == nil {
		t.Fatal("expected error on bad binary for ListRepos")
	}
	if _, err := badInv.ExportCodebase(ctx, json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected error on bad binary for ExportCodebase")
	}
}

func TestEnsurePromptPayload(t *testing.T) {
	// 1. Existing prompt_id remains unchanged
	raw1 := json.RawMessage(`{"prompt_id":"existing-123","prompt":"test"}`)
	out1 := EnsurePromptPayload("job-999", raw1)
	var m1 map[string]any
	if err := json.Unmarshal(out1, &m1); err != nil {
		t.Fatalf("failed unmarshaling out1: %v", err)
	}
	if m1["prompt_id"] != "existing-123" {
		t.Fatalf("expected existing-123, got %v", m1["prompt_id"])
	}

	// 2. Missing prompt_id, has story_id -> prompt-<story_id>
	raw2 := json.RawMessage(`{"story_id":"STORY-ABC","prompt":"test"}`)
	out2 := EnsurePromptPayload("job-999", raw2)
	var m2 map[string]any
	if err := json.Unmarshal(out2, &m2); err != nil {
		t.Fatalf("failed unmarshaling out2: %v", err)
	}
	if m2["prompt_id"] != "prompt-STORY-ABC" {
		t.Fatalf("expected prompt-STORY-ABC, got %v", m2["prompt_id"])
	}

	// 3. Missing prompt_id, missing story_id, has jobID -> prompt-<jobID>
	raw3 := json.RawMessage(`{"prompt":"test"}`)
	out3 := EnsurePromptPayload("job-999", raw3)
	var m3 map[string]any
	if err := json.Unmarshal(out3, &m3); err != nil {
		t.Fatalf("failed unmarshaling out3: %v", err)
	}
	if m3["prompt_id"] != "prompt-job-999" {
		t.Fatalf("expected prompt-job-999, got %v", m3["prompt_id"])
	}

	// 4. Missing prompt_id, story_id, and jobID -> auto-generated prompt-<timestamp>
	raw4 := json.RawMessage(`{"prompt":"test"}`)
	out4 := EnsurePromptPayload("", raw4)
	var m4 map[string]any
	if err := json.Unmarshal(out4, &m4); err != nil {
		t.Fatalf("failed unmarshaling out4: %v", err)
	}
	pID, ok := m4["prompt_id"].(string)
	if !ok || !strings.HasPrefix(pID, "prompt-") {
		t.Fatalf("expected prefix prompt-, got %v", m4["prompt_id"])
	}

	// 5. Empty or invalid JSON
	if string(EnsurePromptPayload("j", nil)) != "" {
		t.Fatal("expected empty for nil")
	}
	if string(EnsurePromptPayload("j", json.RawMessage(`invalid-json`))) != "invalid-json" {
		t.Fatal("expected original raw message for invalid json")
	}
}

func TestCLIInvoker_FleetRootPropagation(t *testing.T) {
	tmpDir := t.TempDir()
	fleetDir := filepath.Join(tmpDir, "sample-fleet")
	childDir := filepath.Join(fleetDir, "components", "child-module")
	_ = os.MkdirAll(childDir, 0755)
	_ = os.WriteFile(filepath.Join(fleetDir, ".gitmodules"), []byte("submodule"), 0644)

	fakeBin := tmpDir + "/fake-harness"
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_HELPER_PROCESS=1\nexec %s -test.run=TestHelperProcess -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(fakeBin, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write fake harness script: %v", err)
	}

	inv := NewCLIInvoker(fakeBin, childDir)
	res, _, err := inv.runCommand(context.Background(), []string{"env"}, nil)
	if err != nil {
		t.Fatalf("runCommand failed: %v", err)
	}
	if strings.TrimSpace(res) != fleetDir {
		t.Fatalf("expected fleetDir %s, got %s", fleetDir, strings.TrimSpace(res))
	}
}

func TestCLIInvoker_WithWorkDir(t *testing.T) {
	inv := NewCLIInvoker("iazio-harness", "/tmp/old")
	inv2 := inv.WithWorkDir("/tmp/new")
	if inv2 == nil {
		t.Fatalf("expected non-nil clone")
	}
}

func TestCLIInvoker_StreamingStoryAllocationCallback(t *testing.T) {
	var allocated string
	inv := NewCLIInvoker("/bin/sh", t.TempDir())
	callbackInv := inv.WithStoryCallback(func(sid string) {
		allocated = sid
	})

	// Invoke script that outputs story allocation token
	payload := json.RawMessage(`{"prompt":"test"}`)
	_, _, err := callbackInv.(*CLIInvoker).runCommand(context.Background(), []string{"-c", "echo 'ALLOCATED_STORY_ID: STORY-STREAM-123'"}, payload)
	if err != nil {
		t.Fatalf("runCommand failed: %v", err)
	}

	if allocated != "STORY-STREAM-123" {
		t.Fatalf("expected allocated story STORY-STREAM-123, got %q", allocated)
	}
}
