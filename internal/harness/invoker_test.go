package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
				StoryID:      "STORY-IAZIO-0131",
				Submodule:    sub,
				StoryPath:    "docs/stories/2026-10-19-test.story.md",
				IndexUpdated: true,
			}
			_ = json.NewEncoder(os.Stdout).Encode(res)
			return
		}
		os.Exit(2)
	case "run":
		if len(args) >= 2 && args[1] == "--json" {
			var payload map[string]any
			_ = json.NewDecoder(os.Stdin).Decode(&payload)
			_ = json.NewEncoder(os.Stdout).Encode(PromptExecutionResult{
				Status: "completed",
				Output: "done",
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

	if res.StoryID != "STORY-IAZIO-0131" {
		t.Errorf("expected story_id STORY-IAZIO-0131, got %s", res.StoryID)
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
