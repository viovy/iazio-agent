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
	"time"
)

const defaultGracePeriod = 2 * time.Second

// StoryExecutionResult represents the output from iazio-harness story create.
type StoryExecutionResult struct {
	StoryID      string `json:"story_id"`
	Submodule    string `json:"submodule"`
	StoryPath    string `json:"story_path"`
	IndexUpdated bool   `json:"index_updated"`
}

// PromptExecutionResult represents the output from iazio-harness prompt execution.
type PromptExecutionResult struct {
	PromptID      string   `json:"prompt_id,omitempty"`
	Repo          string   `json:"repo,omitempty"`
	TargetAgent   string   `json:"target_agent,omitempty"`
	Status        string   `json:"status"`
	ExitCode      int      `json:"exit_code,omitempty"`
	GitHeadSHA    string   `json:"git_head_sha,omitempty"`
	GitTreeSHA    string   `json:"git_tree_sha,omitempty"`
	ModifiedFiles []string `json:"modified_files,omitempty"`
	Summary       string   `json:"summary,omitempty"`
	DurationMs    int64    `json:"duration_ms,omitempty"`
	Output        string   `json:"output,omitempty"`
}

// Invoker defines the contract for invoking iazio-harness on the host.
// Direct calls to autopilot or edits to docs/stories/index.yml are strictly prohibited in iazio-agent.
type Invoker interface {
	CreateStory(ctx context.Context, payload json.RawMessage) (*StoryExecutionResult, error)
	ExecutePrompt(ctx context.Context, payload json.RawMessage) (*PromptExecutionResult, error)
}

// CLIInvoker delegates story scaffolding and prompt runs to the iazio-harness binary.
type CLIInvoker struct {
	HarnessBinary string
	WorkDir       string
	GracePeriod   time.Duration
}

// NewCLIInvoker creates a CLIInvoker with default binary resolution.
func NewCLIInvoker(binaryPath, workDir string) *CLIInvoker {
	if binaryPath == "" {
		if env := os.Getenv("IAZIO_HARNESS_BIN"); env != "" {
			binaryPath = env
		} else {
			binaryPath = "iazio-harness"
		}
	}
	return &CLIInvoker{
		HarnessBinary: binaryPath,
		WorkDir:       workDir,
		GracePeriod:   defaultGracePeriod,
	}
}

// CreateStory invokes `iazio-harness story create --json` passing the payload on stdin.
func (c *CLIInvoker) CreateStory(ctx context.Context, payload json.RawMessage) (*StoryExecutionResult, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload for story create")
	}

	stdout, stderr, err := c.runCommand(ctx, []string{"story", "create", "--json"}, payload)
	if err != nil {
		return nil, fmt.Errorf("iazio-harness story create failed: %w (stderr: %s)", err, strings.TrimSpace(stderr))
	}

	var res StoryExecutionResult
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		return nil, fmt.Errorf("failed to parse iazio-harness story create output %q: %w", stdout, err)
	}

	return &res, nil
}

// ExecutePrompt invokes `iazio-harness prompt run --json` passing the payload on stdin.
func (c *CLIInvoker) ExecutePrompt(ctx context.Context, payload json.RawMessage) (*PromptExecutionResult, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload for prompt execution")
	}

	stdout, stderr, err := c.runCommand(ctx, []string{"prompt", "run", "--json"}, payload)
	if err != nil {
		return nil, fmt.Errorf("iazio-harness prompt run failed: %w (stderr: %s)", err, strings.TrimSpace(stderr))
	}

	var res PromptExecutionResult
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		// Fallback if harness emitted plain text
		return &PromptExecutionResult{
			Status: "completed",
			Output: stdout,
		}, nil
	}

	if res.Output == "" && res.Summary != "" {
		res.Output = res.Summary
	}

	return &res, nil
}

func (c *CLIInvoker) runCommand(ctx context.Context, args []string, stdinData []byte) (string, string, error) {
	cmd := exec.CommandContext(ctx, c.HarnessBinary, args...)
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}

	cmd.Stdin = bytes.NewReader(stdinData)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	stdoutStr := stdoutBuf.String()
	stderrStr := stderrBuf.String()

	return stdoutStr, stderrStr, err
}
