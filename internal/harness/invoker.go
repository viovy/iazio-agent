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
	"path/filepath"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/telemetry"
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
	StoryID       string   `json:"story_id,omitempty"`
	StoryTitle    string   `json:"story_title,omitempty"`
	StoryPath     string   `json:"story_path,omitempty"`
	Status        string   `json:"status"`
	ExitCode      int      `json:"exit_code,omitempty"`
	GitHeadSHA    string   `json:"git_head_sha,omitempty"`
	GitTreeSHA    string   `json:"git_tree_sha,omitempty"`
	ModifiedFiles []string `json:"modified_files,omitempty"`
	Summary       string   `json:"summary,omitempty"`
	DurationMs    int64    `json:"duration_ms,omitempty"`
	Output        string   `json:"output,omitempty"`
}

// CodebaseRepoInfo describes a repository returned by codebase repos.
type CodebaseRepoInfo struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	DefaultBranch string `json:"default_branch"`
	GitTreeSHA    string `json:"git_tree_sha"`
	Description   string `json:"description,omitempty"`
}

// CodebaseReposResult represents the output from iazio-harness codebase repos.
type CodebaseReposResult struct {
	Repos []CodebaseRepoInfo `json:"repos"`
}

// CodebaseChunk describes a source code chunk emitted by codebase export.
type CodebaseChunk struct {
	RepoName    string `json:"repo_name"`
	FilePath    string `json:"file_path"`
	StartLine   int    `json:"start_line"`
	EndLine     int    `json:"end_line"`
	Symbol      string `json:"symbol,omitempty"`
	Content     string `json:"content"`
	FileBlobSHA string `json:"file_blob_sha"`
	GitTreeSHA  string `json:"git_tree_sha"`
}

// CodebaseExportResult represents the output from iazio-harness codebase export.
type CodebaseExportResult struct {
	RepoName     string          `json:"repo_name"`
	BaseTreeSHA  string          `json:"base_tree_sha"`
	GitTreeSHA   string          `json:"git_tree_sha"`
	UpToDate     bool            `json:"up_to_date"`
	DeletedFiles []string        `json:"deleted_files"`
	Chunks       []CodebaseChunk `json:"chunks"`
}

// Invoker defines the contract for invoking iazio-harness on the host.
// Direct calls to autopilot or edits to docs/stories/index.yml are strictly prohibited in iazio-agent.
type Invoker interface {
	CreateStory(ctx context.Context, payload json.RawMessage) (*StoryExecutionResult, error)
	ExecutePrompt(ctx context.Context, payload json.RawMessage) (*PromptExecutionResult, error)
	ListRepos(ctx context.Context) (*CodebaseReposResult, error)
	ExportCodebase(ctx context.Context, payload json.RawMessage) (*CodebaseExportResult, error)
	WithWorkDir(workDir string) Invoker
}

// CLIInvoker delegates story scaffolding and prompt runs to the iazio-harness binary.
type CLIInvoker struct {
	HarnessBinary    string
	WorkDir          string
	GracePeriod      time.Duration
	OnStoryAllocated func(storyID string)
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

	payload = EnsurePromptPayload("", payload)

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

// ListRepos invokes `iazio-harness codebase repos --json`.
func (c *CLIInvoker) ListRepos(ctx context.Context) (*CodebaseReposResult, error) {
	stdout, stderr, err := c.runCommand(ctx, []string{"codebase", "repos", "--json"}, nil)
	if err != nil {
		return nil, fmt.Errorf("iazio-harness codebase repos failed: %w (stderr: %s)", err, strings.TrimSpace(stderr))
	}

	var res CodebaseReposResult
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		return nil, fmt.Errorf("failed to parse iazio-harness codebase repos output %q: %w", stdout, err)
	}

	return &res, nil
}

// ExportCodebase invokes `iazio-harness codebase export --json` passing payload on stdin.
func (c *CLIInvoker) ExportCodebase(ctx context.Context, payload json.RawMessage) (*CodebaseExportResult, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload for codebase export")
	}

	stdout, stderr, err := c.runCommand(ctx, []string{"codebase", "export", "--json"}, payload)
	if err != nil {
		return nil, fmt.Errorf("iazio-harness codebase export failed: %w (stderr: %s)", err, strings.TrimSpace(stderr))
	}

	var res CodebaseExportResult
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		return nil, fmt.Errorf("failed to parse iazio-harness codebase export output %q: %w", stdout, err)
	}

	return &res, nil
}

// ListLocalRepos invokes `iazio-harness codebase repos --json` and returns []RepoManifest.
func (c *CLIInvoker) ListLocalRepos(ctx context.Context) ([]RepoManifest, error) {
	ci := &CLICodebaseInvoker{HarnessBinary: c.HarnessBinary, WorkDir: c.WorkDir}
	return ci.ListLocalRepos(ctx)
}

// ExportRepoDelta invokes `iazio-harness codebase export --json` and returns *ExportResponse.
func (c *CLIInvoker) ExportRepoDelta(ctx context.Context, req ExportRequest) (*ExportResponse, error) {
	ci := &CLICodebaseInvoker{HarnessBinary: c.HarnessBinary, WorkDir: c.WorkDir}
	return ci.ExportRepoDelta(ctx, req)
}

// WithWorkDir returns a shallow copy of the CLIInvoker with the specified workDir.
func (c *CLIInvoker) WithWorkDir(workDir string) Invoker {
	clone := *c
	clone.WorkDir = workDir
	return &clone
}

// WithStoryCallback returns a shallow copy of the CLIInvoker with the specified story allocation callback.
func (c *CLIInvoker) WithStoryCallback(cb func(storyID string)) Invoker {
	clone := *c
	clone.OnStoryAllocated = cb
	return &clone
}

type storyTrackingWriter struct {
	buf              bytes.Buffer
	onStoryAllocated func(string)
}

func (w *storyTrackingWriter) Write(p []byte) (n int, err error) {
	n, err = w.buf.Write(p)
	if w.onStoryAllocated != nil && len(p) > 0 {
		str := string(p)
		for _, prefix := range []string{"ALLOCATED_STORY_ID:", "STORY_ID:", "Bound active story ID:", "Bound dynamically allocated story ID:"} {
			if idx := strings.Index(str, prefix); idx != -1 {
				rem := strings.TrimLeft(str[idx+len(prefix):], " \t")
				if endIdx := strings.IndexAny(rem, "\r\n ,;()"); endIdx != -1 {
					rem = rem[:endIdx]
				}
				storyID := strings.TrimSpace(rem)
				if storyID != "" && storyID != "pending-allocation" && storyID != "pending_harness_creation" {
					w.onStoryAllocated(storyID)
					break
				}
			}
		}
	}
	return n, err
}

func (c *CLIInvoker) runCommand(ctx context.Context, args []string, stdinData []byte) (string, string, error) {
	cmd := exec.CommandContext(ctx, c.HarnessBinary, args...)
	setProcGroup(cmd)
	cmd.Cancel = func() error {
		return killProcGroup(cmd)
	}
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}

	cmd.Env = os.Environ()
	if c.WorkDir != "" {
		fleetRoot := c.WorkDir
		for {
			if _, err := os.Stat(filepath.Join(fleetRoot, ".gitmodules")); err == nil {
				cmd.Env = append(cmd.Env, "IAZIO_FLEET_ROOT="+fleetRoot)
				break
			}
			parent := filepath.Dir(fleetRoot)
			if parent == fleetRoot {
				break
			}
			fleetRoot = parent
		}
	}
	if span := telemetry.SpanFromContext(ctx); span != nil {
		cmd.Env = append(cmd.Env,
			fmt.Sprintf("TRACEPARENT=00-%s-%s-01", span.TraceID, span.ID),
			"ZIPKIN_TRACE_ID="+span.TraceID,
			"TRACE_ID="+span.TraceID,
		)
	}

	cmd.Stdin = bytes.NewReader(stdinData)
	var stdoutWriter storyTrackingWriter
	stdoutWriter.onStoryAllocated = c.OnStoryAllocated
	var stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutWriter
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	stdoutStr := stdoutWriter.buf.String()
	stderrStr := stderrBuf.String()

	return stdoutStr, stderrStr, err
}

// EnsurePromptPayload ensures that a prompt payload JSON contains a valid prompt_id field.
func EnsurePromptPayload(jobID string, raw json.RawMessage) json.RawMessage {
	return EnsurePromptPayloadWithTrace(jobID, "", raw)
}

// EnsurePromptPayloadWithTrace ensures that a prompt payload JSON contains a valid prompt_id and trace_id.
func EnsurePromptPayloadWithTrace(jobID, traceID string, raw json.RawMessage) json.RawMessage {
	return EnsurePromptPayloadWithJob(jobID, traceID, "", "", "", raw)
}

// EnsurePromptPayloadWithJob synchronizes authoritative job envelope attributes (prompt_id, trace_id,
// target_agent, repo, story_id) into the stdin prompt payload JSON before invoking iazio-harness.
func EnsurePromptPayloadWithJob(jobID, traceID, targetAgent, repo, storyID string, raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return raw
	}
	changed := false
	promptID, _ := m["prompt_id"].(string)
	if strings.TrimSpace(promptID) == "" {
		existingStoryID, _ := m["story_id"].(string)
		if strings.TrimSpace(existingStoryID) == "" {
			existingStoryID = strings.TrimSpace(storyID)
		}
		if strings.TrimSpace(existingStoryID) != "" {
			m["prompt_id"] = "prompt-" + strings.TrimSpace(existingStoryID)
		} else if strings.TrimSpace(jobID) != "" {
			m["prompt_id"] = "prompt-" + strings.TrimSpace(jobID)
		} else {
			m["prompt_id"] = fmt.Sprintf("prompt-%d", time.Now().UnixNano())
		}
		changed = true
	}
	pTraceID, _ := m["trace_id"].(string)
	if strings.TrimSpace(pTraceID) == "" && strings.TrimSpace(traceID) != "" {
		m["trace_id"] = strings.TrimSpace(traceID)
		changed = true
	}
	cleanAgent := strings.TrimSpace(targetAgent)
	if cleanAgent != "" && !strings.EqualFold(cleanAgent, "auto") {
		if existingAgent, _ := m["target_agent"].(string); strings.TrimSpace(existingAgent) != cleanAgent {
			m["target_agent"] = cleanAgent
			changed = true
		}
	}
	cleanRepo := strings.TrimSpace(repo)
	if cleanRepo != "" {
		if existingRepo, _ := m["repo"].(string); strings.TrimSpace(existingRepo) == "" {
			m["repo"] = cleanRepo
			changed = true
		}
	}
	cleanStory := strings.TrimSpace(storyID)
	if cleanStory != "" && cleanStory != "pending-allocation" && cleanStory != "pending_harness_creation" {
		if existingStory, _ := m["story_id"].(string); strings.TrimSpace(existingStory) == "" {
			m["story_id"] = cleanStory
			changed = true
		}
	}
	if changed {
		if updated, err := json.Marshal(m); err == nil {
			return updated
		}
	}
	return raw
}
