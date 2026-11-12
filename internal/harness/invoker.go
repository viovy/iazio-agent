// Package harness provides invokers for delegating tasks to the local iazio-harness runtime.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-core/pkg/procutil"
	"github.com/viovy/iazio-core/pkg/telemetry"
)

const defaultGracePeriod = 2 * time.Second

// StoryExecutionResult represents the output from iazio-harness story create.
type StoryExecutionResult struct {
	StoryID              string `json:"story_id"`
	Submodule            string `json:"submodule"`
	StoryPath            string `json:"story_path"`
	IndexUpdated         bool   `json:"index_updated"`
	ConversationID       string `json:"conversation_id,omitempty"`
	ActiveConversationID string `json:"active_conversation_id,omitempty"`
}

// PromptExecutionResult represents the output from iazio-harness prompt execution.
type PromptExecutionResult struct {
	PromptID             string   `json:"prompt_id,omitempty"`
	Repo                 string   `json:"repo,omitempty"`
	WorktreePath         string   `json:"worktree_path,omitempty"`
	Branch               string   `json:"branch,omitempty"`
	TargetAgent          string   `json:"target_agent,omitempty"`
	StoryID              string   `json:"story_id,omitempty"`
	StoryTitle           string   `json:"story_title,omitempty"`
	StoryPath            string   `json:"story_path,omitempty"`
	ConversationID       string   `json:"conversation_id,omitempty"`
	ActiveConversationID string   `json:"active_conversation_id,omitempty"`
	ConversationIDs      []string `json:"conversation_ids,omitempty"`
	Status               string   `json:"status"`
	ExitCode             int      `json:"exit_code,omitempty"`
	FailureCategory      string   `json:"failure_category,omitempty"`
	FailureStage         string   `json:"failure_stage,omitempty"`
	FailureDirection     string   `json:"failure_direction,omitempty"`
	FailureService       string   `json:"failure_service,omitempty"`
	FailureReasonSummary string   `json:"failure_reason_summary,omitempty"`
	GitHeadSHA           string   `json:"git_head_sha,omitempty"`
	GitTreeSHA           string   `json:"git_tree_sha,omitempty"`
	ModifiedFiles        []string `json:"modified_files,omitempty"`
	Summary              string   `json:"summary,omitempty"`
	DurationMs           int64    `json:"duration_ms,omitempty"`
	Output               string   `json:"output,omitempty"`
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
	HarnessBinary           string
	WorkDir                 string
	GracePeriod             time.Duration
	OnStoryAllocated        func(storyID string)
	OnConversationAllocated func(convID string)
}

func resolveDefaultHarnessBinary() string {
	if env := strings.TrimSpace(os.Getenv("IAZIO_HARNESS_BIN")); env != "" {
		return env
	}
	binName := "iazio-harness"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	home := strings.TrimSpace(os.Getenv("HOME"))
	if home == "" {
		if uh, err := os.UserHomeDir(); err == nil {
			home = strings.TrimSpace(uh)
		}
	}
	if home != "" {
		var cands []string
		if runtime.GOOS == "windows" {
			cands = []string{
				filepath.Join(home, "bin", binName),
				filepath.Join(home, ".local", "bin", binName),
				filepath.Join(home, ".iazio", "bin", binName),
			}
		} else {
			cands = []string{
				filepath.Join(home, ".local", "bin", binName),
				filepath.Join(home, "bin", binName),
				filepath.Join(home, ".iazio", "bin", binName),
			}
		}
		for _, cand := range cands {
			if _, err := os.Stat(cand); err == nil {
				return cand
			}
		}
	}
	return "iazio-harness"
}

// NewCLIInvoker creates a CLIInvoker with default binary resolution.
func NewCLIInvoker(binaryPath, workDir string) *CLIInvoker {
	if binaryPath == "" {
		binaryPath = resolveDefaultHarnessBinary()
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
	if err := json.Unmarshal(extractJSONObject(stdout), &res); err != nil {
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
	if err := json.Unmarshal(extractJSONObject(stdout), &res); err != nil {
		// Fallback if harness emitted plain text
		return &PromptExecutionResult{
			Status: "completed",
			Output: stdout,
		}, nil
	}

	if res.Output == "" && res.Summary != "" {
		res.Output = res.Summary
	}

	combinedLower := strings.ToLower(res.Summary + "\n" + res.Output + "\n" + stderr)
	if strings.Contains(combinedLower, "jetski: no output produced") ||
		strings.Contains(combinedLower, "headless mode cannot prompt for") ||
		strings.Contains(combinedLower, "so it was auto-denied") {
		res.Status = "failed"
		if res.ExitCode == 0 {
			res.ExitCode = 1
		}
		return &res, fmt.Errorf("PERMISSION_DENIED: headless agent tool permission was auto-denied: %s", strings.TrimSpace(res.Summary))
	}
	if strings.EqualFold(res.Status, "failed") || res.ExitCode != 0 {
		return &res, fmt.Errorf("iazio-harness prompt execution failed (status=%s exit=%d): %s", res.Status, res.ExitCode, strings.TrimSpace(res.Summary))
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

// WithConversationCallback returns a shallow copy of the CLIInvoker with the specified conversation allocation callback.
func (c *CLIInvoker) WithConversationCallback(cb func(convID string)) Invoker {
	clone := *c
	clone.OnConversationAllocated = cb
	return &clone
}

func extractJSONObject(raw string) []byte {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "{") {
		return []byte(trimmed)
	}
	if idx := strings.Index(trimmed, "\n{"); idx != -1 {
		return []byte(strings.TrimSpace(trimmed[idx+1:]))
	}
	if idx := strings.IndexByte(trimmed, '{'); idx != -1 {
		return []byte(strings.TrimSpace(trimmed[idx:]))
	}
	return []byte(raw)
}

var validAllocatedStoryIDRegex = regexp.MustCompile(`^(?i)STORY-[A-Z0-9]+-[A-Z0-9][A-Z0-9-]*$`)
var validAllocatedConvIDRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{5,127}$`)

func isValidAllocatedStoryID(storyID string) bool {
	s := strings.TrimSpace(storyID)
	if s == "" || strings.EqualFold(s, "pending-allocation") || strings.EqualFold(s, "pending_harness_creation") {
		return false
	}
	if strings.HasSuffix(s, "-") {
		return false
	}
	return validAllocatedStoryIDRegex.MatchString(s)
}

func isValidAllocatedConvID(convID string) bool {
	c := strings.TrimSpace(convID)
	if c == "" || strings.EqualFold(c, "pending") || strings.EqualFold(c, "none") || strings.EqualFold(c, "new_conversation") || strings.EqualFold(c, "pending-allocation") {
		return false
	}
	return validAllocatedConvIDRegex.MatchString(c)
}

type storyTrackingWriter struct {
	buf                     bytes.Buffer
	lineBuf                 bytes.Buffer
	cbMu                    *sync.Mutex
	sharedLastStoryID       *string
	sharedLastConvID        *string
	lastAllocatedID         string
	lastAllocatedConvID     string
	onStoryAllocated        func(string)
	onConversationAllocated func(string)
}

func (w *storyTrackingWriter) Write(p []byte) (n int, err error) {
	n, err = w.buf.Write(p)
	if (w.onStoryAllocated != nil || w.onConversationAllocated != nil) && len(p) > 0 {
		_, _ = w.lineBuf.Write(p)
		for {
			b := w.lineBuf.Bytes()
			nlIdx := bytes.IndexByte(b, '\n')
			if nlIdx == -1 {
				break
			}
			line := string(w.lineBuf.Next(nlIdx + 1))
			w.scanLine(line)
		}
	}
	return n, err
}

// Flush processes any remaining partial line in lineBuf after subprocess completion.
func (w *storyTrackingWriter) Flush() {
	if (w.onStoryAllocated != nil || w.onConversationAllocated != nil) && w.lineBuf.Len() > 0 {
		line := w.lineBuf.String()
		w.lineBuf.Reset()
		w.scanLine(line)
	}
}

func (w *storyTrackingWriter) scanLine(line string) {
	if line == "" {
		return
	}
	if w.onStoryAllocated != nil {
		for _, prefix := range []string{"ALLOCATED_STORY_ID:", "STORY_ID:", "Bound active story ID:", "Bound dynamically allocated story ID:", "Bound governing story:"} {
			if idx := strings.Index(line, prefix); idx != -1 {
				rem := strings.TrimLeft(line[idx+len(prefix):], " \t\"'`")
				if endIdx := strings.IndexAny(rem, "\r\n \t,;()\"'`{}[]"); endIdx != -1 {
					rem = rem[:endIdx]
				}
				storyID := strings.TrimSpace(rem)
				if isValidAllocatedStoryID(storyID) {
					if w.cbMu != nil {
						w.cbMu.Lock()
					}
					lastStory := &w.lastAllocatedID
					if w.sharedLastStoryID != nil {
						lastStory = w.sharedLastStoryID
					}
					if storyID != *lastStory {
						*lastStory = storyID
						w.onStoryAllocated(storyID)
					}
					if w.cbMu != nil {
						w.cbMu.Unlock()
					}
					break
				}
			}
		}
	}
	if w.onConversationAllocated != nil {
		for _, prefix := range []string{"ACTIVE_CONVERSATION_ID:", "CONVERSATION_ID:", "Attached Antigravity conversation:"} {
			if idx := strings.Index(line, prefix); idx != -1 {
				rem := strings.TrimLeft(line[idx+len(prefix):], " \t\"'`")
				if endIdx := strings.IndexAny(rem, "\r\n \t,;()\"'`{}[]"); endIdx != -1 {
					rem = rem[:endIdx]
				}
				convID := strings.TrimSpace(rem)
				if isValidAllocatedConvID(convID) {
					if w.cbMu != nil {
						w.cbMu.Lock()
					}
					lastConv := &w.lastAllocatedConvID
					if w.sharedLastConvID != nil {
						lastConv = w.sharedLastConvID
					}
					if convID != *lastConv {
						*lastConv = convID
						w.onConversationAllocated(convID)
					}
					if w.cbMu != nil {
						w.cbMu.Unlock()
					}
					break
				}
			}
		}
	}
}

func (c *CLIInvoker) runCommand(ctx context.Context, args []string, stdinData []byte) (string, string, error) {
	bin := c.HarnessBinary
	if bin == "" || bin == "iazio-harness" || bin == "iazio-harness.exe" {
		bin = resolveDefaultHarnessBinary()
	}
	cmd := procutil.CommandContext(ctx, bin, args...)
	setProcGroup(cmd)
	cmd.Cancel = func() error {
		return killProcGroup(cmd)
	}
	if c.WorkDir != "" {
		cmd.Dir = c.WorkDir
	}

	cmd.Env = os.Environ()
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		extraBins := []string{
			filepath.Join(home, ".local", "bin"),
			filepath.Join(home, "bin"),
			filepath.Join(home, ".iazio", "bin"),
		}
		curPath := os.Getenv("PATH")
		for _, eb := range extraBins {
			if !strings.Contains(curPath, eb) {
				if curPath == "" {
					curPath = eb
				} else {
					curPath = eb + string(os.PathListSeparator) + curPath
				}
			}
		}
		cmd.Env = append(cmd.Env, "PATH="+curPath)
	}
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
	if len(stdinData) > 0 {
		var envMap map[string]any
		if json.Unmarshal(stdinData, &envMap) == nil && envMap != nil {
			if apiURL, ok := envMap["api_url"].(string); ok && strings.TrimSpace(apiURL) != "" {
				cmd.Env = append(cmd.Env, "IAZIO_HARNESS_API_URL="+strings.TrimSpace(apiURL))
			}
			if tok, ok := envMap["token"].(string); ok && strings.TrimSpace(tok) != "" {
				cmd.Env = append(cmd.Env, "IAZIO_HARNESS_TOKEN="+strings.TrimSpace(tok))
			}
			if jid, ok := envMap["job_id"].(string); ok && strings.TrimSpace(jid) != "" {
				cmd.Env = append(cmd.Env, "IAZIO_JOB_ID="+strings.TrimSpace(jid))
			}
			if lt, ok := envMap["lease_token"].(string); ok && strings.TrimSpace(lt) != "" {
				cmd.Env = append(cmd.Env, "IAZIO_LEASE_TOKEN="+strings.TrimSpace(lt))
			}
			switch ft := envMap["fencing_token"].(type) {
			case float64:
				if int64(ft) > 0 {
					cmd.Env = append(cmd.Env, fmt.Sprintf("IAZIO_FENCING_TOKEN=%d", int64(ft)))
				}
			}
		}
	}

	var resultFile string
	var cleanupResultFile bool
	if len(args) >= 2 && args[0] == "prompt" && args[1] == "run" {
		resultFile = strings.TrimSpace(os.Getenv("IAZIO_RESULT_FILE"))
		if resultFile == "" {
			resultFile = filepath.Join(os.TempDir(), fmt.Sprintf("iazio-result-%d-%d.json", os.Getpid(), time.Now().UnixNano()))
			cleanupResultFile = true
		}
		cmd.Env = append(cmd.Env, "IAZIO_RESULT_FILE="+resultFile)
	}
	if cleanupResultFile && resultFile != "" {
		defer func() { _ = os.Remove(resultFile) }()
	}

	cmd.Stdin = bytes.NewReader(stdinData)
	var cbMu sync.Mutex
	var sharedLastStory, sharedLastConv string
	stdoutWriter := storyTrackingWriter{
		cbMu:                    &cbMu,
		sharedLastStoryID:       &sharedLastStory,
		sharedLastConvID:        &sharedLastConv,
		onStoryAllocated:        c.OnStoryAllocated,
		onConversationAllocated: c.OnConversationAllocated,
	}
	stderrWriter := storyTrackingWriter{
		cbMu:                    &cbMu,
		sharedLastStoryID:       &sharedLastStory,
		sharedLastConvID:        &sharedLastConv,
		onStoryAllocated:        c.OnStoryAllocated,
		onConversationAllocated: c.OnConversationAllocated,
	}
	cmd.Stdout = &stdoutWriter
	cmd.Stderr = &stderrWriter

	err := cmd.Run()
	stdoutWriter.Flush()
	stderrWriter.Flush()
	stdoutStr := stdoutWriter.buf.String()
	stderrStr := stderrWriter.buf.String()

	if resultFile != "" && (strings.TrimSpace(stdoutStr) == "" || !strings.Contains(stdoutStr, "{")) {
		if rawFile, readErr := os.ReadFile(resultFile); readErr == nil && len(bytes.TrimSpace(rawFile)) > 0 {
			stdoutStr = string(rawFile)
		}
	}

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
	if strings.TrimSpace(jobID) != "" {
		if existingJobID, _ := m["job_id"].(string); strings.TrimSpace(existingJobID) == "" {
			m["job_id"] = strings.TrimSpace(jobID)
			changed = true
		}
	}
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
