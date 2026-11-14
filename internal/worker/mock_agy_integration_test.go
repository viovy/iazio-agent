package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/worker"
)

func buildMockAgyAndHarnessBinaries(t *testing.T) (agyBin string, harnessBin string) {
	t.Helper()
	binDir := t.TempDir()
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	agyBin = filepath.Join(binDir, "agy"+ext)
	harnessBin = filepath.Join(binDir, "iazio-harness"+ext)

	agySrc := filepath.Join(binDir, "mock_agy.go")
	if err := os.WriteFile(agySrc, []byte(`package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	args := strings.Join(os.Args[1:], " ")
	if strings.Contains(args, "SLEEP_300MS") {
		time.Sleep(300 * time.Millisecond)
	}
	convID := "conv-mock-agy-default"
	storyID := "STORY-GOV-5263"
	for _, tok := range strings.Fields(args) {
		if strings.HasPrefix(tok, "STORY-") {
			storyID = strings.Trim(tok, ".,:;()[]{}\"'")
		}
		if strings.HasPrefix(tok, "CONV-") {
			convID = strings.ToLower(strings.Trim(tok, ".,:;()[]{}\"'"))
		}
	}
	summary := "mock agy completed " + storyID
	if strings.Contains(args, "PREMATURE_CRAFT") {
		summary = "## CRAFT Closeout\nLaunched autopilot-git-flow land; waiting for CI green."
	}
	fmt.Printf("{\"status\":\"completed\",\"conversation_id\":%q,\"story_id\":%q,\"summary\":%q,\"output\":%q}\n", convID, storyID, summary, summary)
}
`), 0o644); err != nil {
		t.Fatalf("write mock_agy.go: %v", err)
	}

	harnessSrc := filepath.Join(binDir, "mock_harness.go")
	if err := os.WriteFile(harnessSrc, []byte(`package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "prompt" || os.Args[2] != "run" {
		fmt.Fprintf(os.Stderr, "unsupported args: %v\n", os.Args)
		os.Exit(2)
	}
	for _, a := range os.Args[3:] {
		if a != "--json" {
			fmt.Fprintf(os.Stderr, "unexpected CLI flag %q (stdin JSON payload required)\n", a)
			os.Exit(2)
		}
	}
	stdinBytes, err := io.ReadAll(os.Stdin)
	if err != nil || len(strings.TrimSpace(string(stdinBytes))) == 0 {
		fmt.Fprintln(os.Stderr, "empty stdin JSON payload")
		os.Exit(1)
	}
	var req map[string]any
	if err := json.Unmarshal(stdinBytes, &req); err != nil {
		fmt.Fprintf(os.Stderr, "invalid stdin JSON payload: %v\n", err)
		os.Exit(1)
	}
	prompt, _ := req["prompt"].(string)
	worktreePath, _ := req["worktree_path"].(string)
	storyID, _ := req["story_id"].(string)
	if storyID == "" {
		storyID = "STORY-GOV-5263"
	}
	agyExe := os.Getenv("MOCK_AGY_BIN")
	if agyExe == "" {
		ext := ""
		if strings.HasSuffix(strings.ToLower(os.Args[0]), ".exe") {
			ext = ".exe"
		}
		agyExe = filepath.Join(filepath.Dir(os.Args[0]), "agy"+ext)
	}
	cmd := exec.Command(agyExe, "chat", "--mode", "agent", prompt+" "+storyID)
	if worktreePath != "" {
		cmd.Dir = worktreePath
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock agy failed: %v: %s\n", err, string(out))
		os.Exit(1)
	}
	var agyRes map[string]any
	_ = json.Unmarshal(out, &agyRes)
	convID, _ := agyRes["conversation_id"].(string)
	if convID == "" {
		convID = "conv-mock-agy"
	}
	res := map[string]any{
		"prompt_id":              req["prompt_id"],
		"repo":                   req["repo"],
		"worktree_path":          worktreePath,
		"target_agent":           "agy",
		"story_id":               storyID,
		"conversation_id":        convID,
		"active_conversation_id": convID,
		"status":                 "completed",
		"exit_code":              0,
		"summary":                strings.TrimSpace(string(out)),
		"output":                 strings.TrimSpace(string(out)),
	}
	enc, _ := json.Marshal(res)
	fmt.Println(string(enc))
}
`), 0o644); err != nil {
		t.Fatalf("write mock_harness.go: %v", err)
	}

	if out, err := exec.Command("go", "build", "-o", agyBin, agySrc).CombinedOutput(); err != nil {
		t.Fatalf("compile mock agy: %v: %s", err, string(out))
	}
	if out, err := exec.Command("go", "build", "-o", harnessBin, harnessSrc).CombinedOutput(); err != nil {
		t.Fatalf("compile mock iazio-harness: %v: %s", err, string(out))
	}
	return agyBin, harnessBin
}

func initCleanGitWorktree(t *testing.T, storyIDs ...string) string {
	t.Helper()
	remoteDir := filepath.Join(t.TempDir(), "remote.git")
	wtDir := filepath.Join(t.TempDir(), "worktree")
	if err := os.MkdirAll(wtDir, 0o755); err != nil {
		t.Fatalf("mkdir worktree: %v", err)
	}
	runGit := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v in %s failed: %v: %s", args, dir, err, string(out))
		}
	}
	if out, err := exec.Command("git", "init", "--bare", remoteDir).CombinedOutput(); err != nil {
		t.Fatalf("git init bare: %v: %s", err, string(out))
	}
	runGit(wtDir, "init", "-b", "main")
	runGit(wtDir, "config", "user.email", "test@example.com")
	runGit(wtDir, "config", "user.name", "Test Agent")
	runGit(wtDir, "remote", "add", "origin", remoteDir)

	storiesDir := filepath.Join(wtDir, "docs", "stories")
	if err := os.MkdirAll(storiesDir, 0o755); err != nil {
		t.Fatalf("mkdir stories: %v", err)
	}
	var indexBuf bytes.Buffer
	indexBuf.WriteString("stories:\n")
	if len(storyIDs) == 0 {
		storyIDs = []string{"STORY-GOV-5263"}
	}
	for _, sid := range storyIDs {
		fmt.Fprintf(&indexBuf, "  - id: %s\n    status: DONE\n", sid)
	}
	if err := os.WriteFile(filepath.Join(storiesDir, "index.yml"), indexBuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write index.yml: %v", err)
	}
	runGit(wtDir, "add", ".")
	runGit(wtDir, "commit", "-m", "initial clean state")
	runGit(wtDir, "push", "-u", "origin", "main")
	return wtDir
}

type mockQueueControlPlane struct {
	mu          sync.Mutex
	jobs        []*worker.Job
	claimReqs   []worker.ClaimRequest
	completed   map[string]worker.CompleteRequest
	declined    map[string]string
	ackedWorktr map[string]string
}

func newMockQueueControlPlane(jobs ...*worker.Job) *mockQueueControlPlane {
	return &mockQueueControlPlane{
		jobs:        jobs,
		completed:   make(map[string]worker.CompleteRequest),
		declined:    make(map[string]string),
		ackedWorktr: make(map[string]string),
	}
}

func (m *mockQueueControlPlane) ClaimJob(ctx context.Context, req worker.ClaimRequest) (*worker.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimReqs = append(m.claimReqs, req)
	if len(m.jobs) == 0 || len(req.IdleWorktrees) == 0 {
		return nil, nil
	}
	job := m.jobs[0]
	m.jobs = m.jobs[1:]
	cp := *job
	if cp.WorktreePath == "" {
		cp.WorktreePath = req.IdleWorktrees[0]
	}
	return &cp, nil
}

func (m *mockQueueControlPlane) CompleteJob(ctx context.Context, jobID string, req worker.CompleteRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[jobID] = req
	return nil
}

func (m *mockQueueControlPlane) HeartbeatJob(ctx context.Context, jobID, leaseToken string, fencingToken int64, storyID ...string) error {
	return nil
}

func (m *mockQueueControlPlane) BindStory(ctx context.Context, jobID string, req worker.StoryBindingRequest) error {
	return nil
}

func (m *mockQueueControlPlane) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) error {
	return nil
}

func (m *mockQueueControlPlane) AckLeaseWithWorktree(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID, worktreePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackedWorktr[jobID] = worktreePath
	return nil
}

func (m *mockQueueControlPlane) FailoverJob(ctx context.Context, jobID string, req controlplane.AttemptFailure) error {
	return nil
}

func (m *mockQueueControlPlane) DeclineJob(ctx context.Context, jobID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.declined[jobID] = reason
	return nil
}

// TestMockAgy_Discovery1And2_PostASEDeferredToolUpdateDoesNotQuarantineWorktree verifies:
// 1. Post-ASE toolUpdater returning ErrJobDeferred does NOT write .iazio/post-ase-sync-failed,
//    and releases activeJobs + hostGate execution lease BEFORE calling toolUpdater.
// 2. Any pre-existing .iazio/post-ase-sync-failed marker containing "tool update queued/deferred"
//    is automatically healed by repolock.IsWorktreeCleanForNewJob and RepoStateMonitor.
func TestMockAgy_Discovery1And2_PostASEDeferredToolUpdateDoesNotQuarantineWorktree(t *testing.T) {
	_, harnessBin := buildMockAgyAndHarnessBinaries(t)
	wtDir := initCleanGitWorktree(t, "STORY-GOV-5263", "STORY-GOV-5264")

	// Pre-seed a false-positive deferred marker (simulating debian/pc1-wsl/mac-mini slots 02..04)
	markerPath := filepath.Join(wtDir, ".iazio", "post-ase-sync-failed")
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o755); err != nil {
		t.Fatalf("mkdir .iazio: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("tool update failed: tool update queued/deferred\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	// Verify IsWorktreeCleanForNewJob auto-heals the false-positive marker
	clean, reason := repolock.IsWorktreeCleanForNewJob(wtDir)
	if !clean {
		t.Fatalf("expected worktree with false-positive deferred marker to be clean, got clean=false reason=%q", reason)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("expected false-positive .iazio/post-ase-sync-failed marker to be removed, stat err=%v", err)
	}

	job1 := &worker.Job{
		ID:           "job_ase_1",
		Type:         worker.JobTypeStoryExecute,
		Repo:         "meta-repo",
		TargetAgent:  "agy",
		StoryID:      "STORY-GOV-5263",
		FencingToken: 1,
		LeaseToken:   "lease-1",
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution STORY-GOV-5263","story_id":"STORY-GOV-5263"}`),
	}
	job2 := &worker.Job{
		ID:           "job_ase_2",
		Type:         worker.JobTypeStoryExecute,
		Repo:         "meta-repo",
		TargetAgent:  "agy",
		StoryID:      "STORY-GOV-5264",
		FencingToken: 2,
		LeaseToken:   "lease-2",
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution STORY-GOV-5264","story_id":"STORY-GOV-5264"}`),
	}

	cp := newMockQueueControlPlane(job1, job2)
	invoker := harness.NewCLIInvoker(harnessBin, wtDir)
	disp := worker.NewDispatcher(cp, invoker, "debian")
	hostGate := supervisor.NewHostExecutionAndUpdateGate()
	disp.SetHostGate(hostGate)
	disp.SetIdleWorktreesProvider(func() []string {
		return disp.CollectIdleWorktrees([]string{wtDir})
	})

	var activeDuringToolUpdate int
	var gateActiveDuringToolUpdate int
	disp.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
		activeDuringToolUpdate = len(disp.ActiveJobs())
		gateActiveDuringToolUpdate = len(hostGate.ActiveExecutions())
		return worker.ErrJobDeferred
	})

	// Job 1 runs ASE via mock agy and triggers post-ASE toolUpdater returning ErrJobDeferred
	processed1, err1 := disp.PollAndProcess(context.Background())
	if err1 != nil || !processed1 {
		t.Fatalf("expected job1 to process cleanly, got processed=%v err=%v", processed1, err1)
	}
	if activeDuringToolUpdate != 0 || gateActiveDuringToolUpdate != 0 {
		t.Fatalf("expected activeJobs and hostGate lease to be released before post-ASE toolUpdater ran, got activeJobs=%d gateActive=%d", activeDuringToolUpdate, gateActiveDuringToolUpdate)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("expected ErrJobDeferred NOT to write .iazio/post-ase-sync-failed, but file exists")
	}

	// Job 2 must immediately be claimable on the same worktree without quarantine
	processed2, err2 := disp.PollAndProcess(context.Background())
	if err2 != nil || !processed2 {
		t.Fatalf("expected job2 to be claimed and processed immediately after job1, got processed=%v err=%v", processed2, err2)
	}
	if len(cp.completed) != 2 {
		t.Fatalf("expected both jobs to complete via mock agy, got completed=%d", len(cp.completed))
	}
}

// TestMockAgy_Discovery3_ConcurrentMultiWorktreeDispatch verifies that when a host has
// multiple idle worktrees (like mac-mini with meta-repo-02..05), PollAndDispatchConcurrent
// claims and executes jobs concurrently across idle worktrees using mock agy instead of
// blocking the poll loop on the first running job.
func TestMockAgy_Discovery3_ConcurrentMultiWorktreeDispatch(t *testing.T) {
	_, harnessBin := buildMockAgyAndHarnessBinaries(t)
	wt1 := initCleanGitWorktree(t, "STORY-GOV-5263")
	wt2 := initCleanGitWorktree(t, "STORY-GOV-5264")

	jobSlow := &worker.Job{
		ID:           "job_slow_wt1",
		Type:         worker.JobTypePromptExecute,
		Repo:         "meta-repo",
		TargetAgent:  "agy",
		StoryID:      "STORY-GOV-5263",
		FencingToken: 10,
		LeaseToken:   "lease-10",
		Payload:      json.RawMessage(`{"prompt":"SLEEP_300MS run slow task CONV-SLOW1","story_id":"STORY-GOV-5263"}`),
	}
	jobFast := &worker.Job{
		ID:           "job_fast_wt2",
		Type:         worker.JobTypePromptExecute,
		Repo:         "meta-repo",
		TargetAgent:  "agy",
		StoryID:      "STORY-GOV-5264",
		FencingToken: 11,
		LeaseToken:   "lease-11",
		Payload:      json.RawMessage(`{"prompt":"run fast task CONV-FAST2","story_id":"STORY-GOV-5264"}`),
	}

	cp := newMockQueueControlPlane(jobSlow, jobFast)
	invoker := harness.NewCLIInvoker(harnessBin, wt1)
	disp := worker.NewDispatcher(cp, invoker, "mac-mini")
	disp.SetHostGate(supervisor.NewHostExecutionAndUpdateGate())
	disp.SetIdleWorktreesProvider(func() []string {
		return disp.CollectIdleWorktrees([]string{wt1, wt2})
	})

	var wg sync.WaitGroup
	dispatched, err := disp.PollAndDispatchConcurrent(context.Background(), &wg)
	if err != nil {
		t.Fatalf("PollAndDispatchConcurrent error: %v", err)
	}
	if dispatched != 2 {
		t.Fatalf("expected 2 jobs to be claimed and dispatched concurrently across 2 idle worktrees, got %d", dispatched)
	}

	wg.Wait()

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if len(cp.completed) != 2 {
		t.Fatalf("expected both concurrent jobs to complete, got %d", len(cp.completed))
	}
	if cp.ackedWorktr["job_slow_wt1"] == cp.ackedWorktr["job_fast_wt2"] {
		t.Fatalf("expected jobs to execute on distinct worktrees, both ran on %q", cp.ackedWorktr["job_slow_wt1"])
	}
}

// TestMockAgy_Discovery4_FallbackBinRootDriftInspection verifies that FindOutdatedTools
// discovers binaries installed in ~/.local/bin when primary installDir is ~/bin (Windows pc1 layout).
func TestMockAgy_Discovery4_FallbackBinRootDriftInspection(t *testing.T) {
	homeDir := t.TempDir()
	winBin := filepath.Join(homeDir, "bin")
	localBin := filepath.Join(homeDir, ".local", "bin")
	if err := os.MkdirAll(winBin, 0o755); err != nil {
		t.Fatalf("mkdir winBin: %v", err)
	}
	if err := os.MkdirAll(localBin, 0o755); err != nil {
		t.Fatalf("mkdir localBin: %v", err)
	}

	manifest := &supervisor.FleetManifest{
		Tools: []supervisor.ManifestTool{
			{Name: "autopilot-backlog", BinaryName: "autopilot-backlog", TargetVersion: "0.0.610"},
		},
	}

	fallbackBinPath := filepath.Join(localBin, "autopilot-backlog.exe")
	outdated, err := supervisor.FindOutdatedTools(manifest, func(path string) string {
		if filepath.Clean(path) == filepath.Clean(fallbackBinPath) {
			return "0.0.610"
		}
		return ""
	}, winBin, nil, "windows")
	if err != nil {
		t.Fatalf("FindOutdatedTools error: %v", err)
	}
	if len(outdated) != 0 {
		t.Fatalf("expected 0 outdated tools when binary is present in fallback ~/.local/bin, got %+v", outdated)
	}
}

// TestMockAgy_Discovery5_DefaultAgyEscalationStdinJSON verifies that DefaultAgyEscalation
// invokes `iazio-harness prompt run --json` with a valid JSON payload on stdin (routing to mock agy)
// rather than failing with "empty stdin JSON payload".
func TestMockAgy_Discovery5_DefaultAgyEscalationStdinJSON(t *testing.T) {
	agyBin, harnessBin := buildMockAgyAndHarnessBinaries(t)
	wtDir := initCleanGitWorktree(t, "STORY-GOV-5263")

	t.Setenv("MOCK_AGY_BIN", agyBin)
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", filepath.Dir(harnessBin)+string(os.PathListSeparator)+oldPath)

	if _, err := supervisor.DefaultAgyEscalation(context.Background(), wtDir, "Resolve dirty git state STORY-GOV-5263", "git status dirty"); err != nil {
		t.Fatalf("expected DefaultAgyEscalation to succeed with mock iazio-harness + mock agy via stdin JSON, got error: %v", err)
	}
}

// TestMockAgy_RC7_PrematureCraftCloseoutFailsCompletionGate verifies STORY-IAZIO-0204 RC-7:
// mock-agy emits ## CRAFT Closeout while still "waiting for" land, worktree is clean on main,
// and the ASE completion gate fails closed with premature_craft_closeout / ASE_GATE_INCOMPLETE.
func TestMockAgy_RC7_PrematureCraftCloseoutFailsCompletionGate(t *testing.T) {
	_, harnessBin := buildMockAgyAndHarnessBinaries(t)
	wtDir := initCleanGitWorktree(t, "STORY-IAZIO-0204")

	job := &worker.Job{
		ID:           "job_5b4dc7312a463143",
		Type:         worker.JobTypeStoryExecute,
		Repo:         "meta-repo",
		TargetAgent:  "agy",
		StoryID:      "STORY-IAZIO-0204",
		FencingToken: 204,
		LeaseToken:   "lease-rc7",
		Payload:      json.RawMessage(`{"prompt":"/autonomous-story-execution PREMATURE_CRAFT STORY-IAZIO-0204","story_id":"STORY-IAZIO-0204"}`),
	}

	cp := newMockQueueControlPlane(job)
	invoker := harness.NewCLIInvoker(harnessBin, wtDir)
	disp := worker.NewDispatcher(cp, invoker, "debian")
	disp.SetIdleWorktreesProvider(func() []string {
		return disp.CollectIdleWorktrees([]string{wtDir})
	})

	processed, err := disp.PollAndProcess(context.Background())
	if !processed {
		t.Fatalf("expected job to be processed, got processed=false")
	}
	if err == nil || !strings.Contains(err.Error(), "premature_craft_closeout") {
		t.Fatalf("expected premature_craft_closeout gate failure, got err=%v", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	req, ok := cp.completed["job_5b4dc7312a463143"]
	if !ok {
		t.Fatal("expected CompleteJob to be called")
	}
	if !strings.Contains(req.Error, "premature_craft_closeout") {
		t.Fatalf("expected CompleteRequest.Error premature_craft_closeout, got %q", req.Error)
	}
	if len(req.Result) == 0 || !strings.Contains(string(req.Result), "ASE_GATE_INCOMPLETE") {
		t.Fatalf("expected ASE_GATE_INCOMPLETE in result, got %s", string(req.Result))
	}
}
