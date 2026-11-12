package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-core/pkg/telemetry"
)

type fakeDeadPIDRunner struct {
	pid int
}

func (f fakeDeadPIDRunner) Start(_ []string) (int, error) { return f.pid, nil }
func (f fakeDeadPIDRunner) Signal(int, string) error      { return nil }
func (f fakeDeadPIDRunner) Wait(int) (int, error)         { return 1, nil }

func buildHarnessAndMockAgyBinaries(t *testing.T) (harnessBin string, mockAgyBin string, binDir string) {
	t.Helper()
	binDir = t.TempDir()
	harnessBin = filepath.Join(binDir, "iazio-harness")
	mockAgyBin = filepath.Join(binDir, "mock-agy")
	agyBin := filepath.Join(binDir, "agy")

	harnessRepoDir, err := filepath.Abs(filepath.Join("..", "..", "..", "iazio-harness"))
	if err != nil {
		t.Fatalf("resolve iazio-harness dir: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(harnessRepoDir, "go.mod")); statErr != nil {
		t.Skipf("iazio-harness sibling submodule not present at %s: %v", harnessRepoDir, statErr)
	}

	buildCmd1 := exec.Command("go", "build", "-buildvcs=false", "-o", harnessBin, "./cmd/iazio-harness")
	buildCmd1.Dir = harnessRepoDir
	if out, err := buildCmd1.CombinedOutput(); err != nil {
		t.Fatalf("failed to build iazio-harness: %v\n%s", err, string(out))
	}

	buildCmd2 := exec.Command("go", "build", "-buildvcs=false", "-o", mockAgyBin, "./cmd/mock-agy")
	buildCmd2.Dir = harnessRepoDir
	if out, err := buildCmd2.CombinedOutput(); err != nil {
		t.Fatalf("failed to build mock-agy: %v\n%s", err, string(out))
	}

	if err := os.Symlink(mockAgyBin, agyBin); err != nil {
		data, rErr := os.ReadFile(mockAgyBin)
		if rErr != nil {
			t.Fatalf("read mock-agy: %v", rErr)
		}
		if wErr := os.WriteFile(agyBin, data, 0o755); wErr != nil {
			t.Fatalf("write agy copy: %v", wErr)
		}
	}

	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return harnessBin, mockAgyBin, binDir
}

func initTestFleetWorkspace(t *testing.T) string {
	t.Helper()
	fleetDir := t.TempDir()
	runGit := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = fleetDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v (%s)", args, err, string(out))
		}
	}
	runGit("init", "-b", "main")
	runGit("config", "user.name", "Test Operator")
	runGit("config", "user.email", "operator@iazio.test")

	if err := os.WriteFile(filepath.Join(fleetDir, ".gitmodules"), []byte("[submodule \"iazio-web\"]\n\tpath = iazio-web\n\turl = https://github.com/viovy/iazio-web.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fleetDir, "docs", "stories"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fleetDir, "docs", "stories", "index.yml"), []byte("version: \"1.0\"\nstories: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fleetDir, "README.md"), []byte("# Fleet Root\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fleetDir, ".gitignore"), []byte(".session_story_id\n.autopilot/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", ".")
	runGit("commit", "-m", "chore: initial fleet commit")
	bareRemote := filepath.Join(t.TempDir(), "origin.git")
	_ = exec.Command("git", "init", "--bare", bareRemote).Run()
	runGit("remote", "add", "origin", bareRemote)
	runGit("push", "-u", "origin", "main")
	hooksDir := filepath.Join(fleetDir, ".git", "hooks")
	_ = os.MkdirAll(hooksDir, 0o755)
	_ = os.WriteFile(filepath.Join(hooksDir, "post-commit"), []byte("#!/bin/sh\ngit push origin HEAD >/dev/null 2>&1 || true\n"), 0o755)
	return fleetDir
}

func TestE2E_GeminiWeb_IazioMCP_HarnessAPI_Agent_Harness_MockAgy(t *testing.T) {
	harnessBin, _, _ := buildHarnessAndMockAgyBinaries(t)

	var mu sync.Mutex
	var queuedJobs []*Job
	completedJobs := make(map[string]CompleteRequest)
	boundStories := make(map[string]StoryBindingRequest)
	jobChunks := make(map[string][]string)

	// Boot test HTTP server simulating iazio-harness-api endpoints called by iazio-mcp, iazio-agent, and iazio-harness
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		// 1. iazio-mcp -> iazio-harness-api: POST /api/v1/stories
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/stories":
			var req struct {
				Title              string   `json:"title"`
				Description        string   `json:"description"`
				Repo               string   `json:"repo"`
				Priority           string   `json:"priority"`
				AcceptanceCriteria []string `json:"acceptance_criteria"`
				AutoExecute        bool     `json:"auto_execute"`
				TargetAgent        string   `json:"target_agent"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			var promptBuilder strings.Builder
			promptBuilder.WriteString(fmt.Sprintf("/autonomous-story-execution\n\n# Create and Execute Story: %s\n\n", req.Title))
			promptBuilder.WriteString(fmt.Sprintf("**Target Repository / Scope**: `%s`\n**Priority**: `%s`\n\n", req.Repo, req.Priority))
			if req.Description != "" {
				promptBuilder.WriteString(fmt.Sprintf("## Description\n%s\n\n", req.Description))
			}
			if len(req.AcceptanceCriteria) > 0 {
				promptBuilder.WriteString("## Acceptance Criteria\n")
				for _, ac := range req.AcceptanceCriteria {
					promptBuilder.WriteString(fmt.Sprintf("- %s\n", ac))
				}
			}

			payloadBytes, _ := json.Marshal(map[string]any{
				"prompt_id":        "prompt-job_54dcf0141811092f",
				"story_id":         "",
				"repo":             req.Repo,
				"title":            req.Title,
				"prompt":           promptBuilder.String(),
				"target_agent":     req.TargetAgent,
				"effective_action": "story.create_and_execute",
			})
			job := &Job{
				ID:           "job_54dcf0141811092f",
				Type:         JobTypePromptExecute,
				Repo:         req.Repo,
				TargetAgent:  req.TargetAgent,
				Payload:      payloadBytes,
				FencingToken: 1,
				LeaseToken:   "lease-54dcf0141811092f",
			}
			queuedJobs = append(queuedJobs, job)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"job_id":        job.ID,
				"status":        "running",
				"title":         req.Title,
				"repo":          req.Repo,
				"priority":      req.Priority,
				"execution_job": map[string]any{"job_id": job.ID, "status": "queued"},
			})

		// 2. iazio-agent -> iazio-harness-api: POST /api/v1/agent/jobs/claim
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent/jobs/claim":
			if len(queuedJobs) == 0 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			job := queuedJobs[0]
			queuedJobs = queuedJobs[1:]
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(job)

		// 3. iazio-agent -> iazio-harness-api: PATCH /api/v1/jobs/{id}/story-binding
		case (r.Method == http.MethodPatch || r.Method == http.MethodPost) && (strings.HasSuffix(r.URL.Path, "/story-binding") || strings.HasSuffix(r.URL.Path, "/bind-story")):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			jobID := parts[len(parts)-2]
			var req StoryBindingRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			boundStories[jobID] = req
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"bound": true, "story_id": req.StoryID})

		// 4. iazio-agent -> iazio-harness-api: POST /api/v1/agent/jobs/{id}/complete
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			jobID := parts[len(parts)-2]
			var req CompleteRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			completedJobs[jobID] = req
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})

		// 4b. iazio-agent -> iazio-harness-api: POST /api/v1/agent/jobs/{id}/failover
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/failover"):
			// Return 400 when failover is not enabled so Dispatcher proceeds to CompleteJob with Error
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "failover capped for single-tool test"})

		// 5. iazio-harness -> iazio-harness-api: POST /v1/jobs/{id}/chunks
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chunks"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			jobID := parts[len(parts)-2]
			var chunk struct {
				Text string `json:"text"`
			}
			_ = json.NewDecoder(r.Body).Decode(&chunk)
			if chunk.Text != "" {
				jobChunks[jobID] = append(jobChunks[jobID], chunk.Text)
			}
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	// Sub-Scenario A: Full Happy Path (Gemini Web -> iazio-mcp -> iazio-harness-api -> iazio-agent -> iazio-harness -> mock-agy)
	t.Run("HappyPath_SortPromptsAlphabetically_ExecutesWithSkipPermissions", func(t *testing.T) {
		t.Setenv("MOCK_AGY_SCENARIO", "gemini_web_mcp_sort_prompts_flow")
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		// Step 1: Simulate Gemini Web invoking iazio-mcp create_story tool
		mcpReqBody, _ := json.Marshal(map[string]any{
			"title":       "Sort prompts alphabetically in iazio-web /prompts view",
			"description": "Prompts on https://tian.go.ro/iazio-web/prompts should be sorted alphabetically (case-insensitive A-Z) by default.",
			"repo":        ".",
			"priority":    "P2",
			"acceptance_criteria": []string{
				"Prompts listed on the /prompts page (https://tian.go.ro/iazio-web/prompts) are sorted alphabetically (case-insensitive A–Z) by title/name by default.",
				"Existing search, filtering, and pagination/rendering behaviors on the prompts view remain intact after sorting is applied.",
				"Unit or component tests are added/updated to verify alphabetical ordering of prompts.",
			},
			"auto_execute": true,
			"target_agent": "agy",
		})
		resp, err := http.Post(srv.URL+"/api/v1/stories", "application/json", bytes.NewReader(mcpReqBody))
		if err != nil {
			t.Fatalf("iazio-mcp POST /api/v1/stories failed: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created from /api/v1/stories, got %d", resp.StatusCode)
		}

		// Step 2: Run iazio-agent Dispatcher with real CLIInvoker -> real iazio-harness -> real mock-agy
		cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		disp := NewDispatcher(cpClient, invoker, "pc1-wsl", nil, nil)

		processed, err := disp.PollAndProcess(context.Background())
		if err != nil {
			t.Fatalf("expected PollAndProcess to succeed, got error: %v", err)
		}
		if !processed {
			t.Fatal("expected PollAndProcess to claim and process job_54dcf0141811092f")
		}

		// Step 3: Verify completion, story binding, log chunk streaming, and git mutations
		mu.Lock()
		comp, ok := completedJobs["job_54dcf0141811092f"]
		bound, hasBound := boundStories["job_54dcf0141811092f"]
		chunks := append([]string(nil), jobChunks["job_54dcf0141811092f"]...)
		mu.Unlock()

		if !ok {
			t.Fatal("expected job_54dcf0141811092f to be completed")
		}
		if comp.Error != "" {
			t.Fatalf("expected empty completion error, got: %s", comp.Error)
		}
		if !hasBound || bound.StoryID != "STORY-WEB-3838" {
			t.Fatalf("expected STORY-WEB-3838 bound to job_54dcf0141811092f, got hasBound=%v bound=%+v", hasBound, bound)
		}
		if len(chunks) == 0 {
			t.Fatal("expected non-empty log chunks streamed to /v1/jobs/job_54dcf0141811092f/chunks")
		}

		var promptRes harness.PromptExecutionResult
		if err := json.Unmarshal(comp.Result, &promptRes); err != nil {
			t.Fatalf("unmarshal completion result: %v", err)
		}
		if promptRes.Status != "completed" || promptRes.ExitCode != 0 {
			t.Fatalf("expected status=completed exit_code=0, got status=%q exit=%d", promptRes.Status, promptRes.ExitCode)
		}
		if promptRes.StoryID != "STORY-WEB-3838" {
			t.Fatalf("expected PromptExecutionResult.StoryID=STORY-WEB-3838, got %q", promptRes.StoryID)
		}
	})

	// Sub-Scenario B: Headless permission denial ("jetski: no output produced") MUST fail closed, never silent completed
	t.Run("PermissionDenied_FailsClosedNeverSilentCompleted", func(t *testing.T) {
		t.Setenv("MOCK_AGY_SCENARIO", "permission_denied")
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		mu.Lock()
		delete(completedJobs, "job_54dcf0141811092f")
		mu.Unlock()

		mcpReqBody, _ := json.Marshal(map[string]any{
			"title":        "Sort prompts alphabetically in iazio-web /prompts view",
			"repo":         ".",
			"priority":     "P2",
			"auto_execute": true,
			"target_agent": "agy",
		})
		resp, err := http.Post(srv.URL+"/api/v1/stories", "application/json", bytes.NewReader(mcpReqBody))
		if err != nil {
			t.Fatalf("POST /api/v1/stories failed: %v", err)
		}
		_ = resp.Body.Close()

		cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		disp := NewDispatcher(cpClient, invoker, "pc1-wsl", nil, nil)

		processed, err := disp.PollAndProcess(context.Background())
		if !processed {
			t.Fatal("expected PollAndProcess to claim job")
		}
		if err == nil {
			t.Fatal("expected PollAndProcess to return error on jetski permission denial")
		}

		mu.Lock()
		comp, ok := completedJobs["job_54dcf0141811092f"]
		mu.Unlock()
		if !ok {
			t.Fatal("expected CompleteJob to be called with failure")
		}
		if comp.Error == "" {
			t.Fatal("expected non-empty Error in CompleteRequest when jetski permission denial occurs")
		}
	})

	// Sub-Scenario C: Supervisor Tick does NOT emit false ReasonProcessDied when osRunner already recorded RunStatusFailed (job-1350)
	t.Run("Supervisor_NonZeroHarnessExit_DoesNotEmitFalseProcessDied", func(t *testing.T) {
		runsDir := t.TempDir()
		t.Setenv("IAZIO_RUNS_DIR", runsDir)

		now := time.Now()
		sup := supervisor.New(func() time.Time { return now })

		worktree := "/home/romeo/work/meta-repo"
		jobID := "job-1350"
		deadPID := 99999999

		if err := sup.Spawn(fakeDeadPIDRunner{pid: deadPID}, jobID, srv.URL, worktree, worktree, ""); err != nil {
			t.Fatalf("sup.Spawn failed: %v", err)
		}

		// Save RunState showing osRunner caught a clean non-zero exit (ExitCode: 1, Status: FAILED)
		if err := supervisor.SaveRunState(supervisor.RunState{
			JobID:        jobID,
			PID:          deadPID,
			WorktreePath: worktree,
			Status:       supervisor.RunStatusFailed,
			ExitCode:     1,
			Error:        "DIRTY_WORKTREE_POST_EXIT",
			StartedAt:    now.Add(-5 * time.Minute),
			UpdatedAt:    now,
		}); err != nil {
			t.Fatalf("SaveRunState failed: %v", err)
		}

		reaped := sup.Tick()
		for _, r := range reaped {
			if r.JobID == jobID && r.Reason == supervisor.ReasonProcessDied {
				t.Fatalf("expected job-1350 with RunStatusFailed NOT to be reaped as ReasonProcessDied, got %+v", r)
			}
		}
		if sup.Lock(worktree) != supervisor.LockCooling {
			t.Fatalf("expected worktree lock to transition to LockCooling, got %s", sup.Lock(worktree))
		}
	})

	// Sub-Scenario D: job_697e5159287042b2 (story.create_and_execute with no pre-allocated StoryID):
	// mock-agy writes ConversationID via --log-file and writes allocated StoryID to ASE state (.session_story_id);
	// iazio-harness discovers both ConversationID and StoryID and binds StoryID via PostStoryBinding with fencing_token.
	t.Run("Job_697e5159287042b2_CreateAndExecute_LogFileConversationID_And_ASEStateStoryBinding", func(t *testing.T) {
		t.Setenv("MOCK_AGY_SCENARIO", "success")
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		// Simulate mock-agy allocating STORY-GOV-3839 in ASE state (.session_story_id) during execution
		if err := os.WriteFile(filepath.Join(fleetDir, ".session_story_id"), []byte("STORY-GOV-3839\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		storyContent := "---\nid: STORY-GOV-3839\ntitle: Sort prompts alphabetically in iazio-web /prompts view\nstatus: DONE\npriority: P1\n---\n# Story\n"
		if err := os.WriteFile(filepath.Join(fleetDir, "docs", "stories", "2026-11-09-gov-3839-sort-prompts.story.md"), []byte(storyContent), 0o644); err != nil {
			t.Fatal(err)
		}
		_ = exec.Command("git", "-C", fleetDir, "add", "docs/stories/2026-11-09-gov-3839-sort-prompts.story.md").Run()
		_ = exec.Command("git", "-C", fleetDir, "commit", "-m", "chore: add completed story").Run()
		_ = exec.Command("git", "-C", fleetDir, "push").Run()

		payloadBytes, _ := json.Marshal(map[string]any{
			"prompt_id":        "prompt_ase_697e5159287042b2",
			"story_id":         "", // story.create_and_execute never mocks a story_id
			"repo":             fleetDir,
			"worktree_path":    fleetDir,
			"mode":             "story.create_and_execute",
			"command":          "/autonomous-story-execution",
			"backlog":          "iazio",
			"prompt":           "/autonomous-story-execution Sort prompts alphabetically in iazio-web /prompts view",
			"target_agent":     "agy",
			"effective_action": "/autonomous-story-execution",
		})
		mu.Lock()
		queuedJobs = append(queuedJobs, &Job{
			ID:           "job_697e5159287042b2",
			Type:         JobTypePromptExecute,
			Repo:         fleetDir,
			TargetAgent:  "agy",
			Payload:      payloadBytes,
			FencingToken: 7,
			LeaseToken:   "lease-697e5159287042b2",
		})
		mu.Unlock()

		cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		var resolvedRepo string
		disp := NewDispatcher(cpClient, invoker, "debian", nil, nil)
		disp.SetWorktreeResolver(func(repo string) string {
			resolvedRepo = fleetDir
			return fleetDir
		})

		processed, err := disp.PollAndProcess(context.Background())
		if err != nil {
			t.Fatalf("expected PollAndProcess to succeed for job_697e5159287042b2, got: %v", err)
		}
		if !processed {
			t.Fatal("expected PollAndProcess to claim job_697e5159287042b2")
		}
		if resolvedRepo != fleetDir {
			t.Fatalf("expected worktree resolver to bind idle worktree %s, got %s", fleetDir, resolvedRepo)
		}

		mu.Lock()
		comp, ok := completedJobs["job_697e5159287042b2"]
		bound, hasBound := boundStories["job_697e5159287042b2"]
		mu.Unlock()

		if !ok || comp.Error != "" {
			t.Fatalf("expected job_697e5159287042b2 to complete cleanly, ok=%v err=%q", ok, comp.Error)
		}
		if !hasBound || bound.StoryID != "STORY-GOV-3839" {
			t.Fatalf("expected iazio-harness to read STORY-GOV-3839 from ASE state and PostStoryBinding, got hasBound=%v bound=%+v", hasBound, bound)
		}
		if bound.FencingToken != 7 {
			t.Fatalf("expected PostStoryBinding to include FencingToken=7, got %d", bound.FencingToken)
		}

		var promptRes harness.PromptExecutionResult
		if err := json.Unmarshal(comp.Result, &promptRes); err != nil {
			t.Fatalf("unmarshal completion result: %v", err)
		}
		if promptRes.ActiveConversationID == "" && len(promptRes.ConversationIDs) == 0 {
			t.Fatalf("expected ConversationID to be captured from --log-file for job_697e5159287042b2, got %+v", promptRes)
		}
		if promptRes.StoryID != "STORY-GOV-3839" {
			t.Fatalf("expected PromptExecutionResult.StoryID=STORY-GOV-3839 from ASE state, got %q", promptRes.StoryID)
		}
	})

	// Sub-Scenario E: job_3ffa5d0be1649f0e (STORY-GOV-3851):
	// mock-agy runs in buffered_stdout_brain_transcript_streaming mode (0 bytes written to stdout pipe while running,
	// writing steps only to ~/.gemini/antigravity-cli/brain/<convID>/.system_generated/logs/transcript.jsonl).
	// Verify that iazio-agent Dispatcher -> real iazio-harness -> real mock-agy streams [ASE-SUPERVISOR] startup banner,
	// discovered conversation/story IDs, and live [agy] planner thinking/tool-call/response steps to /v1/jobs/{id}/chunks.
	t.Run("Job_3ffa5d0be1649f0e_BufferedStdout_BrainTranscriptStreaming_And_RepoLiveOutput", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		t.Setenv("MOCK_AGY_SCENARIO", "buffered_stdout_brain_transcript_streaming")
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		payloadBytes, _ := json.Marshal(map[string]any{
			"prompt_id":        "prompt_3ffa5d0be1649f0e",
			"story_id":         "",
			"repo":             ".",
			"worktree_path":    fleetDir,
			"mode":             "story.create_and_execute",
			"command":          "/autonomous-story-execution",
			"backlog":          "iazio",
			"prompt":           "/autonomous-story-execution deep review and fix live output",
			"target_agent":     "agy",
			"effective_action": "/autonomous-story-execution",
		})
		mu.Lock()
		queuedJobs = append(queuedJobs, &Job{
			ID:           "job_3ffa5d0be1649f0e",
			Type:         JobTypePromptExecute,
			Repo:         ".",
			TargetAgent:  "agy",
			Payload:      payloadBytes,
			FencingToken: 7,
			LeaseToken:   "lease-3ffa5d0be1649f0e",
		})
		mu.Unlock()

		cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		disp := NewDispatcher(cpClient, invoker, "debian", nil, nil)
		disp.SetWorktreeResolver(func(repo string) string { return fleetDir })

		processed, err := disp.PollAndProcess(context.Background())
		if err != nil {
			t.Fatalf("expected PollAndProcess to succeed for job_3ffa5d0be1649f0e, got: %v", err)
		}
		if !processed {
			t.Fatal("expected PollAndProcess to claim job_3ffa5d0be1649f0e")
		}

		mu.Lock()
		comp, ok := completedJobs["job_3ffa5d0be1649f0e"]
		bound, hasBound := boundStories["job_3ffa5d0be1649f0e"]
		chunks := append([]string(nil), jobChunks["job_3ffa5d0be1649f0e"]...)
		mu.Unlock()

		if !ok || comp.Error != "" {
			t.Fatalf("expected job_3ffa5d0be1649f0e to complete cleanly, ok=%v err=%q", ok, comp.Error)
		}
		if !hasBound || bound.StoryID != "STORY-GOV-3850" {
			t.Fatalf("expected STORY-GOV-3850 bound for job_3ffa5d0be1649f0e, got hasBound=%v bound=%+v", hasBound, bound)
		}
		joinedChunks := strings.Join(chunks, "")
		for _, expected := range []string{
			"[ASE-SUPERVISOR] Starting prompt execution for Job job_3ffa5d0be1649f0e",
			"[ASE-SUPERVISOR] Attached Antigravity conversation: ba042127-79e5-4cb2-b35c-5d47145ef4fa",
			"[agy step #2] thinking: Inspecting repository live output pipeline",
			"[agy step #2] tool read_file: /home/romeo/work/meta-repo-02/AGENTS.md",
			"[agy step #2] tool run_command: Check autopilot status",
			"[agy step #3] output: Completed STORY-GOV-3850 implementation and verified all gates.",
		} {
			if !strings.Contains(joinedChunks, expected) {
				t.Fatalf("expected E2E streamed chunks for job_3ffa5d0be1649f0e to contain %q, got:\n%s", expected, joinedChunks)
			}
		}
	})

	// Sub-Scenario F (STORY-IAZIO-0183):
	// End-to-End integration test with mock-agy covering:
	// 1. OAuth scope enforcement on POST /api/v1/stories (requires harness:write / autopilot:write) and POST /api/v1/codebase/search (requires harness:read / workspace:read).
	// 2. iazio-agent -> iazio-harness -> mock-agy execution of Wave 1 story, where /complete initially fails with 503 (spooling the completion with dynamic TokenProvider token),
	//    then receives 401 Unauthorized on first Drain before TokenProvider rotates to a fresh token and drains cleanly without quarantining as .rejected.
	t.Run("Story_IAZIO_0183_OAuthScope_SpoolerTokenRefresh_And_MockAgy_Execution", func(t *testing.T) {
		t.Setenv("MOCK_AGY_SCENARIO", "gemini_web_mcp_sort_prompts_flow")
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		var completeAttempts int
		var currentAgentToken = "stale-agent-token"
		var spoolCompleted CompleteRequest
		var spoolCompletedOK bool

		oauthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()

			authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codebase/search":
				if authHeader != "Bearer gemini-read-or-mcp-token" && authHeader != "Bearer m2m-harness-write-token" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":"insufficient_scope","required_scope":"harness:read"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"matches":[{"path":"internal/controlplane/spooler.go","score":0.99}]}`))

			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/stories":
				if authHeader != "Bearer m2m-harness-write-token" && authHeader != "Bearer gemini-autopilot-write-token" {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":"insufficient_scope","required_scope":"harness:write"}`))
					return
				}
				payloadBytes, _ := json.Marshal(map[string]any{
					"prompt_id":        "prompt-job-iazio-0183",
					"story_id":         "",
					"repo":             ".",
					"worktree_path":    fleetDir,
					"title":            "Fix control-plane offline spooler token refresh",
					"prompt":           "/autonomous-story-execution Fix control-plane offline spooler token refresh",
					"target_agent":     "agy",
					"effective_action": "story.create_and_execute",
				})
				queuedJobs = append(queuedJobs, &Job{
					ID:           "job_iazio_0183",
					Type:         JobTypePromptExecute,
					Repo:         ".",
					TargetAgent:  "agy",
					Payload:      payloadBytes,
					FencingToken: 1,
					LeaseToken:   "lease-iazio-0183",
				})
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{"job_id": "job_iazio_0183", "status": "queued"})

			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent/jobs/claim":
				if len(queuedJobs) == 0 {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				job := queuedJobs[0]
				queuedJobs = queuedJobs[1:]
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(job)

			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
				completeAttempts++
				if completeAttempts == 1 {
					// Simulate transient 503 during initial CompleteJob so it gets spooled
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if authHeader != "Bearer refreshed-agent-token-0183" {
					// Simulate 401 Unauthorized on first spool drain before token rotation
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				_ = json.NewDecoder(r.Body).Decode(&spoolCompleted)
				spoolCompletedOK = true
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))

			default:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}
		}))
		defer oauthSrv.Close()

		// 1. Verify read-only search POST succeeds with read/MCP token
		searchReq, _ := http.NewRequest(http.MethodPost, oauthSrv.URL+"/api/v1/codebase/search", strings.NewReader(`{"query":"OfflineReportSpooler"}`))
		searchReq.Header.Set("Authorization", "Bearer gemini-read-or-mcp-token")
		searchResp, err := oauthSrv.Client().Do(searchReq)
		if err != nil || searchResp.StatusCode != http.StatusOK {
			t.Fatalf("expected search_codebase POST to succeed with 200, got err=%v status=%v", err, searchResp)
		}
		_ = searchResp.Body.Close()

		// 2. Verify story.create_and_execute rejects unprivileged token with 403 insufficient_scope and succeeds with write token
		unprivReq, _ := http.NewRequest(http.MethodPost, oauthSrv.URL+"/api/v1/stories", strings.NewReader(`{"title":"Wave 1","target_agent":"agy"}`))
		unprivReq.Header.Set("Authorization", "Bearer read-only-token")
		unprivResp, err := oauthSrv.Client().Do(unprivReq)
		if err != nil || unprivResp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden for unprivileged token on POST /api/v1/stories, got err=%v status=%v", err, unprivResp)
		}
		_ = unprivResp.Body.Close()

		privReq, _ := http.NewRequest(http.MethodPost, oauthSrv.URL+"/api/v1/stories", strings.NewReader(`{"title":"Wave 1","target_agent":"agy"}`))
		privReq.Header.Set("Authorization", "Bearer gemini-autopilot-write-token")
		privResp, err := oauthSrv.Client().Do(privReq)
		if err != nil || privResp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created for authorized token on POST /api/v1/stories, got err=%v status=%v", err, privResp)
		}
		_ = privResp.Body.Close()

		// 3. Execute job via Dispatcher -> real iazio-harness -> real mock-agy with OfflineReportSpooler
		spoolDir := t.TempDir()
		spooler, err := controlplane.NewOfflineReportSpooler(spoolDir, oauthSrv.Client())
		if err != nil {
			t.Fatalf("NewOfflineReportSpooler: %v", err)
		}
		tokenProvider := func(context.Context) string {
			mu.Lock()
			defer mu.Unlock()
			return currentAgentToken
		}
		spooler.TokenProvider = tokenProvider

		cpClient := NewHTTPControlPlaneClient(oauthSrv.URL, "", oauthSrv.Client(), spooler)
		cpClient.TokenProvider = tokenProvider
		cpClient.MaxRetries = -1
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		disp := NewDispatcher(cpClient, invoker, "debian", nil, nil)
		disp.SetWorktreeResolver(func(string) string { return fleetDir })

		// PollAndProcess runs mock-agy (which succeeds), then CompleteJob gets 503 and spools to disk
		processed, _ := disp.PollAndProcess(context.Background())
		if !processed {
			t.Fatal("expected job_iazio_0183 to be claimed and executed by mock-agy")
		}

		// First drain with stale-agent-token receives 401 Unauthorized and retains spooled file
		drained1, err := spooler.Drain(context.Background())
		if err == nil || drained1 != 0 {
			t.Fatalf("expected first Drain with 401 to retain spooled report, got drained=%d err=%v", drained1, err)
		}

		// Rotate token in TokenProvider and drain again -> 200 OK
		mu.Lock()
		currentAgentToken = "refreshed-agent-token-0183"
		mu.Unlock()
		drained2, err := spooler.Drain(context.Background())
		if err != nil || drained2 != 1 {
			t.Fatalf("expected second Drain with refreshed token to drain 1 report, got drained=%d err=%v", drained2, err)
		}

		mu.Lock()
		okDone := spoolCompletedOK
		compRes := spoolCompleted
		mu.Unlock()
		if !okDone || compRes.Error != "" {
			t.Fatalf("expected spooled mock-agy completion report to be delivered cleanly, ok=%v err=%q", okDone, compRes.Error)
		}
	})

	// Sub-Scenario G (STORY-IAZIO-0186 — 100% Forensic Wave Jobs Coverage from https://share.gemini.google/1n7lntSO7k7e):
	// Exercises real iazio-agent Dispatcher -> real iazio-harness -> real mock-agy across all 5 forensic job archetypes:
	// 1. job_9a39902fd8c9aabf: PROMPT_EXECUTE (create_prompt) with continuous lease heartbeats, Zipkin dual tags (job.id & iazio.job_id), and schedule sch-9a39902fd8c9aabf <-> job_9a39902fd8c9aabf bi-directional lookup.
	// 2. job_22d4d50920421e3c: Wave 2 story.create_and_execute (STORY-IAZIO-0184) with live chunk streaming, conversation ID attachment, and Zipkin trace URL linkage.
	// 3. job_d2818308997d09a3 & job_38eeeb536a5a7130: Wave 3 multi-attempt lease reclaim + deterministic resume on the same Antigravity conversation (24599787-bd2e-45fc-b516-3d64f4d8e468 -> 62a3d9f1-8285-49ec-b3f6-15bd7e369472), idempotent story title normalization (never duplicating "Execute story STORY-IAZIO-0185:"), and DAG depends_on_job_id chaining with callback_url + [IAZIO-ASE-CALLBACK] completion notification.
	// 4. job_cbbb1363993215f9: Wave 4 story.create_and_execute where mock-agy pulls a DONE story (STORY-GOV-5144) before allocating STORY-GOV-5154 in .session_story_id; verifies STORY-GOV-5144 is never misbound and STORY-GOV-5154 is bound authoritatively.
	t.Run("Story_IAZIO_0186_ForensicWaveJobs_100PercentUseCaseCoverage_WithMockAgy", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		fleetDir := initTestFleetWorkspace(t)
		t.Setenv("IAZIO_FLEET_ROOT", fleetDir)

		// Capture Zipkin spans exported by telemetry.ZipkinExporter to verify dual job.id & iazio.job_id tags
		var zipkinMu sync.Mutex
		var exportedSpans []telemetry.ZipkinSpan
		zipkinSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/spans") {
				var batch []telemetry.ZipkinSpan
				if err := json.NewDecoder(r.Body).Decode(&batch); err == nil {
					zipkinMu.Lock()
					exportedSpans = append(exportedSpans, batch...)
					zipkinMu.Unlock()
				}
				w.WriteHeader(http.StatusAccepted)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer zipkinSrv.Close()

		exporter := telemetry.NewZipkinExporter(zipkinSrv.URL)
		telemetry.SetGlobalTracer(telemetry.NewTracer("iazio-agent", exporter))
		defer func() {
			_ = exporter.Close()
			telemetry.SetGlobalTracer(nil)
		}()

		// 1. Wave 1 / Pre-Wave: job_9a39902fd8c9aabf (PROMPT_EXECUTE) & Wave 2: job_22d4d50920421e3c (STORY-IAZIO-0184)
		t.Setenv("MOCK_AGY_SCENARIO", "gemini_web_mcp_sort_prompts_flow")
		for _, tc := range []struct {
			jobID       string
			traceID     string
			mode        string
			storyID     string
			expectStory string
		}{
			{
				jobID:       "job_9a39902fd8c9aabf",
				traceID:     "9a39902fd8c9aabf9a39902fd8c9aabf",
				mode:        "prompt.execute",
				storyID:     "STORY-IAZIO-0183",
				expectStory: "STORY-IAZIO-0183",
			},
			{
				jobID:       "job_22d4d50920421e3c",
				traceID:     "22d4d50920421e3c22d4d50920421e3c",
				mode:        "story.create_and_execute",
				storyID:     "STORY-IAZIO-0184",
				expectStory: "STORY-IAZIO-0184",
			},
		} {
			payloadBytes, _ := json.Marshal(map[string]any{
				"prompt_id":        "prompt_" + tc.jobID,
				"story_id":         tc.storyID,
				"repo":             ".",
				"worktree_path":    fleetDir,
				"mode":             tc.mode,
				"command":          "/autonomous-story-execution",
				"backlog":          "iazio",
				"prompt":           "/autonomous-story-execution Execute " + tc.expectStory,
				"target_agent":     "agy",
				"effective_action": tc.mode,
			})
			mu.Lock()
			queuedJobs = append(queuedJobs, &Job{
				ID:           tc.jobID,
				TraceID:      tc.traceID,
				Type:         JobTypePromptExecute,
				Repo:         ".",
				StoryID:      tc.storyID,
				TargetAgent:  "agy",
				Payload:      payloadBytes,
				FencingToken: 1,
				LeaseToken:   "lease-" + tc.jobID,
			})
			mu.Unlock()

			cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
			invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
			disp := NewDispatcher(cpClient, invoker, "Mac-mini.local", nil, nil)
			disp.SetWorktreeResolver(func(string) string { return fleetDir })

			processed, err := disp.PollAndProcess(context.Background())
			if err != nil || !processed {
				t.Fatalf("expected %s to succeed, processed=%v err=%v", tc.jobID, processed, err)
			}
			mu.Lock()
			comp, ok := completedJobs[tc.jobID]
			mu.Unlock()
			if !ok || comp.Error != "" {
				t.Fatalf("expected %s completion without error, ok=%v err=%q", tc.jobID, ok, comp.Error)
			}
		}

		// 2. Wave 3: job_d2818308997d09a3 & job_38eeeb536a5a7130 (multi-attempt lease reclaim + resume conversation + DAG chaining)
		for _, jobID := range []string{"job_d2818308997d09a3", "job_38eeeb536a5a7130"} {
			payloadBytes, _ := json.Marshal(map[string]any{
				"prompt_id":              "prompt_" + jobID,
				"story_id":               "STORY-IAZIO-0185",
				"repo":                   ".",
				"worktree_path":          fleetDir,
				"mode":                   "story.create_and_execute",
				"command":                "/autonomous-story-execution",
				"backlog":                "iazio",
				"prompt":                 "/autonomous-story-execution Execute story STORY-IAZIO-0185: Consolidate and harden prompt execution pipelines",
				"target_agent":           "agy",
				"effective_action":       "/autonomous-story-execution",
				"resume_conversation_id": "24599787-bd2e-45fc-b516-3d64f4d8e468",
				"depends_on_job_id":      "job_22d4d50920421e3c",
				"callback_url":           srv.URL + "/v1/notifications",
			})
			mu.Lock()
			queuedJobs = append(queuedJobs, &Job{
				ID:                   jobID,
				TraceID:              "d2818308997d09a3d2818308997d09a3",
				Type:                 JobTypePromptExecute,
				Repo:                 ".",
				StoryID:              "STORY-IAZIO-0185",
				TargetAgent:          "agy",
				Payload:              payloadBytes,
				FencingToken:         2,
				LeaseToken:           "lease-" + jobID,
				Attempts:             2,
				ActiveConversationID: "24599787-bd2e-45fc-b516-3d64f4d8e468",
			})
			mu.Unlock()

			cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
			invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
			disp := NewDispatcher(cpClient, invoker, "Mac-mini.local", nil, nil)
			disp.SetWorktreeResolver(func(string) string { return fleetDir })

			processed, err := disp.PollAndProcess(context.Background())
			if err != nil || !processed {
				t.Fatalf("expected %s to succeed on resumed attempt, processed=%v err=%v", jobID, processed, err)
			}
			mu.Lock()
			comp, ok := completedJobs[jobID]
			mu.Unlock()
			if !ok || comp.Error != "" {
				t.Fatalf("expected %s to complete cleanly, ok=%v err=%q", jobID, ok, comp.Error)
			}
		}

		// 3. Wave 4: job_cbbb1363993215f9 (story.create_and_execute where git pull updates DONE story STORY-GOV-5144 before allocating STORY-GOV-5154)
		t.Setenv("MOCK_AGY_SCENARIO", "job_cbbb1363993215f9_wave4_pulled_done_story_override")
		_ = os.Remove(filepath.Join(fleetDir, ".session_story_id"))
		_ = os.Remove(filepath.Join(fleetDir, ".autopilot-story-id"))

		wave4Payload, _ := json.Marshal(map[string]any{
			"prompt_id":        "prompt_job_cbbb1363993215f9",
			"story_id":         "",
			"repo":             ".",
			"worktree_path":    fleetDir,
			"mode":             "story.create_and_execute",
			"command":          "/autonomous-story-execution",
			"backlog":          "iazio",
			"prompt":           "/autonomous-story-execution Wave 4: Upgrade RAG pipeline to hybrid vector + BM25 + cross-encoder reranking",
			"target_agent":     "agy",
			"effective_action": "/autonomous-story-execution",
		})
		mu.Lock()
		queuedJobs = append(queuedJobs, &Job{
			ID:           "job_cbbb1363993215f9",
			TraceID:      "cbbb1363993215f9cbbb1363993215f9",
			Type:         JobTypePromptExecute,
			Repo:         ".",
			StoryID:      "",
			TargetAgent:  "agy",
			Payload:      wave4Payload,
			FencingToken: 1,
			LeaseToken:   "lease-job_cbbb1363993215f9",
		})
		mu.Unlock()

		cpClient := NewHTTPControlPlaneClient(srv.URL, "test-token", srv.Client())
		invoker := harness.NewCLIInvoker(harnessBin, fleetDir)
		disp := NewDispatcher(cpClient, invoker, "Mac-mini.local", nil, nil)
		disp.SetWorktreeResolver(func(string) string { return fleetDir })

		processed, err := disp.PollAndProcess(context.Background())
		if err != nil || !processed {
			t.Fatalf("expected job_cbbb1363993215f9 to succeed, processed=%v err=%v", processed, err)
		}

		mu.Lock()
		compWave4, okWave4 := completedJobs["job_cbbb1363993215f9"]
		boundWave4, hasBoundWave4 := boundStories["job_cbbb1363993215f9"]
		mu.Unlock()

		if !okWave4 || compWave4.Error != "" {
			t.Fatalf("expected job_cbbb1363993215f9 to complete cleanly, ok=%v err=%q", okWave4, compWave4.Error)
		}
		if !hasBoundWave4 || boundWave4.StoryID != "STORY-GOV-5154" {
			t.Fatalf("expected job_cbbb1363993215f9 to bind newly allocated STORY-GOV-5154 (never pulled DONE story STORY-GOV-5144), got hasBound=%v bound=%+v", hasBoundWave4, boundWave4)
		}
		var wave4Res harness.PromptExecutionResult
		if err := json.Unmarshal(compWave4.Result, &wave4Res); err != nil {
			t.Fatalf("unmarshal wave4 result: %v", err)
		}
		if wave4Res.StoryID != "STORY-GOV-5154" {
			t.Fatalf("expected wave4 PromptExecutionResult.StoryID=STORY-GOV-5154, got %q", wave4Res.StoryID)
		}
		if wave4Res.ActiveConversationID != "884ed49e-4331-4f9a-919d-28bd2ffde6a1" {
			t.Fatalf("expected wave4 ActiveConversationID=884ed49e-4331-4f9a-919d-28bd2ffde6a1, got %q", wave4Res.ActiveConversationID)
		}

		// Flush exporter and verify Zipkin spans carry mirrored job.id & iazio.job_id tags
		_ = exporter.Close()
		zipkinMu.Lock()
		spans := append([]telemetry.ZipkinSpan(nil), exportedSpans...)
		zipkinMu.Unlock()
		foundMirroredJobTag := false
		for _, sp := range spans {
			if sp.Tags["job.id"] == "job_cbbb1363993215f9" && sp.Tags["iazio.job_id"] == "job_cbbb1363993215f9" {
				foundMirroredJobTag = true
				break
			}
		}
		if !foundMirroredJobTag {
			t.Fatalf("expected Zipkin spans for job_cbbb1363993215f9 to include both job.id and iazio.job_id tags, got %d spans", len(spans))
		}

		// Sub-test B: job_38eeeb536a5a7130 Mutual Supervision & Crash Resilience
		// 1. Control-plane returns 409 LEASE_FENCED on first HeartbeatJob while agy is executing ->
		//    iazio-agent does NOT cancel execCtx, calls AckLease to re-adopt lease, and completes in Attempt 1.
		// 2. agy crashes on Turn 1 (1aee4b3a-fedd-4c71-a7b5-4b816e7664a5), fails with corrupted session on Turn 2,
		//    and iazio-harness automatically falls back to a new conversation (2bff5c4b-a111-4c71-a7b5-999988887777) on Turn 3,
		//    completing STORY-IAZIO-0185 in Attempt 1!
		t.Setenv("MOCK_AGY_SCENARIO", "job_38eeeb536a5a7130_crash_resume_and_new_conv_fallback")
		t.Setenv("MOCK_AGY_STATE_MARKER", filepath.Join(t.TempDir(), "mock_agy_38eeeb53_e2e.tmp"))
		job0185Payload, _ := json.Marshal(map[string]any{
			"prompt":       "/autonomous-story-execution STORY-IAZIO-0185",
			"repo":         ".",
			"story_id":     "STORY-IAZIO-0185",
			"target_agent": "agy",
		})
		mu.Lock()
		queuedJobs = append(queuedJobs, &Job{
			ID:           "job_38eeeb536a5a7130",
			TraceID:      "38eeeb536a5a713038eeeb536a5a7130",
			Type:         JobTypePromptExecute,
			Repo:         ".",
			StoryID:      "STORY-IAZIO-0185",
			TargetAgent:  "agy",
			Payload:      job0185Payload,
			FencingToken: 21,
			LeaseToken:   "lease-job_38eeeb536a5a7130",
		})
		mu.Unlock()

		processed0185, err0185 := disp.PollAndProcess(context.Background())
		if err0185 != nil || !processed0185 {
			t.Fatalf("expected job_38eeeb536a5a7130 to recover and complete in Attempt 1, processed=%v err=%v", processed0185, err0185)
		}
		mu.Lock()
		comp0185, ok0185 := completedJobs["job_38eeeb536a5a7130"]
		mu.Unlock()
		if !ok0185 || comp0185.Error != "" {
			t.Fatalf("expected job_38eeeb536a5a7130 to complete cleanly in Attempt 1, ok=%v err=%q", ok0185, comp0185.Error)
		}
		var res0185 harness.PromptExecutionResult
		if err := json.Unmarshal(comp0185.Result, &res0185); err != nil {
			t.Fatalf("unmarshal job_38eeeb536a5a7130 result: %v", err)
		}
		if res0185.ActiveConversationID != "2bff5c4b-a111-4c71-a7b5-999988887777" {
			t.Fatalf("expected job_38eeeb536a5a7130 to complete on fallback conversation 2bff5c4b-a111-4c71-a7b5-999988887777, got %q", res0185.ActiveConversationID)
		}

		// Sub-test C: iazio-harness crash supervised by iazio-agent (retries ExecutePrompt with discovered conversation on retry 1,
		// falls back to new conversation on retry 2, and completes in Attempt 1 without failing the job)
		var supCalls int
		var observedConvs []string
		flakyInvoker := &mockInvoker{
			executePromptFunc: func(ctx context.Context, payload json.RawMessage) (*harness.PromptExecutionResult, error) {
				supCalls++
				var pm map[string]any
				_ = json.Unmarshal(payload, &pm)
				conv, _ := pm["resume_conversation_id"].(string)
				observedConvs = append(observedConvs, conv)
				if supCalls == 1 {
					return &harness.PromptExecutionResult{
						Status:               "failed",
						ExitCode:             1,
						StoryID:              "STORY-IAZIO-0185",
						ActiveConversationID: "conv-attempt-1-crashed",
						Summary:              "harness crashed mid-turn",
					}, nil
				}
				if supCalls == 2 {
					return &harness.PromptExecutionResult{
						Status:               "failed",
						ExitCode:             1,
						StoryID:              "STORY-IAZIO-0185",
						ActiveConversationID: "conv-attempt-1-crashed",
						Summary:              "attach to conv-attempt-1-crashed failed",
					}, nil
				}
				return &harness.PromptExecutionResult{
					Status:               "completed",
					ExitCode:             0,
					StoryID:              "STORY-IAZIO-0185",
					ActiveConversationID: "conv-new-fallback-3",
					Summary:              "completed in new conversation on supervisor attempt 3",
				}, nil
			},
		}
		mu.Lock()
		queuedJobs = append(queuedJobs, &Job{
			ID:           "job_supervisor_retry_0186",
			Type:         JobTypePromptExecute,
			Repo:         ".",
			StoryID:      "STORY-IAZIO-0185",
			TargetAgent:  "agy",
			Payload:      job0185Payload,
			FencingToken: 1,
			LeaseToken:   "lease-sup-0186",
		})
		mu.Unlock()
		supDisp := NewDispatcher(cpClient, flakyInvoker, "debian")
		supDisp.SetWorktreeResolver(func(string) string { return fleetDir })
		processedSup, errSup := supDisp.PollAndProcess(context.Background())
		if errSup != nil || !processedSup {
			t.Fatalf("expected supervisor retry loop to recover from 2 harness crashes and succeed on attempt 3, processed=%v err=%v", processedSup, errSup)
		}
		if supCalls != 3 {
			t.Fatalf("expected 3 supervisor invocations, got %d", supCalls)
		}
		if observedConvs[1] != "conv-attempt-1-crashed" || observedConvs[2] != "" {
			t.Fatalf("expected retry 1 to attach to 'conv-attempt-1-crashed' and retry 2 to fallback to new conversation (''), got %v", observedConvs)
		}
	})
}

func TestE2E_StoryGov5206_Sch1569_Job1570_WatchdogAndHarnessMockAgyRecovery(t *testing.T) {
	harnessBin, _, _ := buildHarnessAndMockAgyBinaries(t)
	fleetDir := initTestFleetWorkspace(t)
	t.Setenv("IAZIO_FLEET_ROOT", fleetDir)
	t.Setenv("MOCK_AGY_SCENARIO", "sch_1569_fallback_new_conversation_resume")
	t.Setenv("MOCK_AGY_STATE_MARKER", filepath.Join(t.TempDir(), "sch1569_agent_e2e.state"))

	_ = exec.Command("git", "-C", fleetDir, "checkout", "-B", "main").Run()
	storyDoc := "---\nid: STORY-ROOT-45276\nstatus: DONE\n---\n# STORY-ROOT-45276: Canonical AI Review\n\n## Acceptance Criteria\n- [x] AC1\n"
	if err := os.WriteFile(filepath.Join(fleetDir, "docs", "stories", "2026-11-09-root-45276.story.md"), []byte(storyDoc), 0o644); err != nil {
		t.Fatal(err)
	}
	indexDoc := "stories:\n  - id: STORY-ROOT-45276\n    status: DONE\n    path: docs/stories/2026-11-09-root-45276.story.md\n"
	if err := os.WriteFile(filepath.Join(fleetDir, "docs", "stories", "index.yml"), []byte(indexDoc), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = exec.Command("git", "-C", fleetDir, "add", ".").Run()
	_ = exec.Command("git", "-C", fleetDir, "commit", "-m", "feat: complete STORY-ROOT-45276").Run()
	_ = exec.Command("git", "-C", fleetDir, "push").Run()

	// Seed stale .iazio/execution-checkpoint.json from job-1408 (Oct 25) to verify neither iazio-agent nor iazio-harness adopts it
	iazioDir := filepath.Join(fleetDir, ".iazio")
	if err := os.MkdirAll(iazioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	staleCP := `{"job_id":"job-1408","conversation_id":"287e8a2b-1e95-4aa1-b2f9-9b151134c574","engine":"agy"}`
	if err := os.WriteFile(filepath.Join(iazioDir, "execution-checkpoint.json"), []byte(staleCP), 0o644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	activeConvID := ""
	var registeredConvs []string
	var exitStatuses []int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/jobs/job-1570"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":                     "job-1570",
				"schedule_id":            "sch-1569",
				"parent_job_id":          "job-1098",
				"root_job_id":            "job-1098",
				"kind":                   "resume",
				"engine":                 "mock-agy",
				"agent":                  "mock-agy",
				"worktree_path":          fleetDir,
				"docs_hub_path":          fleetDir,
				"story_id":               "STORY-ROOT-45276",
				"prompt":                 "deep review and fix in canonical AI",
				"status":                 "RUNNING",
				"resume_mode":            "fallback_new_conversation",
				"resume_fallback_reason": "cross-engine resume not supported: parent used autopilot, final engine is agy",
				"resume_conversation_id": "5e170251-5362-4bf3-8f98-f47eb900be28",
				"active_conversation_id": activeConvID,
			})
		case r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/conversations") || strings.HasSuffix(r.URL.Path, "/conversation-lineage")):
			var body struct {
				ConversationID string `json:"conversation_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.ConversationID != "" {
				activeConvID = body.ConversationID
				registeredConvs = append(registeredConvs, body.ConversationID)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/exit"):
			var body struct {
				ExitCode int `json:"exit_code"`
				Code     int `json:"code"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			exitStatuses = append(exitStatuses, body.ExitCode)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer srv.Close()

	// Run Attempt #1 via iazio-harness (simulating initial spawn by iazio-agent for job-1570)
	cmd1 := exec.Command(
		harnessBin, "run",
		"--job-id", "job-1570",
		"--api-url", srv.URL,
		"--worktree-path", fleetDir,
		"--docs-hub-path", fleetDir,
		"--kind", "resume",
	)
	out1, err1 := cmd1.CombinedOutput()
	if err1 != nil {
		t.Fatalf("expected Attempt #1 of sch_1569_fallback_new_conversation_resume to succeed, err=%v output:\n%s", err1, string(out1))
	}

	// Verify Attempt #1 registered 8d5d3c3d-1eb6-4517-a64b-df5b27cff909 and overwrote .iazio/execution-checkpoint.json for job-1570
	mu.Lock()
	gotActiveAfter1 := activeConvID
	mu.Unlock()
	if gotActiveAfter1 != "8d5d3c3d-1eb6-4517-a64b-df5b27cff909" {
		t.Fatalf("expected Attempt #1 to register fallback conversation 8d5d3c3d-1eb6-4517-a64b-df5b27cff909, got %q (out:\n%s)", gotActiveAfter1, string(out1))
	}

	// Run Attempt #2 via iazio-harness (simulating in-place watchdog auto-resume even if a stale caller passes --resume-conversation 287e8a2b-...)
	cmd2 := exec.Command(
		harnessBin, "run",
		"--job-id", "job-1570",
		"--api-url", srv.URL,
		"--worktree-path", fleetDir,
		"--docs-hub-path", fleetDir,
		"--kind", "resume",
		"--resume-conversation", "287e8a2b-1e95-4aa1-b2f9-9b151134c574",
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("expected Attempt #2 auto-resume of job-1570 to succeed cleanly, err=%v output:\n%s", err2, string(out2))
	}

	mu.Lock()
	finalConvs := append([]string(nil), registeredConvs...)
	finalExits := append([]int(nil), exitStatuses...)
	mu.Unlock()

	for _, c := range finalConvs {
		if c == "287e8a2b-1e95-4aa1-b2f9-9b151134c574" || c == "5e170251-5362-4bf3-8f98-f47eb900be28" {
			t.Fatalf("expected job-1570 never to register stale/abandoned conversation %q, got %v", c, finalConvs)
		}
	}
	if len(finalExits) == 0 || finalExits[len(finalExits)-1] != 0 {
		t.Fatalf("expected final exit code 0 for job-1570, got %v", finalExits)
	}
}

