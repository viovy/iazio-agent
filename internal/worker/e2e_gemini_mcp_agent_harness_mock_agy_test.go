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

	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/supervisor"
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
	runGit("init")
	runGit("config", "user.name", "Test Operator")
	runGit("config", "user.email", "operator@iazio.test")

	if err := os.WriteFile(filepath.Join(fleetDir, ".gitmodules"), []byte("[submodule \"iazio-web\"]\n\tpath = iazio-web\n\turl = https://github.com/viovy/iazio-web.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fleetDir, "docs", "stories"), 0o755); err != nil {
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
}
