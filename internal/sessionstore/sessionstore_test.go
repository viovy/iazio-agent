package sessionstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractStoryID(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"2026-10-09-review-v0-44c767-STORY-APP-0098.md", "STORY-APP-0098"},
		{"docs/stories/2026-10-09-app-0098-feature.story.md", "STORY-APP-0098"},
		{"feat/app-0100-dirty-resumption", "STORY-APP-0100"},
		{"STORY-ENG-3369", "STORY-ENG-3369"},
		{"id: APP-0098", "STORY-APP-0098"},
		{"id: STORY-ENG-1234", "STORY-ENG-1234"},
		{"no-story-here.txt", ""},
	}
	for _, tc := range cases {
		got := ExtractStoryID(tc.input)
		if got != tc.want {
			t.Errorf("ExtractStoryID(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSessionStoreResolver(t *testing.T) {
	tmpDir := t.TempDir()
	worktree := filepath.Join(tmpDir, "my-repo")
	_ = os.MkdirAll(worktree, 0o755)

	spoolDir := filepath.Join(tmpDir, "spool")
	_ = os.MkdirAll(spoolDir, 0o755)

	brainDir := filepath.Join(tmpDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	resolver := &Resolver{
		SpoolDir: spoolDir,
		BrainDir: brainDir,
	}

	// 1. Initially no sessions
	sess, err := resolver.ResolveLatest(worktree)
	if err != ErrNoSessionFound || sess != nil {
		t.Fatalf("expected ErrNoSessionFound, got %v, %v", sess, err)
	}

	// 2. Add a brain session
	conv1 := "11111111-2222-3333-4444-555555555555"
	conv1Dir := filepath.Join(brainDir, conv1, ".system_generated", "logs")
	_ = os.MkdirAll(conv1Dir, 0o755)
	transcriptContent := `{"step_index":0,"content":"<USER_REQUEST>Fix something for STORY-APP-0098</USER_REQUEST>"}
{"step_index":1,"tool_calls":[{"name":"run_command","args":{"Cwd":"` + filepath.ToSlash(worktree) + `"}}]}
`
	_ = os.WriteFile(filepath.Join(conv1Dir, "transcript.jsonl"), []byte(transcriptContent), 0o644)

	// Set mtime to 1 hour ago
	_ = os.Chtimes(filepath.Join(conv1Dir, "transcript.jsonl"), time.Now().Add(-1*time.Hour), time.Now().Add(-1*time.Hour))

	sess, err = resolver.ResolveLatest(worktree)
	if err != nil || sess == nil {
		t.Fatalf("expected to resolve conv1 from brain, got %v, err=%v", sess, err)
	}
	if sess.ConversationID != conv1 {
		t.Fatalf("expected conv1 %s, got %s", conv1, sess.ConversationID)
	}
	if sess.Source != "brain" {
		t.Fatalf("expected source 'brain', got %s", sess.Source)
	}
	if sess.StoryID != "STORY-APP-0098" {
		t.Fatalf("expected story ID 'STORY-APP-0098', got %q", sess.StoryID)
	}

	// 3. Add a newer spool engine log
	conv2 := "66666666-7777-8888-9999-000000000000"
	engineLog := "I1009 05:11:44.883963 1 server.go:1274] Created conversation " + conv2 + "\n" +
		"working in " + worktree + "\n"
	_ = os.WriteFile(filepath.Join(spoolDir, "job-42.engine.log"), []byte(engineLog), 0o644)

	sess, err = resolver.ResolveLatest(worktree)
	if err != nil || sess == nil {
		t.Fatalf("expected to resolve conv2 from spool, got %v, err=%v", sess, err)
	}
	if sess.ConversationID != conv2 {
		t.Fatalf("expected conv2 %s, got %s", conv2, sess.ConversationID)
	}
	if sess.Source != "spool" {
		t.Fatalf("expected source 'spool', got %s", sess.Source)
	}

	// 4. Add a review markdown file with story in filename
	revDir := filepath.Join(worktree, "docs", "reviews")
	_ = os.MkdirAll(revDir, 0o755)
	convRev := "4fc5bf81-6e70-4800-af36-8da466524b99"
	revContent := `---
date: 2026-10-09
tool: agy
story_id: STORY-APP-0098
scheme_iteration: 0
verdict: CLEAN
conversation_id: "` + convRev + `"
---
# Review
`
	revFile := filepath.Join(revDir, "2026-10-09-review-v0-44c767-STORY-APP-0098.md")
	_ = os.WriteFile(revFile, []byte(revContent), 0o644)
	_ = os.Chtimes(revFile, time.Now().Add(5*time.Minute), time.Now().Add(5*time.Minute))

	sess, err = resolver.ResolveForStory(worktree, "STORY-APP-0098")
	if err != nil || sess == nil {
		t.Fatalf("expected to resolve convRev for STORY-APP-0098, got %v, err=%v", sess, err)
	}
	if sess.ConversationID != convRev {
		t.Fatalf("expected convRev %s, got %s", convRev, sess.ConversationID)
	}
	if sess.StoryID != "STORY-APP-0098" {
		t.Fatalf("expected story ID 'STORY-APP-0098', got %q", sess.StoryID)
	}
	if sess.Verdict != "CLEAN" {
		t.Fatalf("expected verdict 'CLEAN', got %q", sess.Verdict)
	}

	// 5. Add a worktree checkpoint with an even newer timestamp
	conv3 := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	iazioDir := filepath.Join(worktree, ".iazio")
	_ = os.MkdirAll(iazioDir, 0o755)
	cpJSON := `{
  "job_id": "job-99",
  "engine": "agy",
  "worktree_path": "` + filepath.ToSlash(worktree) + `",
  "conversation_id": "` + conv3 + `",
  "started_at": "` + time.Now().Add(10*time.Minute).UTC().Format(time.RFC3339) + `"
}`
	_ = os.WriteFile(filepath.Join(iazioDir, "execution-checkpoint.json"), []byte(cpJSON), 0o644)

	sess, err = resolver.ResolveLatest(worktree)
	if err != nil || sess == nil {
		t.Fatalf("expected to resolve conv3 from checkpoint, got %v, err=%v", sess, err)
	}
	if sess.ConversationID != conv3 {
		t.Fatalf("expected conv3 %s, got %s", conv3, sess.ConversationID)
	}
	if sess.Source != "checkpoint" {
		t.Fatalf("expected source 'checkpoint', got %s", sess.Source)
	}
}

func TestGetDirtyFilesParsing(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command("git", "init", dir)
	if err := cmd.Run(); err != nil {
		t.Skip("git not available")
	}
	_ = os.WriteFile(filepath.Join(dir, "committed.txt"), []byte("committed"), 0o644)
	_ = exec.Command("git", "-C", dir, "add", ".").Run()
	_ = exec.Command("git", "-C", dir, "-c", "user.name=tester", "-c", "user.email=tester@test.com", "commit", "-m", "init").Run()

	// 1. Modify tracked file (starts with ' M ')
	_ = os.WriteFile(filepath.Join(dir, "committed.txt"), []byte("changed"), 0o644)

	// 2. Add untracked review file (starts with '?? ')
	revDir := filepath.Join(dir, "docs", "reviews")
	_ = os.MkdirAll(revDir, 0o755)
	revPath := filepath.Join(revDir, "2026-10-09-review-v0-26991d-STORY-APP-0098.md")
	_ = os.WriteFile(revPath, []byte("---\nverdict: CLEAN\n---\n"), 0o644)

	files := getDirtyFiles(dir)
	if !files["committed.txt"] {
		t.Errorf("expected committed.txt in dirty files, got: %+v", files)
	}
	if !files["docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md"] {
		t.Errorf("expected review path in dirty files, got: %+v", files)
	}

	dCtx := DefaultResolver().DetectDirtyWorktreeContext(dir)
	if !dCtx.IsDirty {
		t.Errorf("expected IsDirty true")
	}
	if dCtx.StoryID != "STORY-APP-0098" {
		t.Errorf("expected StoryID STORY-APP-0098, got %q", dCtx.StoryID)
	}
	if dCtx.ReviewFile != "docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md" {
		t.Errorf("expected ReviewFile docs/reviews/..., got %q", dCtx.ReviewFile)
	}
}

func TestResolveForStory_NoPollutionAcrossDifferentStories(t *testing.T) {
	tmpDir := t.TempDir()
	worktree := filepath.Join(tmpDir, "my-repo")
	_ = os.MkdirAll(worktree, 0o755)

	revDir := filepath.Join(worktree, "docs", "reviews")
	_ = os.MkdirAll(revDir, 0o755)

	// Old historical review for STORY-LEGACY-0098
	oldConv := "0ddc4208-8d7a-490b-86bb-1dee2e91ae82"
	revFile := filepath.Join(revDir, "2026-10-09-review-v0-26991d-STORY-LEGACY-0098.md")
	revContent := "---\nstory_id: STORY-LEGACY-0098\nconversation_id: \"" + oldConv + "\"\n---\n"
	_ = os.WriteFile(revFile, []byte(revContent), 0o644)

	resolver := &Resolver{
		BrainDir: t.TempDir(),
		SpoolDir: t.TempDir(),
	}

	// Requesting an unrelated story (e.g. STORY-VIGO-0001) MUST NOT receive 0ddc4208...
	sess, err := resolver.ResolveForStory(worktree, "STORY-VIGO-0001")
	if err != ErrNoSessionFound {
		t.Fatalf("expected ErrNoSessionFound for unrelated story, got %v, err=%v", sess, err)
	}
	if sess != nil {
		t.Fatalf("expected nil session, got %+v", sess)
	}
}

func TestExtractReviewFrontmatter_TranscriptPath(t *testing.T) {
	content := "---\nstory_id: STORY-TEST-123\ntranscript_path: \"/tmp/brain/c550f22a-d19d-4c9a-b091-322e573301e6/transcript.jsonl\"\nscheme_iteration: 2\nverdict: PASS\n---\n"
	tmpFile := filepath.Join(t.TempDir(), "2026-10-22-review-v2-123456-STORY-TEST-123.md")
	_ = os.WriteFile(tmpFile, []byte(content), 0644)

	info := extractReviewFrontmatter(tmpFile)
	if info.StoryID != "STORY-TEST-123" {
		t.Fatalf("expected story ID STORY-TEST-123, got %q", info.StoryID)
	}
	if info.ConversationID != "c550f22a-d19d-4c9a-b091-322e573301e6" {
		t.Fatalf("expected conversation ID c550f22a-d19d-4c9a-b091-322e573301e6, got %q", info.ConversationID)
	}
	if info.Iteration != 2 || info.Verdict != "PASS" {
		t.Fatalf("unexpected info: %+v", info)
	}
}

func TestDetectDirtyWorktreeContext_DirtyStoryFile(t *testing.T) {
	tmpDir := t.TempDir()
	cmd := exec.Command("git", "init", tmpDir)
	_ = cmd.Run()

	storyDir := filepath.Join(tmpDir, "docs", "stories")
	_ = os.MkdirAll(storyDir, 0755)
	storyFile := filepath.Join(storyDir, "2026-10-22-my-feature.story.md")
	_ = os.WriteFile(storyFile, []byte("---\nid: STORY-FEAT-9999\n---\n"), 0644)

	dCtx := DefaultResolver().DetectDirtyWorktreeContext(tmpDir)
	if !dCtx.IsDirty {
		t.Fatalf("expected dirty context")
	}
	if dCtx.StoryID != "STORY-FEAT-9999" {
		t.Fatalf("expected STORY-FEAT-9999, got %q", dCtx.StoryID)
	}
}

func TestIsValidConversationID(t *testing.T) {
	if !IsValidConversationID("d6ae4fed-9a21-4708-a24e-eeb7dac91671") {
		t.Errorf("expected valid UUID conversation ID")
	}
	if IsValidConversationID("") {
		t.Errorf("expected empty conversation ID to be invalid")
	}
	if IsValidConversationID("../escape") {
		t.Errorf("expected traversal conversation ID to be invalid")
	}
	if IsValidConversationID("bad/slash") {
		t.Errorf("expected slash conversation ID to be invalid")
	}
}

func TestSessionExists(t *testing.T) {
	tmpBrain := t.TempDir()
	convID := "test-conv-1234"
	_ = os.MkdirAll(filepath.Join(tmpBrain, convID), 0755)

	res := &Resolver{BrainDir: tmpBrain}
	if !res.SessionExists(convID) {
		t.Errorf("expected session to exist in brain dir")
	}
	if res.SessionExists("nonexistent-conv") {
		t.Errorf("expected nonexistent session to not exist")
	}
	if res.SessionExists("invalid/id") {
		t.Errorf("expected invalid ID to return false")
	}
}
