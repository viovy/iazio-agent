package sessionstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	transcriptContent := `{"step_index":0,"content":"<USER_REQUEST>Fix something</USER_REQUEST>"}
{"step_index":1,"tool_calls":[{"name":"run_command","args":{"Cwd":"` + worktree + `"}}]}
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
	if sess.PromptSnippet != "Fix something" {
		t.Fatalf("expected prompt snippet 'Fix something', got %q", sess.PromptSnippet)
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

	// 4. Add a worktree checkpoint with an even newer timestamp
	conv3 := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	iazioDir := filepath.Join(worktree, ".iazio")
	_ = os.MkdirAll(iazioDir, 0o755)
	cpJSON := `{
  "job_id": "job-99",
  "engine": "agy",
  "worktree_path": "` + worktree + `",
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

	// 5. Test ListRecent with multiple sessions
	list, err := resolver.ListRecent(worktree, 10)
	if err != nil {
		t.Fatalf("ListRecent failed: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(list))
	}
	// Verify order: conv3 (newest), conv2, conv1
	if list[0].ConversationID != conv3 || list[1].ConversationID != conv2 || list[2].ConversationID != conv1 {
		t.Fatalf("unexpected order: %+v", list)
	}
}
