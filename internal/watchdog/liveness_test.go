package watchdog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckLiveness_ActiveSignalsNotStalled(t *testing.T) {
	tmpDir := t.TempDir()
	liveDir := filepath.Join(tmpDir, "liveness")
	brainDir := filepath.Join(tmpDir, "brain")
	_ = os.MkdirAll(liveDir, 0755)
	_ = os.MkdirAll(brainDir, 0755)

	convID := "test-conv-1234"
	now := time.Now().UTC()

	// Write recent harvester liveness record
	rec := map[string]any{
		"conversation_id": convID,
		"last_event_time": now.Add(-10 * time.Second),
		"updated_at":      now.Add(-10 * time.Second),
	}
	data, _ := json.Marshal(rec)
	_ = os.WriteFile(filepath.Join(liveDir, convID+".json"), data, 0644)

	monitor := &ConversationLivenessMonitor{
		StallTimeout: 60 * time.Second,
		Now:          func() time.Time { return now },
		LivenessDir:  liveDir,
		BrainDir:     brainDir,
	}

	snap := monitor.CheckLiveness(tmpDir, convID, 12345, now.Add(-30*time.Second))
	if snap.IsStalled {
		t.Fatalf("expected not stalled, got stalled: %+v", snap)
	}
	if snap.IdleDurationSeconds > 15 {
		t.Errorf("expected idle ~10s, got %f", snap.IdleDurationSeconds)
	}
}

func TestCheckLiveness_IdleSignalsExceedingStallTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	liveDir := filepath.Join(tmpDir, "liveness")
	brainDir := filepath.Join(tmpDir, "brain")
	_ = os.MkdirAll(liveDir, 0755)
	_ = os.MkdirAll(filepath.Join(brainDir, "stalled-conv", ".system_generated", "logs"), 0755)

	convID := "stalled-conv"
	fixedNow := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	pastTime := fixedNow.Add(-400 * time.Second) // 400s idle

	// Write past transcript file
	tFile := filepath.Join(brainDir, convID, ".system_generated", "logs", "transcript.jsonl")
	_ = os.WriteFile(tFile, []byte(`{"step_index": 5, "content": "stalled response"}`+"\n"), 0644)
	_ = os.Chtimes(tFile, pastTime, pastTime)

	monitor := &ConversationLivenessMonitor{
		StallTimeout: 300 * time.Second,
		Now:          func() time.Time { return fixedNow },
		LivenessDir:  liveDir,
		BrainDir:     brainDir,
	}

	snap := monitor.CheckLiveness(tmpDir, convID, 99999, pastTime)
	if !snap.IsStalled {
		t.Fatalf("expected stalled conversation when idle > 300s, got not stalled: %+v", snap)
	}
	if snap.IdleDurationSeconds < 300 {
		t.Errorf("expected idle duration >= 300s, got %f", snap.IdleDurationSeconds)
	}
	if snap.LastTranscriptTurn == "" {
		t.Errorf("expected captured last transcript turn, got empty")
	}
}
