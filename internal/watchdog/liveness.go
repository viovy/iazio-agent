// Package watchdog provides conversation liveness monitoring, stall detection, and autonomous auto-resume.
package watchdog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

// LivenessSnapshot captures the multi-signal liveness telemetry for an active conversation.
type LivenessSnapshot struct {
	ConversationID        string    `json:"conversation_id"`
	LastTranscriptTime    time.Time `json:"last_transcript_time"`
	LastSubprocessTime    time.Time `json:"last_subprocess_time"`
	LastWorkspaceTime     time.Time `json:"last_workspace_time"`
	LastActivityTime      time.Time `json:"last_activity_time"`
	IdleDurationSeconds   float64   `json:"idle_duration_seconds"`
	IsStalled             bool      `json:"is_stalled"`
	ProcessTreeDiagnostic string    `json:"process_tree_diagnostic,omitempty"`
	OpenFilesDiagnostic   string    `json:"open_files_diagnostic,omitempty"`
	LastTranscriptTurn    string    `json:"last_transcript_turn,omitempty"`
}

// ConversationLivenessMonitor inspects real-time signals to identify hung or stalled conversations.
type ConversationLivenessMonitor struct {
	StallTimeout time.Duration
	Now          func() time.Time
	LivenessDir  string
	BrainDir     string
}

// NewConversationLivenessMonitor creates a monitor with standard defaults.
func NewConversationLivenessMonitor(stallTimeout time.Duration) *ConversationLivenessMonitor {
	if stallTimeout <= 0 {
		stallTimeout = 300 * time.Second
	}
	return &ConversationLivenessMonitor{
		StallTimeout: stallTimeout,
		Now:          time.Now,
	}
}

func (m *ConversationLivenessMonitor) now() time.Time {
	if m.Now != nil {
		return m.Now().UTC()
	}
	return time.Now().UTC()
}

// CheckLiveness evaluates the 3 liveness signals for a running conversation.
func (m *ConversationLivenessMonitor) CheckLiveness(worktreePath, convID string, pid int, processStartTime time.Time) LivenessSnapshot {
	now := m.now()
	cleanConvID := strings.TrimSpace(convID)
	cleanConvID = strings.TrimPrefix(cleanConvID, "agy-")

	snap := LivenessSnapshot{
		ConversationID: cleanConvID,
	}

	// 1. Transcript Turn / Streaming Delta Timestamp
	snap.LastTranscriptTime = m.resolveTranscriptTimestamp(cleanConvID)

	// 2. Subprocess Tree Activity Timestamp
	snap.LastSubprocessTime = m.resolveSubprocessTimestamp(pid)

	// 3. Workspace Filesystem Mutation Timestamp
	snap.LastWorkspaceTime = m.resolveWorkspaceTimestamp(worktreePath)

	// If any signal was zero, use processStartTime as baseline if available
	baseline := processStartTime
	if baseline.IsZero() {
		baseline = now
	}

	maxTime := baseline
	if snap.LastTranscriptTime.After(maxTime) {
		maxTime = snap.LastTranscriptTime
	}
	if snap.LastSubprocessTime.After(maxTime) {
		maxTime = snap.LastSubprocessTime
	}
	if snap.LastWorkspaceTime.After(maxTime) {
		maxTime = snap.LastWorkspaceTime
	}

	snap.LastActivityTime = maxTime
	idle := now.Sub(maxTime)
	if idle < 0 {
		idle = 0
	}
	snap.IdleDurationSeconds = idle.Seconds()

	if idle >= m.StallTimeout && pid > 0 {
		snap.IsStalled = true
		snap.ProcessTreeDiagnostic = m.captureProcessTree(pid)
		snap.LastTranscriptTurn = m.captureLastTranscriptTurn(cleanConvID)
	}

	return snap
}

func (m *ConversationLivenessMonitor) resolveTranscriptTimestamp(convID string) time.Time {
	if convID == "" {
		return time.Time{}
	}

	// 1a. Check harvester liveness record
	dir := m.LivenessDir
	if dir == "" {
		if env := os.Getenv("IAZIO_LIVENESS_DIR"); env != "" {
			dir = env
		} else if home, err := os.UserHomeDir(); err == nil && home != "" {
			dir = filepath.Join(home, ".iazio", "liveness")
		}
	}
	if dir != "" {
		livePath := filepath.Join(dir, convID+".json")
		if data, err := os.ReadFile(livePath); err == nil {
			var rec struct {
				LastEventTime time.Time `json:"last_event_time"`
				UpdatedAt     time.Time `json:"updated_at"`
			}
			if err := json.Unmarshal(data, &rec); err == nil {
				if !rec.LastEventTime.IsZero() {
					return rec.LastEventTime.UTC()
				}
				if !rec.UpdatedAt.IsZero() {
					return rec.UpdatedAt.UTC()
				}
			}
		}
	}

	// 1b. Check Antigravity transcript file stat
	bDir := m.BrainDir
	if bDir == "" {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			bDir = filepath.Join(home, ".gemini", "antigravity-cli", "brain")
		}
	}
	if bDir != "" {
		tPath := filepath.Join(bDir, convID, ".system_generated", "logs", "transcript.jsonl")
		if fi, err := os.Stat(tPath); err == nil {
			return fi.ModTime().UTC()
		}
	}

	return time.Time{}
}

func (m *ConversationLivenessMonitor) resolveSubprocessTimestamp(pid int) time.Time {
	if pid <= 0 {
		return time.Time{}
	}

	// Check /proc/<pid>/stat or process mtime if available (Linux)
	procPath := fmt.Sprintf("/proc/%d", pid)
	if fi, err := os.Stat(procPath); err == nil {
		return fi.ModTime().UTC()
	}

	// Darwin/macOS fallback: check if child processes exist under pid
	cmdDarwin := procutil.Command("pgrep", "-P", strconv.Itoa(pid))
	if outDarwin, errDarwin := cmdDarwin.CombinedOutput(); errDarwin == nil && len(strings.TrimSpace(string(outDarwin))) > 0 {
		return time.Now().UTC()
	}

	return time.Time{}
}

func (m *ConversationLivenessMonitor) resolveWorkspaceTimestamp(worktreePath string) time.Time {
	if worktreePath == "" {
		return time.Time{}
	}

	// Check git index mtime
	gitIndex := filepath.Join(worktreePath, ".git", "index")
	if fi, err := os.Stat(gitIndex); err == nil {
		return fi.ModTime().UTC()
	}

	gitLogsHead := filepath.Join(worktreePath, ".git", "logs", "HEAD")
	if fi, err := os.Stat(gitLogsHead); err == nil {
		return fi.ModTime().UTC()
	}

	return time.Time{}
}

func (m *ConversationLivenessMonitor) captureProcessTree(pid int) string {
	if pid <= 0 {
		return ""
	}
	// Try GNU ps first (Linux)
	cmd := procutil.Command("ps", "-f", "--ppid", strconv.Itoa(pid))
	out, err := cmd.CombinedOutput()
	if err == nil && len(out) > 0 {
		return strings.TrimSpace(string(out))
	}
	// Darwin / BSD fallback
	cmdDarwin := procutil.Command("pgrep", "-P", strconv.Itoa(pid))
	if outDarwin, errDarwin := cmdDarwin.CombinedOutput(); errDarwin == nil && len(outDarwin) > 0 {
		return fmt.Sprintf("Child PIDs of %d:\n%s", pid, strings.TrimSpace(string(outDarwin)))
	}
	return fmt.Sprintf("PID %d running", pid)
}

func (m *ConversationLivenessMonitor) captureLastTranscriptTurn(convID string) string {
	if convID == "" {
		return ""
	}
	bDir := m.BrainDir
	if bDir == "" {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			bDir = filepath.Join(home, ".gemini", "antigravity-cli", "brain")
		}
	}
	if bDir == "" {
		return ""
	}
	tPath := filepath.Join(bDir, convID, ".system_generated", "logs", "transcript.jsonl")
	f, err := os.Open(tPath)
	if err != nil {
		return ""
	}
	defer f.Close()

	var lastLine string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		text := scanner.Text()
		if strings.TrimSpace(text) != "" {
			lastLine = text
		}
	}
	if len(lastLine) > 500 {
		return lastLine[len(lastLine)-500:]
	}
	return lastLine
}
