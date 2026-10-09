// Package sessionstore discovers existing engine sessions and conversation IDs
// on the local host to enable resilient in-place prompt resumption.
package sessionstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var ErrNoSessionFound = errors.New("no matching session found")

// Session represents a discovered engine session on the local host.
type Session struct {
	ConversationID string    `json:"conversation_id"`
	Engine         string    `json:"engine"`
	WorktreePath   string    `json:"worktree_path"`
	UpdatedAt      time.Time `json:"updated_at"`
	PromptSnippet  string    `json:"prompt_snippet,omitempty"`
	Source         string    `json:"source"` // "checkpoint", "spool", "brain", "review"
	JobID          string    `json:"job_id,omitempty"`
}

// Resolver scans local host locations for sessions matching a worktree path.
type Resolver struct {
	SpoolDir string
	BrainDir string
}

// DefaultResolver returns a Resolver configured with user home directories.
func DefaultResolver() *Resolver {
	home, _ := os.UserHomeDir()
	spoolDir := ""
	brainDir := ""
	if home != "" {
		spoolDir = filepath.Join(home, ".iazio", "spool")
		brainDir = filepath.Join(home, ".gemini", "antigravity-cli", "brain")
	}
	return &Resolver{
		SpoolDir: spoolDir,
		BrainDir: brainDir,
	}
}

// ResolveLatest finds the most recent conversation ID for the worktree across all tiers.
func (r *Resolver) ResolveLatest(worktreePath string) (*Session, error) {
	sessions, err := r.ListRecent(worktreePath, 1)
	if err != nil {
		return nil, err
	}
	if len(sessions) == 0 {
		return nil, ErrNoSessionFound
	}
	return &sessions[0], nil
}

// ListRecent returns matching sessions sorted by UpdatedAt descending and deduplicated.
func (r *Resolver) ListRecent(worktreePath string, limit int) ([]Session, error) {
	if worktreePath == "" {
		return nil, errors.New("worktreePath is required")
	}
	cleanWorktree := filepath.Clean(worktreePath)
	var candidates []Session

	// Tier 1: Worktree & Spool Checkpoints
	candidates = append(candidates, r.scanCheckpoints(cleanWorktree)...)

	// Tier 2: Spool Engine Logs
	candidates = append(candidates, r.scanSpoolLogs(cleanWorktree)...)

	// Tier 3: Antigravity CLI Brain Sessions
	candidates = append(candidates, r.scanBrainSessions(cleanWorktree)...)

	// Tier 4: Review Markdown files
	candidates = append(candidates, r.scanReviews(cleanWorktree)...)

	if len(candidates) == 0 {
		return nil, nil
	}

	// Sort by UpdatedAt descending
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].UpdatedAt.After(candidates[j].UpdatedAt)
	})

	// Deduplicate by ConversationID
	seen := make(map[string]bool)
	var result []Session
	for _, s := range candidates {
		if s.ConversationID == "" || seen[s.ConversationID] {
			continue
		}
		seen[s.ConversationID] = true
		result = append(result, s)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

// scanCheckpoints reads .iazio/execution-checkpoint.json and spool checkpoints.
func (r *Resolver) scanCheckpoints(worktree string) []Session {
	var sessions []Session

	// 1. Worktree checkpoint
	wtCp := filepath.Join(worktree, ".iazio", "execution-checkpoint.json")
	if s, ok := readCheckpointFile(wtCp); ok && s.ConversationID != "" {
		s.WorktreePath = worktree
		s.Source = "checkpoint"
		sessions = append(sessions, s)
	}

	// 2. Spool checkpoints
	if r.SpoolDir != "" {
		entries, err := os.ReadDir(r.SpoolDir)
		if err == nil {
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".checkpoint.json") {
					p := filepath.Join(r.SpoolDir, e.Name())
					if s, ok := readCheckpointFile(p); ok && s.ConversationID != "" {
						if filepath.Clean(s.WorktreePath) == worktree {
							s.Source = "checkpoint"
							sessions = append(sessions, s)
						}
					}
				}
			}
		}
	}
	return sessions
}

func readCheckpointFile(p string) (Session, bool) {
	data, err := os.ReadFile(p)
	if err != nil {
		return Session{}, false
	}
	var raw struct {
		JobID          string `json:"job_id"`
		Engine         string `json:"engine"`
		WorktreePath   string `json:"worktree_path"`
		ConversationID string `json:"conversation_id"`
		StartedAt      string `json:"started_at"`
		UpdatedAt      string `json:"updated_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.ConversationID == "" {
		return Session{}, false
	}
	ts := time.Now()
	if raw.UpdatedAt != "" {
		if t, err := time.Parse(time.RFC3339, raw.UpdatedAt); err == nil {
			ts = t
		}
	} else if raw.StartedAt != "" {
		if t, err := time.Parse(time.RFC3339, raw.StartedAt); err == nil {
			ts = t
		}
	}
	return Session{
		ConversationID: raw.ConversationID,
		Engine:         raw.Engine,
		WorktreePath:   raw.WorktreePath,
		UpdatedAt:      ts,
		JobID:          raw.JobID,
	}, true
}

// scanSpoolLogs scans spool *.engine.log files for conversation IDs.
func (r *Resolver) scanSpoolLogs(worktree string) []Session {
	var sessions []Session
	if r.SpoolDir == "" {
		return sessions
	}
	entries, err := os.ReadDir(r.SpoolDir)
	if err != nil {
		return sessions
	}

	markers := []string{
		"Created conversation ",
		"Print mode: conversation=",
		"conversation=",
		"Streaming conversation ",
	}

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".engine.log") {
			continue
		}
		p := filepath.Join(r.SpoolDir, e.Name())
		fi, err := e.Info()
		if err != nil {
			continue
		}

		jobID := strings.TrimSuffix(e.Name(), ".engine.log")
		// Check corresponding job log or engine log for worktree match
		matched := false
		var convID string
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		lineCount := 0
		for scanner.Scan() && lineCount < 200 {
			line := scanner.Text()
			lineCount++
			if strings.Contains(line, worktree) {
				matched = true
			}
			if convID == "" {
				for _, m := range markers {
					if idx := strings.Index(line, m); idx != -1 {
						rest := line[idx+len(m):]
						fields := strings.FieldsFunc(rest, func(rn rune) bool {
							return rn == ' ' || rn == '\t' || rn == ',' || rn == '"' || rn == '\''
						})
						if len(fields) > 0 && len(fields[0]) >= 32 {
							convID = strings.Trim(fields[0], " .,;:\"'")
							break
						}
					}
				}
			}
			if matched && convID != "" {
				break
			}
		}
		_ = f.Close()

		if matched && convID != "" {
			sessions = append(sessions, Session{
				ConversationID: convID,
				Engine:         "agy",
				WorktreePath:   worktree,
				UpdatedAt:      fi.ModTime(),
				Source:         "spool",
				JobID:          jobID,
			})
		}
	}
	return sessions
}

// scanBrainSessions iterates ~/.gemini/antigravity-cli/brain/*/ looking for matching transcripts.
func (r *Resolver) scanBrainSessions(worktree string) []Session {
	var sessions []Session
	if r.BrainDir == "" {
		return sessions
	}
	entries, err := os.ReadDir(r.BrainDir)
	if err != nil {
		return sessions
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		convID := e.Name()
		if len(convID) < 32 {
			continue
		}
		transcriptPath := filepath.Join(r.BrainDir, convID, ".system_generated", "logs", "transcript.jsonl")
		fi, err := os.Stat(transcriptPath)
		if err != nil {
			continue
		}

		f, err := os.Open(transcriptPath)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1024*1024), 2*1024*1024)
		matched := false
		promptSnippet := ""

		for i := 0; i < 10 && scanner.Scan(); i++ {
			text := scanner.Text()
			if strings.Contains(text, worktree) {
				matched = true
			}
			if promptSnippet == "" && strings.Contains(text, "<USER_REQUEST>") {
				start := strings.Index(text, "<USER_REQUEST>") + len("<USER_REQUEST>")
				end := strings.Index(text, "</USER_REQUEST>")
				if end > start {
					snippet := strings.TrimSpace(text[start:end])
					if len(snippet) > 80 {
						snippet = snippet[:80] + "..."
					}
					promptSnippet = snippet
				}
			}
			if matched && promptSnippet != "" {
				break
			}
		}
		_ = f.Close()

		if matched {
			sessions = append(sessions, Session{
				ConversationID: convID,
				Engine:         "agy",
				WorktreePath:   worktree,
				UpdatedAt:      fi.ModTime(),
				PromptSnippet:  promptSnippet,
				Source:         "brain",
			})
		}
	}
	return sessions
}

// scanReviews reads frontmatter from review markdown files under <worktree>/docs/reviews/.
func (r *Resolver) scanReviews(worktree string) []Session {
	var sessions []Session
	revDir := filepath.Join(worktree, "docs", "reviews")
	entries, err := os.ReadDir(revDir)
	if err != nil {
		return sessions
	}

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		p := filepath.Join(revDir, e.Name())
		fi, err := e.Info()
		if err != nil {
			continue
		}
		convID, engine := extractReviewFrontmatter(p)
		if convID != "" {
			sessions = append(sessions, Session{
				ConversationID: convID,
				Engine:         engine,
				WorktreePath:   worktree,
				UpdatedAt:      fi.ModTime(),
				Source:         "review",
			})
		}
	}
	return sessions
}

func extractReviewFrontmatter(p string) (convID, engine string) {
	f, err := os.Open(p)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	inFrontmatter := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "---" {
			if !inFrontmatter {
				inFrontmatter = true
				continue
			}
			break
		}
		if inFrontmatter {
			if strings.HasPrefix(line, "conversation_id:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					convID = strings.Trim(parts[1], " \"'")
				}
			} else if strings.HasPrefix(line, "tool:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					engine = strings.Trim(parts[1], " \"'")
				}
			}
		}
	}
	return convID, engine
}
