// Package sessionstore discovers existing engine sessions and conversation IDs
// on the local host to enable resilient in-place prompt resumption.
package sessionstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrNoSessionFound = errors.New("no matching session found")

var (
	storyIDRegex = regexp.MustCompile(`(?i)\b(STORY-[A-Z0-9]+-[0-9]+)\b`)
	prefixStoryRegex = regexp.MustCompile(`(?i)\b([a-z0-9]{2,10})-([0-9]{3,5})\b`)
	uuidRegex    = regexp.MustCompile(`(?i)\b([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\b`)
)

// ExtractStoryID finds the canonical story ID (e.g. STORY-APP-0098) in a string or filename.
func ExtractStoryID(s string) string {
	if m := storyIDRegex.FindStringSubmatch(s); len(m) > 1 {
		return strings.ToUpper(m[1])
	}
	if m := prefixStoryRegex.FindStringSubmatch(s); len(m) > 2 {
		return "STORY-" + strings.ToUpper(m[1]) + "-" + m[2]
	}
	return ""
}

// Session represents a discovered engine session on the local host.
type Session struct {
	ConversationID string    `json:"conversation_id"`
	Engine         string    `json:"engine"`
	WorktreePath   string    `json:"worktree_path"`
	UpdatedAt      time.Time `json:"updated_at"`
	PromptSnippet  string    `json:"prompt_snippet,omitempty"`
	Source         string    `json:"source"` // "checkpoint", "spool", "brain", "review"
	JobID          string    `json:"job_id,omitempty"`
	StoryID        string    `json:"story_id,omitempty"`
	Verdict        string    `json:"verdict,omitempty"`
	Iteration      int       `json:"iteration,omitempty"`
	ReviewPath     string    `json:"review_path,omitempty"`
	IsDirty        bool      `json:"is_dirty,omitempty"`
}

// DirtyContext holds diagnostic information about an in-flight or halted worktree.
type DirtyContext struct {
	StoryID        string `json:"story_id,omitempty"`
	ReviewFile     string `json:"review_file,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	Verdict        string `json:"verdict,omitempty"`
	Iteration      int    `json:"iteration,omitempty"`
	IsDirty        bool   `json:"is_dirty"`
	Branch         string `json:"branch,omitempty"`
}

// ReviewInfo holds metadata extracted from review files.
type ReviewInfo struct {
	ConversationID string
	Engine         string
	StoryID        string
	Verdict        string
	Iteration      int
	TranscriptPath string
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

// ResolveForStory finds the most recent conversation ID matching a specific story ID.
func (r *Resolver) ResolveForStory(worktreePath, storyID string) (*Session, error) {
	normTarget := strings.ToUpper(strings.TrimSpace(storyID))
	if normTarget == "" {
		return r.ResolveLatest(worktreePath)
	}

	// 1. Check recent sessions for worktree
	sessions, _ := r.ListRecent(worktreePath, 20)
	for _, s := range sessions {
		if strings.ToUpper(s.StoryID) == normTarget || (s.StoryID != "" && ExtractStoryID(s.StoryID) == normTarget) {
			return &s, nil
		}
	}

	// 2. Scan brain sessions specifically for this story ID
	brainSessions := r.scanBrainSessionsByStory(worktreePath, normTarget)
	if len(brainSessions) > 0 {
		return &brainSessions[0], nil
	}

	// 3. Fallback to latest session
	return r.ResolveLatest(worktreePath)
}

// DetectDirtyWorktreeContext inspects git status and files to diagnose an interrupted story.
func (r *Resolver) DetectDirtyWorktreeContext(worktree string) DirtyContext {
	ctx := DirtyContext{}
	if worktree == "" {
		return ctx
	}
	dirtyFiles := getDirtyFiles(worktree)
	if len(dirtyFiles) == 0 {
		return ctx
	}
	ctx.IsDirty = true

	// 1. Look for review files in dirty set
	for rel := range dirtyFiles {
		if strings.HasPrefix(rel, "docs/reviews/") && strings.HasSuffix(rel, ".md") {
			ctx.ReviewFile = rel
			fullPath := filepath.Join(worktree, rel)
			info := extractReviewFrontmatter(fullPath)
			ctx.StoryID = info.StoryID
			ctx.ConversationID = info.ConversationID
			ctx.Verdict = info.Verdict
			ctx.Iteration = info.Iteration
			break
		}
	}

	// 2. If no story ID from review, look for dirty story files
	if ctx.StoryID == "" {
		for rel := range dirtyFiles {
			if strings.HasPrefix(rel, "docs/stories/") && strings.HasSuffix(rel, ".story.md") {
				if sid := ExtractStoryID(filepath.Base(rel)); sid != "" {
					ctx.StoryID = sid
					break
				}
			}
		}
	}

	// 3. If still no story ID, inspect current branch
	gitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(gitCtx, "git", "-C", worktree, "branch", "--show-current")
	if out, err := cmd.CombinedOutput(); err == nil {
		branch := strings.TrimSpace(string(out))
		ctx.Branch = branch
		if ctx.StoryID == "" {
			ctx.StoryID = ExtractStoryID(branch)
		}
	}

	// 4. If conversation ID is missing, attempt resolution via story ID
	if ctx.ConversationID == "" && ctx.StoryID != "" {
		if sess, err := r.ResolveForStory(worktree, ctx.StoryID); err == nil && sess != nil {
			ctx.ConversationID = sess.ConversationID
		}
	}

	return ctx
}

// ListRecent returns matching sessions sorted by priority and recency.
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

	// Tier 3: Review Markdown files (includes dirty review inspection)
	candidates = append(candidates, r.scanReviews(cleanWorktree)...)

	// Tier 4: Antigravity CLI Brain Sessions
	candidates = append(candidates, r.scanBrainSessions(cleanWorktree)...)

	if len(candidates) == 0 {
		return nil, nil
	}

	// Sort by Dirty status first, then UpdatedAt descending
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].IsDirty != candidates[j].IsDirty {
			return candidates[i].IsDirty
		}
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
		StoryID        string `json:"story_id"`
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
		StoryID:        raw.StoryID,
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
		storyID := ""

		for i := 0; i < 15 && scanner.Scan(); i++ {
			text := scanner.Text()
			if strings.Contains(text, worktree) {
				matched = true
			}
			if storyID == "" {
				storyID = ExtractStoryID(text)
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
				StoryID:        storyID,
			})
		}
	}
	return sessions
}

func (r *Resolver) scanBrainSessionsByStory(worktree, storyID string) []Session {
	var sessions []Session
	if r.BrainDir == "" || storyID == "" {
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

		for i := 0; i < 20 && scanner.Scan(); i++ {
			text := scanner.Text()
			if strings.Contains(text, storyID) || strings.Contains(strings.ToLower(text), strings.ToLower(storyID)) {
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
				StoryID:        storyID,
			})
		}
	}

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
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

	dirtyFiles := getDirtyFiles(worktree)

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		p := filepath.Join(revDir, e.Name())
		fi, err := e.Info()
		if err != nil {
			continue
		}

		isDirty := dirtyFiles[e.Name()] || dirtyFiles[filepath.Join("docs", "reviews", e.Name())]
		info := extractReviewFrontmatter(p)

		convID := info.ConversationID
		// If conversation ID is missing, attempt to resolve from brain by story ID
		if convID == "" && info.StoryID != "" {
			if brainSess := r.scanBrainSessionsByStory(worktree, info.StoryID); len(brainSess) > 0 {
				convID = brainSess[0].ConversationID
			}
		}

		if convID != "" {
			modTime := fi.ModTime()
			if isDirty {
				modTime = time.Now()
			}
			sessions = append(sessions, Session{
				ConversationID: convID,
				Engine:         info.Engine,
				WorktreePath:   worktree,
				UpdatedAt:      modTime,
				Source:         "review",
				StoryID:        info.StoryID,
				Verdict:        info.Verdict,
				Iteration:      info.Iteration,
				ReviewPath:     p,
				IsDirty:        isDirty,
			})
		}
	}
	return sessions
}

func extractReviewFrontmatter(p string) ReviewInfo {
	info := ReviewInfo{
		StoryID: ExtractStoryID(filepath.Base(p)),
	}
	f, err := os.Open(p)
	if err != nil {
		return info
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
					info.ConversationID = strings.Trim(parts[1], " \"'")
				}
			} else if strings.HasPrefix(line, "tool:") || strings.HasPrefix(line, "requested_tool:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 && info.Engine == "" {
					info.Engine = strings.Trim(parts[1], " \"'")
				}
			} else if strings.HasPrefix(line, "story_id:") || strings.HasPrefix(line, "story_slug:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					sid := strings.Trim(parts[1], " \"'")
					if sid != "" {
						info.StoryID = sid
					}
				}
			} else if strings.HasPrefix(line, "verdict:") || strings.HasPrefix(line, "state:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 && info.Verdict == "" {
					info.Verdict = strings.Trim(parts[1], " \"'")
				}
			} else if strings.HasPrefix(line, "scheme_iteration:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						info.Iteration = n
					}
				}
			} else if strings.HasPrefix(line, "transcript_path:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					info.TranscriptPath = strings.Trim(parts[1], " \"'")
					if info.ConversationID == "" {
						if m := uuidRegex.FindString(info.TranscriptPath); m != "" {
							info.ConversationID = m
						}
					}
				}
			}
		}
	}
	return info
}

func getDirtyFiles(worktree string) map[string]bool {
	dirty := make(map[string]bool)
	if worktree == "" {
		return dirty
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", worktree, "status", "--porcelain")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return dirty
	}
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) < 4 {
			continue
		}
		pathPart := strings.TrimSpace(line[3:])
		if idx := strings.Index(pathPart, " -> "); idx != -1 {
			pathPart = strings.TrimSpace(pathPart[idx+4:])
		}
		dirty[pathPart] = true
		dirty[filepath.Base(pathPart)] = true
	}
	return dirty
}
