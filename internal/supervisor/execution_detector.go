package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-core/pkg/procutil"
)

// ActiveHarnessSignal represents a detected active execution indicator.
type ActiveHarnessSignal struct {
	Source     string    `json:"source"` // "in_memory", "sentinel", "process_table", "control_plane"
	RepoPath   string    `json:"repo_path,omitempty"`
	JobID      string    `json:"job_id,omitempty"`
	PID        int       `json:"pid,omitempty"`
	Command    string    `json:"command,omitempty"`
	Details    string    `json:"details,omitempty"`
	DetectedAt time.Time `json:"detected_at"`
}

// BackendJobChecker queries the control plane for active jobs.
type BackendJobChecker func(ctx context.Context) ([]ActiveHarnessSignal, error)

// ProcessScanner scans the OS process table for active harness processes.
type ProcessScanner func(repoPaths []string) ([]ActiveHarnessSignal, error)

// MultiSignalExecutionDetector coordinates the 4 independent harness execution detection layers.
type MultiSignalExecutionDetector struct {
	sup            *Sup
	gate           *HostExecutionAndUpdateGate
	getRepos       func() ([]RepoConfigItem, error)
	backendChecker BackendJobChecker
	processScanner ProcessScanner
	isAlive        func(pid int) bool
}

// NewMultiSignalExecutionDetector creates a new detector.
func NewMultiSignalExecutionDetector(
	sup *Sup,
	gate *HostExecutionAndUpdateGate,
	getRepos func() ([]RepoConfigItem, error),
	backendChecker BackendJobChecker,
) *MultiSignalExecutionDetector {
	return &MultiSignalExecutionDetector{
		sup:            sup,
		gate:           gate,
		getRepos:       getRepos,
		backendChecker: backendChecker,
		processScanner: DefaultOSProcessScanner,
		isAlive:        isProcessAlive,
	}
}

// SetProcessScanner overrides the process table scanner (used for testing).
func (d *MultiSignalExecutionDetector) SetProcessScanner(scanner ProcessScanner) {
	d.processScanner = scanner
}

// SetIsAlive overrides the process liveness check (used for testing).
func (d *MultiSignalExecutionDetector) SetIsAlive(fn func(pid int) bool) {
	d.isAlive = fn
}

// DetectActiveExecutions evaluates all 4 execution signals across host repos.
func (d *MultiSignalExecutionDetector) DetectActiveExecutions(ctx context.Context) (isExecuting bool, signals []ActiveHarnessSignal) {
	now := time.Now().UTC()

	// 1. In-Memory Agent Job Supervisor & Gate leases
	if d.sup != nil && d.sup.HasRunningHarness() {
		jobs := d.sup.RunningJobs()
		if len(jobs) > 0 {
			for _, j := range jobs {
				signals = append(signals, ActiveHarnessSignal{
					Source:     "in_memory",
					RepoPath:   j.WorktreePath,
					JobID:      j.JobID,
					PID:        j.PID,
					Details:    "active supervisor running job",
					DetectedAt: now,
				})
			}
		} else {
			signals = append(signals, ActiveHarnessSignal{
				Source:     "in_memory",
				Details:    "supervisor reports active or cooling harness lock",
				DetectedAt: now,
			})
		}
	}
	if d.gate != nil && d.gate.ActiveCount() > 0 {
		for _, ex := range d.gate.ActiveExecutions() {
			signals = append(signals, ActiveHarnessSignal{
				Source:     "in_memory",
				RepoPath:   ex.RepoPath,
				JobID:      ex.JobID,
				Details:    "active host gate shared execution lease",
				DetectedAt: now,
			})
		}
	}

	var repos []RepoConfigItem
	if d.getRepos != nil {
		if r, err := d.getRepos(); err == nil {
			repos = r
		}
	}

	var repoPaths []string
	for _, it := range repos {
		if it.Path != "" {
			repoPaths = append(repoPaths, it.Path)
		}
	}

	// 2. Workspace Lock & Sentinel Inspection
	for _, repoPath := range repoPaths {
		sentinelSignals := d.inspectWorkspaceSentinels(repoPath, now)
		signals = append(signals, sentinelSignals...)
	}

	// 3. OS Process Table Verification
	if d.processScanner != nil && len(repoPaths) > 0 {
		if psSignals, err := d.processScanner(repoPaths); err == nil && len(psSignals) > 0 {
			signals = append(signals, psSignals...)
		}
	}

	// 4. Control-Plane Lease Reconciliation
	if d.backendChecker != nil {
		if cpSignals, err := d.backendChecker(ctx); err == nil && len(cpSignals) > 0 {
			signals = append(signals, cpSignals...)
		}
	}

	// Deduplicate signals by Source + RepoPath + JobID + PID
	deduped := make([]ActiveHarnessSignal, 0, len(signals))
	seen := make(map[string]bool)
	for _, s := range signals {
		key := fmt.Sprintf("%s:%s:%s:%d", s.Source, s.RepoPath, s.JobID, s.PID)
		if !seen[key] {
			seen[key] = true
			deduped = append(deduped, s)
		}
	}

	return len(deduped) > 0, deduped
}

func (d *MultiSignalExecutionDetector) inspectWorkspaceSentinels(repoPath string, now time.Time) []ActiveHarnessSignal {
	var signals []ActiveHarnessSignal

	sentinels := []string{
		filepath.Join(repoPath, ".autopilot", "active_job.json"),
		filepath.Join(repoPath, ".autopilot", "job.lock"),
		filepath.Join(repoPath, ".autopilot", "lock"),
		filepath.Join(repoPath, ".harness.lock"),
		filepath.Join(repoPath, "tmp", "harness.lock"),
	}

	for _, sPath := range sentinels {
		fi, err := os.Stat(sPath)
		if err != nil {
			continue
		}

		pid := 0
		jobID := ""
		data, rErr := os.ReadFile(sPath)
		if rErr == nil {
			var parsed struct {
				JobID string `json:"job_id"`
				PID   int    `json:"pid"`
			}
			if jErr := json.Unmarshal(data, &parsed); jErr == nil {
				jobID = parsed.JobID
				pid = parsed.PID
			} else {
				// Try reading as raw PID integer or string
				txt := strings.TrimSpace(string(data))
				if n, cErr := strconv.Atoi(txt); cErr == nil {
					pid = n
				}
			}
		}

		// Verify process liveness if PID is available
		if pid > 0 {
			if d.isAlive != nil && !d.isAlive(pid) {
				// Process is dead: stale sentinel
				continue
			}
			signals = append(signals, ActiveHarnessSignal{
				Source:     "sentinel",
				RepoPath:   repoPath,
				JobID:      jobID,
				PID:        pid,
				Details:    fmt.Sprintf("live sentinel file present: %s (pid %d)", filepath.Base(sPath), pid),
				DetectedAt: now,
			})
			continue
		}

		// If no PID in sentinel, check file age (active if created/modified within 15 minutes)
		if now.Sub(fi.ModTime()) < 15*time.Minute {
			signals = append(signals, ActiveHarnessSignal{
				Source:     "sentinel",
				RepoPath:   repoPath,
				JobID:      jobID,
				Details:    fmt.Sprintf("recent sentinel file present: %s", filepath.Base(sPath)),
				DetectedAt: now,
			})
		}
	}

	return signals
}

// DefaultOSProcessScanner scans the OS process table for live harness or agent execution processes.
func DefaultOSProcessScanner(repoPaths []string) ([]ActiveHarnessSignal, error) {
	if runtime.GOOS == "windows" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := procutil.CommandContext(ctx, "ps", "-eo", "pid,args")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	myPID := os.Getpid()
	var signals []ActiveHarnessSignal
	now := time.Now().UTC()

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid == myPID || pid <= 1 {
			continue
		}
		commandLine := strings.Join(fields[1:], " ")

		// Check for target binaries
		isHarness := strings.Contains(commandLine, "iazio-harness run") ||
			strings.Contains(commandLine, "autopilot ase") ||
			strings.Contains(commandLine, "autopilot story run") ||
			strings.Contains(commandLine, "agy --agent") ||
			strings.Contains(commandLine, "agy -a ")

		if !isHarness {
			continue
		}

		// Check if command is bound to any registered repository path or name.
		// First check full path boundary matches (preferring longer paths so /work/meta-repo-02 beats /work/meta-repo),
		// then fall back to exact token basename matches.
		matchingRepo := matchCommandLineRepoPath(commandLine, repoPaths)

		if matchingRepo != "" || (len(repoPaths) == 0 && strings.Contains(commandLine, "iazio-harness run")) {
			signals = append(signals, ActiveHarnessSignal{
				Source:     "process_table",
				RepoPath:   matchingRepo,
				PID:        pid,
				Command:    commandLine,
				Details:    fmt.Sprintf("live process %d executing harness/autopilot", pid),
				DetectedAt: now,
			})
		}
	}

	return signals, nil
}

// matchCommandLineRepoPath matches a command line against registered repository paths
// using path-boundary rules (preferring longer paths first) so that a prefix path like
// "/home/romeo/work/meta-repo" never falsely matches "/home/romeo/work/meta-repo-02".
func matchCommandLineRepoPath(commandLine string, repoPaths []string) string {
	if len(repoPaths) == 0 || strings.TrimSpace(commandLine) == "" {
		return ""
	}
	type candidate struct {
		raw   string
		clean string
		base  string
	}
	candidates := make([]candidate, 0, len(repoPaths))
	for _, rp := range repoPaths {
		trimmed := strings.TrimSpace(rp)
		if trimmed == "" {
			continue
		}
		clean := filepath.Clean(trimmed)
		candidates = append(candidates, candidate{
			raw:   rp,
			clean: clean,
			base:  filepath.Base(clean),
		})
	}
	// Sort by descending cleaned path length so longer paths (/work/meta-repo-02) are checked before shorter prefixes (/work/meta-repo).
	sort.SliceStable(candidates, func(i, j int) bool {
		return len(candidates[i].clean) > len(candidates[j].clean)
	})

	for _, c := range candidates {
		if commandLineContainsRepoPath(commandLine, c.clean) {
			return c.raw
		}
	}

	// Only fall back to exact token basename matching when no full path in the command line matched any candidate.
	for _, c := range candidates {
		if c.base == "" || c.base == "." || c.base == "/" {
			continue
		}
		if commandLineContainsExactBaseToken(commandLine, c.base) {
			return c.raw
		}
	}
	return ""
}

func commandLineContainsRepoPath(commandLine, cleanRP string) bool {
	if cleanRP == "" || cleanRP == "." || cleanRP == "/" {
		return false
	}
	searchFrom := 0
	for searchFrom < len(commandLine) {
		idx := strings.Index(commandLine[searchFrom:], cleanRP)
		if idx < 0 {
			return false
		}
		start := searchFrom + idx
		end := start + len(cleanRP)
		validStart := start == 0 || isCommandPathStartBoundary(commandLine[start-1])
		validEnd := end == len(commandLine) || isCommandPathEndBoundary(commandLine[end])
		if validStart && validEnd {
			return true
		}
		searchFrom = start + 1
	}
	return false
}

func commandLineContainsExactBaseToken(commandLine, base string) bool {
	searchFrom := 0
	for searchFrom < len(commandLine) {
		idx := strings.Index(commandLine[searchFrom:], base)
		if idx < 0 {
			return false
		}
		start := searchFrom + idx
		end := start + len(base)
		// Basename token must be preceded by whitespace or '=' or quote (not '/' which is part of a full path)
		validStart := start == 0 || commandLine[start-1] == ' ' || commandLine[start-1] == '\t' || commandLine[start-1] == '=' || commandLine[start-1] == '"' || commandLine[start-1] == '\''
		validEnd := end == len(commandLine) || commandLine[end] == ' ' || commandLine[end] == '\t' || commandLine[end] == '"' || commandLine[end] == '\''
		if validStart && validEnd {
			return true
		}
		searchFrom = start + 1
	}
	return false
}

func isCommandPathStartBoundary(ch byte) bool {
	switch ch {
	case ' ', '\t', '=', '"', '\'', ':', ';', ',':
		return true
	default:
		return false
	}
}

func isCommandPathEndBoundary(ch byte) bool {
	switch ch {
	case '/', '\\', ' ', '\t', '"', '\'', ':', ';', ',':
		return true
	default:
		return false
	}
}
