package supervisor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RunStatus defines the lifecycle state of a detached harness run.
type RunStatus string

const (
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusCompleted RunStatus = "COMPLETED"
	RunStatusFailed    RunStatus = "FAILED"
)

// RunState records persistent state for a harness run in ~/.iazio/runs/<job-id>/state.json.
type RunState struct {
	JobID        string    `json:"job_id"`
	PID          int       `json:"pid"`
	WorktreePath string    `json:"worktree_path"`
	DocsHubPath  string    `json:"docs_hub_path,omitempty"`
	APIURL       string    `json:"api_url,omitempty"`
	Status       RunStatus `json:"status"`
	ExitCode     int       `json:"exit_code"`
	StartedAt    time.Time `json:"started_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	Error        string    `json:"error,omitempty"`
}

var runStateMu sync.Mutex

// RunsBaseDir returns the base directory for detached run states.
func RunsBaseDir() string {
	if dir := strings.TrimSpace(os.Getenv("IAZIO_RUNS_DIR")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		return filepath.Join(home, ".iazio", "runs")
	}
	return filepath.Join(os.TempDir(), "iazio-runs")
}

// RunDir returns the specific directory for a job run.
func RunDir(jobID string) string {
	return filepath.Join(RunsBaseDir(), jobID)
}

// SaveRunState atomically writes the state.json file in the job's run directory.
func SaveRunState(st RunState) error {
	if st.JobID == "" {
		return fmt.Errorf("job_id cannot be empty")
	}
	runStateMu.Lock()
	defer runStateMu.Unlock()

	dir := RunDir(st.JobID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}

	target := filepath.Join(dir, "state.json")
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// LoadRunState reads the state.json file for a given job.
func LoadRunState(jobID string) (*RunState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job_id cannot be empty")
	}
	runStateMu.Lock()
	defer runStateMu.Unlock()

	target := filepath.Join(RunDir(jobID), "state.json")
	data, err := os.ReadFile(target)
	if err != nil {
		return nil, err
	}
	var st RunState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// ListRunStates reads all recorded run states from the runs directory.
func ListRunStates() ([]RunState, error) {
	runStateMu.Lock()
	defer runStateMu.Unlock()

	base := RunsBaseDir()
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var list []RunState
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		target := filepath.Join(base, ent.Name(), "state.json")
		data, err := os.ReadFile(target)
		if err != nil {
			continue
		}
		var st RunState
		if err := json.Unmarshal(data, &st); err == nil {
			list = append(list, st)
		}
	}
	return list, nil
}
