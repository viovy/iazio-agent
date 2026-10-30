package supervisor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type waitResult struct {
	exitCode int
	err      error
}

type processEntry struct {
	cmd      *exec.Cmd
	pid      int
	jobID    string
	attached bool
	done     chan struct{}
	res      waitResult
	once     sync.Once
}

// OSRunner executes child processes with their own process group.
type OSRunner struct {
	mu      sync.Mutex
	entries map[int]*processEntry
	Stdout  io.Writer
	Stderr  io.Writer
}

// NewOSRunner returns a new OSRunner.
func NewOSRunner() *OSRunner {
	return &OSRunner{
		entries: make(map[int]*processEntry),
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

// ResolveBinary finds the binary on PATH or standard user directories.
// It verifies symlinks and resolves canonical targets to prevent dangling inode execution.
func ResolveBinary(name string) string {
	if filepath.IsAbs(name) {
		if lfi, err := os.Lstat(name); err == nil && (lfi.Mode()&os.ModeSymlink != 0) {
			if resolved, err := filepath.EvalSymlinks(name); err == nil {
				return resolved
			}
		}
		return name
	}
	if resolved, err := exec.LookPath(name); err == nil {
		if lfi, err := os.Lstat(resolved); err == nil && (lfi.Mode()&os.ModeSymlink != 0) {
			if realPath, err := filepath.EvalSymlinks(resolved); err == nil {
				return realPath
			}
		}
		return resolved
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates := []string{
			filepath.Join(home, ".local", "bin", name),
		}
		if runtime.GOOS == "windows" {
			candidates = append(candidates, filepath.Join(home, "bin", name))
		}
		for _, c := range candidates {
			target := c
			if runtime.GOOS == "windows" && filepath.Ext(target) == "" {
				target += ".exe"
			}
			if lfi, err := os.Lstat(target); err == nil && !lfi.IsDir() {
				if lfi.Mode()&os.ModeSymlink != 0 {
					if realTarget, err := filepath.EvalSymlinks(target); err == nil {
						if fi, err := os.Stat(realTarget); err == nil && !fi.IsDir() {
							return realTarget
						}
					}
					continue
				}
				if fi, err := os.Stat(target); err == nil && !fi.IsDir() {
					return target
				}
			}
		}
	}
	return name
}

// AugmentedEnv returns env with PATH updated to include standard user directories.
// It also ensures HOME and USER are populated if missing.
func AugmentedEnv(base []string) []string {
	home, _ := os.UserHomeDir()
	var userDirs []string
	if home != "" {
		userDirs = append(userDirs, filepath.Join(home, ".local", "bin"))
		if runtime.GOOS == "windows" {
			userDirs = append(userDirs, filepath.Join(home, "bin"))
		}
	}
	if runtime.GOOS == "darwin" {
		userDirs = append(userDirs, "/opt/homebrew/bin", "/usr/local/bin")
	}
	existingPath := os.Getenv("PATH")
	var newPathParts []string
	for _, d := range userDirs {
		if _, err := os.Stat(d); err == nil {
			newPathParts = append(newPathParts, d)
		}
	}
	if existingPath != "" {
		newPathParts = append(newPathParts, filepath.SplitList(existingPath)...)
	}
	fullPath := strings.Join(dedupeDirs(newPathParts), string(filepath.ListSeparator))

	env := make([]string, 0, len(base)+3)
	pathSet := false
	homeSet := false
	userSet := false
	for _, kv := range base {
		if strings.HasPrefix(kv, "PATH=") || strings.HasPrefix(kv, "Path=") {
			env = append(env, "PATH="+fullPath)
			pathSet = true
		} else {
			if strings.HasPrefix(kv, "HOME=") {
				homeSet = true
			}
			if strings.HasPrefix(kv, "USER=") {
				userSet = true
			}
			env = append(env, kv)
		}
	}
	if !pathSet {
		env = append(env, "PATH="+fullPath)
	}
	if !homeSet && home != "" {
		env = append(env, "HOME="+home)
	}
	if !userSet {
		if u := os.Getenv("USER"); u != "" {
			env = append(env, "USER="+u)
		} else if curr, err := user.Current(); err == nil && curr.Username != "" {
			env = append(env, "USER="+curr.Username)
		}
	}
	return env
}

func dedupeDirs(dirs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		clean := filepath.Clean(dir)
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	return out
}

// Start launches a new process group for argv.
func (r *OSRunner) Start(argv []string) (int, error) {
	if len(argv) == 0 {
		return 0, errors.New("empty argv")
	}
	bin := ResolveBinary(argv[0])
	cmd := exec.Command(bin, argv[1:]...)
	cmd.Env = AugmentedEnv(os.Environ())

	var jobID string
	for i := 0; i < len(argv)-1; i++ {
		if argv[i] == "--job-id" {
			jobID = argv[i+1]
			break
		}
	}

	stdout := r.Stdout
	stderr := r.Stderr
	if jobID != "" {
		runDir := RunDir(jobID)
		if err := os.MkdirAll(runDir, 0755); err == nil {
			logPath := filepath.Join(runDir, "output.log")
			if logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
				if stdout != nil {
					stdout = io.MultiWriter(stdout, logFile)
				} else {
					stdout = logFile
				}
				if stderr != nil {
					stderr = io.MultiWriter(stderr, logFile)
				} else {
					stderr = logFile
				}
			}
		}
	}

	cmd.Stdout = stdout
	cmd.Stderr = stderr
	setProcGroup(cmd)
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	postStartProc(cmd)
	pid := cmd.Process.Pid
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[int]*processEntry)
	}
	r.entries[pid] = &processEntry{
		cmd:   cmd,
		pid:   pid,
		jobID: jobID,
		done:  make(chan struct{}),
	}
	r.mu.Unlock()
	return pid, nil
}

// Attach registers an existing external or surviving process for waiting/signaling.
func (r *OSRunner) Attach(pid int, jobID string) error {
	if pid <= 0 {
		return errors.New("invalid pid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[int]*processEntry)
	}
	if _, exists := r.entries[pid]; exists {
		return nil
	}
	entry := &processEntry{
		pid:      pid,
		jobID:    jobID,
		attached: true,
		done:     make(chan struct{}),
	}
	r.entries[pid] = entry
	return nil
}

// Signal sends a signal (e.g. "TERM", "KILL") to the process group.
func (r *OSRunner) Signal(pid int, sig string) error {
	return signalPID(pid, sig)
}

// Wait waits for the child process to exit and returns its exit code.
// Safe for concurrent and sequential invocations from multiple observers (e.g. supervisor and watchdog).
func (r *OSRunner) Wait(pid int) (int, error) {
	r.mu.Lock()
	entry := r.entries[pid]
	r.mu.Unlock()
	if entry == nil {
		return -1, errors.New("no such process")
	}

	entry.once.Do(func() {
		var exitCode int
		var exitErr *exec.ExitError
		var err error
		if entry.cmd != nil {
			err = entry.cmd.Wait()
			if err != nil {
				if errors.As(err, &exitErr) {
					exitCode = exitErr.ExitCode()
					err = nil
				} else {
					exitCode = -1
				}
			}
		} else if entry.attached {
			for {
				if !isProcessAlive(pid) {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if entry.jobID != "" {
				if st, errLoad := LoadRunState(entry.jobID); errLoad == nil && st.ExitCode != 0 {
					exitCode = st.ExitCode
				}
			}
		}

		if entry.jobID != "" {
			if st, loadErr := LoadRunState(entry.jobID); loadErr == nil {
				if exitCode == 0 && err == nil {
					st.Status = RunStatusCompleted
				} else {
					st.Status = RunStatusFailed
				}
				st.ExitCode = exitCode
				st.UpdatedAt = time.Now()
				if err != nil {
					st.Error = err.Error()
				}
				_ = SaveRunState(*st)
			}
		}

		entry.res = waitResult{exitCode: exitCode, err: err}
		close(entry.done)
	})

	<-entry.done
	return entry.res.exitCode, entry.res.err
}

// DockerConfig holds configuration parameters for launching an ephemeral containerized agent.
type DockerConfig struct {
	Image         string            `json:"image"`
	Network       string            `json:"network,omitempty"`
	MemoryMB      int64             `json:"memory_mb,omitempty"`
	CPUs          float64           `json:"cpus,omitempty"`
	CloneURL      string            `json:"clone_url,omitempty"`
	WorktreeMount string            `json:"worktree_mount,omitempty"`
	EnvVars       map[string]string `json:"env_vars,omitempty"`
}

// DockerRunner scaffolds the containerized isolation runner interface for autonomous story execution (ASE).
// Full container lifecycle is executed when docker daemon interaction is enabled.
type DockerRunner struct {
	Config DockerConfig
	host   Runner
}

// NewDockerRunner creates a new DockerRunner wrapping the provided configuration and fallback host runner.
func NewDockerRunner(cfg DockerConfig, fallback Runner) *DockerRunner {
	if fallback == nil {
		fallback = NewOSRunner()
	}
	return &DockerRunner{
		Config: cfg,
		host:   fallback,
	}
}

// Start launches either a docker container or delegates to the underlying host runner.
func (d *DockerRunner) Start(argv []string) (int, error) {
	if len(argv) == 0 {
		return 0, errors.New("empty argv")
	}
	// If no image is configured, execute via host runner
	if d.Config.Image == "" {
		return d.host.Start(argv)
	}

	dockerArgv := []string{"docker", "run", "--rm"}
	if d.Config.WorktreeMount != "" {
		dockerArgv = append(dockerArgv, "-v", fmt.Sprintf("%s:/workspace", d.Config.WorktreeMount))
	}
	for k, v := range d.Config.EnvVars {
		dockerArgv = append(dockerArgv, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	dockerArgv = append(dockerArgv, d.Config.Image)
	dockerArgv = append(dockerArgv, argv...)
	return d.host.Start(dockerArgv)
}

// Signal sends a signal to the running agent.
func (d *DockerRunner) Signal(pid int, sig string) error {
	return d.host.Signal(pid, sig)
}

// Wait waits for the agent execution to finish.
func (d *DockerRunner) Wait(pid int) (int, error) {
	return d.host.Wait(pid)
}
