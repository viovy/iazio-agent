package supervisor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// OSRunner executes child processes with their own process group.
type OSRunner struct {
	mu   sync.Mutex
	cmds map[int]*exec.Cmd
}

// NewOSRunner returns a new OSRunner.
func NewOSRunner() *OSRunner {
	return &OSRunner{
		cmds: make(map[int]*exec.Cmd),
	}
}

// ResolveBinary finds the binary on PATH or standard user directories.
func ResolveBinary(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	if resolved, err := exec.LookPath(name); err == nil {
		return resolved
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates := []string{
			filepath.Join(home, ".local", "bin", name),
			filepath.Join(home, ".iazio", "bin", name),
		}
		if runtime.GOOS == "windows" {
			candidates = append(candidates, filepath.Join(home, "bin", name))
		}
		for _, c := range candidates {
			target := c
			if runtime.GOOS == "windows" && filepath.Ext(target) == "" {
				target += ".exe"
			}
			if fi, err := os.Stat(target); err == nil && !fi.IsDir() {
				return target
			}
		}
	}
	return name
}

// AugmentedEnv returns env with PATH updated to include standard user directories.
func AugmentedEnv(base []string) []string {
	home, _ := os.UserHomeDir()
	var userDirs []string
	if home != "" {
		userDirs = append(userDirs, filepath.Join(home, ".local", "bin"), filepath.Join(home, ".iazio", "bin"))
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

	env := make([]string, 0, len(base)+1)
	pathSet := false
	for _, kv := range base {
		if strings.HasPrefix(kv, "PATH=") || strings.HasPrefix(kv, "Path=") {
			env = append(env, "PATH="+fullPath)
			pathSet = true
		} else {
			env = append(env, kv)
		}
	}
	if !pathSet {
		env = append(env, "PATH="+fullPath)
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
	setProcGroup(cmd)
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	r.mu.Lock()
	if r.cmds == nil {
		r.cmds = make(map[int]*exec.Cmd)
	}
	r.cmds[pid] = cmd
	r.mu.Unlock()
	return pid, nil
}

// Signal sends a signal (e.g. "TERM", "KILL") to the process group.
func (r *OSRunner) Signal(pid int, sig string) error {
	return signalPID(pid, sig)
}

// Wait waits for the child process to exit and returns its exit code.
func (r *OSRunner) Wait(pid int) (int, error) {
	r.mu.Lock()
	cmd := r.cmds[pid]
	delete(r.cmds, pid)
	r.mu.Unlock()

	if cmd == nil {
		return -1, errors.New("no such process")
	}
	err := cmd.Wait()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode(), nil
		}
		return -1, err
	}
	return 0, nil
}
