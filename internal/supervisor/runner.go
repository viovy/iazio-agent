package supervisor

import (
	"errors"
	"os/exec"
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

// Start launches a new process group for argv.
func (r *OSRunner) Start(argv []string) (int, error) {
	if len(argv) == 0 {
		return 0, errors.New("empty argv")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
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
		return 0, errors.New("no such process")
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
