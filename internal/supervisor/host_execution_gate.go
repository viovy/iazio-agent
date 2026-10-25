package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrExecutionLeaseTimeout is returned when a job dispatch times out waiting for an exclusive maintenance lease to release.
	ErrExecutionLeaseTimeout = errors.New("timed out waiting for maintenance lease to release")
)

// ActiveExecution records an active harness job lease.
type ActiveExecution struct {
	JobID        string    `json:"job_id"`
	RepoPath     string    `json:"repo_path"`
	StartedAt    time.Time `json:"started_at"`
	FencingToken int64     `json:"fencing_token,omitempty"`
}

// HostExecutionAndUpdateGate coordinates concurrent job executions and exclusive tool binary updates (Drain-Aware RWMutex).
type HostExecutionAndUpdateGate struct {
	mu            sync.Mutex
	cond          *sync.Cond
	activeJobs    map[string]ActiveExecution
	maintenance   bool
	maintenanceID string
	waitingJobs   int
}

// NewHostExecutionAndUpdateGate creates a new execution and update coordinator gate.
func NewHostExecutionAndUpdateGate() *HostExecutionAndUpdateGate {
	g := &HostExecutionAndUpdateGate{
		activeJobs: make(map[string]ActiveExecution),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// AcquireExecutionLease acquires a shared execution lease for a harness job.
// If an exclusive maintenance lease is currently held, it waits cleanly until the maintenance finishes.
func (g *HostExecutionAndUpdateGate) AcquireExecutionLease(ctx context.Context, jobID, repoPath string, fencingToken ...int64) (release func(), err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for g.maintenance {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %v", ErrExecutionLeaseTimeout, ctx.Err())
		}
		g.waitingJobs++

		waitDone := make(chan struct{})
		go func() {
			select {
			case <-waitDone:
			case <-ctx.Done():
				g.mu.Lock()
				g.cond.Broadcast()
				g.mu.Unlock()
			}
		}()

		g.cond.Wait()
		close(waitDone)
		g.waitingJobs--

		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %v", ErrExecutionLeaseTimeout, ctx.Err())
		}
	}

	var ft int64
	if len(fencingToken) > 0 {
		ft = fencingToken[0]
	}

	exec := ActiveExecution{
		JobID:        jobID,
		RepoPath:     repoPath,
		StartedAt:    time.Now().UTC(),
		FencingToken: ft,
	}
	g.activeJobs[jobID] = exec

	var once sync.Once
	release = func() {
		once.Do(func() {
			g.mu.Lock()
			delete(g.activeJobs, jobID)
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
	return release, nil
}

// TryAcquireMaintenanceLease attempts to acquire the exclusive maintenance lease for tool updates.
// Returns (release, true) if acquired, or (nil, false) if any harness jobs are currently executing or another maintenance is in progress.
func (g *HostExecutionAndUpdateGate) TryAcquireMaintenanceLease(maintenanceID string) (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.maintenance || len(g.activeJobs) > 0 {
		return nil, false
	}

	g.maintenance = true
	g.maintenanceID = maintenanceID

	var once sync.Once
	release = func() {
		once.Do(func() {
			g.mu.Lock()
			g.maintenance = false
			g.maintenanceID = ""
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
	return release, true
}

// IsMaintenanceHeld reports whether an exclusive tool update lease is currently held.
func (g *HostExecutionAndUpdateGate) IsMaintenanceHeld() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maintenance
}

// ActiveExecutions returns a copy of currently executing jobs holding a shared lease.
func (g *HostExecutionAndUpdateGate) ActiveExecutions() []ActiveExecution {
	g.mu.Lock()
	defer g.mu.Unlock()
	res := make([]ActiveExecution, 0, len(g.activeJobs))
	for _, v := range g.activeJobs {
		res = append(res, v)
	}
	return res
}

// ActiveCount returns the number of currently executing jobs holding a shared lease.
func (g *HostExecutionAndUpdateGate) ActiveCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.activeJobs)
}

// WaitingCount returns the number of incoming job dispatches waiting for tool updates to release.
func (g *HostExecutionAndUpdateGate) WaitingCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waitingJobs
}
