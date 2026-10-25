package supervisor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestHostExecutionGate_ConcurrentJobsAndMaintenanceBlock(t *testing.T) {
	gate := NewHostExecutionAndUpdateGate()
	ctx := context.Background()

	// 1. Acquire two shared execution leases
	rel1, err := gate.AcquireExecutionLease(ctx, "job-1", "/path/repo-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	rel2, err := gate.AcquireExecutionLease(ctx, "job-2", "/path/repo-2")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if gate.ActiveCount() != 2 {
		t.Errorf("expected 2 active jobs, got %d", gate.ActiveCount())
	}

	// 2. TryAcquireMaintenanceLease must fail while jobs are executing
	mRel, ok := gate.TryAcquireMaintenanceLease("update-pass-1")
	if ok || mRel != nil {
		t.Fatalf("expected maintenance lease acquisition to fail while jobs are active")
	}

	// 3. Release jobs
	rel1()
	if gate.ActiveCount() != 1 {
		t.Errorf("expected 1 active job, got %d", gate.ActiveCount())
	}
	rel2()
	if gate.ActiveCount() != 0 {
		t.Errorf("expected 0 active jobs, got %d", gate.ActiveCount())
	}

	// 4. Now maintenance lease must succeed
	mRel, ok = gate.TryAcquireMaintenanceLease("update-pass-1")
	if !ok || mRel == nil {
		t.Fatalf("expected maintenance lease acquisition to succeed when idle")
	}
	if !gate.IsMaintenanceHeld() {
		t.Errorf("expected maintenance to be held")
	}

	// 5. Incoming job dispatch must wait while maintenance is held
	dispatchStarted := make(chan struct{})
	dispatchFinished := make(chan struct{})
	var dispatchErr error

	go func() {
		close(dispatchStarted)
		jRel, err := gate.AcquireExecutionLease(ctx, "job-3", "/path/repo-3")
		dispatchErr = err
		if jRel != nil {
			jRel()
		}
		close(dispatchFinished)
	}()

	<-dispatchStarted
	time.Sleep(50 * time.Millisecond)

	select {
	case <-dispatchFinished:
		t.Fatalf("dispatch finished prematurely while maintenance was held")
	default:
		// correctly waiting
	}

	if gate.WaitingCount() != 1 {
		t.Errorf("expected 1 waiting job, got %d", gate.WaitingCount())
	}

	// 6. Release maintenance lease -> dispatch should unblock and complete
	mRel()

	select {
	case <-dispatchFinished:
		if dispatchErr != nil {
			t.Errorf("dispatch returned unexpected error: %v", dispatchErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for dispatch to unblock after maintenance released")
	}

	if gate.IsMaintenanceHeld() {
		t.Errorf("expected maintenance not to be held")
	}
}

func TestHostExecutionGate_DispatchTimeout(t *testing.T) {
	gate := NewHostExecutionAndUpdateGate()

	mRel, ok := gate.TryAcquireMaintenanceLease("update-pass")
	if !ok || mRel == nil {
		t.Fatalf("failed to acquire maintenance lease")
	}
	defer mRel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := gate.AcquireExecutionLease(ctx, "job-timeout", "/path/repo")
	if err == nil || !errors.Is(err, ErrExecutionLeaseTimeout) {
		t.Fatalf("expected ErrExecutionLeaseTimeout, got: %v", err)
	}
}

func TestHostExecutionGate_ConcurrentSafety(t *testing.T) {
	gate := NewHostExecutionAndUpdateGate()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			jRel, err := gate.AcquireExecutionLease(ctx, "concurrent-job", "/path/repo")
			if err == nil {
				time.Sleep(5 * time.Millisecond)
				jRel()
			}
		}(i)
	}

	wg.Wait()
	if gate.ActiveCount() != 0 {
		t.Errorf("expected 0 active jobs after all released, got %d", gate.ActiveCount())
	}
}
