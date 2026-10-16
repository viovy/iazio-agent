package supervisor

import (
	"testing"
)

func TestLoopDetector_Consecutive(t *testing.T) {
	d := NewLoopDetector(10, 3, 2)

	r1 := d.Record("make validate-local", "error: syntax error in main.go:10")
	if r1.IsLoop {
		t.Fatalf("first action should not be a loop")
	}

	r2 := d.Record("make validate-local", "error: syntax error in main.go:10")
	if r2.IsLoop {
		t.Fatalf("second action should not be a loop")
	}

	r3 := d.Record("make validate-local", "error: syntax error in main.go:10")
	if !r3.IsLoop {
		t.Fatalf("third consecutive identical action should trigger loop detection")
	}
	if r3.RepeatCount != 3 {
		t.Errorf("expected RepeatCount=3, got %d", r3.RepeatCount)
	}

	// Reset clears loop state
	d.Reset()
	r4 := d.Record("make validate-local", "error")
	if r4.IsLoop {
		t.Fatalf("action after reset should not be a loop")
	}
}

func TestLoopDetector_Cyclic(t *testing.T) {
	d := NewLoopDetector(10, 5, 2)

	d.Record("action-A", "out-1")
	d.Record("action-B", "out-2")
	d.Record("action-A", "out-1")
	r := d.Record("action-B", "out-2")

	if !r.IsLoop {
		t.Fatalf("A-B-A-B cycle should trigger loop detection")
	}
}

func TestLoopDetector_OutputFailureThrashing(t *testing.T) {
	d := NewLoopDetector(10, 3, 2)

	d.Record("cat file1.txt", "fatal: permission denied")
	d.Record("cat file2.txt", "fatal: permission denied")
	r := d.Record("cat file3.txt", "fatal: permission denied")

	if !r.IsLoop {
		t.Fatalf("consecutive identical error outputs across different actions should trigger loop detection")
	}
}

