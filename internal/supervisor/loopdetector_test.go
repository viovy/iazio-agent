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

func TestLoopDetector_WorkingTreeStateLoop(t *testing.T) {
	d := NewLoopDetector(10, 5, 5)

	stateA := "state-hash-A"
	stateB := "state-hash-B"

	// Action toggle sequence: State A -> State B
	r1 := d.RecordWithState("edit file.go (approach 1)", "error in approach 1", stateA)
	if r1.IsLoop {
		t.Fatal("first state should not be a loop")
	}

	r2 := d.RecordWithState("edit file.go (approach 2)", "error in approach 2", stateB)
	if r2.IsLoop {
		t.Fatal("second state should not be a loop")
	}

	// Now agent reverts back to State A (Toggle A -> Error B -> Toggle B -> Error A)
	// Even if action command string or error wording varies slightly!
	r3 := d.RecordWithState("edit file.go (reverting with slight variation)", "different error message", stateA)
	if !r3.IsLoop {
		t.Fatal("reverting to previously seen working-tree state hash must trigger loop detection immediately")
	}
	if r3.RepeatCount != 2 {
		t.Errorf("expected RepeatCount=2, got %d", r3.RepeatCount)
	}
}

func TestComputeWorkingTreeHash(t *testing.T) {
	files1 := map[string][]byte{
		"main.go": []byte("package main\nfunc main() {}\n"),
		"util.go": []byte("package main\nfunc Util() {}\n"),
	}
	files2 := map[string][]byte{
		"util.go": []byte("package main\nfunc Util() {}\n"),
		"main.go": []byte("package main\nfunc main() {}\n"),
	}
	files3 := map[string][]byte{
		"main.go": []byte("package main\nfunc main() { panic(1) }\n"),
		"util.go": []byte("package main\nfunc Util() {}\n"),
	}

	h1 := ComputeWorkingTreeHash(files1)
	h2 := ComputeWorkingTreeHash(files2)
	h3 := ComputeWorkingTreeHash(files3)

	if h1 != h2 {
		t.Errorf("expected deterministic hash independent of map key iteration, got %q and %q", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("expected different hash for modified content, got identical %q", h1)
	}

	diff1 := "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"
	diff2 := "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"
	if ComputeDiffHash(diff1) != ComputeDiffHash(diff2) {
		t.Error("expected identical diff hashes")
	}
}


