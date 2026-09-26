package preflight

import "testing"

func TestDecide(t *testing.T) {
	ok := Report{
		Kind: KindOrdinary, FreeBytes: MinFreeBytes, DocsHubOK: true, GitAuthOK: true,
		GitWorkTree: true, HeadAttached: true, Branch: "main", DefaultBranch: "main",
	}
	if d := Decide(ok); d.Reason != "" {
		t.Fatal(d)
	}
	low := ok
	low.Kind = KindIntervention
	low.FreeBytes = 1
	if d := Decide(low); d.Reason != ReasonDisk || d.PauseQueue {
		t.Fatal(d)
	}
	dirty := ok
	dirty.WorkPorcelain = " M a"
	if d := Decide(dirty); d.Reason != ReasonDirty || !d.PauseQueue {
		t.Fatal(d)
	}
	iv := ok
	iv.Kind = KindIntervention
	iv.WorkPorcelain = " M a"
	iv.HeadAttached = false
	if d := Decide(iv); d.Reason != "" {
		t.Fatal(d)
	}
}
