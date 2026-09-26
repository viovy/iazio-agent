package preflight

import "testing"

func TestDecideMatrix(t *testing.T) {
	ok := Report{
		Kind: KindOrdinary, FreeBytes: MinFreeBytes, DocsHubOK: true, GitAuthOK: true,
		GitWorkTree: true, HeadAttached: true, Branch: "main", DefaultBranch: "main", HasUpstream: true,
	}
	tests := []struct {
		name  string
		edit  func(*Report)
		code  string
		pause bool
	}{
		{name: "exact free space", code: ""},
		{name: "one byte under disk", edit: func(r *Report) { r.FreeBytes = MinFreeBytes - 1 }, code: ReasonDisk},
		{name: "disk wins over dirty", edit: func(r *Report) { r.FreeBytes = 1; r.WorkPorcelain = " M a" }, code: ReasonDisk},
		{name: "missing docs hub", edit: func(r *Report) { r.DocsHubOK = false }, code: ReasonNoDocsHub},
		{name: "docs hub not a work tree", edit: func(r *Report) { r.GitWorkTree = false }, code: ReasonNoDocsHub},
		{name: "git probe", edit: func(r *Report) { r.GitAuthOK = false }, code: ReasonGitAuth},
		{name: "hub missing before git probe", edit: func(r *Report) { r.DocsHubOK = false; r.GitAuthOK = false }, code: ReasonNoDocsHub},
		{name: "work porcelain", edit: func(r *Report) { r.WorkPorcelain = " M a" }, code: ReasonDirty, pause: true},
		{name: "hub porcelain", edit: func(r *Report) { r.HubPorcelain = "?? x" }, code: ReasonDirty, pause: true},
		{name: "dirty before detached", edit: func(r *Report) { r.WorkPorcelain = " M a"; r.HeadAttached = false }, code: ReasonDirty, pause: true},
		{name: "detached", edit: func(r *Report) { r.HeadAttached = false }, code: ReasonDetached, pause: true},
		{name: "feature without upstream", edit: func(r *Report) { r.Branch = "topic"; r.HasUpstream = false }, code: ReasonUntracked, pause: true},
		{name: "default without upstream"},
		{name: "feature with upstream", edit: func(r *Report) { r.Branch = "topic" }},
		{name: "blank porcelain"},
		{name: "whitespace porcelain", edit: func(r *Report) { r.WorkPorcelain = " \n"; r.HubPorcelain = "\t" }},
		{name: "loop dirty", edit: func(r *Report) { r.Kind = KindLoop; r.WorkPorcelain = " M a" }, code: ReasonDirty, pause: true},
		{name: "resume detached", edit: func(r *Report) { r.Kind = KindResume; r.HeadAttached = false }, code: ReasonDetached, pause: true},
		{name: "refinement ignores work porcelain", edit: func(r *Report) { r.Kind = KindRefinement; r.WorkPorcelain = " M a" }},
		{name: "refinement hub porcelain", edit: func(r *Report) { r.Kind = KindRefinement; r.HubPorcelain = " M a" }, code: ReasonDirty, pause: true},
		{name: "refinement ignores detached", edit: func(r *Report) {
			r.Kind = KindRefinement
			r.HeadAttached = false
			r.HasUpstream = false
			r.Branch = "topic"
		}},
		{name: "intervention dirty detached untracked", edit: func(r *Report) {
			r.Kind = KindIntervention
			r.WorkPorcelain = " M a"
			r.HubPorcelain = "?? x"
			r.HeadAttached = false
			r.HasUpstream = false
			r.Branch = "topic"
		}},
		{name: "intervention disk", edit: func(r *Report) { r.Kind = KindIntervention; r.FreeBytes = 1 }, code: ReasonDisk},
		{name: "intervention missing hub", edit: func(r *Report) { r.Kind = KindIntervention; r.GitWorkTree = false }, code: ReasonNoDocsHub},
		{name: "intervention git probe", edit: func(r *Report) { r.Kind = KindIntervention; r.GitAuthOK = false }, code: ReasonGitAuth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := ok
			if tt.edit != nil {
				tt.edit(&req)
			}
			got := Decide(req)
			if got.Reason != tt.code || got.PauseQueue != tt.pause || got.IncrementHealing {
				t.Fatalf("Decide() = %+v, want code %q pause %v healing false", got, tt.code, tt.pause)
			}
		})
	}
}

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
