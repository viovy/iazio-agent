package gate

import (
	"strings"
	"testing"
)

func TestIsPrematureCraftCloseout_CraftPlusWaitingFor(t *testing.T) {
	summary := "## CRAFT Closeout\nLaunched autopilot-git-flow land; waiting for CI to go green."
	if !IsPrematureCraftCloseout(summary, "") {
		t.Fatal("expected premature CRAFT when closeout coexists with waiting-for mid-flow markers")
	}
	details := PrematureCraftCloseoutDetails(summary, "")
	if details == "" || !strings.Contains(details, "craft closeout") || !strings.Contains(details, "waiting for") {
		t.Fatalf("expected details to list claim and mid-flow markers, got %q", details)
	}
}

func TestIsPrematureCraftCloseout_AwaitingCompletionAndLaunched(t *testing.T) {
	output := "Launched autopilot git-flow land\nAwaiting completion of PR merge.\n## CRAFT Closeout\n"
	if !IsPrematureCraftCloseout("", output) {
		t.Fatal("expected premature when CRAFT Closeout + awaiting completion / launched autopilot git-flow")
	}
}

func TestIsPrematureCraftCloseout_WillWaitForWithSuccessProse(t *testing.T) {
	summary := "Successfully completed STORY-GOV-5282. Will wait for autopilot git-flow assert."
	if !IsPrematureCraftCloseout(summary, "") {
		t.Fatal("expected premature when success prose coexists with will wait for")
	}
}

func TestIsPrematureCraftCloseout_HonestCraftWithoutMidFlow(t *testing.T) {
	summary := "## CRAFT Closeout\nAll PRs merged and fleet pointers reconciled."
	if IsPrematureCraftCloseout(summary, "") {
		t.Fatal("honest CRAFT without mid-flow markers must not trip the detector")
	}
}

func TestIsPrematureCraftCloseout_MidFlowAloneIsNotPremature(t *testing.T) {
	summary := "Launched autopilot-git-flow land; waiting for checks."
	if IsPrematureCraftCloseout(summary, "") {
		t.Fatal("mid-flow markers alone without CRAFT/success claims must not trip the detector")
	}
}

func TestIsPrematureCraftCloseout_EmptyIsFalse(t *testing.T) {
	if IsPrematureCraftCloseout("", "") {
		t.Fatal("empty summary/output must not be premature")
	}
}

func TestVerify_PrematureCraftFailsEvenWhenGitClean(t *testing.T) {
	tmpDir := t.TempDir()
	mockGit := func(args ...string) (string, error) {
		switch args[0] {
		case "status":
			return "", nil
		case "rev-parse":
			return "main", nil
		case "submodule":
			return "", nil
		default:
			return "", nil
		}
	}
	res := Verify(GateOptions{
		WorktreePath: tmpDir,
		GitRunner:    mockGit,
		Summary:      "## CRAFT Closeout\nWaiting for land to finish.",
	})
	if res.Passed {
		t.Fatal("expected Verify to fail on premature CRAFT even with clean git")
	}
	if !res.PrematureCraft {
		t.Fatal("expected PrematureCraft=true")
	}
	found := false
	for _, g := range res.UnmetGates {
		if g == "premature_craft_closeout" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected unmet gate premature_craft_closeout, got %v", res.UnmetGates)
	}
}
