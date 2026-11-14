package gate

import "strings"

// Mid-flow markers that indicate Phase 5 land/assert is still in flight (RC-7 / STORY-IAZIO-0204).
var prematureCraftMidFlowMarkers = []string{
	"waiting for",
	"will wait for",
	"awaiting completion",
	"launched autopilot-git-flow",
	"launched autopilot git-flow",
}

// Completion-claim markers: CRAFT closeout or completed-looking success prose.
var prematureCraftCompletionMarkers = []string{
	"## craft closeout",
	"craft closeout",
	"successfully completed",
	"story completed successfully",
	"all prs merged",
	"ase flow complete",
	"definition of done met",
	"all acceptance criteria",
}

// IsPrematureCraftCloseout reports whether summary/output claims CRAFT/success while still
// containing mid-flow land/wait markers (false terminal completion).
func IsPrematureCraftCloseout(summary, output string) bool {
	return PrematureCraftCloseoutDetails(summary, output) != ""
}

// PrematureCraftCloseoutDetails returns a short diagnostic string when premature CRAFT is
// detected, or empty string when the transcript is not premature.
func PrematureCraftCloseoutDetails(summary, output string) string {
	combined := strings.ToLower(summary + "\n" + output)
	if strings.TrimSpace(combined) == "" {
		return ""
	}

	var midHits []string
	for _, m := range prematureCraftMidFlowMarkers {
		if strings.Contains(combined, m) {
			midHits = append(midHits, m)
		}
	}
	if len(midHits) == 0 {
		return ""
	}

	var claimHits []string
	for _, c := range prematureCraftCompletionMarkers {
		if strings.Contains(combined, c) {
			claimHits = append(claimHits, c)
		}
	}
	if len(claimHits) == 0 {
		return ""
	}

	return "claims=" + strings.Join(claimHits, ",") + "; mid_flow=" + strings.Join(midHits, ",")
}
