package agentbudget

import (
	"fmt"
	"sort"
)

// Observed carries the meristem-observable facts of one agent run, resolved by
// the caller. The reducer never reads clocks or projections itself. A field
// left at zero is treated as "measured zero" (e.g. no tool calls made), not
// "unknown"; callers that genuinely cannot observe a dimension should leave
// the corresponding Budget ceiling at zero so it is not enforced.
type Observed struct {
	ContextFiles   int
	PromptWords    int
	ResponseWords  int
	Turns          int
	RuntimeSeconds int
	ToolCalls      int
	Artifacts      int
}

// Disposition is the budget verdict: a run either stayed within budget or
// breached at least one enforced dimension.
type Disposition string

const (
	// Accept means every enforced dimension stayed at or under its ceiling.
	Accept Disposition = "accept"
	// Reject means at least one enforced dimension was exceeded; the caller
	// transitions or blocks the owning work_item with Reason.
	Reject Disposition = "reject"
)

// Breach names one exceeded dimension and the observed-vs-ceiling values that
// tripped it. Breaches are ordered by dimension name so the verdict, and any
// deterministic reason built from it, is stable across runs.
type Breach struct {
	Dimension string `json:"dimension"`
	Observed  int    `json:"observed"`
	Ceiling   int    `json:"ceiling"`
}

// Decision is the reducer output. Reason is a single deterministic string
// suitable for a work_item transition reason; Breaches carries the structured
// detail for events.
type Decision struct {
	Disposition Disposition
	Reason      string
	Breaches    []Breach
}

// Reduce judges an observed run against a budget. It is pure and
// deterministic: the same (budget, observed) always yields the same decision,
// including Reason. Only dimensions with a positive ceiling are enforced; a
// zero ceiling disables that dimension.
func Reduce(budget Budget, obs Observed) Decision {
	dims := []struct {
		name    string
		ceiling int
		got     int
	}{
		{"max_context_files", budget.MaxContextFiles, obs.ContextFiles},
		{"max_prompt_words", budget.MaxPromptWords, obs.PromptWords},
		{"max_response_words", budget.MaxResponseWords, obs.ResponseWords},
		{"max_turns", budget.MaxTurns, obs.Turns},
		{"max_runtime_seconds", budget.MaxRuntimeSeconds, obs.RuntimeSeconds},
		{"max_tool_calls", budget.MaxToolCalls, obs.ToolCalls},
		{"max_artifacts", budget.MaxArtifacts, obs.Artifacts},
	}

	var breaches []Breach
	for _, d := range dims {
		if d.ceiling > 0 && d.got > d.ceiling {
			breaches = append(breaches, Breach{Dimension: d.name, Observed: d.got, Ceiling: d.ceiling})
		}
	}
	if len(breaches) == 0 {
		return Decision{Disposition: Accept, Reason: "within budget"}
	}
	sort.Slice(breaches, func(i, j int) bool { return breaches[i].Dimension < breaches[j].Dimension })

	reason := "budget breached: "
	for i, b := range breaches {
		if i > 0 {
			reason += ", "
		}
		reason += fmt.Sprintf("%s %d>%d", b.Dimension, b.Observed, b.Ceiling)
	}
	return Decision{Disposition: Reject, Reason: reason, Breaches: breaches}
}
