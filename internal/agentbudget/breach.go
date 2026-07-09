package agentbudget

import "strings"

// BreachPayload is the structural payload of an agent_budget.breached event
// (domain.EventAgentBudgetBreached). The subject is the owning work_item. It
// carries the budget identity and the sorted breached dimensions so the event
// is replayable and its cause is legible without re-running the reducer.
type BreachPayload struct {
	PayloadVersion int      `json:"payload_version,omitempty"`
	BudgetHash     string   `json:"budget_hash"`
	Breaches       []Breach `json:"breaches"`
	Reason         string   `json:"reason"`
}

// BreachEvent turns a reducer Decision into the payload and event
// discriminator for an agent_budget.breached event. It returns ok=false when
// the decision did not breach, so callers append nothing for an in-budget run.
//
// The discriminator is (budget_hash | sorted breached dimensions). Feeding it
// into the events writer's deterministic id means re-observing the same breach
// of the same budget collapses onto one row — satisfying the idempotency
// criterion that "retrying after a breach does not erase or mutate the
// original breach event" — while a different budget or a newly-breached
// dimension mints a distinct event.
func BreachEvent(budget Budget, decision Decision) (payload BreachPayload, discriminator string, ok bool) {
	if decision.Disposition != Reject || len(decision.Breaches) == 0 {
		return BreachPayload{}, "", false
	}
	hash := BudgetHash(budget)
	dims := make([]string, len(decision.Breaches))
	for i, b := range decision.Breaches {
		dims[i] = b.Dimension
	}
	// decision.Breaches is already sorted by dimension (see Reduce), so the
	// discriminator is stable across runs without re-sorting here.
	return BreachPayload{
		BudgetHash: hash,
		Breaches:   decision.Breaches,
		Reason:     decision.Reason,
	}, hash + "|" + strings.Join(dims, ","), true
}
