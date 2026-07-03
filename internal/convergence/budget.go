package convergence

import (
	"fmt"

	"github.com/jbmopper/meristem/internal/domain"
)

// Escalation is what a convergence loop does when its patience budget is
// exhausted (spec rule #3: "New patterns ship with their escalation rule or
// they do not ship"). The engine does not *perform* the escalation — that is
// the worker reconciler's job — but it defines the closed vocabulary so a
// budget cannot be declared without naming its exit.
type Escalation string

const (
	// EscalateFail terminates the work_item as failed when attempts run out.
	EscalateFail Escalation = "fail"
	// EscalateRequestApproval routes to the approval system (default-deny;
	// v1). The owner decides whether to continue.
	EscalateRequestApproval Escalation = "request_approval"
	// EscalateHandToHuman blocks the work_item for direct human attention
	// (human_review_status → blocked) rather than auto-failing.
	EscalateHandToHuman Escalation = "hand_to_human"
)

// Valid reports whether e is one of the accepted escalation rules.
func (e Escalation) Valid() bool {
	switch e {
	case EscalateFail, EscalateRequestApproval, EscalateHandToHuman:
		return true
	}
	return false
}

// Budget is the bounded-patience declaration for a convergence loop: how many
// attempts a reject may consume before the loop must escalate, and how. It is
// the deterministic accounting the worker applies; the reducer never sees it.
//
// A Budget is required to ship with a convergence pattern. The zero value is
// invalid on purpose (Validate fails) so a pattern cannot quietly omit its
// escalation rule.
type Budget struct {
	// MaxAttempts is the number of reductions the loop may run before it is
	// exhausted. Must be >= 1.
	MaxAttempts int
	// Escalation is the exit taken once MaxAttempts is reached without an
	// accept.
	Escalation Escalation
}

// Validate enforces that a budget names a positive attempt cap and a valid
// escalation rule.
func (b Budget) Validate() error {
	if b.MaxAttempts < 1 {
		return fmt.Errorf("convergence: budget MaxAttempts must be >= 1, got %d", b.MaxAttempts)
	}
	if !b.Escalation.Valid() {
		return fmt.Errorf("convergence: budget Escalation %q is not a valid rule", b.Escalation)
	}
	return nil
}

// Exhausted reports whether a 1-based attempt counter has reached the cap.
// The loop should escalate (per b.Escalation) rather than run attempt+1 when
// this is true and the latest verdict was not an accept.
func (b Budget) Exhausted(attempt int) bool {
	return attempt >= b.MaxAttempts
}

// Outcome folds a verdict and the attempt accounting into the action a worker
// should take next. It keeps the "what happens after a reduction" decision in
// one pure, tested place rather than scattered across the reconciler.
type Outcome string

const (
	// OutcomeAccept: the candidate converged; advance the work_item.
	OutcomeAccept Outcome = "accept"
	// OutcomeRetry: rejected but budget remains; run another attempt.
	OutcomeRetry Outcome = "retry"
	// OutcomeEscalate: escalate per the budget's rule (rejected with no
	// budget left, or the reducer escalated outright).
	OutcomeEscalate Outcome = "escalate"
)

// Next maps (verdict disposition, attempt, budget) to the worker's next
// action. It is the deterministic reduction *of the reduction* — the bounded-
// patience half of the loop:
//
//   - accept → OutcomeAccept, always.
//   - escalate → OutcomeEscalate, always (the reducer could not dispose).
//   - reject → OutcomeRetry while attempts remain, else OutcomeEscalate.
//
// Returning the budget's escalation rule alongside the outcome lets the
// caller act without re-reading the budget.
func (b Budget) Next(v Verdict, attempt int) (Outcome, Escalation) {
	switch v.Disposition {
	case domain.VerdictAccept:
		return OutcomeAccept, ""
	case domain.VerdictEscalate:
		return OutcomeEscalate, b.Escalation
	default: // reject
		if b.Exhausted(attempt) {
			return OutcomeEscalate, b.Escalation
		}
		return OutcomeRetry, ""
	}
}
