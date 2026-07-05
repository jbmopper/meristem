package worker

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestDefaultBudgetsCoversAllNonTerminalStates(t *testing.T) {
	b := DefaultBudgets()
	for _, s := range []domain.WorkItemState{
		domain.WorkItemCaptured,
		domain.WorkItemTriaged,
		domain.WorkItemPlanned,
		domain.WorkItemAwaitingApproval,
		domain.WorkItemRunning,
		domain.WorkItemBlocked,
	} {
		if _, ok := b.ByState[s]; !ok {
			t.Errorf("DefaultBudgets is missing non-terminal state %q", s)
		}
	}
	for s := range b.ByState {
		if s.Terminal() {
			t.Errorf("DefaultBudgets includes terminal state %q", s)
		}
	}
}

func TestBudgetsValidateRejectsTerminalState(t *testing.T) {
	b := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemDone: time.Hour,
	}}
	err := b.validate()
	if err == nil {
		t.Fatal("expected validate() to reject a terminal-state budget")
	}
	if !strings.Contains(err.Error(), "terminal") {
		t.Errorf("expected error to mention 'terminal', got: %v", err)
	}
}

func TestBudgetsValidateRejectsUnknownState(t *testing.T) {
	b := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemState("nonsense"): time.Hour,
	}}
	if err := b.validate(); err == nil {
		t.Fatal("expected validate() to reject unknown state")
	}
}

func TestBudgetsValidateRejectsNegativeDuration(t *testing.T) {
	b := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: -time.Second,
	}}
	if err := b.validate(); err == nil {
		t.Fatal("expected validate() to reject negative budget")
	}
}

func TestBudgetsStatesSkipsZeroAndNegative(t *testing.T) {
	b := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
		domain.WorkItemTriaged:  0,
		domain.WorkItemPlanned:  time.Minute,
	}}
	got := b.states()
	want := []string{string(domain.WorkItemCaptured), string(domain.WorkItemPlanned)}
	if len(got) != len(want) {
		t.Fatalf("states() returned %v, want %v", got, want)
	}
	// Order is alphabetical for stability.
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("states()[%d] = %q, want %q (full got=%v)", i, got[i], want[i], got)
		}
	}
}

func TestEvaluateBreachesSkipsItemsUnderBudget(t *testing.T) {
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	cs := []Candidate{
		{ID: uuid.New(), State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-30 * time.Minute), Budget: time.Hour},
		{ID: uuid.New(), State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-90 * time.Minute), Budget: time.Hour},
	}
	breaches := EvaluateBreaches(now, cs)
	if len(breaches) != 1 {
		t.Fatalf("expected 1 breach, got %d", len(breaches))
	}
	if breaches[0].Candidate.ID != cs[1].ID {
		t.Errorf("expected the older candidate to breach, got %s", breaches[0].Candidate.ID)
	}
	if breaches[0].Budget != time.Hour {
		t.Errorf("expected budget=1h, got %v", breaches[0].Budget)
	}
	if breaches[0].Age != 90*time.Minute {
		t.Errorf("expected age=90m, got %v", breaches[0].Age)
	}
}

func TestEvaluateBreachesSkipsItemsExactlyAtBudget(t *testing.T) {
	// Boundary: an item whose dwell equals the budget exactly is *not* in
	// breach yet. The condition is strict inequality (age > budget); equal
	// means "right at the line", which the spec language "longer than"
	// excludes.
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	cs := []Candidate{
		{ID: uuid.New(), State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-time.Hour), Budget: time.Hour},
	}
	if breaches := EvaluateBreaches(now, cs); len(breaches) != 0 {
		t.Fatalf("expected 0 breaches at the boundary, got %d", len(breaches))
	}
}

func TestEvaluateBreachesSkipsStatesWithoutBudget(t *testing.T) {
	// A candidate with no resolved budget is implicitly infinite. This is the
	// opt-in property: gradual budget rollout cannot accidentally breach states
	// the operator has not yet reasoned about.
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	cs := []Candidate{
		{ID: uuid.New(), State: domain.WorkItemTriaged, StateEnteredAt: now.Add(-365 * 24 * time.Hour)},
	}
	if breaches := EvaluateBreaches(now, cs); len(breaches) != 0 {
		t.Fatalf("expected 0 breaches when state has no budget, got %d (%+v)", len(breaches), breaches)
	}
}

func TestEvaluateBreachesPreservesInputOrder(t *testing.T) {
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	a := uuid.New()
	b := uuid.New()
	c := uuid.New()
	cs := []Candidate{
		{ID: a, State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-2 * time.Hour), Budget: time.Hour},
		{ID: b, State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-30 * time.Minute), Budget: time.Hour},
		{ID: c, State: domain.WorkItemCaptured, StateEnteredAt: now.Add(-3 * time.Hour), Budget: time.Hour},
	}
	breaches := EvaluateBreaches(now, cs)
	if len(breaches) != 2 {
		t.Fatalf("expected 2 breaches, got %d", len(breaches))
	}
	// Input order is (a, c) for the two over-budget items; the under-budget
	// b is skipped. The pure function preserves the input ordering for the
	// surviving items, which is what makes the downstream emit deterministic.
	if breaches[0].Candidate.ID != a || breaches[1].Candidate.ID != c {
		t.Errorf("EvaluateBreaches did not preserve input order; got [%s, %s], want [%s, %s]",
			breaches[0].Candidate.ID, breaches[1].Candidate.ID, a, c)
	}
}

func TestNewRejectsNilPool(t *testing.T) {
	_, err := New(nil, nil, DefaultBudgets(), nil, nil)
	if err == nil {
		t.Fatal("expected New to reject nil pool")
	}
	if !strings.Contains(err.Error(), "pool") {
		t.Errorf("expected error to mention 'pool', got: %v", err)
	}
}

// New's writer-nil and budgets-validate paths are covered by integration
// tests (which can hand a real pool to bypass the first guard). Avoiding a
// pgxpool stub here keeps this file dependency-free; the validate logic
// itself is fully exercised by the TestBudgetsValidate* tests above.
