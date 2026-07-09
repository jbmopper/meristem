package agentbudget

import "testing"

func sampleBudget() Budget {
	return Budget{
		MaxContextFiles:   50,
		MaxPromptWords:    8000,
		MaxResponseWords:  4000,
		MaxTurns:          20,
		MaxRuntimeSeconds: 1800,
		MaxToolCalls:      100,
		MaxArtifacts:      10,
	}
}

func TestBudgetHashIsDeterministicAndFieldSensitive(t *testing.T) {
	b := sampleBudget()
	if BudgetHash(b) != BudgetHash(b) {
		t.Fatal("same budget hashed to different values")
	}
	// A changed ceiling mints a new identity.
	b2 := sampleBudget()
	b2.MaxToolCalls = 101
	if BudgetHash(b) == BudgetHash(b2) {
		t.Fatal("different budgets hashed to the same value")
	}
	// Field order in the struct literal must not matter (it does not, but pin it).
	reordered := Budget{
		MaxArtifacts:      10,
		MaxToolCalls:      100,
		MaxRuntimeSeconds: 1800,
		MaxTurns:          20,
		MaxResponseWords:  4000,
		MaxPromptWords:    8000,
		MaxContextFiles:   50,
	}
	if BudgetHash(b) != BudgetHash(reordered) {
		t.Fatal("logically equal budgets hashed differently")
	}
}

func TestBudgetValidRejectsNegative(t *testing.T) {
	if err := sampleBudget().Valid(); err != nil {
		t.Fatalf("valid budget rejected: %v", err)
	}
	bad := sampleBudget()
	bad.MaxRuntimeSeconds = -1
	if err := bad.Valid(); err == nil {
		t.Fatal("negative ceiling accepted")
	}
	if err := (Budget{}).Valid(); err != nil {
		t.Fatalf("zero budget (all dimensions disabled) rejected: %v", err)
	}
}

func TestEnforced(t *testing.T) {
	if (Budget{}).Enforced() {
		t.Fatal("all-zero budget should not be enforced")
	}
	if !(Budget{MaxToolCalls: 1}).Enforced() {
		t.Fatal("a single positive ceiling should be enforced")
	}
	if !sampleBudget().Enforced() {
		t.Fatal("populated budget should be enforced")
	}
}

func TestReduceZeroBudgetAcceptsAnything(t *testing.T) {
	d := Reduce(Budget{}, Observed{ToolCalls: 1_000_000, RuntimeSeconds: 999999})
	if d.Disposition != Accept {
		t.Fatalf("unbudgeted run should accept, got %s (%s)", d.Disposition, d.Reason)
	}
}

func TestReduceAtCeilingAccepts(t *testing.T) {
	b := sampleBudget()
	d := Reduce(b, Observed{
		ContextFiles: 50, PromptWords: 8000, ResponseWords: 4000,
		Turns: 20, RuntimeSeconds: 1800, ToolCalls: 100, Artifacts: 10,
	})
	if d.Disposition != Accept {
		t.Fatalf("exactly-at-ceiling run should accept, got %s (%s)", d.Disposition, d.Reason)
	}
}

func TestReduceReportsBreachesSortedAndDeterministic(t *testing.T) {
	b := sampleBudget()
	obs := Observed{RuntimeSeconds: 3600, ToolCalls: 250, ContextFiles: 60}
	d := Reduce(b, obs)
	if d.Disposition != Reject {
		t.Fatalf("over-budget run should reject, got %s", d.Disposition)
	}
	if len(d.Breaches) != 3 {
		t.Fatalf("expected 3 breaches, got %d: %+v", len(d.Breaches), d.Breaches)
	}
	// Sorted by dimension name for a stable reason.
	want := []string{"max_context_files", "max_runtime_seconds", "max_tool_calls"}
	for i, br := range d.Breaches {
		if br.Dimension != want[i] {
			t.Fatalf("breach %d: want %s, got %s", i, want[i], br.Dimension)
		}
	}
	if d.Reason != Reduce(b, obs).Reason {
		t.Fatal("reason is not deterministic")
	}
	if d.Reason != "budget breached: max_context_files 60>50, max_runtime_seconds 3600>1800, max_tool_calls 250>100" {
		t.Fatalf("unexpected reason: %q", d.Reason)
	}
}
