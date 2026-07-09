package agentbudget

import "testing"

func TestBreachEventNoneWhenInBudget(t *testing.T) {
	d := Reduce(sampleBudget(), Observed{ToolCalls: 10})
	if _, _, ok := BreachEvent(sampleBudget(), d); ok {
		t.Fatal("in-budget decision should produce no breach event")
	}
}

func TestBreachEventDeterministicDiscriminator(t *testing.T) {
	b := sampleBudget()
	obs := Observed{ToolCalls: 250, RuntimeSeconds: 3600}
	p1, disc1, ok1 := BreachEvent(b, Reduce(b, obs))
	p2, disc2, ok2 := BreachEvent(b, Reduce(b, obs))
	if !ok1 || !ok2 {
		t.Fatal("over-budget decision should produce a breach event")
	}
	if disc1 != disc2 {
		t.Fatalf("discriminator not deterministic: %q vs %q", disc1, disc2)
	}
	if p1.BudgetHash != BudgetHash(b) {
		t.Fatalf("payload budget hash %q != %q", p1.BudgetHash, BudgetHash(b))
	}
	if p1.Reason != p2.Reason || len(p1.Breaches) != 2 {
		t.Fatalf("payload not stable: %+v vs %+v", p1, p2)
	}
	// The discriminator names the budget and the sorted breached dimensions.
	want := BudgetHash(b) + "|max_runtime_seconds,max_tool_calls"
	if disc1 != want {
		t.Fatalf("discriminator = %q, want %q", disc1, want)
	}
}

func TestBreachEventDiscriminatorSensitiveToBreachSet(t *testing.T) {
	b := sampleBudget()
	_, discOne, _ := BreachEvent(b, Reduce(b, Observed{ToolCalls: 250}))
	_, discTwo, _ := BreachEvent(b, Reduce(b, Observed{ToolCalls: 250, RuntimeSeconds: 3600}))
	if discOne == discTwo {
		t.Fatal("a newly breached dimension must change the discriminator (a distinct event)")
	}
}
