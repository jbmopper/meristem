package convergence

import (
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestBudgetValidate(t *testing.T) {
	if (Budget{}).Validate() == nil {
		t.Fatal("zero budget must be invalid (no escalation rule)")
	}
	if (Budget{MaxAttempts: 0, Escalation: EscalateFail}).Validate() == nil {
		t.Fatal("MaxAttempts 0 must be invalid")
	}
	if (Budget{MaxAttempts: 3, Escalation: "nonsense"}).Validate() == nil {
		t.Fatal("bad escalation must be invalid")
	}
	if err := (Budget{MaxAttempts: 3, Escalation: EscalateFail}).Validate(); err != nil {
		t.Fatalf("valid budget rejected: %v", err)
	}
}

func TestBudgetNext(t *testing.T) {
	b := Budget{MaxAttempts: 3, Escalation: EscalateHandToHuman}

	accept := Verdict{Disposition: domain.VerdictAccept}
	if out, _ := b.Next(accept, 1); out != OutcomeAccept {
		t.Fatalf("accept should be OutcomeAccept, got %q", out)
	}

	escalate := Verdict{Disposition: domain.VerdictEscalate}
	out, rule := b.Next(escalate, 1)
	if out != OutcomeEscalate || rule != EscalateHandToHuman {
		t.Fatalf("escalate verdict should escalate with rule, got %q/%q", out, rule)
	}

	reject := Verdict{Disposition: domain.VerdictReject}
	if out, _ := b.Next(reject, 1); out != OutcomeRetry {
		t.Fatalf("reject with budget left should retry, got %q", out)
	}
	if out, rule := b.Next(reject, 3); out != OutcomeEscalate || rule != EscalateHandToHuman {
		t.Fatalf("reject at cap should escalate, got %q/%q", out, rule)
	}
}

func TestRegistry(t *testing.T) {
	reg := DefaultRegistry()
	if _, err := reg.Get("majority_vote"); err != nil {
		t.Fatalf("majority_vote should be registered: %v", err)
	}
	if _, err := reg.Get("does_not_exist"); err == nil {
		t.Fatal("unknown reducer should error")
	}
	if err := reg.Register(MajorityVote{}); err == nil {
		t.Fatal("duplicate identity should be refused")
	}
	if err := reg.Register(nil); err == nil {
		t.Fatal("nil reducer should be refused")
	}
}
