package worker

import (
	"testing"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestDecideConvergenceStepAppendsFirstVerdict(t *testing.T) {
	current := reductionForDecision(1, "digest-a", domain.VerdictReject)
	decision := decideConvergenceStep(nil, current, defaultConvergenceBudget)

	if !decision.AppendCurrent {
		t.Fatal("first verdict should be appended")
	}
	if decision.SkipStale {
		t.Fatal("first verdict must not be treated as stale")
	}
	if decision.Outcome != convergence.OutcomeRetry {
		t.Fatalf("outcome = %q, want retry", decision.Outcome)
	}
	if decision.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", decision.Attempt)
	}
}

func TestDecideConvergenceStepSkipsStaleRejectedInputs(t *testing.T) {
	last := &convergenceVerdictRecord{
		Attempt:      1,
		InputsDigest: "digest-a",
		Verdict: convergence.Verdict{
			Disposition: domain.VerdictReject,
			Reason:      "missing: tests_green",
		},
	}
	current := reductionForDecision(2, "digest-a", domain.VerdictReject)

	decision := decideConvergenceStep(last, current, defaultConvergenceBudget)

	if decision.AppendCurrent {
		t.Fatal("unchanged rejected inputs must not append another verdict")
	}
	if !decision.SkipStale {
		t.Fatal("unchanged rejected inputs under budget should be skipped")
	}
	if decision.Attempt != 1 {
		t.Fatalf("attempt = %d, want latest recorded attempt 1", decision.Attempt)
	}
}

func TestDecideConvergenceStepReplaysTerminalActionForUnchangedInputs(t *testing.T) {
	lastAccept := &convergenceVerdictRecord{
		Attempt:      1,
		InputsDigest: "digest-a",
		Verdict: convergence.Verdict{
			Disposition: domain.VerdictAccept,
			Reason:      "all checks passed",
		},
	}
	acceptDecision := decideConvergenceStep(lastAccept, reductionForDecision(2, "digest-a", domain.VerdictAccept), defaultConvergenceBudget)
	if acceptDecision.AppendCurrent || acceptDecision.SkipStale {
		t.Fatalf("accept replay should apply prior action without appending/skipping: %+v", acceptDecision)
	}
	if acceptDecision.Outcome != convergence.OutcomeAccept || acceptDecision.Attempt != 1 {
		t.Fatalf("accept decision = %+v, want prior accept attempt 1", acceptDecision)
	}

	lastRejectAtBudget := &convergenceVerdictRecord{
		Attempt:      defaultConvergenceMaxAttempts,
		InputsDigest: "digest-b",
		Verdict: convergence.Verdict{
			Disposition: domain.VerdictReject,
			Reason:      "failing: tests_green",
		},
	}
	escalateDecision := decideConvergenceStep(lastRejectAtBudget, reductionForDecision(defaultConvergenceMaxAttempts+1, "digest-b", domain.VerdictReject), defaultConvergenceBudget)
	if escalateDecision.AppendCurrent || escalateDecision.SkipStale {
		t.Fatalf("exhausted replay should apply escalation without appending/skipping: %+v", escalateDecision)
	}
	if escalateDecision.Outcome != convergence.OutcomeEscalate || escalateDecision.Escalation != convergence.EscalateHandToHuman {
		t.Fatalf("escalate decision = %+v, want hand-to-human escalation", escalateDecision)
	}
	if escalateDecision.Attempt != defaultConvergenceMaxAttempts {
		t.Fatalf("attempt = %d, want latest recorded attempt", escalateDecision.Attempt)
	}
}

func TestDecideConvergenceStepAppendsWhenInputsChange(t *testing.T) {
	last := &convergenceVerdictRecord{
		Attempt:      1,
		InputsDigest: "digest-a",
		Verdict: convergence.Verdict{
			Disposition: domain.VerdictReject,
			Reason:      "missing: tests_green",
		},
	}
	current := reductionForDecision(2, "digest-b", domain.VerdictReject)

	decision := decideConvergenceStep(last, current, defaultConvergenceBudget)

	if !decision.AppendCurrent {
		t.Fatal("changed inputs should append the next attempt")
	}
	if decision.SkipStale {
		t.Fatal("changed inputs must not be skipped as stale")
	}
	if decision.Attempt != 2 {
		t.Fatalf("attempt = %d, want current attempt 2", decision.Attempt)
	}
}

func reductionForDecision(attempt int, digest string, disposition domain.Verdict) convergence.Reduction {
	return convergence.Reduction{
		Attempt:      attempt,
		InputsDigest: digest,
		Verdict: convergence.Verdict{
			Disposition: disposition,
			Reason:      "test reason",
		},
	}
}
