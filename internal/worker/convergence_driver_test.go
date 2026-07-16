package worker

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/workitems"
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

func TestDecodeEventAppendedInnerToleratesLegacyShapes(t *testing.T) {
	cases := []struct {
		name          string
		payload       string
		wantInnerKind string
		wantObject    bool
		wantMalformed bool
	}{
		{
			name:          "object inner",
			payload:       `{"inner_kind":"provider.progress","inner":{"pass":true}}`,
			wantInnerKind: "provider.progress",
			wantObject:    true,
		},
		{
			name:          "string-encoded object inner is recovered",
			payload:       `{"inner_kind":"human_response_recorded","inner":"{\"decision\":\"approved\"}"}`,
			wantInnerKind: "human_response_recorded",
			wantObject:    true,
		},
		{
			name:          "prose string inner is a benign non-signal",
			payload:       `{"inner_kind":"human_response_recorded","inner":"just some prose"}`,
			wantInnerKind: "human_response_recorded",
		},
		{
			name:          "numeric inner is a benign non-signal",
			payload:       `{"inner_kind":"agent.status","inner":7}`,
			wantInnerKind: "agent.status",
		},
		{
			name:          "array inner is a benign non-signal",
			payload:       `{"inner_kind":"agent.status","inner":[1,2]}`,
			wantInnerKind: "agent.status",
		},
		{
			name:          "missing inner is benign",
			payload:       `{"inner_kind":"agent.status"}`,
			wantInnerKind: "agent.status",
		},
		{
			name:          "null inner is benign",
			payload:       `{"inner_kind":"agent.status","inner":null}`,
			wantInnerKind: "agent.status",
		},
		{
			name:          "non-envelope payload is malformed",
			payload:       `"not an envelope at all"`,
			wantMalformed: true,
		},
		{
			name:          "envelope with non-string inner_kind is malformed",
			payload:       `{"inner_kind":7,"inner":{"pass":true}}`,
			wantMalformed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner, innerKind, reason := decodeEventAppendedInner([]byte(tc.payload))
			if (reason != "") != tc.wantMalformed {
				t.Fatalf("reason = %q, wantMalformed = %v", reason, tc.wantMalformed)
			}
			if (inner != nil) != tc.wantObject {
				t.Fatalf("inner = %v, wantObject = %v", inner, tc.wantObject)
			}
			if innerKind != tc.wantInnerKind {
				t.Fatalf("innerKind = %q, want %q", innerKind, tc.wantInnerKind)
			}
		})
	}
}

func TestCollectEventAppendedSignalsDerivesLatestAttributedReviewVerdict(t *testing.T) {
	actor := uuid.New()
	acceptedID := uuid.New()
	blockingID := uuid.New()
	rows := []eventAppendedSignalRow{
		testEventAppendedRow(uuid.New(), actor, `{"inner_kind":"checklist.item:event:review.verdict_recorded","inner":{"pass":true}}`),
		testEventAppendedRow(acceptedID, actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"accepted","pass":false,"reviewer":"forged","event_id":"forged"}}`),
		testEventAppendedRow(blockingID, actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"blocking_finding","pass":true}}`),
	}

	signals, unusable := collectEventAppendedSignals(rows)
	if len(unusable) != 0 {
		t.Fatalf("unusable = %+v, want none", unusable)
	}
	if len(signals) != 1 {
		t.Fatalf("signals = %+v, want one latest review signal", signals)
	}
	got := signals[0]
	if got.Kind != workitems.ReviewVerdictCheckKind || got.Pass == nil || *got.Pass {
		t.Fatalf("latest blocking review signal = %+v, want reserved pass:false", got)
	}
	if got.Source.EventID != blockingID.String() {
		t.Fatalf("source event id = %q, want %s", got.Source.EventID, blockingID)
	}
}

func TestCollectEventAppendedSignalsAcceptedWithFindingSupersedesBlocking(t *testing.T) {
	actor := uuid.New()
	acceptedID := uuid.New()
	rows := []eventAppendedSignalRow{
		testEventAppendedRow(uuid.New(), actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"blocking_finding"}}`),
		testEventAppendedRow(acceptedID, actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"accepted_with_finding"}}`),
	}

	signals, unusable := collectEventAppendedSignals(rows)
	if len(unusable) != 0 || len(signals) != 1 {
		t.Fatalf("signals/unusable = %+v/%+v, want one usable signal", signals, unusable)
	}
	if signals[0].Pass == nil || !*signals[0].Pass || signals[0].Source.EventID != acceptedID.String() {
		t.Fatalf("latest accepted_with_finding signal = %+v, want attributed pass:true", signals[0])
	}
}

func TestCollectEventAppendedSignalsLatestInvalidReviewFailsClosed(t *testing.T) {
	actor := uuid.New()
	invalidID := uuid.New()
	rows := []eventAppendedSignalRow{
		testEventAppendedRow(uuid.New(), actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"accepted"}}`),
		testEventAppendedRow(invalidID, actor, `{"inner_kind":"review.verdict_recorded","inner":{"verdict":"unknown"}}`),
	}

	signals, unusable := collectEventAppendedSignals(rows)
	if len(signals) != 0 {
		t.Fatalf("signals = %+v, latest invalid verdict must suppress older acceptance", signals)
	}
	if len(unusable) != 1 || unusable[0].id != invalidID {
		t.Fatalf("unusable = %+v, want latest invalid event %s", unusable, invalidID)
	}
}

func TestCollectEventAppendedSignalsReviewWithoutActorFailsClosed(t *testing.T) {
	id := uuid.New()
	rows := []eventAppendedSignalRow{{
		id:      id,
		payload: json.RawMessage(`{"inner_kind":"review.verdict_recorded","inner":{"verdict":"accepted"}}`),
	}}
	signals, unusable := collectEventAppendedSignals(rows)
	if len(signals) != 0 || len(unusable) != 1 || unusable[0].id != id {
		t.Fatalf("signals/unusable = %+v/%+v, unattributed review must fail closed", signals, unusable)
	}
}

func TestReservedReviewChecklistSignalCannotBeInjected(t *testing.T) {
	payload := map[string]any{"pass": true}
	if got := toConvergenceSignal(workitems.ReviewVerdictCheckKind, payload); got != nil {
		t.Fatalf("event-appended reserved signal was accepted: %+v", got)
	}
	if got := toConvergenceSignalFromSignalRow(workitems.ReviewVerdictCheckKind, "external", payload); got != nil {
		t.Fatalf("signals-table reserved signal was accepted: %+v", got)
	}
}

func testEventAppendedRow(id, actor uuid.UUID, payload string) eventAppendedSignalRow {
	return eventAppendedSignalRow{
		id:           id,
		actorTokenID: uuid.NullUUID{UUID: actor, Valid: true},
		payload:      json.RawMessage(payload),
	}
}
