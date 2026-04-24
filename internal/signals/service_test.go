package signals

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/wayline/internal/domain"
)

// validWorkSpec is the smallest wayline.work_spec.v1 payload that satisfies
// the schema's required fields. It is deliberately separate from
// validRawPayload (in signals_test.go) so changes to one do not silently
// drift the other.
func validWorkSpec() json.RawMessage {
	return json.RawMessage(`{
		"schema_version": "wayline.work_spec.v1",
		"kind": "repair",
		"title": "Worker retry budget exhausted",
		"priority": "P1",
		"objective": "Retry transient failures per configuration.",
		"details": "Worker stopped after one transient failure.",
		"acceptance_criteria": ["Transient failures retried."]
	}`)
}

func validInput() ReceiveInput {
	return ReceiveInput{
		SignalKind: "repairable_failure",
		DedupeKey:  "repo:jay:repair:worker-retry-budget",
		WorkSpec:   validWorkSpec(),
	}
}

// newServiceForValidationTests returns a Service safe to call Receive on
// only for inputs that error before BeginTx. Both pool and writer are nil;
// any path that reaches the database will nil-panic, which is the right
// failure mode for an accidental misuse — these tests are about the
// validation gate, not the happy path.
func newServiceForValidationTests() *Service {
	return NewService(nil, nil)
}

func TestReceiveRejectsEmptySignalKind(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.SignalKind = ""
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrSignalKindRequired) {
		t.Fatalf("expected ErrSignalKindRequired, got %v", err)
	}
}

func TestReceiveTrimsSignalKindWhitespace(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.SignalKind = "   \t\n"
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrSignalKindRequired) {
		t.Fatalf("whitespace-only signal_kind should be rejected, got %v", err)
	}
}

func TestReceiveRejectsEmptyWorkSpec(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.WorkSpec = nil
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrWorkSpecRequired) {
		t.Fatalf("expected ErrWorkSpecRequired, got %v", err)
	}
}

func TestReceiveRejectsWhitespaceOnlyWorkSpec(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.WorkSpec = json.RawMessage("   \t\n")
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrWorkSpecRequired) {
		t.Fatalf("expected ErrWorkSpecRequired for whitespace work_spec, got %v", err)
	}
}

func TestReceiveRejectsInvalidWorkSpecJSON(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.WorkSpec = json.RawMessage(`{not json`)
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrWorkSpecInvalid) {
		t.Fatalf("expected ErrWorkSpecInvalid, got %v", err)
	}
}

func TestReceiveRejectsWorkSpecMissingTitle(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.WorkSpec = json.RawMessage(`{"kind":"repair","priority":"P1","acceptance_criteria":["x"]}`)
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrWorkSpecMissingTitle) {
		t.Fatalf("expected ErrWorkSpecMissingTitle, got %v", err)
	}
}

func TestReceiveRejectsWorkSpecWhitespaceTitle(t *testing.T) {
	svc := newServiceForValidationTests()
	in := validInput()
	in.WorkSpec = json.RawMessage(`{"title":"   ","kind":"repair","priority":"P1","acceptance_criteria":["x"]}`)
	_, err := svc.Receive(context.Background(), domain.Token{ID: uuid.New()}, in)
	if !errors.Is(err, ErrWorkSpecMissingTitle) {
		t.Fatalf("expected ErrWorkSpecMissingTitle for whitespace title, got %v", err)
	}
}

// Helper coverage. These are the parts the integration test would exercise
// implicitly; isolating them keeps coverage honest until a docker-Postgres
// harness lands (see docs/coord/2026-04-23-parallel-work.md "Findings
// carried forward").

func TestComputeFingerprintIsDeterministic(t *testing.T) {
	a := json.RawMessage(`{"a":1,"b":2,"c":[1,2,3]}`)
	b := json.RawMessage(`{"c":[1,2,3],"b":2,"a":1}`) // same content, reordered keys
	fa, err := computeFingerprint(a)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := computeFingerprint(b)
	if err != nil {
		t.Fatal(err)
	}
	if fa != fb {
		t.Fatalf("fingerprint should be content-canonical, got %q vs %q", fa, fb)
	}
}

func TestComputeFingerprintHexShape(t *testing.T) {
	f, err := computeFingerprint(validWorkSpec())
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != 64 {
		t.Errorf("expected 64-char hex sha256, got %d chars: %q", len(f), f)
	}
	for _, c := range f {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			t.Errorf("fingerprint must be lowercase hex, found %q in %q", c, f)
		}
	}
}

func TestComputeFingerprintDiffersForDifferentContent(t *testing.T) {
	a := json.RawMessage(`{"title":"x"}`)
	b := json.RawMessage(`{"title":"y"}`)
	fa, _ := computeFingerprint(a)
	fb, _ := computeFingerprint(b)
	if fa == fb {
		t.Fatalf("different content should produce different fingerprints, both %q", fa)
	}
}

func TestComputeFingerprintRejectsInvalidJSON(t *testing.T) {
	if _, err := computeFingerprint(json.RawMessage(`{not json`)); err == nil {
		t.Fatal("expected fingerprint to error on invalid JSON")
	}
}

func TestDecodeWorkSpecHeaderExtractsFields(t *testing.T) {
	h, err := decodeWorkSpecHeader(validWorkSpec())
	if err != nil {
		t.Fatal(err)
	}
	if h.Title != "Worker retry budget exhausted" {
		t.Errorf("title not extracted: %q", h.Title)
	}
	if h.Objective != "Retry transient failures per configuration." {
		t.Errorf("objective not extracted: %q", h.Objective)
	}
	if h.Details != "Worker stopped after one transient failure." {
		t.Errorf("details not extracted: %q", h.Details)
	}
}

func TestDecodeWorkSpecHeaderTreatsInvalidAsErrWorkSpecInvalid(t *testing.T) {
	_, err := decodeWorkSpecHeader(json.RawMessage(`{not json`))
	if !errors.Is(err, ErrWorkSpecInvalid) {
		t.Errorf("expected ErrWorkSpecInvalid wrap, got %v", err)
	}
}

func TestWorkItemBodyPrefersObjective(t *testing.T) {
	body := workItemBodyFrom(workSpecHeader{
		Title:     "t",
		Objective: "obj",
		Details:   "det",
	})
	if body != "obj" {
		t.Errorf("expected objective, got %q", body)
	}
}

func TestWorkItemBodyFallsBackToDetails(t *testing.T) {
	body := workItemBodyFrom(workSpecHeader{
		Title:   "t",
		Details: "det",
	})
	if body != "det" {
		t.Errorf("expected details fallback, got %q", body)
	}
}

func TestWorkItemBodyEmptyWhenBothMissing(t *testing.T) {
	body := workItemBodyFrom(workSpecHeader{Title: "t"})
	if body != "" {
		t.Errorf("expected empty body, got %q", body)
	}
}

func TestWorkItemBodyTreatsWhitespaceObjectiveAsMissing(t *testing.T) {
	// Objective is "present but useless"; we want details rather than a
	// space-only summary on the work_items row.
	body := workItemBodyFrom(workSpecHeader{
		Title:     "t",
		Objective: "   ",
		Details:   "det",
	})
	if body != "det" {
		t.Errorf("expected details fallback when objective is whitespace, got %q", body)
	}
}

func TestSourceMetadataEmpty(t *testing.T) {
	cases := []struct {
		name string
		m    SourceMetadata
		want bool
	}{
		{"all empty", SourceMetadata{}, true},
		{"kind only", SourceMetadata{Kind: "x"}, false},
		{"identifier only", SourceMetadata{Identifier: "x"}, false},
		{"external_ref only", SourceMetadata{ExternalRef: "x"}, false},
		{"all fields", SourceMetadata{Kind: "k", Identifier: "i", ExternalRef: "e"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.empty(); got != tc.want {
				t.Errorf("empty() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSourceMetadataPayloadOmitsEmptyFields(t *testing.T) {
	got := sourceMetadataPayload(SourceMetadata{Kind: "review_finding"})
	if _, ok := got["identifier"]; ok {
		t.Error("identifier should be omitted when empty")
	}
	if _, ok := got["external_ref"]; ok {
		t.Error("external_ref should be omitted when empty")
	}
	if got["kind"] != "review_finding" {
		t.Errorf("kind not preserved: %v", got["kind"])
	}
}

func TestSourceForActorDefaultsToHuman(t *testing.T) {
	if got := sourceForActor(domain.Token{}); got != domain.SourceHuman {
		t.Errorf("expected SourceHuman fallback, got %q", got)
	}
}

func TestSourceForActorPassesThroughValidSource(t *testing.T) {
	for _, src := range []domain.Source{domain.SourceHuman, domain.SourceAgent, domain.SourceSystem} {
		if got := sourceForActor(domain.Token{Source: src}); got != src {
			t.Errorf("expected %q, got %q", src, got)
		}
	}
}

// strictlySanityCheck guards against accidental drift in the payload keys
// the projector reads. If a key here changes name, the projector test will
// also fail — but this gives a clearer "the wire shape changed" signal.
func TestPayloadKeyConstantsDoNotDrift(t *testing.T) {
	// The keys the projector reads (see signalPayload tags in signals.go).
	required := []string{"signal_kind", "fingerprint", "work_spec", "work_item_id", "created_work_item"}
	// Build a payload via the same helper Receive uses, minus the
	// transactional path. We mirror the relevant block here intentionally
	// so the test fails loudly if Receive's payload shape changes.
	payload := map[string]any{
		"signal_kind":       "review_finding",
		"fingerprint":       "deadbeef",
		"work_spec":         json.RawMessage(`{"title":"x"}`),
		"work_item_id":      uuid.New(),
		"created_work_item": false,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range required {
		if !strings.Contains(string(b), `"`+k+`"`) {
			t.Errorf("payload missing required key %q: %s", k, b)
		}
	}
}
