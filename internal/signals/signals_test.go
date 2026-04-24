package signals

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// validRawPayload returns the map[string]any shape the /v1/signals handler
// is expected to pass to events.Writer.Append. Mirrors the documented
// signal.received event contract.
func validRawPayload() map[string]any {
	return map[string]any{
		"signal_kind":       "review_finding",
		"dedupe_key":        "repo:jay:repair:worker-retry-budget",
		"fingerprint":       "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		"work_spec":         json.RawMessage(`{"schema_version":"legacy.work_spec.v1","kind":"repair","title":"x","priority":"P1","acceptance_criteria":["x"]}`),
		"work_item_id":      "11111111-1111-1111-1111-111111111111",
		"created_work_item": true,
	}
}

func TestProjectorAdvertisesCanonicalKind(t *testing.T) {
	if (receivedProjector{}).Kind() != domain.EventSignalReceived {
		t.Errorf("projector kind drifted from domain.EventSignalReceived: got %q", (receivedProjector{}).Kind())
	}
	if domain.EventSignalReceived != "signal.received" {
		t.Errorf("event kind constant drifted from spec: got %q", domain.EventSignalReceived)
	}
	if domain.SubjectSignal != "signal" {
		t.Errorf("subject_kind constant drifted from spec: got %q", domain.SubjectSignal)
	}
}

func TestDecodeAcceptsValidPayload(t *testing.T) {
	p, err := decodeSignalPayload(validRawPayload())
	if err != nil {
		t.Fatal(err)
	}
	if p.SignalKind != "review_finding" {
		t.Errorf("signal_kind lost: %q", p.SignalKind)
	}
	if p.WorkItemID == uuid.Nil {
		t.Error("work_item_id lost")
	}
	if len(p.fingerprintBytes) != 32 {
		t.Errorf("fingerprint should decode to 32 bytes, got %d", len(p.fingerprintBytes))
	}
}

func TestDecodeRequiresSignalKind(t *testing.T) {
	raw := validRawPayload()
	delete(raw, "signal_kind")
	if _, err := decodeSignalPayload(raw); err == nil || !strings.Contains(err.Error(), "signal_kind") {
		t.Errorf("expected signal_kind error, got %v", err)
	}
}

func TestDecodeRequiresFingerprint(t *testing.T) {
	raw := validRawPayload()
	delete(raw, "fingerprint")
	if _, err := decodeSignalPayload(raw); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Errorf("expected fingerprint error, got %v", err)
	}
}

func TestDecodeRejectsNonHexFingerprint(t *testing.T) {
	raw := validRawPayload()
	raw["fingerprint"] = "not-hex-zzz"
	if _, err := decodeSignalPayload(raw); err == nil || !strings.Contains(err.Error(), "hex") {
		t.Errorf("expected hex error, got %v", err)
	}
}

func TestDecodeRejectsInvalidWorkSpecJSON(t *testing.T) {
	// Invalid JSON in work_spec is caught at one of two points: the outer
	// marshal step (json.RawMessage's MarshalJSON refuses to emit invalid
	// bytes) or the explicit json.Valid check in decodeSignalPayload.
	// Either is correct; the contract is "invalid JSON is rejected", not
	// "rejected at exactly this line".
	raw := validRawPayload()
	raw["work_spec"] = json.RawMessage(`{not json`)
	if _, err := decodeSignalPayload(raw); err == nil {
		t.Error("expected error from invalid work_spec JSON, got nil")
	}
}

func TestDecodeRequiresWorkItemID(t *testing.T) {
	raw := validRawPayload()
	delete(raw, "work_item_id")
	if _, err := decodeSignalPayload(raw); err == nil || !strings.Contains(err.Error(), "work_item_id") {
		t.Errorf("expected work_item_id error, got %v", err)
	}
}

func TestDecodeAcceptsNestedMapWorkSpec(t *testing.T) {
	// During a rebuild from `events`, work_spec arrives as map[string]any
	// (the JSONB decoder's natural shape) rather than as json.RawMessage.
	// Roundtrip must accept both — that's what keeps the projector
	// rebuild-safe per AGENTS.md.
	raw := validRawPayload()
	raw["work_spec"] = map[string]any{
		"schema_version":      "legacy.work_spec.v1",
		"kind":                "repair",
		"title":               "x",
		"priority":            "P1",
		"acceptance_criteria": []any{"x"},
	}
	p, err := decodeSignalPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(p.WorkSpec) {
		t.Error("nested map work_spec did not roundtrip to valid JSON")
	}
}

func TestDecodeOmitsDedupeKeyWhenAbsent(t *testing.T) {
	raw := validRawPayload()
	delete(raw, "dedupe_key")
	p, err := decodeSignalPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.DedupeKey != "" {
		t.Errorf("expected empty dedupe_key, got %q", p.DedupeKey)
	}
}
