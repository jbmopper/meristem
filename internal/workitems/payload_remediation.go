package workitems

// Append-only remediation of historical string-shaped inner payloads
// (audit e5a975cb: legacy MCP writers stringified objects before the
// write-side boundary sealed the seam at eed6ae0).
//
// The contract, per owner direction recorded on e5a975cb: original events are
// immutable and are never rewritten; a remediation is a NEW event, authored
// and attributed to the remediator, that references the immutable source
// event by id and supplies the object form of its inner payload. Consumers
// that already recover string-encoded objects keep doing so — a remediation
// only speaks where direct recovery yields nothing.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// PayloadShapeRemediatedInnerKind is the annotation inner kind. The event is
// appended through the ordinary boundary (object payload, remediator's own
// actor attribution) on the same work item as the source event.
const PayloadShapeRemediatedInnerKind = "payload_shape.remediated"

// PayloadShapeRemediation is the parsed annotation payload.
type PayloadShapeRemediation struct {
	// SourceEventID names the immutable event whose string-shaped inner this
	// annotation interprets.
	SourceEventID uuid.UUID
	// Parsed is the object form of the source's inner payload.
	Parsed map[string]any
}

// ParsePayloadShapeRemediation validates an annotation's inner object.
// Malformed annotations must be ignored by consumers (with durable evidence
// where the consumer records evidence), never partially applied.
func ParsePayloadShapeRemediation(inner map[string]any) (PayloadShapeRemediation, error) {
	if inner == nil {
		return PayloadShapeRemediation{}, fmt.Errorf("payload_shape.remediated payload must be an object")
	}
	rawID, ok := inner["source_event_id"].(string)
	if !ok || strings.TrimSpace(rawID) == "" {
		return PayloadShapeRemediation{}, fmt.Errorf("payload_shape.remediated requires source_event_id")
	}
	id, err := uuid.Parse(strings.TrimSpace(rawID))
	if err != nil || id == uuid.Nil {
		return PayloadShapeRemediation{}, fmt.Errorf("payload_shape.remediated source_event_id must be a valid uuid")
	}
	parsed, ok := inner["parsed"].(map[string]any)
	if !ok || parsed == nil {
		return PayloadShapeRemediation{}, fmt.Errorf("payload_shape.remediated requires parsed as a JSON object")
	}
	return PayloadShapeRemediation{SourceEventID: id, Parsed: parsed}, nil
}

// StringInnerClass is the dry-run classification of one historical
// string-shaped inner. The classifier is pure and idempotent so repeated
// dry runs over the same events reconcile to identical counts.
type StringInnerClass string

const (
	// StringInnerLeftAsHistory: the item is terminal; history is not
	// annotated (per the remediation contract, terminal items are untouched).
	StringInnerLeftAsHistory StringInnerClass = "left_as_history"
	// StringInnerRecoveredByReducer: the string parses as a JSON object, so
	// the universal legacy recovery already reads it; no annotation needed.
	StringInnerRecoveredByReducer StringInnerClass = "recovered_by_reducer"
	// StringInnerRemediationRequired: non-terminal item, string does not
	// parse as an object; only this class is a live-annotation candidate,
	// and each candidate requires accepted review before any append.
	StringInnerRemediationRequired StringInnerClass = "remediation_required"
)

// ClassifyStringInner classifies one string-shaped inner given its item's
// lifecycle state.
func ClassifyStringInner(state domain.WorkItemState, inner string) StringInnerClass {
	if state.Terminal() {
		return StringInnerLeftAsHistory
	}
	trimmed := strings.TrimSpace(inner)
	var obj map[string]any
	if err := json.Unmarshal([]byte(trimmed), &obj); err == nil && obj != nil {
		return StringInnerRecoveredByReducer
	}
	return StringInnerRemediationRequired
}
