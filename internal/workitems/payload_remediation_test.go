package workitems

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestClassifyStringInner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state domain.WorkItemState
		inner string
		want  StringInnerClass
	}{
		{"terminal always history", domain.WorkItemDone, `{"pass": true}`, StringInnerLeftAsHistory},
		{"canceled always history", domain.WorkItemCanceled, "prose", StringInnerLeftAsHistory},
		{"failed always history", domain.WorkItemFailed, `{"x":1}`, StringInnerLeftAsHistory},
		{"nonterminal object recovers", domain.WorkItemRunning, ` {"pass": true} `, StringInnerRecoveredByReducer},
		{"nonterminal prose needs remediation", domain.WorkItemRunning, "free prose", StringInnerRemediationRequired},
		{"nonterminal truncated json needs remediation", domain.WorkItemBlocked, `{"pass": true`, StringInnerRemediationRequired},
		{"nonterminal array needs remediation", domain.WorkItemPlanned, `[1,2]`, StringInnerRemediationRequired},
	} {
		if got := ClassifyStringInner(tc.state, tc.inner); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestParsePayloadShapeRemediation(t *testing.T) {
	valid := map[string]any{"source_event_id": uuid.New().String(), "parsed": map[string]any{"pass": true}}
	if _, err := ParsePayloadShapeRemediation(valid); err != nil {
		t.Fatalf("valid annotation rejected: %v", err)
	}
	for name, inner := range map[string]map[string]any{
		"nil payload":       nil,
		"missing source":    {"parsed": map[string]any{}},
		"malformed source":  {"source_event_id": "nope", "parsed": map[string]any{}},
		"nil-uuid source":   {"source_event_id": "00000000-0000-0000-0000-000000000000", "parsed": map[string]any{}},
		"missing parsed":    {"source_event_id": uuid.New().String()},
		"non-object parsed": {"source_event_id": uuid.New().String(), "parsed": "text"},
	} {
		if _, err := ParsePayloadShapeRemediation(inner); err == nil {
			t.Errorf("%s: malformed annotation accepted", name)
		}
	}
}
