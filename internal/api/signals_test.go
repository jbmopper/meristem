package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/signals"
)

func validSignalWorkSpec() json.RawMessage {
	return json.RawMessage(`{
		"schema_version": "legacy.work_spec.v1",
		"kind": "repair",
		"title": "Worker retry budget is exhausted too early",
		"priority": "P1",
		"objective": "Retry transient worker failures per configuration.",
		"details": "The worker stopped after one transient failure.",
		"source": {
			"kind": "system_event",
			"identifier": "abc123",
			"external_ref": "logs/system_events.jsonl#L4291"
		},
		"target": {
			"repo": "jay",
			"path": "issue_workflow/repair.py",
			"line_start": 1,
			"line_end": 10
		},
		"acceptance_criteria": [
			"Transient failures are retried according to configuration."
		],
		"validation": {
			"commands": ["uv run pytest"],
			"notes": ["Run targeted tests first."]
		},
		"constraints": ["Do not change idempotency keys."],
		"labels": ["repair"],
		"implementation_notes": ["Keep the fix narrow."]
	}`)
}

func TestValidateWorkSpecAcceptsSchemaSubset(t *testing.T) {
	if err := validateWorkSpec(validSignalWorkSpec()); err != nil {
		t.Fatalf("expected valid work_spec, got %v", err)
	}
}

func TestValidateWorkSpecRejectsMissingRequiredFields(t *testing.T) {
	raw := json.RawMessage(`{"schema_version":"legacy.work_spec.v1","kind":"repair","title":"x","priority":"P1"}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "acceptance_criteria") {
		t.Fatalf("expected acceptance_criteria error, got %v", err)
	}
}

func TestValidateWorkSpecRejectsWrongSchemaVersion(t *testing.T) {
	raw := json.RawMessage(`{"schema_version":"wrong","kind":"repair","title":"x","priority":"P1","acceptance_criteria":["x"]}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("expected schema_version error, got %v", err)
	}
}

func TestValidateWorkSpecRejectsInvalidPriority(t *testing.T) {
	raw := json.RawMessage(`{"schema_version":"legacy.work_spec.v1","kind":"repair","title":"x","priority":"P9","acceptance_criteria":["x"]}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("expected priority error, got %v", err)
	}
}

func TestValidateWorkSpecRejectsUnknownTopLevelField(t *testing.T) {
	raw := json.RawMessage(`{"schema_version":"legacy.work_spec.v1","kind":"repair","title":"x","priority":"P1","acceptance_criteria":["x"],"surprise":true}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestValidateWorkSpecRejectsNestedShapeErrors(t *testing.T) {
	raw := json.RawMessage(`{
		"schema_version":"legacy.work_spec.v1",
		"kind":"repair",
		"title":"x",
		"priority":"P1",
		"acceptance_criteria":["x"],
		"source":{"kind":"review_finding"},
		"target":{"line_start":0},
		"validation":{"commands":["ok"]}
	}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "source.identifier") {
		t.Fatalf("expected source.identifier error first, got %v", err)
	}

	raw = json.RawMessage(`{
		"schema_version":"legacy.work_spec.v1",
		"kind":"repair",
		"title":"x",
		"priority":"P1",
		"acceptance_criteria":["x"],
		"target":{"line_start":0}
	}`)
	if err := validateWorkSpec(raw); err == nil || !strings.Contains(err.Error(), "line_start") {
		t.Fatalf("expected line_start error, got %v", err)
	}
}

func TestSignalResponseFromPrefixesFingerprintAndIncludesEvents(t *testing.T) {
	signalID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	signalEventID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	workItemID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	workItemEventID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	resp := signalResponseFrom(signals.ReceiveResult{
		SignalID:        signalID,
		SignalEventID:   signalEventID,
		WorkItemID:      workItemID,
		WorkItemEventID: workItemEventID,
		CreatedWorkItem: true,
		Fingerprint:     strings.Repeat("a", 64),
		DedupeKey:       "repo:x",
	}, "idem-1")

	if resp.Idempotency.Key != "idem-1" {
		t.Fatalf("unexpected idempotency block: %+v", resp.Idempotency)
	}
	body, err := json.Marshal(resp.Idempotency)
	if err != nil {
		t.Fatalf("marshal idempotency block: %v", err)
	}
	if strings.Contains(string(body), `"replayed"`) {
		t.Fatalf("idempotency block must not surface a replayed boolean (would be a frozen lie on cache hits): %s", body)
	}
	if resp.Resource.Kind != "signal" || resp.Resource.ID != signalID {
		t.Fatalf("unexpected resource block: %+v", resp.Resource)
	}
	if resp.WorkItem.ID != workItemID {
		t.Fatalf("unexpected work_item block: %+v", resp.WorkItem)
	}
	if resp.Events.SignalReceived != signalEventID {
		t.Fatalf("signal event not surfaced: %+v", resp.Events)
	}
	if resp.Events.WorkItemCreated == nil || *resp.Events.WorkItemCreated != workItemEventID {
		t.Fatalf("work item event not surfaced: %+v", resp.Events)
	}
	if resp.Fingerprint != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("fingerprint should be algorithm-prefixed, got %q", resp.Fingerprint)
	}
}
