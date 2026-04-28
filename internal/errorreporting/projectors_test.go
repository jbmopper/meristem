package errorreporting

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestReportedProjectorRejectsWrongSubjectKind(t *testing.T) {
	ev := validReportedEvent()
	ev.SubjectKind = domain.SubjectWorkItem
	err := (reportedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "expected subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}
}

func TestReportedProjectorValidatesPayload(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{"component", map[string]any{"code": "x", "message": "m", "severity": "error", "details": map[string]any{}}, "component is required"},
		{"code", map[string]any{"component": "c", "message": "m", "severity": "error", "details": map[string]any{}}, "code is required"},
		{"message", map[string]any{"component": "c", "code": "x", "severity": "error", "details": map[string]any{}}, "message is required"},
		{"severity", map[string]any{"component": "c", "code": "x", "message": "m", "severity": "strange", "details": map[string]any{}}, "invalid severity"},
		{"details", map[string]any{"component": "c", "code": "x", "message": "m", "severity": "error", "details": []any{"x"}}, "details must be a JSON object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := validReportedEvent()
			ev.Payload = tc.payload
			err := (reportedProjector{}).Apply(context.Background(), nil, ev)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestMaskProjectorsRejectWrongSubjectKind(t *testing.T) {
	ev := domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   uuid.New(),
		Kind:        domain.EventDeterministicErrorMasked,
		Source:      domain.SourceSystem,
		Payload:     map[string]any{"reason": "noise"},
		OccurredAt:  time.Unix(0, 0),
	}
	err := (maskedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "expected subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}

	ev.Kind = domain.EventDeterministicErrorUnmasked
	err = (unmaskedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "expected subject_kind") {
		t.Fatalf("expected subject_kind error, got %v", err)
	}
}

func validReportedEvent() domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectDeterministicError,
		SubjectID:   uuid.New(),
		Kind:        domain.EventDeterministicErrorReported,
		Source:      domain.SourceSystem,
		Payload: map[string]any{
			"component": "projections",
			"code":      "projection_failed",
			"message":   "projection failed",
			"severity":  "error",
			"details":   map[string]any{"kind": "work_item.created"},
		},
		OccurredAt: time.Unix(0, 0),
	}
}
