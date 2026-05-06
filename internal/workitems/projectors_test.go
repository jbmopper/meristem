package workitems

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestCreatedProjector_RejectsBlankTitle(t *testing.T) {
	ev := domain.Event{
		ID:           uuid.New(),
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    uuid.New(),
		Kind:         domain.EventWorkItemCreated,
		Source:       domain.SourceHuman,
		ActorTokenID: ptrUUID(uuid.New()),
		Payload:      map[string]any{"title": "", "body": "x"},
		OccurredAt:   time.Unix(0, 0),
	}
	// Pass nil tx: the validation check fires before tx.Exec, so nothing
	// dereferences it. If a future change moves the title check below the
	// Exec call, this test will panic and surface the regression.
	if err := (createdProjector{}).Apply(context.Background(), nil, ev); err == nil || !strings.Contains(err.Error(), "title is required") {
		t.Errorf("expected title-required error, got %v", err)
	}
}

func TestCreatedProjector_RejectsInvalidMetadata(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{
			name: "blank convergence check",
			payload: map[string]any{
				"title": "x",
				"suggested_convergence_checks": []string{
					"go test ./...",
					" ",
				},
			},
			want: "suggested_convergence_checks[1] is blank",
		},
		{
			name: "invalid human review status",
			payload: map[string]any{
				"title":               "x",
				"human_review_status": "rubber_stamped",
			},
			want: "invalid human_review_status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := domain.Event{
				ID:          uuid.New(),
				SubjectKind: domain.SubjectWorkItem,
				SubjectID:   uuid.New(),
				Kind:        domain.EventWorkItemCreated,
				Source:      domain.SourceHuman,
				Payload:     tc.payload,
				OccurredAt:  time.Unix(0, 0),
			}
			if err := (createdProjector{}).Apply(context.Background(), nil, ev); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestTransitionedProjector_RejectsBlankTo(t *testing.T) {
	ev := domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   uuid.New(),
		Kind:        domain.EventWorkItemTransitioned,
		Source:      domain.SourceHuman,
		Payload:     map[string]any{"to": ""},
		OccurredAt:  time.Unix(0, 0),
	}
	if err := (transitionedProjector{}).Apply(context.Background(), nil, ev); err == nil || !strings.Contains(err.Error(), "to is required") {
		t.Errorf("expected to-required error, got %v", err)
	}
}

func TestMetadataUpdatedProjector_RejectsInvalidMetadata(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{
			name: "blank convergence check",
			payload: map[string]any{
				"to": map[string]any{
					"suggested_convergence_checks": []string{"ok", "\t"},
					"human_review_status":          "waved_through",
				},
			},
			want: "suggested_convergence_checks[1] is blank",
		},
		{
			name: "invalid human review status",
			payload: map[string]any{
				"to": map[string]any{
					"human_review_status": "looks_good",
				},
			},
			want: "invalid human_review_status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := domain.Event{
				ID:          uuid.New(),
				SubjectKind: domain.SubjectWorkItem,
				SubjectID:   uuid.New(),
				Kind:        domain.EventWorkItemMetadataUpdated,
				Source:      domain.SourceHuman,
				Payload:     tc.payload,
				OccurredAt:  time.Unix(0, 0),
			}
			if err := (metadataUpdatedProjector{}).Apply(context.Background(), nil, ev); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestRelationAddedProjector_RejectsNilParticipants(t *testing.T) {
	cases := []map[string]any{
		{"parent_id": uuid.Nil, "child_id": uuid.New()},
		{"parent_id": uuid.New(), "child_id": uuid.Nil},
		{"parent_id": uuid.Nil, "child_id": uuid.Nil},
		{},
	}
	for i, payload := range cases {
		ev := domain.Event{
			ID:          uuid.New(),
			SubjectKind: domain.SubjectWorkItem,
			SubjectID:   uuid.New(),
			Kind:        domain.EventWorkItemRelationAdded,
			Source:      domain.SourceHuman,
			Payload:     payload,
			OccurredAt:  time.Unix(0, 0),
		}
		err := (relationAddedProjector{}).Apply(context.Background(), nil, ev)
		if err == nil || !strings.Contains(err.Error(), "parent_id and child_id are required") {
			t.Errorf("case %d: expected required-error, got %v", i, err)
		}
	}
}

// CanTransition is the v0 transition matrix every transitionedProjector
// write rides on. The rules in the spec are: same-state is allowed
// (no-op transition is legal), terminal states are sealed, every other
// non-terminal hop is permitted in v0. Pin that surface so a future
// reconciler tightening doesn't silently change semantics for anyone
// already depending on it.
func TestCanTransition_v0Matrix(t *testing.T) {
	terminals := []domain.WorkItemState{domain.WorkItemDone, domain.WorkItemFailed, domain.WorkItemCanceled}
	nonTerminals := []domain.WorkItemState{
		domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemPlanned,
		domain.WorkItemAwaitingApproval, domain.WorkItemRunning, domain.WorkItemBlocked,
	}

	for _, term := range terminals {
		if !domain.CanTransition(term, term) {
			t.Errorf("same-state transition %s->%s should be a legal no-op", term, term)
		}
		for _, other := range append(terminals, nonTerminals...) {
			if other == term {
				continue
			}
			if domain.CanTransition(term, other) {
				t.Errorf("terminal %s must not transition to %s", term, other)
			}
		}
	}

	for _, from := range nonTerminals {
		for _, to := range append(append([]domain.WorkItemState{}, nonTerminals...), terminals...) {
			if !domain.CanTransition(from, to) {
				t.Errorf("v0 should permit %s -> %s", from, to)
			}
		}
	}

	if domain.CanTransition(domain.WorkItemState("nonsense"), domain.WorkItemDone) {
		t.Error("invalid from state must not be transitionable")
	}
	if domain.CanTransition(domain.WorkItemCaptured, domain.WorkItemState("nonsense")) {
		t.Error("invalid to state must not be transitionable")
	}
}

func ptrUUID(id uuid.UUID) *uuid.UUID { return &id }
