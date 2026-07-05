package registry

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestTropismDefinedProjectorValidatesPayload(t *testing.T) {
	ev := domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectTropism,
		SubjectID:   TropismSubjectID("bad"),
		Kind:        domain.EventTropismDefined,
		Source:      domain.SourceHuman,
		OccurredAt:  time.Unix(0, 0),
		Payload: map[string]any{
			"name":    "bad",
			"version": 1,
			"reducer": map[string]any{
				"identity": "judge_vote",
				"version":  1,
			},
			"params": map[string]any{},
		},
	}
	err := (tropismDefinedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "unknown_reducer") {
		t.Fatalf("expected unknown_reducer before tx use, got %v", err)
	}
}

func TestCultivarDefinedProjectorValidatesPayload(t *testing.T) {
	ev := domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectCultivar,
		SubjectID:   CultivarSubjectID("bad-worker"),
		Kind:        domain.EventCultivarDefined,
		Source:      domain.SourceHuman,
		OccurredAt:  time.Unix(0, 0),
		Payload: map[string]any{
			"name":      "bad-worker",
			"version":   1,
			"rootstock": true,
			"tropism": map[string]any{
				"name":    "checklist-all",
				"version": 1,
			},
			"profile": map[string]any{
				"briefing":        "briefings/bad.md",
				"scopes_template": []string{"work_items.read"},
			},
			"xylem": map[string]any{
				"max_attempts":     0,
				"max_wall_seconds": 10,
				"max_depth":        1,
			},
			"phloem": "projection:bad",
		},
	}
	err := (cultivarDefinedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "xylem.max_attempts") {
		t.Fatalf("expected xylem validation before tx use, got %v", err)
	}
}

func TestCultivarDefinedProjectorValidatesEventRateClasses(t *testing.T) {
	ev := domain.Event{
		ID:          uuid.New(),
		SubjectKind: domain.SubjectCultivar,
		SubjectID:   CultivarSubjectID("bad-event-rate-worker"),
		Kind:        domain.EventCultivarDefined,
		Source:      domain.SourceHuman,
		OccurredAt:  time.Unix(0, 0),
		Payload: map[string]any{
			"name":      "bad-event-rate-worker",
			"version":   1,
			"rootstock": false,
			"tropism": map[string]any{
				"name":    "checklist-all",
				"version": 1,
			},
			"profile": map[string]any{
				"briefing":        "briefings/bad-event-rate.md",
				"scopes_template": []string{"work_items.read"},
			},
			"xylem": map[string]any{
				"max_attempts":     1,
				"max_wall_seconds": 10,
				"max_depth":        1,
				"max_events_per_item_per_hour_by_class": map[string]any{
					"admin": 1,
				},
			},
			"phloem": "projection:bad",
		},
	}
	err := (cultivarDefinedProjector{}).Apply(context.Background(), nil, ev)
	if err == nil || !strings.Contains(err.Error(), "max_events_per_item_per_hour_by_class[admin]") {
		t.Fatalf("expected event-rate class validation before tx use, got %v", err)
	}
}

func TestSubjectIDsAreStable(t *testing.T) {
	if TropismSubjectID("checklist-all") != uuid.NewSHA1(subjectNamespace, []byte("tropism|checklist-all")) {
		t.Fatal("tropism subject id derivation drifted")
	}
	if CultivarSubjectID("convergence-scribe") != uuid.NewSHA1(subjectNamespace, []byte("cultivar|convergence-scribe")) {
		t.Fatal("cultivar subject id derivation drifted")
	}
}
