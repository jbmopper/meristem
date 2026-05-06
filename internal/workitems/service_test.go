package workitems

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
)

// All Service tests in this file exercise paths that return *before* the
// service ever asks pgx for a transaction. The DB-bound paths live in the
// integration tests under internal/api.

func TestCreate_RejectsBlankTitle(t *testing.T) {
	s := NewService(nil, nil)
	for _, in := range []string{"", "   ", "\n\t  \n"} {
		_, err := s.Create(context.Background(), CreateInput{Title: in, Actor: testActor()})
		if err == nil {
			t.Fatalf("expected error for blank title %q", in)
		}
		if !strings.Contains(err.Error(), "title is required") {
			t.Errorf("expected title-required error, got %v", err)
		}
	}
}

func TestCreate_RejectsInvalidState(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.Create(context.Background(), CreateInput{
		Title: "thing",
		State: domain.WorkItemState("not_a_real_state"),
		Actor: testActor(),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid state") {
		t.Errorf("expected invalid-state error, got %v", err)
	}
}

func TestCreate_RejectsInvalidMetadata(t *testing.T) {
	s := NewService(nil, nil)
	cases := []struct {
		name string
		in   CreateInput
		want string
	}{
		{
			name: "blank convergence check",
			in: CreateInput{
				Title:                      "thing",
				SuggestedConvergenceChecks: []string{"go test ./...", "  "},
				HumanReviewStatus:          domain.HumanReviewWavedThrough,
				Actor:                      testActor(),
			},
			want: "suggested_convergence_checks[1] is blank",
		},
		{
			name: "invalid human review status",
			in: CreateInput{
				Title:             "thing",
				HumanReviewStatus: domain.HumanReviewStatus("maybe"),
				Actor:             testActor(),
			},
			want: "invalid human_review_status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Create(context.Background(), tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestSpawnChild_RejectsBlankTitle(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.SpawnChild(context.Background(), uuid.New(), CreateInput{Actor: testActor()})
	if err == nil || !strings.Contains(err.Error(), "title is required") {
		t.Errorf("expected title-required error, got %v", err)
	}
}

func TestSpawnChild_RejectsInvalidState(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.SpawnChild(context.Background(), uuid.New(), CreateInput{
		Title: "child",
		State: domain.WorkItemState("not_a_real_state"),
		Actor: testActor(),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid state") {
		t.Errorf("expected invalid-state error, got %v", err)
	}
}

// ErrRelationCycle is what the API and MCP transports translate to a
// distinct error code (relation_cycle / 409). The deeper-cycle case
// requires a database round-trip and lives in the integration tests; this
// just pins the sentinel and confirms it survives errors.Is wrapping.
func TestErrRelationCycle_IsSentinel(t *testing.T) {
	if !errors.Is(ErrRelationCycle, ErrRelationCycle) {
		t.Error("ErrRelationCycle must be its own sentinel")
	}
	wrapped := errors.Join(ErrRelationCycle, errors.New("decoration"))
	if !errors.Is(wrapped, ErrRelationCycle) {
		t.Error("wrapped ErrRelationCycle must still match errors.Is")
	}
}

func TestTransition_RejectsInvalidTo(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.Transition(context.Background(), uuid.New(), domain.WorkItemState("not_real"), "", testActor())
	if err == nil || !strings.Contains(err.Error(), "invalid state") {
		t.Errorf("expected invalid-state error, got %v", err)
	}
}

func TestAppendEvent_RejectsBlankKind(t *testing.T) {
	s := NewService(nil, nil)
	for _, k := range []string{"", "   "} {
		err := s.AppendEvent(context.Background(), uuid.New(), k, nil, testActor())
		if err == nil || !strings.Contains(err.Error(), "event kind is required") {
			t.Errorf("kind=%q: expected kind-required error, got %v", k, err)
		}
	}
}

func TestUpdateMetadata_RejectsInvalidMetadata(t *testing.T) {
	s := NewService(nil, nil)
	cases := []struct {
		name string
		in   UpdateMetadataInput
		want string
	}{
		{
			name: "blank convergence check",
			in: UpdateMetadataInput{
				SuggestedConvergenceChecks: []string{"ok", ""},
				HumanReviewStatus:          domain.HumanReviewWavedThrough,
				Actor:                      testActor(),
			},
			want: "suggested_convergence_checks[1] is blank",
		},
		{
			name: "invalid human review status",
			in: UpdateMetadataInput{
				HumanReviewStatus: domain.HumanReviewStatus("reviewed-ish"),
				Actor:             testActor(),
			},
			want: "invalid human_review_status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.UpdateMetadata(context.Background(), uuid.New(), tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestNormalizeHumanReviewStatus_DefaultsToWavedThrough(t *testing.T) {
	got, err := normalizeHumanReviewStatus("")
	if err != nil {
		t.Fatalf("normalizeHumanReviewStatus: %v", err)
	}
	if got != domain.HumanReviewWavedThrough {
		t.Fatalf("got %q, want %q", got, domain.HumanReviewWavedThrough)
	}
}

func TestSourceForActor_DefaultsToHuman(t *testing.T) {
	if got := sourceForActor(domain.Token{}); got != domain.SourceHuman {
		t.Errorf("zero token: got %q, want human", got)
	}
	if got := sourceForActor(domain.Token{Source: "bogus"}); got != domain.SourceHuman {
		t.Errorf("invalid source should default to human, got %q", got)
	}
	if got := sourceForActor(domain.Token{Source: domain.SourceAgent}); got != domain.SourceAgent {
		t.Errorf("agent source should round-trip, got %q", got)
	}
	if got := sourceForActor(domain.Token{Source: domain.SourceSystem}); got != domain.SourceSystem {
		t.Errorf("system source should round-trip, got %q", got)
	}
}

func TestNewSubjectID_UsesIdempotencyContext(t *testing.T) {
	ctx := idempotency.WithRequest(context.Background(), idempotency.Request{
		TokenID:     uuid.New(),
		Scope:       "POST /v1/work-items",
		Key:         "k1",
		RequestHash: []byte("body"),
	})

	a := newSubjectID(ctx, "work_item")
	b := newSubjectID(ctx, "work_item")
	if a != b {
		t.Errorf("same identity + label must converge: %s vs %s", a, b)
	}
	if c := newSubjectID(ctx, "child_work_item"); c == a {
		t.Errorf("different label must diverge from %s", a)
	}
}

func TestNewSubjectID_FallsBackToFreshUUID(t *testing.T) {
	a := newSubjectID(context.Background(), "work_item")
	b := newSubjectID(context.Background(), "work_item")
	if a == b {
		t.Errorf("without context every call must mint a fresh id; got %s twice", a)
	}
}

// Sanity check: ErrNotFound is the canonical "no such work_item" error
// the API/MCP layers translate to 404. Keeping the sentinel exported and
// errors.Is-friendly is part of the contract.
func TestErrNotFound_IsSentinel(t *testing.T) {
	if !errors.Is(ErrNotFound, ErrNotFound) {
		t.Error("ErrNotFound must be its own sentinel")
	}
	wrapped := errors.Join(ErrNotFound, errors.New("decoration"))
	if !errors.Is(wrapped, ErrNotFound) {
		t.Error("wrapped ErrNotFound must still match errors.Is")
	}
}

func testActor() domain.Token {
	return domain.Token{ID: uuid.New(), Name: "test", Source: domain.SourceHuman}
}
