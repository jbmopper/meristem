package inbox

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
)

// All tests in this file exercise paths that return *before* CaptureText
// touches the pool. The pgx-driven path needs the integration harness in
// internal/api; those tests live alongside it.

func TestCaptureText_RejectsEmptyText(t *testing.T) {
	cases := []string{"", "   ", "\n\t  \n"}
	for _, in := range cases {
		t.Run(strings.TrimSpace(in), func(t *testing.T) {
			s := NewService(nil, nil)
			_, err := s.CaptureText(context.Background(), domain.Token{ID: uuid.New(), Source: domain.SourceHuman}, in)
			if err == nil {
				t.Fatalf("expected error for blank text %q, got nil", in)
			}
			if !strings.Contains(err.Error(), "text is required") {
				t.Errorf("expected text-required error, got %v", err)
			}
		})
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

// newSubjectID is the seam between idempotency context and event subject
// ids. Both branches matter: with context, retries converge on the same
// id; without, every call gets a fresh uuid.
func TestNewSubjectID_UsesIdempotencyContext(t *testing.T) {
	const label = "inbox.work_item"
	const key = "test-key"

	keyed := idempotency.WithRequest(context.Background(), idempotency.Request{
		TokenID:     uuid.New(),
		Scope:       "POST /v1/inbox/messages",
		Key:         key,
		RequestHash: []byte("same-body"),
	})

	a := newSubjectID(keyed, label)
	b := newSubjectID(keyed, label)
	if a != b {
		t.Errorf("same identity + same label must derive identical ids: %s vs %s", a, b)
	}

	other := newSubjectID(keyed, "inbox.message")
	if a == other {
		t.Errorf("different labels must derive different ids; got %s for both", a)
	}
}

func TestNewSubjectID_FallsBackToFreshUUID(t *testing.T) {
	a := newSubjectID(context.Background(), "inbox.work_item")
	b := newSubjectID(context.Background(), "inbox.work_item")
	if a == b {
		t.Errorf("without idempotency context, two calls must produce different ids; got %s twice", a)
	}
	if a == uuid.Nil || b == uuid.Nil {
		t.Errorf("derived ids must not be nil")
	}
}
