package projections

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

// recorder is a Projector test double: it records every event it sees and
// optionally returns a configured error to exercise the error path.
type recorder struct {
	kind  string
	calls []domain.Event
	fail  error
}

func (r *recorder) Kind() string { return r.kind }

func (r *recorder) Apply(_ context.Context, _ pgx.Tx, event domain.Event) error {
	r.calls = append(r.calls, event)
	return r.fail
}

func mkEvent(kind string) domain.Event {
	return domain.Event{
		ID:          uuid.New(),
		Source:      domain.SourceSystem,
		SubjectKind: "test",
		SubjectID:   uuid.New(),
		Kind:        kind,
	}
}

func TestRegistryRoutesByKind(t *testing.T) {
	r := NewRegistry()
	a := &recorder{kind: "a.b"}
	b := &recorder{kind: "c.d"}
	r.Register(a)
	r.Register(b)

	if err := r.Apply(context.Background(), nil, mkEvent("a.b")); err != nil {
		t.Fatal(err)
	}
	if len(a.calls) != 1 {
		t.Errorf("a should have 1 call, got %d", len(a.calls))
	}
	if len(b.calls) != 0 {
		t.Errorf("b should have 0 calls, got %d", len(b.calls))
	}
}

func TestRegistryFiresMultipleProjectorsForSameKindInOrder(t *testing.T) {
	r := NewRegistry()
	first := &recorder{kind: "shared"}
	second := &recorder{kind: "shared"}
	r.Register(first)
	r.Register(second)

	if err := r.Apply(context.Background(), nil, mkEvent("shared")); err != nil {
		t.Fatal(err)
	}
	if len(first.calls) != 1 || len(second.calls) != 1 {
		t.Errorf("both projectors should fire once, got %d and %d", len(first.calls), len(second.calls))
	}
}

func TestRegistryUnknownKindIsNoOp(t *testing.T) {
	r := NewRegistry()
	r.Register(&recorder{kind: "registered"})
	if err := r.Apply(context.Background(), nil, mkEvent("unregistered")); err != nil {
		t.Errorf("unknown kind should be no-op, got %v", err)
	}
}

func TestRegistryStopsOnFirstError(t *testing.T) {
	r := NewRegistry()
	boom := errors.New("boom")
	first := &recorder{kind: "a.b", fail: boom}
	second := &recorder{kind: "a.b"}
	r.Register(first)
	r.Register(second)

	err := r.Apply(context.Background(), nil, mkEvent("a.b"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("expected wrapped boom error, got %v", err)
	}
	if len(second.calls) != 0 {
		t.Errorf("second projector should not fire after first errored, got %d calls", len(second.calls))
	}
}

func TestRegistryNilProjectorIgnored(t *testing.T) {
	r := NewRegistry()
	r.Register(nil)
	if err := r.Apply(context.Background(), nil, mkEvent("anything")); err != nil {
		t.Errorf("registry with only nil should be a no-op, got %v", err)
	}
}
