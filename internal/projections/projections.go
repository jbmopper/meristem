// Package projections holds the registry that routes appended events to
// projection writers, and defines the Projector contract those writers
// satisfy.
//
// Projection writers are the *only* code in wayline that may INSERT or
// UPDATE non-`events` tables. They run synchronously in the same
// transaction as the event append (see internal/events.Writer.Append), so
// "the event log is truth" is enforced: a row exists in `messages` or
// `work_items` only because an event caused it, and rolling back the event
// rolls back the projection.
//
// Each Projector consumes one event kind. Multiple Projectors may register
// for the same kind (e.g. a future search index alongside the primary
// projection); they fire in registration order and any error aborts the
// transaction.
package projections

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/wayline/internal/domain"
)

// Projector derives projection rows from an appended event. Implementations
// must be:
//
//   - Idempotent: applying the same event twice yields the same rows. The
//     normal write path will not call Apply twice for the same event id, but
//     rebuilds and tests do, and a non-idempotent projector breaks both.
//   - Pure with respect to the event payload: no clock reads (use
//     event.OccurredAt), no random ids (derive from event.ID or payload),
//     no environment lookups.
//   - Rebuild-safe: dropping every projection table and folding all events
//     through the registered projectors must reproduce the tables byte-for-
//     byte (modulo timestamps that come from clocks rather than event
//     metadata).
//
// Apply runs in the transaction the event was inserted in. Errors abort the
// transaction; partial projections cannot persist.
type Projector interface {
	// Kind is the event kind this projector consumes. One projector handles
	// exactly one kind; register multiple instances for fan-out.
	Kind() string
	// Apply derives projection rows from event.
	Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error
}

// Registry routes events to interested projectors. The zero value is not
// usable; always construct via NewRegistry.
type Registry struct {
	byKind map[string][]Projector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byKind: make(map[string][]Projector)}
}

// Register adds p to the registry. Multiple projectors may share a kind;
// they fire in registration order. A nil p is silently ignored, which lets
// callers do conditional registration without nil-checks at the call site.
func (r *Registry) Register(p Projector) {
	if p == nil {
		return
	}
	r.byKind[p.Kind()] = append(r.byKind[p.Kind()], p)
}

// Apply fires every projector registered for event.Kind, in registration
// order, against tx. Returns the first projector error (wrapped with the
// projector's concrete type and the event kind for diagnostics) and stops;
// the transaction is the caller's to roll back.
//
// An event kind with no registered projectors is a no-op, not an error.
// New event kinds may legitimately ship before their projectors do.
func (r *Registry) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	for _, p := range r.byKind[event.Kind] {
		if err := p.Apply(ctx, tx, event); err != nil {
			return fmt.Errorf("projector %T for %s: %w", p, event.Kind, err)
		}
	}
	return nil
}
