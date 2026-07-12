// Package events owns the only sanctioned write path to the `events` table.
//
// Per AGENTS.md principles (1) and (2), the event log is the system: every
// state change must be recorded as one event, and every non-`events` row
// must be derived from an event by a projection writer in the same
// transaction. This package enforces both: callers describe a Spec, the
// Writer derives a deterministic id, inserts the event with ON CONFLICT
// DO NOTHING (idempotent replay), and — only on a fresh insert — fires
// every projector registered for that event's kind.
//
// The transaction is owned by the caller. Append never commits or rolls
// back; that lets callers compose multiple appends and other queries into
// one atomic unit when needed.
package events

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// Spec is the input to Append. The id is derived from the spec; callers do
// not set it.
//
// Identity-bearing fields (SubjectKind, SubjectID, Kind, Payload,
// Discriminator) determine the event id; changing any one yields a different
// id. Attribution fields (Source, ActorTokenID) are metadata and do *not*
// influence the id, so a replay attributed to a different token still
// collapses to one event row.
//
// Discriminator distinguishes two *distinct logical actions* that happen to
// produce identical payloads on the same subject — e.g. a work_item cycling
// running→blocked twice with the same reason, or the same progress payload
// appended twice on purpose. Without it, the second action's event collides
// with the first and is silently dropped along with its projections. Callers
// exposed to repeatable payloads must set it to a value that is stable across
// retries of one action but different across actions; the caller's
// idempotency identity (see idempotency.EventDiscriminator) is the canonical
// choice. Empty means "payload alone identifies the event", which is only
// safe for payloads that cannot legitimately repeat (e.g. token.created
// carries a fresh random hash).
type Spec struct {
	SubjectKind   string
	SubjectID     uuid.UUID
	Kind          string
	Source        domain.Source
	ActorTokenID  *uuid.UUID
	Payload       any
	Discriminator string
}

func (s Spec) validate() error {
	if s.SubjectKind == "" {
		return errors.New("events: SubjectKind is required")
	}
	if s.SubjectID == uuid.Nil {
		return errors.New("events: SubjectID is required")
	}
	if s.Kind == "" {
		return errors.New("events: Kind is required")
	}
	if !s.Source.Valid() {
		return fmt.Errorf("events: Source %q is not one of human|agent|system", s.Source)
	}
	return nil
}

// DeterministicID computes the event id from the spec. Replays produce
// identical ids; PK conflict on insert is therefore the natural dedupe.
//
// The id is a name-based UUID (RFC 4122 version 5) derived from a SHA-256
// digest of the canonical wire form of the identity-bearing fields. Two
// callers in different processes producing the "same" event will compute
// the same id without coordination.
func DeterministicID(s Spec) (uuid.UUID, error) {
	if err := s.validate(); err != nil {
		return uuid.Nil, err
	}

	canonical, err := CanonicalJSON(s.Payload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("events: canonical payload: %w", err)
	}

	h := sha256.New()
	h.Write([]byte(s.SubjectKind))
	h.Write([]byte{':'})
	h.Write(s.SubjectID[:])
	h.Write([]byte{':'})
	h.Write([]byte(s.Kind))
	h.Write([]byte{':'})
	h.Write(canonical)
	// The discriminator only contributes when set so that ids of
	// discriminator-free specs remain identical to those produced before the
	// field existed; event logs written by older binaries replay cleanly.
	if s.Discriminator != "" {
		h.Write([]byte{':'})
		h.Write([]byte(s.Discriminator))
	}
	sum := h.Sum(nil)

	var id uuid.UUID
	copy(id[:], sum[:16])
	// Stamp UUID version 5 (name-based, SHA-1 in the RFC; we use SHA-256
	// truncated, so this is a "v5-shaped" id) and the RFC 4122 variant. The
	// result is a syntactically valid UUID rather than 16 random bytes that
	// merely look like one.
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}

// Writer is the only sanctioned writer of the `events` table. It is also
// the only place projection writers fire, which is what keeps the
// "non-`events` row implies an event that caused it" invariant true.
type Writer struct {
	registry *projections.Registry
}

// NewWriter constructs a Writer backed by registry. A nil registry is
// treated as empty, useful in early bootstrap and in tests that exercise
// the writer in isolation.
func NewWriter(registry *projections.Registry) *Writer {
	if registry == nil {
		registry = projections.NewRegistry()
	}
	return &Writer{registry: registry}
}

// Append inserts the event derived from spec into `events`, then — on a
// fresh insert only — fires every projector registered for spec.Kind in the
// same transaction.
//
// Returns the event id (deterministic per the spec) and a fresh flag.
// fresh=false means the event id already existed in the table (replay);
// projection writers are not re-run, since they will already have fired
// during the original append. To reapply projectors after registering a new
// one, use the rebuild tool (planned), not a replay.
//
// Errors from the insert or any projector cause the caller's transaction to
// be the unit of failure: this function never commits or rolls back; the
// caller decides.
func (w *Writer) Append(ctx context.Context, tx pgx.Tx, spec Spec) (id uuid.UUID, fresh bool, err error) {
	id, err = DeterministicID(spec)
	if err != nil {
		return uuid.Nil, false, err
	}

	payload, err := json.Marshal(spec.Payload)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("events: marshal payload: %w", err)
	}
	// Normalize to an empty object for storage so the column's NOT NULL
	// invariant is preserved and downstream readers never see JSON `null`
	// where they expect an object.
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte(`{}`)
	}

	var occurredAt time.Time
	var seq int64
	err = tx.QueryRow(ctx, `
		INSERT INTO events (id, actor_token_id, source, subject_kind, subject_id, kind, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
		RETURNING occurred_at, seq
	`, id, spec.ActorTokenID, string(spec.Source), spec.SubjectKind, spec.SubjectID, spec.Kind, payload).Scan(&occurredAt, &seq)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return id, false, nil
		}
		return uuid.Nil, false, fmt.Errorf("events: insert: %w", err)
	}
	fresh = true

	event := domain.Event{
		ID:           id,
		Seq:          seq,
		OccurredAt:   occurredAt,
		ActorTokenID: spec.ActorTokenID,
		Source:       spec.Source,
		SubjectKind:  spec.SubjectKind,
		SubjectID:    spec.SubjectID,
		Kind:         spec.Kind,
		Payload:      spec.Payload,
	}
	if err := w.registry.Apply(ctx, tx, event); err != nil {
		return uuid.Nil, false, err
	}
	return id, true, nil
}
