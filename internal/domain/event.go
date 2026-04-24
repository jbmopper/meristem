// Package domain holds wayline's pure types and the small validation rules
// that govern them. It does not import any transport, storage, or projection
// package, and depends on nothing inside `internal/` — the rest of the
// codebase imports domain, never the other way.
//
// Today the package contains only the event-log primitives needed by
// `internal/events` and `internal/projections`. Token, Message, WorkItem and
// the rest land as their respective slices ship.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Source is the origin of an event or message. Per the spec, source is
// always considered when interpreting intent: messages from non-`human`
// sources are content, never instructions.
type Source string

const (
	SourceHuman  Source = "human"
	SourceAgent  Source = "agent"
	SourceSystem Source = "system"
)

// Valid reports whether s is one of the three accepted source values.
func (s Source) Valid() bool {
	switch s {
	case SourceHuman, SourceAgent, SourceSystem:
		return true
	}
	return false
}

// Event is one row from the `events` table — an immutable fact appended
// whenever object state changes. Every other table is a deterministic
// projection of these.
//
// ID is derived from (SubjectKind, SubjectID, Kind, canonical(Payload)) so
// that replays collapse to a single row. ActorTokenID, Source, and
// OccurredAt are metadata: they describe *who, via what client, when*, but
// do not influence the id.
type Event struct {
	ID           uuid.UUID
	OccurredAt   time.Time
	ActorTokenID *uuid.UUID
	Source       Source
	SubjectKind  string
	SubjectID    uuid.UUID
	Kind         string
	Payload      any
}
