package domain

import (
	"time"

	"github.com/google/uuid"
)

const (
	EventTokenCreated          = "token.created"
	EventTokenRevoked          = "token.revoked"
	EventIdempotencyRecorded   = "idempotency.recorded"
	EventMessageCaptured       = "message.captured"
	EventWorkItemCreated       = "work_item.created"
	EventWorkItemTransitioned  = "work_item.transitioned"
	EventWorkItemEventAppended = "work_item.event_appended"
	EventWorkItemRelationAdded = "work_item.relation_added"
	EventSignalReceived        = "signal.received"
	// EventPatienceBreached records that a non-terminal work_item has been
	// in its current state longer than the configured patience budget for
	// that state. Recorded by `wayline worker --once` (see internal/worker).
	// The event is record-only in this slice: it carries the breach observation
	// and the budget against which it was measured, but does not by itself
	// drive any further action. Subsequent slices add the convergence behavior
	// the spec calls "bounded patience" — escalation, retry, or forced fail.
	//
	// The deterministic event_id is keyed on (work_item_id, state, kind, payload)
	// where payload includes the state observed; rerunning the worker for an
	// item still breached in the same state produces the same id and dedupes
	// to one row. A subsequent transition out of the state ends the breach;
	// re-entering would breach again on the next budget elapse, which is a
	// new (state, payload) and therefore a new event.
	EventPatienceBreached = "patience.breached"
)

// AllEventKinds enumerates every event kind the system knows how to append.
// Adding a new event constant above MUST also add it here. Downstream
// consumers (notably internal/feed's drift guard) rely on this slice to
// force a feed-policy decision for every kind: if a kind is not classified
// as either feed-visible or feed-excluded, the test fails.
//
// Order is the declaration order above; treat the slice as immutable.
var AllEventKinds = []string{
	EventTokenCreated,
	EventTokenRevoked,
	EventIdempotencyRecorded,
	EventMessageCaptured,
	EventWorkItemCreated,
	EventWorkItemTransitioned,
	EventWorkItemEventAppended,
	EventWorkItemRelationAdded,
	EventSignalReceived,
	EventPatienceBreached,
}

const (
	SubjectToken          = "token"
	SubjectIdempotencyKey = "idempotency_key"
	SubjectMessage        = "message"
	SubjectWorkItem       = "work_item"
	SubjectSignal         = "signal"
)

// Token is the active client credential projection. The raw bearer secret is
// never stored; Hash is the SHA-256 digest of the random token string.
type Token struct {
	ID        uuid.UUID
	Name      string
	Hash      []byte
	IsRoot    bool
	Scopes    []string
	Source    Source
	CreatedAt time.Time
	RevokedAt *time.Time
}

// WorkItemState is the lifecycle state projected for a work item.
type WorkItemState string

const (
	WorkItemCaptured         WorkItemState = "captured"
	WorkItemTriaged          WorkItemState = "triaged"
	WorkItemPlanned          WorkItemState = "planned"
	WorkItemAwaitingApproval WorkItemState = "awaiting_approval"
	WorkItemRunning          WorkItemState = "running"
	WorkItemBlocked          WorkItemState = "blocked"
	WorkItemDone             WorkItemState = "done"
	WorkItemFailed           WorkItemState = "failed"
	WorkItemCanceled         WorkItemState = "canceled"
)

// Valid reports whether s is one of the lifecycle states in docs/spec.md.
func (s WorkItemState) Valid() bool {
	switch s {
	case WorkItemCaptured, WorkItemTriaged, WorkItemPlanned,
		WorkItemAwaitingApproval, WorkItemRunning, WorkItemBlocked,
		WorkItemDone, WorkItemFailed, WorkItemCanceled:
		return true
	}
	return false
}

// Terminal reports whether no further automatic transition is expected.
func (s WorkItemState) Terminal() bool {
	switch s {
	case WorkItemDone, WorkItemFailed, WorkItemCanceled:
		return true
	}
	return false
}

// CanTransition reports whether a v0 manual transition is legal. v0 is
// intentionally permissive enough for owner/agent-driven bootstrap work while
// still rejecting moves out of terminal states.
func CanTransition(from, to WorkItemState) bool {
	if !from.Valid() || !to.Valid() {
		return false
	}
	if from == to {
		return true
	}
	if from.Terminal() {
		return false
	}
	return true
}

// WorkItem is the current-state projection for work_items.
type WorkItem struct {
	ID          uuid.UUID
	Title       string
	Body        string
	State       WorkItemState
	StateReason *string
	CreatedBy   *uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Message is the v0 text-only inbox projection.
type Message struct {
	ID           uuid.UUID
	Source       Source
	ActorTokenID *uuid.UUID
	WorkItemID   *uuid.UUID
	Text         string
	CreatedAt    time.Time
}

// Signal is the projection for one accepted POST /v1/signals reception.
// See docs/signals.md for the wider contract; this is the read-side type
// services consume after the projector has run.
type Signal struct {
	ID              uuid.UUID
	ReceivedAt      time.Time
	ActorTokenID    *uuid.UUID
	Source          Source
	SignalKind      string
	DedupeKey       *string
	Fingerprint     []byte
	WorkSpec        []byte
	WorkItemID      *uuid.UUID
	CreatedWorkItem bool
}
