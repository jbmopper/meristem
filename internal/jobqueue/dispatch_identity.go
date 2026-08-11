package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

var ErrInvalidDispatchDemand = errors.New("jobqueue: invalid dispatch demand identity")

// DispatchStateEntry is one authoritative work-item state entry. Same-state
// transition no-ops are lifecycle facts, but not entries and therefore never
// mint a new dispatch epoch.
type DispatchStateEntry struct {
	ID         uuid.UUID
	Seq        int64
	Kind       string
	State      domain.WorkItemState
	OccurredAt time.Time
	Payload    json.RawMessage
}

// DispatchIdentity is the validated logical identity of dispatch.requested.
// Legacy events derive StateEntry from log order. New events additionally
// carry an explicit identity which must agree with that derivation.
type DispatchIdentity struct {
	ID             uuid.UUID
	Seq            int64
	WorkItemID     uuid.UUID
	State          domain.WorkItemState
	StateEnteredAt time.Time
	StateEntryID   uuid.UUID
	Explicit       bool
}

type DispatchIdentityQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

const dispatchStateEntrySQL = `
	WITH lifecycle AS (
		SELECT fact.*,
		       lag(fact.state) OVER (ORDER BY fact.seq) AS prior_state
		FROM (
			SELECT e.id, e.seq, e.kind, e.occurred_at, e.payload,
			       CASE
			         WHEN e.kind = 'work_item.created'
			         THEN COALESCE(NULLIF(e.payload->>'state', ''), 'captured')
			         ELSE NULLIF(e.payload->>'to', '')
			       END AS state
			FROM events e
			WHERE e.subject_kind = 'work_item'
			  AND e.subject_id = $1
			  AND e.kind IN ('work_item.created', 'work_item.transitioned')
			  AND jsonb_typeof(e.payload) = 'object'
		) fact
	)
	SELECT id, seq, kind, state, occurred_at, payload
	FROM lifecycle
	WHERE ($2::bigint IS NULL OR seq < $2::bigint)
	  AND state IS NOT NULL
	  AND (kind = 'work_item.created' OR state IS DISTINCT FROM prior_state)
	ORDER BY seq DESC
	LIMIT 1
`

// ResolveCurrentStateEntry returns the latest actual state entry.
func ResolveCurrentStateEntry(ctx context.Context, q DispatchIdentityQuerier, workItemID uuid.UUID) (DispatchStateEntry, error) {
	return resolveStateEntry(ctx, q, workItemID, nil)
}

func resolveStateEntry(ctx context.Context, q DispatchIdentityQuerier, workItemID uuid.UUID, beforeSeq *int64) (DispatchStateEntry, error) {
	var entry DispatchStateEntry
	var state string
	err := q.QueryRow(ctx, dispatchStateEntrySQL, workItemID, beforeSeq).Scan(
		&entry.ID, &entry.Seq, &entry.Kind, &state, &entry.OccurredAt, &entry.Payload,
	)
	if err != nil {
		return DispatchStateEntry{}, err
	}
	entry.State = domain.WorkItemState(state)
	if entry.ID == uuid.Nil || !entry.State.Valid() {
		return DispatchStateEntry{}, fmt.Errorf("%w: work item %s has invalid state-entry metadata", ErrInvalidDispatchDemand, workItemID)
	}
	return entry, nil
}

// ResolveDispatchIdentity validates one immutable dispatch event, including
// exact state/unix metadata. A legacy event is accepted only when its readable
// state/unix pair names exactly one preceding state entry; otherwise rapid
// same-second re-entry is ambiguous and fails closed.
func ResolveDispatchIdentity(ctx context.Context, q DispatchIdentityQuerier, demandID uuid.UUID) (DispatchIdentity, error) {
	var (
		identity DispatchIdentity
		raw      json.RawMessage
	)
	err := q.QueryRow(ctx, `
		SELECT seq, subject_id, payload
		FROM events
		WHERE id = $1
		  AND subject_kind = 'work_item'
		  AND kind = 'dispatch.requested'
	`, demandID).Scan(&identity.Seq, &identity.WorkItemID, &raw)
	if err != nil {
		return DispatchIdentity{}, err
	}
	identity.ID = demandID
	entry, err := resolveStateEntry(ctx, q, identity.WorkItemID, &identity.Seq)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DispatchIdentity{}, fmt.Errorf("%w: demand %s has no preceding state entry", ErrInvalidDispatchDemand, demandID)
		}
		return DispatchIdentity{}, fmt.Errorf("jobqueue: resolve demand %s state entry: %w", demandID, err)
	}

	var payload struct {
		WorkItemID        uuid.UUID            `json:"work_item_id"`
		State             domain.WorkItemState `json:"state"`
		StateEnteredEpoch *int64               `json:"state_entered_at_unix"`
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return DispatchIdentity{}, fmt.Errorf("%w: demand %s payload: %v", ErrInvalidDispatchDemand, demandID, err)
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return DispatchIdentity{}, fmt.Errorf("%w: demand %s payload object: %v", ErrInvalidDispatchDemand, demandID, err)
	}
	if payload.WorkItemID != identity.WorkItemID || payload.WorkItemID == uuid.Nil ||
		!payload.State.Valid() || payload.State != entry.State || payload.StateEnteredEpoch == nil ||
		*payload.StateEnteredEpoch != entry.OccurredAt.Unix() {
		return DispatchIdentity{}, fmt.Errorf("%w: demand %s metadata does not match its preceding state entry", ErrInvalidDispatchDemand, demandID)
	}

	if rawStateEventID, present := fields["state_event_id"]; present {
		var explicit uuid.UUID
		if err := json.Unmarshal(rawStateEventID, &explicit); err != nil || explicit == uuid.Nil || explicit != entry.ID {
			return DispatchIdentity{}, fmt.Errorf("%w: demand %s state_event_id does not match %s", ErrInvalidDispatchDemand, demandID, entry.ID)
		}
		identity.Explicit = true
	} else {
		count, err := matchingStateEntries(ctx, q, identity.WorkItemID, identity.Seq, payload.State, *payload.StateEnteredEpoch)
		if err != nil {
			return DispatchIdentity{}, err
		}
		if count != 1 {
			return DispatchIdentity{}, fmt.Errorf("%w: legacy demand %s state/unix metadata matches %d preceding entries", ErrInvalidDispatchDemand, demandID, count)
		}
	}
	identity.State = entry.State
	identity.StateEnteredAt = entry.OccurredAt
	identity.StateEntryID = entry.ID
	return identity, nil
}

func matchingStateEntries(ctx context.Context, q DispatchIdentityQuerier, workItemID uuid.UUID, beforeSeq int64, state domain.WorkItemState, epoch int64) (int, error) {
	var count int
	err := q.QueryRow(ctx, `
		WITH lifecycle AS (
			SELECT fact.*,
			       lag(fact.state) OVER (ORDER BY fact.seq) AS prior_state
			FROM (
				SELECT e.seq, e.kind, e.occurred_at,
				       CASE
				         WHEN e.kind = 'work_item.created'
				         THEN COALESCE(NULLIF(e.payload->>'state', ''), 'captured')
				         ELSE NULLIF(e.payload->>'to', '')
				       END AS state
				FROM events e
				WHERE e.subject_kind = 'work_item'
				  AND e.subject_id = $1
				  AND e.kind IN ('work_item.created', 'work_item.transitioned')
				  AND jsonb_typeof(e.payload) = 'object'
			) fact
		)
		SELECT count(*)
		FROM lifecycle
		WHERE seq < $2
		  AND state = $3
		  AND floor(extract(epoch FROM occurred_at))::bigint = $4
		  AND (kind = 'work_item.created' OR state IS DISTINCT FROM prior_state)
	`, workItemID, beforeSeq, state, epoch).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("jobqueue: count matching state entries: %w", err)
	}
	return count, nil
}

// LatestValidDispatch validates the highest-sequence raw demand for one item.
// A malformed immutable fact fails closed and shadows older generations until
// a newer valid repair is appended.
func LatestValidDispatch(ctx context.Context, q DispatchIdentityQuerier, workItemID uuid.UUID) (DispatchIdentity, error) {
	var id uuid.UUID
	err := q.QueryRow(ctx, `
		SELECT id
		FROM events
		WHERE subject_kind = 'work_item'
		  AND subject_id = $1
		  AND kind = 'dispatch.requested'
		ORDER BY seq DESC
		LIMIT 1
	`, workItemID).Scan(&id)
	if err != nil {
		return DispatchIdentity{}, err
	}
	return ResolveDispatchIdentity(ctx, q, id)
}

// DispatchDemandDone reports whether any valid generation for the same exact
// state entry has already completed.
func DispatchDemandDone(ctx context.Context, q DispatchIdentityQuerier, target DispatchIdentity) (bool, error) {
	rows, err := q.Query(ctx, `
		SELECT id
		FROM job_queue
		WHERE kind = $1 AND work_item_id = $2 AND state = 'done'
	`, KindDispatch, target.WorkItemID)
	if err != nil {
		return false, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		identity, err := ResolveDispatchIdentity(ctx, q, id)
		if err == nil && identity.StateEntryID == target.StateEntryID {
			return true, nil
		}
		if err != nil && !errors.Is(err, ErrInvalidDispatchDemand) && !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
	}
	return false, nil
}

// CausallyAdmitsDemand reports the narrow reviewer coexistence case: the
// current state entry is the running transition produced by exactly this
// dispatch generation.
func CausallyAdmitsDemand(entry DispatchStateEntry, demandID uuid.UUID) bool {
	if entry.Kind != domain.EventWorkItemTransitioned || entry.State != domain.WorkItemRunning {
		return false
	}
	var payload struct {
		DispatchEventID uuid.UUID `json:"dispatch_event_id"`
	}
	return json.Unmarshal(entry.Payload, &payload) == nil && payload.DispatchEventID == demandID
}
