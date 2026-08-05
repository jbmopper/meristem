package workitems

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/safety"
)

const assignmentPayloadVersion = 1

type assignedPayload struct {
	PayloadVersion  int                           `json:"payload_version"`
	AssigneeTokenID uuid.UUID                     `json:"assignee_token_id"`
	Mode            domain.WorkItemAssignmentMode `json:"mode"`
	LeaseSeconds    int64                         `json:"lease_seconds"`
	LeaseSource     string                        `json:"lease_source,omitempty"`
	// Lease chronology is explicit because events.Writer intentionally relies
	// on events.occurred_at's transaction-time column default. A transaction
	// may begin before waiting on row locks, while the lease begins after them.
	ClaimedAt time.Time `json:"claimed_at"`
	ExpiresAt time.Time `json:"expires_at"`
	// Listener binding (absent for ordinary claims): the listener-bound claim
	// operation records which listener, policy revision, and durable demand
	// event authorized this generation. All three travel together.
	ListenerID    *uuid.UUID `json:"listener_id,omitempty"`
	DemandEventID *uuid.UUID `json:"demand_event_id,omitempty"`
	PolicyEventID *uuid.UUID `json:"policy_event_id,omitempty"`
}

type assignmentReleasedPayload struct {
	PayloadVersion    int                            `json:"payload_version"`
	AssignmentEventID uuid.UUID                      `json:"assignment_event_id"`
	AssigneeTokenID   uuid.UUID                      `json:"assignee_token_id"`
	Mode              domain.WorkItemAssignmentMode  `json:"mode"`
	Reason            domain.AssignmentReleaseReason `json:"reason"`
	TerminalState     domain.WorkItemState           `json:"terminal_state,omitempty"`
	ReleasedAt        time.Time                      `json:"released_at"`
}

type assignedProjector struct{}

func (assignedProjector) Kind() string { return domain.EventWorkItemAssigned }

func (assignedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("work_item.assigned: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	payload, err := decodeAssignedPayload(event.Payload)
	if err != nil {
		return err
	}
	if event.ActorTokenID == nil || *event.ActorTokenID == uuid.Nil {
		return fmt.Errorf("work_item.assigned: attributed actor_token_id is required")
	}
	if payload.Mode == domain.WorkItemAssignmentClaim &&
		*event.ActorTokenID != payload.AssigneeTokenID {
		return fmt.Errorf("work_item.assigned: claim assignee_token_id must match attributed actor_token_id")
	}
	maxLeaseSeconds := int64(safety.MaxPatienceBudget / time.Second)
	if payload.LeaseSeconds > maxLeaseSeconds {
		return fmt.Errorf("work_item.assigned: lease exceeds bounded-patience cap %s", safety.MaxPatienceBudget)
	}
	lease := time.Duration(payload.LeaseSeconds) * time.Second
	if payload.ClaimedAt.IsZero() || payload.ExpiresAt.IsZero() {
		return fmt.Errorf("work_item.assigned: claimed_at and expires_at are required")
	}
	if payload.ClaimedAt.Before(event.OccurredAt) {
		return fmt.Errorf("work_item.assigned: claimed_at cannot precede event occurred_at")
	}
	if !payload.ExpiresAt.After(payload.ClaimedAt) || payload.ExpiresAt.Sub(payload.ClaimedAt) != lease {
		return fmt.Errorf("work_item.assigned: expires_at must equal claimed_at plus lease_seconds")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state
		SET holder_token_id = $2,
		    mode = $3,
		    assignment_event_id = $4,
		    claimed_at = $5,
		    expires_at = $6,
		    last_release_reason = NULL,
		    terminal_state = NULL,
		    terminal_addressee_token_id = NULL,
		    listener_id = $8,
		    demand_event_id = $9,
		    policy_event_id = $10,
		    state_event_id = $4,
		    state_event_seq = $7,
		    updated_at = $5
		WHERE work_item_id = $1
		  AND terminal_state IS NULL
		  AND EXISTS (
		      SELECT 1 FROM work_items wi
		      WHERE wi.id = $1 AND wi.state NOT IN ('done', 'failed', 'canceled')
		  )
		  AND (
		      state_event_id = $4
		      OR (holder_token_id IS NULL AND state_event_seq < $7)
		  )
	`, event.SubjectID, payload.AssigneeTokenID, payload.Mode, event.ID, payload.ClaimedAt, payload.ExpiresAt, event.Seq,
		payload.ListenerID, payload.DemandEventID, payload.PolicyEventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		advanced, err := assignmentProjectionAdvancedPast(ctx, tx, event)
		if err != nil {
			return fmt.Errorf("work_item.assigned: %w", err)
		}
		if advanced {
			return nil
		}
		return fmt.Errorf("work_item.assigned: assignment placeholder missing or already active for %s", event.SubjectID)
	}
	_, err = tx.Exec(ctx, `UPDATE work_items SET updated_at = GREATEST(updated_at, $2) WHERE id = $1`, event.SubjectID, payload.ClaimedAt)
	return err
}

type assignmentReleasedProjector struct{}

func (assignmentReleasedProjector) Kind() string {
	return domain.EventWorkItemAssignmentReleased
}

func (assignmentReleasedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("work_item.assignment_released: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	payload, err := decodeAssignmentReleasedPayload(event.Payload)
	if err != nil {
		return err
	}
	if event.ActorTokenID == nil || *event.ActorTokenID == uuid.Nil {
		return fmt.Errorf("work_item.assignment_released: attributed actor_token_id is required")
	}
	if payload.Reason == domain.AssignmentReleaseYield &&
		(event.ActorTokenID == nil || *event.ActorTokenID != payload.AssigneeTokenID) {
		return fmt.Errorf("work_item.assignment_released: yield must be attributed to the current holder")
	}
	if payload.ReleasedAt.Before(event.OccurredAt) {
		return fmt.Errorf("work_item.assignment_released: released_at cannot precede event occurred_at")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state
		SET holder_token_id = NULL,
		    mode = NULL,
		    assignment_event_id = NULL,
		    claimed_at = NULL,
		    expires_at = NULL,
		    last_release_reason = $8,
		    terminal_state = NULLIF($9, ''),
		    terminal_addressee_token_id = NULL,
		    listener_id = NULL,
		    demand_event_id = NULL,
		    policy_event_id = NULL,
		    state_event_id = $5,
		    state_event_seq = $6,
		    updated_at = $7
		WHERE work_item_id = $1
		  AND (
		      state_event_id = $5
		      OR (
		          holder_token_id = $2
		          AND mode = $3
		          AND assignment_event_id = $4
		          AND state_event_seq < $6
		          AND claimed_at <= $7
		          AND ($8 <> 'expired' OR expires_at <= $7)
		      )
		  )
	`, event.SubjectID, payload.AssigneeTokenID, payload.Mode, payload.AssignmentEventID, event.ID, event.Seq, payload.ReleasedAt, payload.Reason, payload.TerminalState)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		advanced, err := assignmentProjectionAdvancedPast(ctx, tx, event)
		if err != nil {
			return fmt.Errorf("work_item.assignment_released: %w", err)
		}
		if advanced {
			return nil
		}
		return fmt.Errorf("work_item.assignment_released: referenced assignment epoch is not active for %s", event.SubjectID)
	}
	_, err = tx.Exec(ctx, `UPDATE work_items SET updated_at = GREATEST(updated_at, $2) WHERE id = $1`, event.SubjectID, payload.ReleasedAt)
	return err
}

// applyTerminalAssignmentTransition is the second table fold owned by the
// single work_item.transitioned projector. It closes an active assignment on
// any terminal outcome and advances even an unassigned row's terminal
// sentinel, preventing a later assigned event from resurrecting the item.
func applyTerminalAssignmentTransition(ctx context.Context, tx pgx.Tx, event domain.Event, from, to domain.WorkItemState) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("work_item.transitioned assignment fold: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	if from.Terminal() {
		// Only an exact terminal same-state transition is a legal lifecycle
		// no-op. Validate the already-folded sentinel before leaving its entering
		// event identity untouched. The caller has already derived the prior state
		// from the immutable lifecycle log, so neither request payload nor mutable
		// projection state can authorize a terminal escape.
		if to != from {
			return fmt.Errorf("work_item.transitioned assignment fold: terminal state %s cannot transition to %s", from, to)
		}
		return validateTerminalAssignmentNoop(ctx, tx, event, to)
	}
	if !to.Terminal() {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state
		SET terminal_addressee_token_id = CASE
		        WHEN state_event_id = $2 THEN terminal_addressee_token_id
		        ELSE holder_token_id
		    END,
		    holder_token_id = NULL,
		    mode = NULL,
		    assignment_event_id = NULL,
		    claimed_at = NULL,
		    expires_at = NULL,
		    last_release_reason = $4,
		    terminal_state = $5,
		    listener_id = NULL,
		    demand_event_id = NULL,
		    policy_event_id = NULL,
		    state_event_id = $2,
		    state_event_seq = $3,
		    updated_at = $6
		WHERE work_item_id = $1
		  AND (
		      state_event_id = $2
		      OR (state_event_seq < $3 AND terminal_state IS NULL)
		  )
	`, event.SubjectID, event.ID, event.Seq, domain.AssignmentReleaseDone, to, event.OccurredAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		advanced, err := assignmentProjectionAdvancedPast(ctx, tx, event)
		if err != nil {
			return fmt.Errorf("work_item.transitioned assignment fold: %w", err)
		}
		if !advanced {
			return fmt.Errorf("work_item.transitioned assignment fold: event conflicts with assignment state for %s", event.SubjectID)
		}
	}
	return nil
}

func validateTerminalAssignmentNoop(ctx context.Context, tx pgx.Tx, event domain.Event, to domain.WorkItemState) error {
	var terminalState pgtype.Text
	var hasActiveFields bool
	if err := tx.QueryRow(ctx, `
		SELECT terminal_state,
		       holder_token_id IS NOT NULL
		       OR mode IS NOT NULL
		       OR assignment_event_id IS NOT NULL
		       OR claimed_at IS NOT NULL
		       OR expires_at IS NOT NULL
		FROM work_item_assignment_state
		WHERE work_item_id = $1
		FOR UPDATE
	`, event.SubjectID).Scan(&terminalState, &hasActiveFields); err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("work_item.transitioned assignment fold: assignment placeholder missing for %s", event.SubjectID)
		}
		return fmt.Errorf("work_item.transitioned assignment fold: validate terminal no-op: %w", err)
	}
	if !terminalState.Valid || terminalState.String != string(to) || hasActiveFields {
		return fmt.Errorf(
			"work_item.transitioned assignment fold: terminal no-op conflicts with assignment state for %s: terminal_state=%q active=%t",
			event.SubjectID,
			terminalState.String,
			hasActiveFields,
		)
	}
	return nil
}

// assignmentProjectionAdvancedPast distinguishes a stale replay (a valid
// no-op) from an invariant violation. Equal sequence with a different event
// id is conflicting history; a missing placeholder is always an error.
func assignmentProjectionAdvancedPast(ctx context.Context, tx pgx.Tx, event domain.Event) (bool, error) {
	var currentID uuid.UUID
	var currentSeq int64
	if err := tx.QueryRow(ctx, `
		SELECT state_event_id, state_event_seq
		FROM work_item_assignment_state
		WHERE work_item_id = $1
	`, event.SubjectID).Scan(&currentID, &currentSeq); err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("assignment placeholder missing for %s", event.SubjectID)
		}
		return false, err
	}
	if currentSeq > event.Seq {
		return true, nil
	}
	if currentSeq == event.Seq && currentID != event.ID {
		return false, fmt.Errorf("event sequence %d conflicts with state event %s", event.Seq, currentID)
	}
	return false, nil
}

func decodeAssignedPayload(raw any) (assignedPayload, error) {
	var payload assignedPayload
	if err := decodeAssignmentPayload(raw, &payload); err != nil {
		return assignedPayload{}, fmt.Errorf("work_item.assigned: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = assignmentPayloadVersion
	}
	if payload.PayloadVersion != assignmentPayloadVersion {
		return assignedPayload{}, fmt.Errorf("work_item.assigned: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.AssigneeTokenID == uuid.Nil || !payload.Mode.Valid() || payload.LeaseSeconds <= 0 {
		return assignedPayload{}, fmt.Errorf("work_item.assigned: assignee_token_id, valid mode, and positive lease_seconds are required")
	}
	// Listener binding travels whole or not at all: a partially attributed
	// claim would make restart derivation ambiguous.
	bound := 0
	for _, field := range []*uuid.UUID{payload.ListenerID, payload.DemandEventID, payload.PolicyEventID} {
		if field != nil && *field != uuid.Nil {
			bound++
		}
	}
	if bound != 0 && bound != 3 {
		return assignedPayload{}, fmt.Errorf("work_item.assigned: listener_id, demand_event_id, and policy_event_id must be present together")
	}
	return payload, nil
}

func decodeAssignmentReleasedPayload(raw any) (assignmentReleasedPayload, error) {
	var payload assignmentReleasedPayload
	if err := decodeAssignmentPayload(raw, &payload); err != nil {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = assignmentPayloadVersion
	}
	if payload.PayloadVersion != assignmentPayloadVersion {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.AssignmentEventID == uuid.Nil || payload.AssigneeTokenID == uuid.Nil ||
		!payload.Mode.Valid() || !payload.Reason.Valid() {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: assignment_event_id, assignee_token_id, valid mode, and valid reason are required")
	}
	// v1 release events are only voluntary yield or lease expiry. Terminal
	// cleanup is derived exclusively from work_item.transitioned in the same
	// projector transaction, so a standalone release can never manufacture a
	// terminal assignment sentinel while work_items remains non-terminal.
	if payload.Reason != domain.AssignmentReleaseYield && payload.Reason != domain.AssignmentReleaseExpired {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: v1 reason must be yield|expired; done is derived from work_item.transitioned")
	}
	if payload.TerminalState != "" {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: terminal_state is derived from work_item.transitioned")
	}
	if payload.ReleasedAt.IsZero() {
		return assignmentReleasedPayload{}, fmt.Errorf("work_item.assignment_released: released_at is required")
	}
	return payload, nil
}

func decodeAssignmentPayload(raw any, out any) error {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(encoded, out); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}
