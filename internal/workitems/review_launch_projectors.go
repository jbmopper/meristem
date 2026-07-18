package workitems

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

// review_launch rows are event-caused projections (accepted design rev 4
// implementation constraint): the reservation, handle, termination-due, and
// outcome facts are appended as work_item.review_launch_* events and folded
// here in the same transaction. Only job_queue lease fields keep the
// operational direct-update exception.
//
// Every projector honors the Projector idempotence contract: applying the
// EXACT same event again is a no-op (replay/rebuild), while a DIFFERENT
// event claiming the same lifecycle key fails loudly.

// reviewLaunchAppliedBy reports whether the projection row already reflects
// the given event (exact replay) by matching the PER-STEP causal event id
// column. The row remembers every lifecycle step's causing event, so
// replaying an earlier exact event after later transitions still no-ops
// (round-2 finding: remembering only the latest update was not enough).
func reviewLaunchAppliedBy(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID, roundSeq int64, attempt int, column string, eventID uuid.UUID) (bool, error) {
	switch column {
	case "created_event_id", "handle_event_id", "resolved_event_id", "termination_event_id":
	default:
		return false, fmt.Errorf("review_launch projection lookup: unknown causal column %q", column)
	}
	var applied bool
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(`+column+` = $4, FALSE)
		FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, workItemID, roundSeq, attempt, eventID).Scan(&applied)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("review_launch projection lookup: %w", err)
	}
	return applied, nil
}

type reviewLaunchReservedProjector struct{}

func (reviewLaunchReservedProjector) Kind() string { return domain.EventReviewLaunchReserved }

func (reviewLaunchReservedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("review_launch_reserved: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	var payload reviewLaunchReservedPayload
	if err := decodeAssignmentPayload(event.Payload, &payload); err != nil {
		return fmt.Errorf("review_launch_reserved: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = reviewLaunchPayloadVersionV1
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersionV1 && payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_reserved: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.JobID == uuid.Nil || payload.AssignmentEventID == uuid.Nil ||
		payload.ReviewerTokenID == uuid.Nil || payload.Attempt <= 0 || payload.Deadline.IsZero() {
		return fmt.Errorf("review_launch_reserved: job_id, assignment_event_id, reviewer_token_id, positive attempt, and deadline are required")
	}
	// v1 (exact-parent) reservations predate the fencing fields and project
	// with NULLs — every fenced operation fails closed on them. v2 requires
	// the full incarnation identity.
	var issuer, leaseOwner *uuid.UUID
	var leaseGeneration *int64
	if payload.PayloadVersion >= reviewLaunchPayloadVersion {
		if payload.IssuerTokenID == uuid.Nil || payload.LeaseOwner == uuid.Nil {
			return fmt.Errorf("review_launch_reserved: v2 requires issuer_token_id and lease_owner")
		}
		issuer, leaseOwner = &payload.IssuerTokenID, &payload.LeaseOwner
		leaseGeneration = &payload.LeaseGeneration
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO review_launch (
			work_item_id, round_seq, attempt, job_id, assignment_event_id,
			reviewer_token_id, issuer_token_id, lease_owner, lease_generation,
			state, deadline,
			created_event_id, updated_event_id, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'reserved', $10, $11, $11, $12, $12)
		ON CONFLICT (work_item_id, round_seq, attempt) DO NOTHING
	`, event.SubjectID, payload.RoundSeq, payload.Attempt, payload.JobID,
		payload.AssignmentEventID, payload.ReviewerTokenID, issuer,
		leaseOwner, leaseGeneration, payload.Deadline,
		event.ID, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("review_launch_reserved: insert projection: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Conflict: an exact replay of the creating event is a no-op; a
	// different reservation claiming the same key is a real corruption.
	var createdEventID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT created_event_id FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, event.SubjectID, payload.RoundSeq, payload.Attempt).Scan(&createdEventID); err != nil {
		return fmt.Errorf("review_launch_reserved: conflict lookup: %w", err)
	}
	if createdEventID == event.ID {
		return nil
	}
	return fmt.Errorf("review_launch_reserved: distinct reservation already exists for (%s, %d, %d)", event.SubjectID, payload.RoundSeq, payload.Attempt)
}

type reviewLaunchHandleProjector struct{}

func (reviewLaunchHandleProjector) Kind() string { return domain.EventReviewLaunchHandleRecorded }

func (reviewLaunchHandleProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("review_launch_handle_recorded: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	var payload reviewLaunchHandlePayload
	if err := decodeAssignmentPayload(event.Payload, &payload); err != nil {
		return fmt.Errorf("review_launch_handle_recorded: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = reviewLaunchPayloadVersionV1
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersionV1 && payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_handle_recorded: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.Pid <= 0 || payload.Pgid <= 0 || payload.StartToken == "" || payload.AssignmentEventID == uuid.Nil {
		return fmt.Errorf("review_launch_handle_recorded: pid, pgid, start_token, and assignment_event_id are required")
	}
	applied, err := reviewLaunchAppliedBy(ctx, tx, event.SubjectID, payload.RoundSeq, payload.Attempt, "handle_event_id", event.ID)
	if err != nil {
		return fmt.Errorf("review_launch_handle_recorded: %w", err)
	}
	if applied {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE review_launch
		SET state = 'handled',
		    handle_pid = $4,
		    handle_pgid = $5,
		    handle_start_token = $6,
		    handle_event_id = $7,
		    updated_event_id = $7,
		    updated_at = $8
		WHERE work_item_id = $1
		  AND round_seq = $2
		  AND attempt = $3
		  AND state = 'reserved'
		  AND assignment_event_id = $9
	`, event.SubjectID, payload.RoundSeq, payload.Attempt,
		payload.Pid, payload.Pgid, payload.StartToken,
		event.ID, event.OccurredAt, payload.AssignmentEventID)
	if err != nil {
		return fmt.Errorf("review_launch_handle_recorded: update projection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("review_launch_handle_recorded: no reserved launch for (%s, %d, %d) and assignment %s", event.SubjectID, payload.RoundSeq, payload.Attempt, payload.AssignmentEventID)
	}
	return nil
}

type reviewLaunchResolvedProjector struct{}

func (reviewLaunchResolvedProjector) Kind() string { return domain.EventReviewLaunchResolved }

func (reviewLaunchResolvedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("review_launch_resolved: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	var payload reviewLaunchResolvedPayload
	if err := decodeAssignmentPayload(event.Payload, &payload); err != nil {
		return fmt.Errorf("review_launch_resolved: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = reviewLaunchPayloadVersionV1
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersionV1 && payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_resolved: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.PayloadVersion == reviewLaunchPayloadVersionV1 && payload.Outcome == ReviewLaunchExited {
		return fmt.Errorf("review_launch_resolved: outcome exited requires payload_version %d", reviewLaunchPayloadVersion)
	}
	// succeeded marks a RUNNING reviewer from a handled reservation; exited
	// terminally confirms death/exit from succeeded; failed closes any
	// pre-exit live state; abandoned only leaves reserved.
	var stateFilter string
	switch payload.Outcome {
	case ReviewLaunchSucceeded:
		if payload.Stage != "" {
			return fmt.Errorf("review_launch_resolved: succeeded outcome carries no stage")
		}
		stateFilter = `state = 'handled'`
	case ReviewLaunchExited:
		if payload.Stage == "" {
			return fmt.Errorf("review_launch_resolved: outcome exited requires a stage")
		}
		stateFilter = `state = 'succeeded'`
	case ReviewLaunchFailed:
		if payload.Stage == "" {
			return fmt.Errorf("review_launch_resolved: outcome failed requires a stage")
		}
		stateFilter = `state IN ('reserved', 'handled', 'succeeded', 'abandoned')`
	case ReviewLaunchAbandoned:
		if payload.Stage == "" {
			return fmt.Errorf("review_launch_resolved: outcome abandoned requires a stage")
		}
		stateFilter = `state = 'reserved'`
	default:
		return fmt.Errorf("review_launch_resolved: unknown outcome %q", payload.Outcome)
	}
	applied, err := reviewLaunchAppliedBy(ctx, tx, event.SubjectID, payload.RoundSeq, payload.Attempt, "resolved_event_id", event.ID)
	if err != nil {
		return fmt.Errorf("review_launch_resolved: %w", err)
	}
	if applied {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE review_launch
		SET state = $4,
		    stage = NULLIF($5, ''),
		    resolved_event_id = $6,
		    updated_event_id = $6,
		    updated_at = $7
		WHERE work_item_id = $1
		  AND round_seq = $2
		  AND attempt = $3
		  AND `+stateFilter+`
	`, event.SubjectID, payload.RoundSeq, payload.Attempt,
		string(payload.Outcome), payload.Stage, event.ID, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("review_launch_resolved: update projection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("review_launch_resolved: no launch in a legal state for (%s, %d, %d) outcome %s", event.SubjectID, payload.RoundSeq, payload.Attempt, payload.Outcome)
	}
	return nil
}

type reviewLaunchTerminationDueProjector struct{}

func (reviewLaunchTerminationDueProjector) Kind() string {
	return domain.EventReviewLaunchTerminationDue
}

func (reviewLaunchTerminationDueProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("review_launch_termination_due: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	var payload reviewLaunchTerminationDuePayload
	if err := decodeAssignmentPayload(event.Payload, &payload); err != nil {
		return fmt.Errorf("review_launch_termination_due: %w", err)
	}
	if payload.PayloadVersion == 0 {
		payload.PayloadVersion = reviewLaunchPayloadVersionV1
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersionV1 && payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_termination_due: unsupported payload_version %d", payload.PayloadVersion)
	}
	applied, err := reviewLaunchAppliedBy(ctx, tx, event.SubjectID, payload.RoundSeq, payload.Attempt, "termination_event_id", event.ID)
	if err != nil {
		return fmt.Errorf("review_launch_termination_due: %w", err)
	}
	if applied {
		return nil
	}
	// The mark never changes state and never frees anything: it flags a
	// handled or running launch whose termination is due.
	tag, err := tx.Exec(ctx, `
		UPDATE review_launch
		SET termination_due = TRUE,
		    termination_event_id = $4,
		    updated_event_id = $4,
		    updated_at = $5
		WHERE work_item_id = $1
		  AND round_seq = $2
		  AND attempt = $3
		  AND state IN ('handled', 'succeeded')
	`, event.SubjectID, payload.RoundSeq, payload.Attempt, event.ID, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("review_launch_termination_due: update projection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("review_launch_termination_due: no handled or running launch for (%s, %d, %d)", event.SubjectID, payload.RoundSeq, payload.Attempt)
	}
	return nil
}
