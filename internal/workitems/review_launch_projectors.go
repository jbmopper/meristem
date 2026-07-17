package workitems

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

// review_launch rows are event-caused projections (accepted design rev 4
// implementation constraint): the reservation, handle, and outcome facts are
// appended as work_item.review_launch_* events and folded here in the same
// transaction. Only job_queue lease fields keep the operational
// direct-update exception.

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
		payload.PayloadVersion = reviewLaunchPayloadVersion
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_reserved: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.JobID == uuid.Nil || payload.AssignmentEventID == uuid.Nil ||
		payload.ReviewerTokenID == uuid.Nil || payload.Attempt <= 0 || payload.Deadline.IsZero() {
		return fmt.Errorf("review_launch_reserved: job_id, assignment_event_id, reviewer_token_id, positive attempt, and deadline are required")
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO review_launch (
			work_item_id, round_seq, attempt, job_id, assignment_event_id,
			reviewer_token_id, state, deadline,
			created_event_id, updated_event_id, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, 'reserved', $7, $8, $8, $9, $9)
		ON CONFLICT (work_item_id, round_seq, attempt) DO NOTHING
	`, event.SubjectID, payload.RoundSeq, payload.Attempt, payload.JobID,
		payload.AssignmentEventID, payload.ReviewerTokenID, payload.Deadline,
		event.ID, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("review_launch_reserved: insert projection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		// The events writer fires projectors only on fresh event inserts, so
		// a conflicting row means two distinct reservations claimed the same
		// (work_item, round, attempt); fail loudly rather than mask it.
		return fmt.Errorf("review_launch_reserved: reservation already exists for (%s, %d, %d)", event.SubjectID, payload.RoundSeq, payload.Attempt)
	}
	return nil
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
		payload.PayloadVersion = reviewLaunchPayloadVersion
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_handle_recorded: unsupported payload_version %d", payload.PayloadVersion)
	}
	if payload.Pid <= 0 || payload.Pgid <= 0 || payload.StartToken == "" || payload.AssignmentEventID == uuid.Nil {
		return fmt.Errorf("review_launch_handle_recorded: pid, pgid, start_token, and assignment_event_id are required")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE review_launch
		SET state = 'handled',
		    handle_pid = $4,
		    handle_pgid = $5,
		    handle_start_token = $6,
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
		payload.PayloadVersion = reviewLaunchPayloadVersion
	}
	if payload.PayloadVersion != reviewLaunchPayloadVersion {
		return fmt.Errorf("review_launch_resolved: unsupported payload_version %d", payload.PayloadVersion)
	}
	switch payload.Outcome {
	case ReviewLaunchSucceeded:
		if payload.Stage != "" {
			return fmt.Errorf("review_launch_resolved: succeeded outcome carries no stage")
		}
	case ReviewLaunchFailed, ReviewLaunchAbandoned:
		if payload.Stage == "" {
			return fmt.Errorf("review_launch_resolved: outcome %s requires a stage", payload.Outcome)
		}
	default:
		return fmt.Errorf("review_launch_resolved: unknown outcome %q", payload.Outcome)
	}
	// succeeded requires a handle (design rev 4); failed may close any live
	// state including abandoned; abandoned only leaves reserved.
	var stateFilter string
	switch payload.Outcome {
	case ReviewLaunchSucceeded:
		stateFilter = `state = 'handled'`
	case ReviewLaunchFailed:
		stateFilter = `state IN ('reserved', 'handled', 'abandoned')`
	case ReviewLaunchAbandoned:
		stateFilter = `state = 'reserved'`
	}
	tag, err := tx.Exec(ctx, `
		UPDATE review_launch
		SET state = $4,
		    stage = NULLIF($5, ''),
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
