package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/jobqueue"
)

const (
	reviewDispatchJobKind      = "dispatch"
	reviewDispatchCultivarRoot = "reviewer"
)

// ReviewDispatchOutcome describes how a claimed reviewer job was resolved.
// Queue state is operational coordination, while the transition (when one is
// needed) remains an event-backed domain mutation.
type ReviewDispatchOutcome string

const (
	ReviewDispatchStarted     ReviewDispatchOutcome = "started"
	ReviewDispatchAlreadyDone ReviewDispatchOutcome = "already_done"
	ReviewDispatchCanceled    ReviewDispatchOutcome = "canceled"
	ReviewDispatchDormant     ReviewDispatchOutcome = "dormant"
)

type ReviewDispatchResult struct {
	Outcome      ReviewDispatchOutcome
	JobID        uuid.UUID
	WorkItemID   uuid.UUID
	Transitioned bool
}

type reviewDispatchPayload struct {
	WorkItemID         uuid.UUID            `json:"work_item_id"`
	State              domain.WorkItemState `json:"state"`
	StateEnteredAtUnix *int64               `json:"state_entered_at_unix"`
	StateEventID       *uuid.UUID           `json:"state_event_id,omitempty"`
	Cultivar           string               `json:"cultivar"`
}

// StartReviewDispatch atomically moves one leased reviewer child into running
// and closes its queue row. The queue claim and this transaction are separate
// so a crashed worker merely leaves a bounded lease; the event transition and
// job completion are deliberately one transaction so a restart cannot repeat
// a lifecycle transition or strand a successfully admitted job.
//
// The dispatch payload is advisory. Before changing lifecycle state, this
// method revalidates the current projection epoch, human gate, checklist, and
// reviewer cultivar against both the payload and the event-backed creation
// metadata. Terminal/stale/malformed jobs are canceled. Human- or
// lifecycle-blocked jobs are returned to pending without a lease and remain
// dormant until their gate changes.
func (s *Service) StartReviewDispatch(ctx context.Context, jobID uuid.UUID, expectedAttempt int, actor domain.Token) (ReviewDispatchResult, error) {
	if jobID == uuid.Nil {
		return ReviewDispatchResult{}, fmt.Errorf("%w: review dispatch job id is required", ErrInvalidRequest)
	}
	if expectedAttempt <= 0 {
		return ReviewDispatchResult{}, fmt.Errorf("%w: review dispatch expected attempt must be positive", ErrInvalidRequest)
	}
	if actor.ID == uuid.Nil || actor.Source != domain.SourceSystem || actor.IsRoot {
		return ReviewDispatchResult{}, fmt.Errorf("%w: review dispatch requires a dedicated system actor", ErrInvalidRequest)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		kind        string
		workItemID  uuid.UUID
		jobState    string
		jobAttempt  int
		leaseUntil  *time.Time
		leaseActive bool
		rawPayload  []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT kind, work_item_id, state, attempts, lease_until,
		       COALESCE(lease_until > clock_timestamp(), false), payload
		FROM job_queue
		WHERE id = $1
		FOR UPDATE
	`, jobID).Scan(&kind, &workItemID, &jobState, &jobAttempt, &leaseUntil, &leaseActive, &rawPayload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReviewDispatchResult{}, fmt.Errorf("workitems: review dispatch job %s not found", jobID)
		}
		return ReviewDispatchResult{}, fmt.Errorf("workitems: lock review dispatch job: %w", err)
	}

	result := ReviewDispatchResult{JobID: jobID, WorkItemID: workItemID}
	if jobAttempt != expectedAttempt {
		return ReviewDispatchResult{}, fmt.Errorf("workitems: review dispatch job %s attempt is %d, want %d", jobID, jobAttempt, expectedAttempt)
	}
	switch jobState {
	case "done":
		result.Outcome = ReviewDispatchAlreadyDone
		if err := tx.Commit(ctx); err != nil {
			return ReviewDispatchResult{}, err
		}
		return result, nil
	case "canceled":
		result.Outcome = ReviewDispatchCanceled
		if err := tx.Commit(ctx); err != nil {
			return ReviewDispatchResult{}, err
		}
		return result, nil
	case "leased":
		if leaseUntil == nil || !leaseActive {
			return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
		}
		// Continue with the claim revalidation below.
	default:
		return ReviewDispatchResult{}, fmt.Errorf("workitems: review dispatch job %s is %s, want leased", jobID, jobState)
	}

	if kind != reviewDispatchJobKind {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	var payload reviewDispatchPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil ||
		payload.WorkItemID == uuid.Nil ||
		payload.WorkItemID != workItemID ||
		payload.StateEnteredAtUnix == nil ||
		!reviewDispatchState(payload.State) ||
		cultivarRoot(payload.Cultivar) != reviewDispatchCultivarRoot {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	current, err := scanWorkItemForUpdate(ctx, tx, workItemID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
		}
		return ReviewDispatchResult{}, err
	}
	if current.State.Terminal() {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}
	if current.State == domain.WorkItemBlocked || current.HumanReviewStatus == domain.HumanReviewBlocked {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
	}

	// Claim and admission are separate transactions. Resolve the immutable
	// demand's exact state-entry predecessor while holding the work-item lock,
	// then require both the latest valid generation and the current state entry.
	// A malformed later immutable fact shadows older demand until the producer
	// appends a newer causally-linked repair generation.
	identity, err := jobqueue.ResolveDispatchIdentity(ctx, tx, jobID)
	if err != nil || identity.WorkItemID != workItemID || identity.State != payload.State ||
		identity.StateEnteredAt.Unix() != *payload.StateEnteredAtUnix ||
		(identity.Explicit && (payload.StateEventID == nil || *payload.StateEventID != identity.StateEntryID)) ||
		(!identity.Explicit && payload.StateEventID != nil) {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}
	currentEntry, err := jobqueue.ResolveCurrentStateEntry(ctx, tx, workItemID)
	if err != nil || currentEntry.State != current.State || !currentEntry.OccurredAt.Equal(current.StateEnteredAt) {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}
	latest, err := jobqueue.LatestValidDispatch(ctx, tx, workItemID)
	if err != nil || latest.ID != jobID {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}
	done, err := jobqueue.DispatchDemandDone(ctx, tx, identity)
	if err != nil {
		return ReviewDispatchResult{}, fmt.Errorf("workitems: check completed review dispatch generation: %w", err)
	}
	if done || (currentEntry.ID != identity.StateEntryID && !jobqueue.CausallyAdmitsDemand(currentEntry, jobID)) {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}
	if !hasReviewVerdictCheck(current.SuggestedConvergenceChecks) {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	launch, err := workItemLaunchMetadata(ctx, tx, workItemID)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	if cultivarRoot(launch.Cultivar) != reviewDispatchCultivarRoot {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	spec := reviewDispatchTransitionSpec(jobID, payload.State, workItemID, actor.ID)
	transitionID, err := events.DeterministicID(spec)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	transitionExists, err := eventExistsByID(ctx, tx, transitionID)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	if transitionExists {
		// This also repairs a job left leased by an older non-atomic executor:
		// the exact transition identity is already durable, so closing the lease
		// cannot duplicate lifecycle state.
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchAlreadyDone, "done")
	}

	// time.Time.Unix uses the whole-second floor of a timestamp. This is the
	// same epoch contract used by the dispatch producer; do not round a .9
	// fractional timestamp into the following second.
	if currentEntry.ID == identity.StateEntryID &&
		(current.State != payload.State || current.StateEnteredAt.Unix() != *payload.StateEnteredAtUnix) {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	exhausted, _, err := s.enforceConcurrentRunningBudget(ctx, tx, current, actor)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	if exhausted {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
	}
	// The row lock prevents a competing renewal, but wall time can pass while
	// policy and budget checks run. Fence once more at the lifecycle boundary
	// using the database clock so an expired attempt cannot append a transition.
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(lease_until > clock_timestamp(), false)
		FROM job_queue
		WHERE id = $1
		  AND state = 'leased'
		  AND attempts = $2
	`, jobID, expectedAttempt).Scan(&leaseActive); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
		}
		return ReviewDispatchResult{}, fmt.Errorf("workitems: fence review dispatch lease: %w", err)
	}
	if !leaseActive {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
	}
	exhausted, _, err = s.appendWorkItemEventWithRateBudget(ctx, tx, current, spec, "", actor)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	if exhausted {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchDormant, "pending")
	}

	result.Transitioned = true
	return finishReviewDispatch(ctx, tx, result, ReviewDispatchStarted, "done")
}

func reviewDispatchTransitionSpec(jobID uuid.UUID, from domain.WorkItemState, workItemID uuid.UUID, actorID uuid.UUID) events.Spec {
	return events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     workItemID,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        domain.SourceSystem,
		ActorTokenID:  &actorID,
		Discriminator: "dispatch_job:" + jobID.String(),
		Payload: map[string]any{
			"from":              from,
			"to":                domain.WorkItemRunning,
			"reason":            "review dispatch claimed from job " + jobID.String(),
			"dispatch_event_id": jobID,
		},
	}
}

func reviewDispatchState(state domain.WorkItemState) bool {
	switch state {
	case domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemPlanned:
		return true
	default:
		return false
	}
}

func hasReviewVerdictCheck(checks []string) bool {
	for _, check := range checks {
		if strings.TrimSpace(check) == ReviewVerdictCheck {
			return true
		}
	}
	return false
}

func cultivarRoot(ref string) string {
	root, _, _ := strings.Cut(strings.TrimSpace(ref), "@")
	return strings.TrimSpace(root)
}

func finishReviewDispatch(ctx context.Context, tx pgx.Tx, result ReviewDispatchResult, outcome ReviewDispatchOutcome, state string) (ReviewDispatchResult, error) {
	if _, err := tx.Exec(ctx, `
		UPDATE job_queue
		SET state = $2,
		    lease_until = NULL,
		    updated_at = now()
		WHERE id = $1
	`, result.JobID, state); err != nil {
		return ReviewDispatchResult{}, fmt.Errorf("workitems: finish review dispatch: %w", err)
	}
	result.Outcome = outcome
	if err := tx.Commit(ctx); err != nil {
		return ReviewDispatchResult{}, err
	}
	return result, nil
}
