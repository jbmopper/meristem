package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
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
func (s *Service) StartReviewDispatch(ctx context.Context, jobID uuid.UUID, actor domain.Token) (ReviewDispatchResult, error) {
	return s.startReviewDispatch(ctx, jobID, actor, false)
}

// StartReviewDispatchForLaunch is the launch-protocol admission (ee916614
// slice 3a): a Started outcome leaves the queue row leased instead of done,
// because dispatch is not irreversibly complete until a durable
// review_launch outcome proves the reviewer process was created for the
// exact binding (ProvisionSpawnedReview and its outcome events own that).
// Every gate, revalidation, and dormant/cancel path is identical to
// StartReviewDispatch, which remains the non-launch legacy semantics.
func (s *Service) StartReviewDispatchForLaunch(ctx context.Context, jobID uuid.UUID, actor domain.Token) (ReviewDispatchResult, error) {
	return s.startReviewDispatch(ctx, jobID, actor, true)
}

func (s *Service) startReviewDispatch(ctx context.Context, jobID uuid.UUID, actor domain.Token, launchProtocol bool) (ReviewDispatchResult, error) {
	if jobID == uuid.Nil {
		return ReviewDispatchResult{}, fmt.Errorf("%w: review dispatch job id is required", ErrInvalidRequest)
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
		kind       string
		workItemID uuid.UUID
		jobState   string
		rawPayload []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT kind, work_item_id, state, payload
		FROM job_queue
		WHERE id = $1
		FOR UPDATE
	`, jobID).Scan(&kind, &workItemID, &jobState, &rawPayload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReviewDispatchResult{}, fmt.Errorf("workitems: review dispatch job %s not found", jobID)
		}
		return ReviewDispatchResult{}, fmt.Errorf("workitems: lock review dispatch job: %w", err)
	}

	result := ReviewDispatchResult{JobID: jobID, WorkItemID: workItemID}
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
		if launchProtocol {
			// Launch-protocol retry (round-1 finding): the admission already
			// happened, but the job is NOT complete until a launch outcome —
			// commit with the lease intact so the caller proceeds to
			// provisioning against the same admitted child.
			result.Outcome = ReviewDispatchStarted
			if err := tx.Commit(ctx); err != nil {
				return ReviewDispatchResult{}, err
			}
			return result, nil
		}
		// This also repairs a job left leased by an older non-atomic executor:
		// the exact transition identity is already durable, so closing the lease
		// cannot duplicate lifecycle state.
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchAlreadyDone, "done")
	}

	// time.Time.Unix uses the whole-second floor of a timestamp. This is the
	// same epoch contract used by the dispatch producer; do not round a .9
	// fractional timestamp into the following second.
	if current.State != payload.State || current.StateEnteredAt.Unix() != *payload.StateEnteredAtUnix {
		return finishReviewDispatch(ctx, tx, result, ReviewDispatchCanceled, "canceled")
	}

	exhausted, _, err := s.enforceConcurrentRunningBudget(ctx, tx, current, actor)
	if err != nil {
		return ReviewDispatchResult{}, err
	}
	if exhausted {
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
	if launchProtocol {
		// Admission commits with the lease intact: the queue row completes
		// only on a succeeded launch outcome, goes dormant on capacity
		// shortage, or retries within its budget on launch failure.
		result.Outcome = ReviewDispatchStarted
		if err := tx.Commit(ctx); err != nil {
			return ReviewDispatchResult{}, err
		}
		return result, nil
	}
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
			"from":   from,
			"to":     domain.WorkItemRunning,
			"reason": "review dispatch claimed from job " + jobID.String(),
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
	// A dormant outcome returns the job to pending having done no review work,
	// so the claim's attempt is refunded: attempts count startable work, never
	// gate collisions. The gates themselves park the row rather than let it
	// spin — human/lifecycle blocks are skipped by the claim predicate, and a
	// budget exhaustion escalates the child hand_to_human, which blocks it
	// too (55d7995 accepted-review nit; ee916614 slice 1).
	if _, err := tx.Exec(ctx, `
		UPDATE job_queue
		SET state = $2,
		    attempts = CASE WHEN $2 = 'pending' THEN GREATEST(attempts - 1, 0) ELSE attempts END,
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
