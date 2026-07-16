// Package jobqueue owns the durable Postgres-backed worker queue.
//
// Queue rows are caused by durable events, but lease state is operational
// coordination: pending rows can be rebuilt from dispatch.requested, while
// leased/done/failed/canceled state is how competing workers avoid duplicate
// execution at runtime.
package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidLease = errors.New("jobqueue: lease duration must be positive")

const (
	reviewerCultivarRoot = "reviewer"
	reviewVerdictCheck   = "event:review.verdict_recorded"
)

type JobState string

const (
	JobPending  JobState = "pending"
	JobLeased   JobState = "leased"
	JobDone     JobState = "done"
	JobFailed   JobState = "failed"
	JobCanceled JobState = "canceled"
)

type Job struct {
	ID         uuid.UUID
	Kind       string
	WorkItemID uuid.UUID
	State      JobState
	Payload    json.RawMessage
	Attempts   int
	LeaseUntil *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// ReconcileDispatchJobs cancels dispatch jobs that can no longer represent a
// runnable state epoch. Queue state is operational coordination (not a durable
// domain projection), so this is intentionally a direct update just like lease
// and terminal-state changes.
//
// Human-review-blocked and lifecycle-blocked rows are deliberately excluded:
// they remain pending and dormant so removing the gate can make the same valid
// state epoch claimable. Unknown job kinds and otherwise-valid non-review
// dispatches are also left untouched.
func (s *Service) ReconcileDispatchJobs(ctx context.Context) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("jobqueue: begin dispatch reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// A worker may lease a job just before a gate appears. Once that bounded
	// lease expires, normalize it back to pending so its dormant state is
	// explicit and removing the gate can make the same job claimable again.
	if _, err := tx.Exec(ctx, `
		UPDATE job_queue jq
		SET state = 'pending',
		    lease_until = NULL,
		    updated_at = now()
		FROM work_items wi
		WHERE jq.kind = $1
		  AND jq.work_item_id = wi.id
		  AND jq.state = 'leased'
		  AND jq.lease_until <= now()
		  AND wi.state NOT IN ('done', 'failed', 'canceled')
		  AND (
		    wi.state = 'blocked'
		    OR wi.human_review_status = 'blocked'
		  )
	`, KindDispatch); err != nil {
		return 0, fmt.Errorf("jobqueue: release dormant dispatch leases: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		WITH stale AS (
			SELECT jq.id
			FROM job_queue jq
			LEFT JOIN work_items wi ON wi.id = jq.work_item_id
			WHERE jq.kind = $1
			  AND (
			    jq.state = 'pending'
			    OR (jq.state = 'leased' AND jq.lease_until <= now())
			  )
			  AND (
			    wi.id IS NULL
			    OR wi.state IN ('done', 'failed', 'canceled')
			    OR (
			      wi.state <> 'blocked'
			      AND wi.human_review_status <> 'blocked'
			      AND (
			        jsonb_typeof(jq.payload) IS DISTINCT FROM 'object'
			        OR COALESCE(jq.payload->>'work_item_id', '') <> jq.work_item_id::text
			        OR COALESCE(jq.payload->>'state', '') <> wi.state
			        OR btrim(COALESCE(jq.payload->>'cultivar', '')) = ''
			        OR COALESCE(jq.payload->>'state_entered_at_unix', '') !~ '^-?[0-9]+$'
			        OR floor(extract(epoch FROM wi.state_entered_at)) <>
			           CASE
			             WHEN COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			             THEN (jq.payload->>'state_entered_at_unix')::numeric
			             ELSE NULL
			           END
			        OR jsonb_array_length(wi.suggested_convergence_checks) = 0
			        OR (
			          (
			            split_part(btrim(COALESCE(jq.payload->>'cultivar', '')), '@', 1) = $2
			            OR EXISTS (
			              SELECT 1
			              FROM events created
			              WHERE created.subject_kind = 'work_item'
			                AND created.subject_id = wi.id
			                AND created.kind = 'work_item.created'
			                AND split_part(btrim(COALESCE(created.payload->>'cultivar', '')), '@', 1) = $2
			            )
			          )
			          AND (
			            split_part(btrim(COALESCE(jq.payload->>'cultivar', '')), '@', 1) <> $2
			            OR NOT EXISTS (
			              SELECT 1
			              FROM events created
			              WHERE created.subject_kind = 'work_item'
			                AND created.subject_id = wi.id
			                AND created.kind = 'work_item.created'
			                AND split_part(btrim(COALESCE(created.payload->>'cultivar', '')), '@', 1) = $2
			            )
			            OR NOT (wi.suggested_convergence_checks ? $3)
			          )
			        )
			      )
			    )
			  )
		)
		UPDATE job_queue jq
		SET state = 'canceled',
		    lease_until = NULL,
		    updated_at = now()
		FROM stale
		WHERE jq.id = stale.id
	`, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck)
	if err != nil {
		return 0, fmt.Errorf("jobqueue: reconcile dispatch jobs: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("jobqueue: commit dispatch reconciliation: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ClaimNextReview leases one ready reviewer dispatch. The queue may also hold
// ordinary checklist-worker dispatches; production review automation must not
// claim those and thereby pretend the deterministic reconciler executed agent
// work. Reviewer identity is cross-checked against both the dispatch payload
// and the work item's event-backed launch metadata.
func (s *Service) ClaimNextReview(ctx context.Context, lease time.Duration) (Job, bool, error) {
	if lease <= 0 {
		return Job{}, false, ErrInvalidLease
	}
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, found, err := claimNextReviewInTx(ctx, tx, leaseMillis)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, found, nil
}

func claimNextReviewInTx(ctx context.Context, tx pgx.Tx, leaseMillis int64) (Job, bool, error) {
	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT jq.id
			FROM job_queue jq
			JOIN work_items wi ON wi.id = jq.work_item_id
			WHERE jq.kind = $2
			  AND (
			    jq.state = 'pending'
			    OR (jq.state = 'leased' AND jq.lease_until <= now())
			  )
			  AND wi.state IN ('captured', 'triaged', 'planned')
			  AND wi.human_review_status <> 'blocked'
			  AND jsonb_array_length(wi.suggested_convergence_checks) > 0
			  AND wi.suggested_convergence_checks ? $4
			  AND jsonb_typeof(jq.payload) = 'object'
			  AND jq.payload->>'work_item_id' = jq.work_item_id::text
			  AND jq.payload->>'state' = wi.state
			  AND COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			  AND floor(extract(epoch FROM wi.state_entered_at)) =
			      CASE
			        WHEN COALESCE(jq.payload->>'state_entered_at_unix', '') ~ '^-?[0-9]+$'
			        THEN (jq.payload->>'state_entered_at_unix')::numeric
			        ELSE NULL
			      END
			  AND split_part(btrim(COALESCE(jq.payload->>'cultivar', '')), '@', 1) = $3
			  AND EXISTS (
			    SELECT 1
			    FROM events created
			    WHERE created.subject_kind = 'work_item'
			      AND created.subject_id = wi.id
			      AND created.kind = 'work_item.created'
			      AND split_part(btrim(COALESCE(created.payload->>'cultivar', '')), '@', 1) = $3
			  )
			ORDER BY jq.created_at ASC, jq.id ASC
			FOR UPDATE OF jq SKIP LOCKED
			LIMIT 1
		)
		UPDATE job_queue jq
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_until = now() + ($1::bigint * interval '1 millisecond'),
		    updated_at = now()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.created_at, jq.updated_at
	`, leaseMillis, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("jobqueue: claim next review: %w", err)
	}
	return job, true, nil
}

// ClaimNext leases one ready job using SELECT ... FOR UPDATE SKIP LOCKED.
// Competing callers skip rows already locked by another transaction and
// therefore claim disjoint jobs without any process-local coordination.
func (s *Service) ClaimNext(ctx context.Context, lease time.Duration) (Job, bool, error) {
	if lease <= 0 {
		return Job{}, false, ErrInvalidLease
	}
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, found, err := claimNextInTx(ctx, tx, leaseMillis)
	if err != nil {
		return Job{}, false, err
	}
	if !found {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return Job{}, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func claimNextInTx(ctx context.Context, tx pgx.Tx, leaseMillis int64) (Job, bool, error) {
	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM job_queue
			WHERE state = 'pending'
			   OR (state = 'leased' AND lease_until <= now())
			ORDER BY created_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE job_queue jq
		SET state = 'leased',
		    attempts = attempts + 1,
		    lease_until = now() + ($1::bigint * interval '1 millisecond'),
		    updated_at = now()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.created_at, jq.updated_at
	`, leaseMillis)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("jobqueue: claim next: %w", err)
	}
	return job, true, nil
}

func scanJob(row pgx.Row) (Job, error) {
	var (
		job   Job
		state string
	)
	if err := row.Scan(
		&job.ID,
		&job.Kind,
		&job.WorkItemID,
		&state,
		&job.Payload,
		&job.Attempts,
		&job.LeaseUntil,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}
	job.State = JobState(state)
	return job, nil
}

func (s *Service) MarkDone(ctx context.Context, id uuid.UUID) error {
	return s.markTerminal(ctx, id, JobDone)
}

func (s *Service) MarkFailed(ctx context.Context, id uuid.UUID) error {
	return s.markTerminal(ctx, id, JobFailed)
}

func (s *Service) MarkCanceled(ctx context.Context, id uuid.UUID) error {
	return s.markTerminal(ctx, id, JobCanceled)
}

func (s *Service) markTerminal(ctx context.Context, id uuid.UUID, state JobState) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_queue
		SET state = $2,
		    lease_until = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND state = 'leased'
	`, id, string(state))
	if err != nil {
		return fmt.Errorf("jobqueue: mark %s: %w", state, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
