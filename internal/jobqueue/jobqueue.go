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
	// LeaseOwner and LeaseGeneration make a lease a concrete fenced fact
	// (0037): provisioning operations verify the exact owner and incarnation
	// under lock instead of trusting state='leased'. Owner-less legacy claims
	// leave LeaseOwner nil and cannot pass that fence. Lease fields remain
	// the narrow operational direct-update exception.
	LeaseOwner      *uuid.UUID
	LeaseGeneration int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
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
			  -- Launch-protocol lane (ee916614 slice 3a): an admitted review
			  -- child is running with its queue row deliberately open until a
			  -- launch outcome; its payload epoch no longer matches by design,
			  -- so the generic staleness sweep must not cancel it.
			  AND NOT (
			    wi.state = 'running'
			    AND wi.suggested_convergence_checks ? $3
			    AND EXISTS (
			      SELECT 1
			      FROM events launch_created
			      WHERE launch_created.subject_kind = 'work_item'
			        AND launch_created.subject_id = wi.id
			        AND launch_created.kind = 'work_item.created'
			        AND split_part(btrim(COALESCE(launch_created.payload->>'cultivar', '')), '@', 1) = $2
			    )
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
	return s.claimNextReview(ctx, nil, lease)
}

// ClaimNextReviewAs is ClaimNextReview with a concrete lease owner stamped on
// the row. Provisioning (workitems.ProvisionSpawnedReview) fences on the
// exact owner and lease generation, so a launch-capable executor must claim
// through this path; owner-less legacy claims fail that fence closed.
func (s *Service) ClaimNextReviewAs(ctx context.Context, owner uuid.UUID, lease time.Duration) (Job, bool, error) {
	if owner == uuid.Nil {
		return Job{}, false, fmt.Errorf("jobqueue: claim owner is required")
	}
	return s.claimNextReview(ctx, &owner, lease)
}

// ClaimAdmittedReviewAs reclaims a dispatch job whose review child was
// already admitted to running under the launch protocol and whose lease was
// lost (worker crash) or returned (capacity dormancy). The ordinary claim
// predicate demands a pre-admission lifecycle state and a matching payload
// epoch, both of which admission legitimately moved past; this path instead
// requires the running state, the reviewer cultivar recorded at creation,
// and the verdict check — and stamps the concrete owner/generation fence
// every launch operation demands.
func (s *Service) ClaimAdmittedReviewAs(ctx context.Context, owner uuid.UUID, lease time.Duration) (Job, bool, error) {
	if owner == uuid.Nil {
		return Job{}, false, fmt.Errorf("jobqueue: claim owner is required")
	}
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
			  AND wi.state = 'running'
			  AND wi.human_review_status <> 'blocked'
			  AND wi.suggested_convergence_checks ? $4
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
		    lease_owner = $5,
		    lease_generation = jq.lease_generation + 1,
		    updated_at = now()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.lease_owner, jq.lease_generation,
		          jq.created_at, jq.updated_at
	`, leaseMillis, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck, owner)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return Job{}, false, err
			}
			return Job{}, false, nil
		}
		return Job{}, false, fmt.Errorf("jobqueue: claim admitted review: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func (s *Service) claimNextReview(ctx context.Context, owner *uuid.UUID, lease time.Duration) (Job, bool, error) {
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

	job, found, err := claimNextReviewInTx(ctx, tx, owner, leaseMillis)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, found, nil
}

func claimNextReviewInTx(ctx context.Context, tx pgx.Tx, owner *uuid.UUID, leaseMillis int64) (Job, bool, error) {
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
		    lease_owner = $5,
		    lease_generation = jq.lease_generation + 1,
		    updated_at = now()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.lease_owner, jq.lease_generation,
		          jq.created_at, jq.updated_at
	`, leaseMillis, KindDispatch, reviewerCultivarRoot, reviewVerdictCheck, owner)
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
		    lease_owner = NULL,
		    lease_generation = jq.lease_generation + 1,
		    updated_at = now()
		FROM candidate
		WHERE jq.id = candidate.id
		RETURNING jq.id, jq.kind, jq.work_item_id, jq.state, jq.payload,
		          jq.attempts, jq.lease_until, jq.lease_owner, jq.lease_generation,
		          jq.created_at, jq.updated_at
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
		&job.LeaseOwner,
		&job.LeaseGeneration,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}
	job.State = JobState(state)
	return job, nil
}

// RenewReviewLease extends the caller's own leased review job for a retry
// attempt after a failed launch: the child is already running, so the
// pending-claim predicate can never re-lease this row, and the retry instead
// advances the SAME lease under its owner — attempts and generation both
// increment, so every provisioning attempt stays uniquely keyed and fenced.
// The xylem attempt budget is enforced by the launch executor before it
// renews (3b wiring); this operation only guarantees fencing.
func (s *Service) RenewReviewLease(ctx context.Context, id uuid.UUID, owner uuid.UUID, generation int64, lease time.Duration) (Job, error) {
	if owner == uuid.Nil {
		return Job{}, fmt.Errorf("jobqueue: lease renewal requires the lease owner")
	}
	if lease <= 0 {
		return Job{}, ErrInvalidLease
	}
	leaseMillis := lease.Milliseconds()
	if leaseMillis <= 0 {
		leaseMillis = 1
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE job_queue
		SET attempts = attempts + 1,
		    lease_generation = lease_generation + 1,
		    lease_until = now() + ($4::bigint * interval '1 millisecond'),
		    updated_at = now()
		WHERE id = $1
		  AND state = 'leased'
		  AND lease_owner = $2
		  AND lease_generation = $3
		RETURNING id, kind, work_item_id, state, payload,
		          attempts, lease_until, lease_owner, lease_generation,
		          created_at, updated_at
	`, id, owner, generation, leaseMillis)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, pgx.ErrNoRows
		}
		return Job{}, fmt.Errorf("jobqueue: renew review lease: %w", err)
	}
	return job, nil
}

// ReturnReviewDormant parks a leased review job back to pending and refunds
// its claim attempt, fenced on the exact lease owner and generation. It is
// the pre-binding capacity path (accepted design rev 3): a launch that could
// not reserve capacity did no review work, so the attempt must not count,
// and a stale or stolen lease must not park a row it no longer owns.
func (s *Service) ReturnReviewDormant(ctx context.Context, id uuid.UUID, owner uuid.UUID, generation int64) error {
	if owner == uuid.Nil {
		return fmt.Errorf("jobqueue: dormant return requires the lease owner")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_queue
		SET state = 'pending',
		    attempts = GREATEST(attempts - 1, 0),
		    lease_until = NULL,
		    lease_owner = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND state = 'leased'
		  AND lease_owner = $2
		  AND lease_generation = $3
	`, id, owner, generation)
	if err != nil {
		return fmt.Errorf("jobqueue: return review dormant: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
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
