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
