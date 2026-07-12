package crossnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
)

const (
	// CommandQueuePatience is the fixed Stage 1 deadline from command.queued.
	CommandQueuePatience = 24 * time.Hour
	// MaxCommandAttempts bounds retryable local executions before expiry.
	MaxCommandAttempts = 5
)

// CommandOutcome is a terminal command_queue state.
type CommandOutcome string

const (
	CommandDone    CommandOutcome = "done"
	CommandRefused CommandOutcome = "refused"
	CommandFailed  CommandOutcome = "failed"
	CommandExpired CommandOutcome = "expired"
)

// ExpiryReason records which deterministic patience bound fired first.
type ExpiryReason string

const (
	ExpiryDeadline          ExpiryReason = "deadline"
	ExpiryAttemptsExhausted ExpiryReason = "attempts_exhausted"
)

func (r ExpiryReason) Valid() bool {
	return r == ExpiryDeadline || r == ExpiryAttemptsExhausted
}

type attemptedPayload struct {
	PayloadVersion int       `json:"payload_version,omitempty"`
	CommandQueueID uuid.UUID `json:"command_queue_id"`
	TargetNodeID   string    `json:"target_node_id"`
	AttemptKey     string    `json:"attempt_key"`
}

type expiredPayload struct {
	PayloadVersion int          `json:"payload_version,omitempty"`
	CommandQueueID uuid.UUID    `json:"command_queue_id"`
	TargetNodeID   string       `json:"target_node_id"`
	Reason         ExpiryReason `json:"reason"`
	ExpiredAt      time.Time    `json:"expired_at"`
	AttemptCount   int          `json:"attempt_count"`
}

// RecordAttemptInput identifies one logical local execution. AttemptKey must
// be stable across retries of recording that execution and unique across
// distinct executions. Now is supplied by the reconciler for deadline checks.
type RecordAttemptInput struct {
	CommandQueueID uuid.UUID
	AttemptKey     string
	Now            time.Time
	ActorTokenID   uuid.UUID
	Source         domain.Source
}

type RecordAttemptResult struct {
	EventID      uuid.UUID
	AttemptCount int
	Fresh        bool
}

var (
	ErrInvalidAttemptInput      = errors.New("crossnode: command, attempt_key, actor, source, and now are required")
	ErrInvalidExpiryInput       = errors.New("crossnode: expiry actor, source, and now are required")
	ErrCommandNotPending        = errors.New("crossnode: command is not pending")
	ErrCommandPatienceExhausted = errors.New("crossnode: command patience exhausted")
)

// RecordAttempt appends command.attempted under a caller-supplied logical
// attempt identity. It serializes against acknowledgements/expiry on the queue
// row, rejects a sixth or post-deadline execution, and collapses a retry of the
// same AttemptKey without incrementing the projection twice.
func (s *QueueService) RecordAttempt(ctx context.Context, in RecordAttemptInput) (RecordAttemptResult, error) {
	if in.CommandQueueID == uuid.Nil || in.AttemptKey == "" || in.Now.IsZero() || in.ActorTokenID == uuid.Nil || !in.Source.Valid() {
		return RecordAttemptResult{}, ErrInvalidAttemptInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RecordAttemptResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var target, state string
	var attempts int
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `
		SELECT target_node_id, state, attempt_count, expires_at
		FROM command_queue WHERE id = $1 FOR UPDATE
	`, in.CommandQueueID).Scan(&target, &state, &attempts, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RecordAttemptResult{}, ErrUnknownCommand
		}
		return RecordAttemptResult{}, fmt.Errorf("crossnode: lock attempted command: %w", err)
	}

	spec := events.Spec{
		SubjectKind:  domain.SubjectNode,
		SubjectID:    nodes.NodeSubjectID(target),
		Kind:         domain.EventCommandAttempted,
		Source:       in.Source,
		ActorTokenID: &in.ActorTokenID,
		Payload: attemptedPayload{
			CommandQueueID: in.CommandQueueID,
			TargetNodeID:   target,
			AttemptKey:     in.AttemptKey,
		},
	}
	id, err := events.DeterministicID(spec)
	if err != nil {
		return RecordAttemptResult{}, err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, id).Scan(&exists); err != nil {
		return RecordAttemptResult{}, fmt.Errorf("crossnode: check attempt replay: %w", err)
	}
	if exists {
		return RecordAttemptResult{EventID: id, AttemptCount: attempts, Fresh: false}, nil
	}
	if state != "pending" {
		return RecordAttemptResult{}, ErrCommandNotPending
	}
	if attempts >= MaxCommandAttempts || !in.Now.Before(expiresAt) {
		return RecordAttemptResult{}, ErrCommandPatienceExhausted
	}
	id, fresh, err := s.writer.Append(ctx, tx, spec)
	if err != nil {
		return RecordAttemptResult{}, fmt.Errorf("crossnode: append command.attempted: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RecordAttemptResult{}, err
	}
	return RecordAttemptResult{EventID: id, AttemptCount: attempts + 1, Fresh: fresh}, nil
}

// ExpireDueInput supplies the reconciler's deterministic observation time and
// fully attributed system actor. Limit bounds one SKIP LOCKED reconciliation
// batch; non-positive values use the queue read default.
type ExpireDueInput struct {
	Now          time.Time
	Limit        int
	ActorTokenID uuid.UUID
	Source       domain.Source
}

type ExpireResult struct {
	CommandQueueID uuid.UUID
	EventID        uuid.UUID
	TargetNodeID   string
	Reason         ExpiryReason
}

// ExpireDue appends terminal command.expired events for rows whose 24-hour
// deadline or five-attempt budget is exhausted. Rows are claimed in stable
// order with SKIP LOCKED; the projector's pending guard is the deterministic
// first-terminal-wins reducer shared with command.acked.
func (s *QueueService) ExpireDue(ctx context.Context, in ExpireDueInput) ([]ExpireResult, error) {
	if in.Now.IsZero() || in.ActorTokenID == uuid.Nil || !in.Source.Valid() {
		return nil, ErrInvalidExpiryInput
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultPendingLimit
	}
	if limit > maxPendingLimit {
		limit = maxPendingLimit
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, target_node_id, attempt_count
		FROM command_queue
		WHERE state = 'pending' AND (attempt_count >= $1 OR expires_at <= $2)
		ORDER BY expires_at, queued_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $3
	`, MaxCommandAttempts, in.Now, limit)
	if err != nil {
		return nil, fmt.Errorf("crossnode: select due commands: %w", err)
	}
	type dueRow struct {
		id       uuid.UUID
		target   string
		attempts int
	}
	var due []dueRow
	for rows.Next() {
		var row dueRow
		if err := rows.Scan(&row.id, &row.target, &row.attempts); err != nil {
			rows.Close()
			return nil, fmt.Errorf("crossnode: scan due command: %w", err)
		}
		due = append(due, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("crossnode: iterate due commands: %w", err)
	}
	rows.Close()

	results := make([]ExpireResult, 0, len(due))
	for _, row := range due {
		reason := ExpiryDeadline
		if row.attempts >= MaxCommandAttempts {
			reason = ExpiryAttemptsExhausted
		}
		id, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectNode,
			SubjectID:    nodes.NodeSubjectID(row.target),
			Kind:         domain.EventCommandExpired,
			Source:       in.Source,
			ActorTokenID: &in.ActorTokenID,
			Payload: expiredPayload{
				CommandQueueID: row.id,
				TargetNodeID:   row.target,
				Reason:         reason,
				ExpiredAt:      in.Now.UTC(),
				AttemptCount:   row.attempts,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("crossnode: append command.expired: %w", err)
		}
		results = append(results, ExpireResult{CommandQueueID: row.id, EventID: id, TargetNodeID: row.target, Reason: reason})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}
