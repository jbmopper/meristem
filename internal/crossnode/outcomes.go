package crossnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
)

// QueueOutcome is immutable terminal evidence served by a queue host to the
// command's origin. RemoteEventSeq is meaningful only within QueueHostNodeID.
type QueueOutcome struct {
	RemoteEventSeq        int64          `json:"remote_event_seq"`
	RemoteTerminalEventID uuid.UUID      `json:"remote_terminal_event_id"`
	CommandQueueID        uuid.UUID      `json:"command_queue_id"`
	OriginNodeID          string         `json:"origin_node_id"`
	TargetNodeID          string         `json:"target_node_id"`
	CausingWorkItemID     *uuid.UUID     `json:"causing_work_item_id,omitempty"`
	Outcome               CommandOutcome `json:"outcome"`
	StatusCode            *int           `json:"status_code,omitempty"`
	TerminalReason        *string        `json:"terminal_reason,omitempty"`
	RemoteOccurredAt      time.Time      `json:"remote_occurred_at"`
}

const defaultOutcomeLimit = 100
const maxOutcomeLimit = 500

// OutcomesForOrigin reads terminal queue rows in the queue host's immutable
// event order. The caller must separately authenticate and authorize origin.
func (s *QueueService) OutcomesForOrigin(ctx context.Context, origin string, after int64, limit int) ([]QueueOutcome, error) {
	if !domain.ValidNodeID(origin) {
		return nil, ErrInvalidOriginNodeID
	}
	if after < 0 {
		return nil, ErrInvalidOutcomeCursor
	}
	if limit <= 0 {
		limit = defaultOutcomeLimit
	}
	if limit > maxOutcomeLimit {
		limit = maxOutcomeLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.seq, cq.terminal_event_id, cq.id, cq.origin_node_id,
		       cq.target_node_id, cq.causing_work_item_id, cq.state,
		       cq.outcome_status_code, cq.terminal_reason, e.occurred_at
		FROM command_queue AS cq
		JOIN events AS e ON e.id = cq.terminal_event_id
		WHERE cq.origin_node_id = $1 AND cq.state <> 'pending' AND e.seq > $2
		ORDER BY e.seq, cq.id
		LIMIT $3
	`, origin, after, limit)
	if err != nil {
		return nil, fmt.Errorf("crossnode: query origin outcomes: %w", err)
	}
	defer rows.Close()
	out := make([]QueueOutcome, 0, limit)
	for rows.Next() {
		var item QueueOutcome
		if err := rows.Scan(&item.RemoteEventSeq, &item.RemoteTerminalEventID,
			&item.CommandQueueID, &item.OriginNodeID, &item.TargetNodeID,
			&item.CausingWorkItemID, &item.Outcome, &item.StatusCode,
			&item.TerminalReason, &item.RemoteOccurredAt); err != nil {
			return nil, fmt.Errorf("crossnode: scan origin outcome: %w", err)
		}
		if err := validateQueueOutcome(item); err != nil {
			return nil, fmt.Errorf("crossnode: invalid projected outcome: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("crossnode: iterate origin outcomes: %w", err)
	}
	return out, nil
}

type outcomeObservedPayload struct {
	PayloadVersion  int             `json:"payload_version,omitempty"`
	QueueHostNodeID string          `json:"queue_host_node_id"`
	CauseResolution CauseResolution `json:"cause_resolution"`
	QueueOutcome
}

type ObserveOutcomesInput struct {
	QueueHostNodeID string
	LocalNodeID     string
	LocalActor      domain.Token
	Outcomes        []QueueOutcome
}

type ObserveOutcomesResult struct {
	Cursor           int64
	Observed         int
	CauseTransitions int
}

var (
	ErrInvalidOutcomeCursor = errors.New("crossnode: outcome cursor must be non-negative")
	ErrInvalidOutcome       = errors.New("crossnode: invalid terminal outcome")
	ErrOutcomeConflict      = errors.New("crossnode: conflicting terminal outcome observation")
)

// OutcomeObserver folds an authenticated outbound read into the origin's own
// log. It never mutates the target-homed object described by the command.
type OutcomeObserver struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewOutcomeObserver(pool *pgxpool.Pool, writer *events.Writer) *OutcomeObserver {
	return &OutcomeObserver{pool: pool, writer: writer}
}

func (s *OutcomeObserver) Cursor(ctx context.Context, queueHostNodeID, originNodeID string) (int64, error) {
	if s == nil || s.pool == nil || !domain.ValidNodeID(queueHostNodeID) || !domain.ValidNodeID(originNodeID) {
		return 0, ErrInvalidOutcome
	}
	var cursor int64
	err := s.pool.QueryRow(ctx, `
		SELECT remote_event_seq FROM crossnode_outcome_cursors
		WHERE queue_host_node_id = $1 AND origin_node_id = $2
	`, queueHostNodeID, originNodeID).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("crossnode: read outcome cursor: %w", err)
	}
	return cursor, nil
}

// Observe validates and atomically records one ascending page. Exact stale
// observations are no-ops; a different terminal fact for one command aborts
// the page. An observed expiry fails a non-terminal local causing work item in
// the same transaction and only when LocalNodeID is the immutable origin.
func (s *OutcomeObserver) Observe(ctx context.Context, in ObserveOutcomesInput) (ObserveOutcomesResult, error) {
	if s == nil || s.pool == nil || s.writer == nil || !domain.ValidNodeID(in.QueueHostNodeID) ||
		!domain.ValidNodeID(in.LocalNodeID) || in.LocalActor.ID == uuid.Nil ||
		(in.LocalActor.Source != domain.SourceAgent && in.LocalActor.Source != domain.SourceSystem) ||
		AuthorizeOutcomeObserve(in.LocalActor, in.QueueHostNodeID, in.LocalNodeID) != nil {
		return ObserveOutcomesResult{}, ErrInvalidOutcome
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ObserveOutcomesResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := outcomeCursorTx(ctx, tx, in.QueueHostNodeID, in.LocalNodeID)
	if err != nil {
		return ObserveOutcomesResult{}, err
	}
	result := ObserveOutcomesResult{Cursor: current}
	lastPageSeq := int64(0)
	for _, outcome := range in.Outcomes {
		if err := validateQueueOutcome(outcome); err != nil || outcome.OriginNodeID != in.LocalNodeID ||
			(lastPageSeq != 0 && outcome.RemoteEventSeq <= lastPageSeq) {
			return ObserveOutcomesResult{}, ErrInvalidOutcome
		}
		lastPageSeq = outcome.RemoteEventSeq
		exact, exists, _, err := matchingObservation(ctx, tx, in.QueueHostNodeID, outcome)
		if err != nil {
			return ObserveOutcomesResult{}, err
		}
		if exists {
			if !exact {
				return ObserveOutcomesResult{}, ErrOutcomeConflict
			}
			if outcome.RemoteEventSeq > result.Cursor {
				return ObserveOutcomesResult{}, ErrOutcomeConflict
			}
			continue
		}
		if outcome.RemoteEventSeq <= current {
			return ObserveOutcomesResult{}, ErrOutcomeConflict
		}
		causeResolution, causeState, err := resolveObservedCause(ctx, tx, outcome)
		if err != nil {
			return ObserveOutcomesResult{}, err
		}
		_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectNode,
			SubjectID:    nodes.NodeSubjectID(in.QueueHostNodeID),
			Kind:         domain.EventCommandOutcomeObserved,
			Source:       in.LocalActor.Source,
			ActorTokenID: &in.LocalActor.ID,
			Payload: outcomeObservedPayload{
				QueueHostNodeID: in.QueueHostNodeID,
				CauseResolution: causeResolution,
				QueueOutcome:    outcome,
			},
		})
		if err != nil {
			return ObserveOutcomesResult{}, fmt.Errorf("crossnode: append observed outcome: %w", err)
		}
		if !fresh {
			return ObserveOutcomesResult{}, ErrOutcomeConflict
		}
		result.Observed++
		result.Cursor = outcome.RemoteEventSeq
		if causeResolution == CauseLocalFailed {
			transitioned, err := failExpiredCause(ctx, tx, s.writer, in.LocalActor, in.QueueHostNodeID, outcome, causeState)
			if err != nil {
				return ObserveOutcomesResult{}, err
			}
			if transitioned {
				result.CauseTransitions++
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ObserveOutcomesResult{}, err
	}
	return result, nil
}

func resolveObservedCause(ctx context.Context, tx pgx.Tx, outcome QueueOutcome) (CauseResolution, domain.WorkItemState, error) {
	if outcome.Outcome != CommandExpired || outcome.CausingWorkItemID == nil {
		return CauseNone, "", nil
	}
	var state domain.WorkItemState
	err := tx.QueryRow(ctx, `SELECT state FROM work_items WHERE id = $1 FOR UPDATE`, *outcome.CausingWorkItemID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return CauseLocalMissing, "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("crossnode: lock causing work item: %w", err)
	}
	if state.Terminal() {
		return CauseLocalAlreadyTerminal, state, nil
	}
	return CauseLocalFailed, state, nil
}

func failExpiredCause(ctx context.Context, tx pgx.Tx, writer *events.Writer, actor domain.Token, queueHost string, outcome QueueOutcome, state domain.WorkItemState) (bool, error) {
	_, fresh, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     *outcome.CausingWorkItemID,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        actor.Source,
		ActorTokenID:  &actor.ID,
		Discriminator: queueHost + ":" + outcome.RemoteTerminalEventID.String(),
		Payload: map[string]any{
			"from":                     state,
			"to":                       domain.WorkItemFailed,
			"reason":                   CrossNodeDeliveryExpiredReason,
			"command_queue_id":         outcome.CommandQueueID,
			"queue_host_node_id":       queueHost,
			"remote_terminal_event_id": outcome.RemoteTerminalEventID,
		},
	})
	if err != nil {
		return false, fmt.Errorf("crossnode: fail origin causing work item: %w", err)
	}
	return fresh, nil
}

func outcomeCursorTx(ctx context.Context, tx pgx.Tx, host, origin string) (int64, error) {
	var cursor int64
	err := tx.QueryRow(ctx, `
		SELECT remote_event_seq FROM crossnode_outcome_cursors
		WHERE queue_host_node_id = $1 AND origin_node_id = $2
		FOR UPDATE
	`, host, origin).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("crossnode: lock outcome cursor: %w", err)
	}
	return cursor, nil
}

func matchingObservation(ctx context.Context, tx pgx.Tx, host string, outcome QueueOutcome) (bool, bool, CauseResolution, error) {
	var existing QueueOutcome
	var resolution CauseResolution
	err := tx.QueryRow(ctx, `
		SELECT remote_event_seq, remote_terminal_event_id, command_queue_id,
		       origin_node_id, target_node_id, causing_work_item_id, outcome,
		       status_code, terminal_reason, remote_occurred_at, cause_resolution
		FROM crossnode_outcome_observations
		WHERE queue_host_node_id = $1 AND command_queue_id = $2
		FOR UPDATE
	`, host, outcome.CommandQueueID).Scan(&existing.RemoteEventSeq, &existing.RemoteTerminalEventID,
		&existing.CommandQueueID, &existing.OriginNodeID, &existing.TargetNodeID,
		&existing.CausingWorkItemID, &existing.Outcome, &existing.StatusCode,
		&existing.TerminalReason, &existing.RemoteOccurredAt, &resolution)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, "", nil
	}
	if err != nil {
		return false, false, "", fmt.Errorf("crossnode: read observed outcome: %w", err)
	}
	return queueOutcomesEqual(existing, outcome), true, resolution, nil
}

func validateQueueOutcome(item QueueOutcome) error {
	if item.RemoteEventSeq <= 0 || item.RemoteTerminalEventID == uuid.Nil || item.CommandQueueID == uuid.Nil ||
		!domain.ValidNodeID(item.OriginNodeID) || !domain.ValidNodeID(item.TargetNodeID) || item.RemoteOccurredAt.IsZero() {
		return ErrInvalidOutcome
	}
	switch item.Outcome {
	case CommandDone, CommandRefused, CommandFailed:
		if item.StatusCode == nil || *item.StatusCode < 100 || *item.StatusCode > 599 || item.TerminalReason != nil {
			return ErrInvalidOutcome
		}
	case CommandExpired:
		if item.StatusCode != nil || item.TerminalReason == nil || (*item.TerminalReason != string(ExpiryDeadline) && *item.TerminalReason != string(ExpiryAttemptsExhausted)) {
			return ErrInvalidOutcome
		}
	default:
		return ErrInvalidOutcome
	}
	return nil
}

func queueOutcomesEqual(a, b QueueOutcome) bool {
	return a.RemoteEventSeq == b.RemoteEventSeq && a.RemoteTerminalEventID == b.RemoteTerminalEventID &&
		a.CommandQueueID == b.CommandQueueID && a.OriginNodeID == b.OriginNodeID &&
		a.TargetNodeID == b.TargetNodeID && equalUUIDPtr(a.CausingWorkItemID, b.CausingWorkItemID) &&
		a.Outcome == b.Outcome && equalIntPtr(a.StatusCode, b.StatusCode) &&
		equalStringPtr(a.TerminalReason, b.TerminalReason) && a.RemoteOccurredAt.Equal(b.RemoteOccurredAt)
}

func equalUUIDPtr(a, b *uuid.UUID) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
func equalIntPtr(a, b *int) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
func equalStringPtr(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
