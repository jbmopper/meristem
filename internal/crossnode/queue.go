package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/nodes"
)

// queuedPayload is the field-minimal structural payload of a command.queued
// event (docs/cerberus-reducer-event-contracts.md: a field is structural iff
// deterministic code, a projection, or an auth/idempotency boundary must read
// it without parsing prose). Every field here is read by the command_queue
// projector or the future drain path (work item bc1da2c5):
//
//   - target_node_id       — which node's queue this belongs to / drains it.
//   - command_path         — the home-node REST path the target replays.
//   - command_body         — the JSON body of that call, verbatim.
//   - origin_idempotency_key — replayed as the target's Idempotency-Key so the
//     drained execution collapses with any direct retry.
//   - origin_actor_token_id — the originating actor on the queuing node, for
//     the cross-boundary attribution trail (§2 "Cross-node identity").
//
// payload_version is absent (== 1) per docs/payload-versioning.md; additive
// fields will not bump it.
type queuedPayload struct {
	PayloadVersion       int             `json:"payload_version,omitempty"`
	TargetNodeID         string          `json:"target_node_id"`
	CommandPath          string          `json:"command_path"`
	CommandBody          json.RawMessage `json:"command_body"`
	OriginIdempotencyKey string          `json:"origin_idempotency_key"`
	OriginActorTokenID   *uuid.UUID      `json:"origin_actor_token_id,omitempty"`
}

// EnqueueInput is one request to durably park a command for an inbound-less
// target node.
type EnqueueInput struct {
	// TargetNodeID is the DNS-safe home node the command is bound for. It must
	// name a node other than the receiver (the caller enforces that).
	TargetNodeID string
	// CommandPath and CommandBody are the home-node call to replay on drain.
	CommandPath string
	CommandBody json.RawMessage
	// OriginIdempotencyKey is the originating request's Idempotency-Key.
	OriginIdempotencyKey string
	// OriginActorTokenID is the resolved token id of the queuing actor, if any.
	OriginActorTokenID *uuid.UUID
	// Source is the resolved source of the actor that caused the enqueue. A
	// zero value preserves the historical system attribution for non-transport
	// callers; authenticated transports should pass their request-context
	// token source.
	Source domain.Source
}

// EnqueueResult reports the command.queued event id the enqueue produced.
type EnqueueResult struct {
	EventID uuid.UUID
}

// ackedPayload is the field-minimal structural payload of a command.acked
// event. Every field here is read by the command_queue ack projector:
//
//   - command_queue_id — the queued command's event id, i.e. the command_queue
//     row this ack folds onto.
//   - target_node_id   — the node that drained and acked (the row's target);
//     it also anchors the event subject, so a node's acks share its stream.
//   - status_code      — the HTTP status the target's local execution returned.
//   - ok               — whether that execution succeeded (2xx). The projector
//     maps ok -> state done, !ok -> state failed.
//
// payload_version is absent (== 1) per docs/payload-versioning.md.
type ackedPayload struct {
	PayloadVersion int       `json:"payload_version,omitempty"`
	CommandQueueID uuid.UUID `json:"command_queue_id"`
	TargetNodeID   string    `json:"target_node_id"`
	StatusCode     int       `json:"status_code"`
	OK             bool      `json:"ok"`
}

// QueuedCommand is one pending row the drain read returns: the home-node call
// to replay locally plus the identity the spoke needs to ack it.
type QueuedCommand struct {
	// EventID is the command.queued event id / command_queue row id. It is the
	// {event_id} the spoke posts its ack to.
	EventID uuid.UUID `json:"event_id"`
	// TargetNodeID is the node this command is queued for.
	TargetNodeID string `json:"target_node_id"`
	// CommandPath and CommandBody are the home-node REST call to replay locally.
	CommandPath string          `json:"command_path"`
	CommandBody json.RawMessage `json:"command_body"`
	// OriginIdempotencyKey is replayed as the Idempotency-Key of the local
	// execution so a drained command collapses with any direct retry.
	OriginIdempotencyKey string `json:"origin_idempotency_key"`
	// QueuedAt is when the command was durably parked (oldest drained first).
	QueuedAt time.Time `json:"queued_at"`
}

// AckInput is one acknowledgement of a drained command's structural outcome.
type AckInput struct {
	// CommandQueueID is the queued command's event id (the command_queue row).
	CommandQueueID uuid.UUID
	// StatusCode and OK are the structural outcome of the local execution.
	StatusCode int
	OK         bool
	// ActorTokenID is the resolved token id of the acking (hub-side) actor.
	ActorTokenID *uuid.UUID
	// Source is the resolved source of the actor that caused the ack. A zero
	// value preserves the historical system attribution for existing callers.
	Source domain.Source
}

// AckResult reports the command.acked event id and the target it folded onto.
type AckResult struct {
	EventID      uuid.UUID
	TargetNodeID string
}

// ErrInvalidTargetNodeID is returned when an enqueue names a target that is
// not a DNS-safe node id.
var ErrInvalidTargetNodeID = errors.New("crossnode: target_node_id is not a DNS-safe node id")

// ErrUnknownCommand is returned when an ack references a command_queue id that
// does not exist (unknown or already pruned queued command).
var ErrUnknownCommand = errors.New("crossnode: unknown queued command")

// defaultPendingLimit bounds a drain read that supplies no positive limit.
const defaultPendingLimit = 100

// maxPendingLimit caps how many pending rows one drain read returns.
const maxPendingLimit = 500

// QueueService appends command.queued events and lets its projector fold them
// into command_queue. It is the server-side half of this package: the client
// half is Select/Deliver.
type QueueService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

// NewQueueService constructs a QueueService over an open pool and the shared
// event writer.
func NewQueueService(pool *pgxpool.Pool, writer *events.Writer) *QueueService {
	return &QueueService{pool: pool, writer: writer}
}

// Enqueue appends a command.queued event for in.TargetNodeID and commits it,
// folding it into command_queue via the projector in the same transaction. The
// event subject is the target node (SubjectNode, keyed by nodes.NodeSubjectID)
// so a node's queued commands share the node's subject stream. The idempotency
// discriminator ties the event identity to the originating request so distinct
// commands never collapse and a replay of the same request does.
func (s *QueueService) Enqueue(ctx context.Context, in EnqueueInput) (EnqueueResult, error) {
	if !domain.ValidNodeID(in.TargetNodeID) {
		return EnqueueResult{}, ErrInvalidTargetNodeID
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return EnqueueResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	disc, _ := idempotency.EventDiscriminator(ctx)
	id, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectNode,
		SubjectID:     nodes.NodeSubjectID(in.TargetNodeID),
		Kind:          domain.EventCommandQueued,
		Source:        eventSource(in.Source),
		ActorTokenID:  in.OriginActorTokenID,
		Discriminator: disc,
		Payload: queuedPayload{
			TargetNodeID:         in.TargetNodeID,
			CommandPath:          in.CommandPath,
			CommandBody:          normalizeBody(in.CommandBody),
			OriginIdempotencyKey: in.OriginIdempotencyKey,
			OriginActorTokenID:   in.OriginActorTokenID,
		},
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("crossnode: append command.queued: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{EventID: id}, nil
}

// PendingForTarget reads target's still-pending queued commands oldest-first,
// capped at limit (a non-positive limit uses defaultPendingLimit; the read is
// hard-capped at maxPendingLimit). It is the hub half of the drain: a target
// polls it outbound, executes each command locally, and acks the outcome.
func (s *QueueService) PendingForTarget(ctx context.Context, target string, limit int) ([]QueuedCommand, error) {
	if !domain.ValidNodeID(target) {
		return nil, ErrInvalidTargetNodeID
	}
	if limit <= 0 {
		limit = defaultPendingLimit
	}
	if limit > maxPendingLimit {
		limit = maxPendingLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, target_node_id, command_path, command_body, origin_idempotency_key, queued_at
		FROM command_queue
		WHERE target_node_id = $1 AND state = 'pending'
		ORDER BY queued_at, id
		LIMIT $2
	`, target, limit)
	if err != nil {
		return nil, fmt.Errorf("crossnode: query pending commands: %w", err)
	}
	defer rows.Close()

	out := make([]QueuedCommand, 0, limit)
	for rows.Next() {
		var c QueuedCommand
		var body []byte
		if err := rows.Scan(&c.EventID, &c.TargetNodeID, &c.CommandPath, &body, &c.OriginIdempotencyKey, &c.QueuedAt); err != nil {
			return nil, fmt.Errorf("crossnode: scan pending command: %w", err)
		}
		c.CommandBody = json.RawMessage(body)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("crossnode: iterate pending commands: %w", err)
	}
	return out, nil
}

// Ack appends a command.acked event for in.CommandQueueID and commits it,
// folding the structural outcome onto the command_queue row via the projector
// in the same transaction. It resolves the row's target_node_id first (so the
// event subject is the target node and an unknown id fails cleanly), then makes
// the event identity depend on the originating request's idempotency
// discriminator so a replayed ack collapses onto the same event.
func (s *QueueService) Ack(ctx context.Context, in AckInput) (AckResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AckResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var target string
	// Serialize acknowledgements on the queue row before appending their
	// events. This makes the first row-lock winner also the first event in log
	// order, so live projection and a seq-ordered rebuild choose the same
	// terminal decision under concurrent, contradictory acks.
	err = tx.QueryRow(ctx, `
		SELECT target_node_id
		FROM command_queue
		WHERE id = $1
		FOR UPDATE
	`, in.CommandQueueID).Scan(&target)
	if errors.Is(err, pgx.ErrNoRows) {
		return AckResult{}, ErrUnknownCommand
	}
	if err != nil {
		return AckResult{}, fmt.Errorf("crossnode: resolve command target: %w", err)
	}

	disc, _ := idempotency.EventDiscriminator(ctx)
	id, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectNode,
		SubjectID:     nodes.NodeSubjectID(target),
		Kind:          domain.EventCommandAcked,
		Source:        eventSource(in.Source),
		ActorTokenID:  in.ActorTokenID,
		Discriminator: disc,
		Payload: ackedPayload{
			CommandQueueID: in.CommandQueueID,
			TargetNodeID:   target,
			StatusCode:     in.StatusCode,
			OK:             in.OK,
		},
	})
	if err != nil {
		return AckResult{}, fmt.Errorf("crossnode: append command.acked: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AckResult{}, err
	}
	return AckResult{EventID: id, TargetNodeID: target}, nil
}

// eventSource keeps existing internal callers source-compatible while giving
// authenticated transports an explicit seam for request-context attribution.
// Invalid non-empty sources are deliberately passed through so events.Spec
// fails closed instead of silently rewriting bad attribution as system.
func eventSource(source domain.Source) domain.Source {
	if source == "" {
		return domain.SourceSystem
	}
	return source
}
