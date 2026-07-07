package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
}

// EnqueueResult reports the command.queued event id the enqueue produced.
type EnqueueResult struct {
	EventID uuid.UUID
}

// ErrInvalidTargetNodeID is returned when an enqueue names a target that is
// not a DNS-safe node id.
var ErrInvalidTargetNodeID = errors.New("crossnode: target_node_id is not a DNS-safe node id")

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
		Source:        domain.SourceSystem,
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
