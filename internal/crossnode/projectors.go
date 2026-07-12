package crossnode

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the command_queue projection writers to registry:
// the enqueue writer (command.queued -> one row) and the ack writer
// (command.acked -> the row's terminal outcome).
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(commandQueuedProjector{})
	registry.Register(commandAckedProjector{})
	registry.Register(commandAttemptedProjector{})
	registry.Register(commandExpiredProjector{})
}

type commandQueuedProjector struct{}

func (commandQueuedProjector) Kind() string { return domain.EventCommandQueued }

// Apply folds a command.queued event into command_queue as one row keyed on
// the deterministic event id, so a replayed queue POST (same id) folds to the
// same row via ON CONFLICT DO NOTHING. The subject is the target node.
func (commandQueuedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("command.queued: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyQueuedV1(ctx, tx, event)
	default:
		return fmt.Errorf("command.queued: unknown payload_version %d", v)
	}
}

func applyQueuedV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p queuedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("command.queued: decode payload: %w", err)
	}
	if !domain.ValidNodeID(p.TargetNodeID) {
		return fmt.Errorf("command.queued: target_node_id %q is not a DNS-safe node id", p.TargetNodeID)
	}
	if p.CommandPath == "" {
		return fmt.Errorf("command.queued: command_path is required")
	}
	if p.OriginIdempotencyKey == "" {
		return fmt.Errorf("command.queued: origin_idempotency_key is required")
	}
	originNodeID := p.OriginNodeID
	if originNodeID == "" {
		originNodeID = "legacy-unknown"
	} else if !domain.ValidNodeID(originNodeID) {
		return fmt.Errorf("command.queued: invalid origin_node_id %q", originNodeID)
	}
	originSource := p.OriginActorSource
	if originSource == "" {
		originSource = event.Source
	}
	if !originSource.Valid() {
		return fmt.Errorf("command.queued: invalid origin_actor_source %q", originSource)
	}
	body := p.CommandBody
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO command_queue (
			id, target_node_id, command_path, command_body,
			origin_idempotency_key, origin_actor_token_id, origin_node_id,
			origin_actor_source, causing_work_item_id, queued_at, expires_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, p.TargetNodeID, p.CommandPath, []byte(body), p.OriginIdempotencyKey,
		p.OriginActorTokenID, originNodeID, originSource,
		p.CausingWorkItemID, event.OccurredAt, event.OccurredAt.Add(CommandQueuePatience))
	if err != nil {
		return fmt.Errorf("command.queued: insert projection: %w", err)
	}
	return nil
}

type commandAckedProjector struct{}

func (commandAckedProjector) Kind() string { return domain.EventCommandAcked }

// Apply folds a command.acked event onto its command_queue row, advancing state
// pending -> done (ok) / failed (not ok) and recording the structural outcome
// the target observed. acked_at is the event clock (never wall time) so a
// rebuild reproduces the row. Only pending rows advance; QueueService rejects
// a later non-identical acknowledgement, while the projector guard preserves
// first-terminal-wins during replay and against any other append path.
func (commandAckedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("command.acked: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyAckedV1(ctx, tx, event)
	default:
		return fmt.Errorf("command.acked: unknown payload_version %d", v)
	}
}

func applyAckedV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p ackedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("command.acked: decode payload: %w", err)
	}
	if p.CommandQueueID == uuid.Nil {
		return fmt.Errorf("command.acked: command_queue_id is required")
	}
	state, ok, err := ackedOutcome(p)
	if err != nil {
		return fmt.Errorf("command.acked: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE command_queue
		SET state = $2, outcome_status_code = $3, outcome_ok = $4,
		    acked_at = $5, terminal_event_id = $6, terminal_reason = NULL
		WHERE id = $1 AND target_node_id = $7 AND state = 'pending'
	`, p.CommandQueueID, state, p.StatusCode, ok, event.OccurredAt, event.ID, p.TargetNodeID)
	if err != nil {
		return fmt.Errorf("command.acked: update projection: %w", err)
	}
	return nil
}

func ackedOutcome(p ackedPayload) (string, bool, error) {
	if p.Outcome == "" {
		if p.OK {
			return string(CommandDone), true, nil
		}
		return string(CommandFailed), false, nil
	}
	switch p.Outcome {
	case CommandDone:
		return string(p.Outcome), true, nil
	case CommandRefused, CommandFailed:
		return string(p.Outcome), false, nil
	default:
		return "", false, fmt.Errorf("outcome %q is not an acknowledgement outcome", p.Outcome)
	}
}

type commandAttemptedProjector struct{}

func (commandAttemptedProjector) Kind() string { return domain.EventCommandAttempted }

func (commandAttemptedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("command.attempted: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	if payloadVersion(event.Payload) != 1 {
		return fmt.Errorf("command.attempted: unknown payload_version %d", payloadVersion(event.Payload))
	}
	var p attemptedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("command.attempted: decode payload: %w", err)
	}
	if p.CommandQueueID == uuid.Nil || p.AttemptKey == "" || !domain.ValidNodeID(p.TargetNodeID) {
		return fmt.Errorf("command.attempted: command_queue_id, target_node_id, and attempt_key are required")
	}
	_, err := tx.Exec(ctx, `
		UPDATE command_queue
		SET attempt_count = attempt_count + 1, last_attempt_at = $3
		WHERE id = $1 AND target_node_id = $2 AND state = 'pending'
		  AND attempt_count < $4
	`, p.CommandQueueID, p.TargetNodeID, event.OccurredAt, MaxCommandAttempts)
	if err != nil {
		return fmt.Errorf("command.attempted: update projection: %w", err)
	}
	return nil
}

type commandExpiredProjector struct{}

func (commandExpiredProjector) Kind() string { return domain.EventCommandExpired }

func (commandExpiredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectNode {
		return fmt.Errorf("command.expired: expected subject_kind %q, got %q", domain.SubjectNode, event.SubjectKind)
	}
	if payloadVersion(event.Payload) != 1 {
		return fmt.Errorf("command.expired: unknown payload_version %d", payloadVersion(event.Payload))
	}
	var p expiredPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("command.expired: decode payload: %w", err)
	}
	if p.CommandQueueID == uuid.Nil || !domain.ValidNodeID(p.TargetNodeID) || !p.Reason.Valid() {
		return fmt.Errorf("command.expired: valid command_queue_id, target_node_id, and reason are required")
	}
	_, err := tx.Exec(ctx, `
		UPDATE command_queue
		SET state = 'expired', acked_at = $3, terminal_event_id = $4,
		    terminal_reason = $5, outcome_status_code = NULL, outcome_ok = false
		WHERE id = $1 AND target_node_id = $2 AND state = 'pending'
	`, p.CommandQueueID, p.TargetNodeID, event.OccurredAt, event.ID, string(p.Reason))
	if err != nil {
		return fmt.Errorf("command.expired: update projection: %w", err)
	}
	return nil
}

// payloadVersion reads payload_version, treating absence or a malformed value
// as 1 per docs/payload-versioning.md; the version switch fails closed on an
// unknown version, so this helper never needs to.
func payloadVersion(raw any) int {
	b, err := json.Marshal(raw)
	if err != nil {
		return 1
	}
	var probe struct {
		PayloadVersion int `json:"payload_version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil || probe.PayloadVersion == 0 {
		return 1
	}
	return probe.PayloadVersion
}

func decode(raw any, dst any) error {
	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
