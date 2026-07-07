package crossnode

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the command_queue projection writer to registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(commandQueuedProjector{})
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
	body := p.CommandBody
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO command_queue (
			id, target_node_id, command_path, command_body,
			origin_idempotency_key, origin_actor_token_id, queued_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, p.TargetNodeID, p.CommandPath, []byte(body), p.OriginIdempotencyKey, p.OriginActorTokenID, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("command.queued: insert projection: %w", err)
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
