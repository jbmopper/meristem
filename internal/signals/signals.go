// Package signals defines the signal.received projector and the domain
// service that turns an accepted signal into a signal row plus, when needed,
// a work_item in the same transaction.
//
// A signal is a non-human structured input (review finding, repairable
// runtime failure, webhook report) that wayline explicitly converts into a
// work_item under policy. See docs/signals.md for the full contract and
// docs/schemas/wayline.work_spec.v1.json for the work_spec shape.
package signals

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/wayline/internal/domain"
	"github.com/jbmopper/wayline/internal/projections"
)

// RegisterProjectors adds the signal projection writers to registry. Mirrors
// the convention used by internal/auth, internal/inbox, internal/workitems.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(receivedProjector{})
}

type receivedProjector struct{}

func (receivedProjector) Kind() string { return domain.EventSignalReceived }

func (receivedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectSignal {
		return fmt.Errorf("signal.received: expected subject_kind %q, got %q", domain.SubjectSignal, event.SubjectKind)
	}
	payload, err := decodeSignalPayload(event.Payload)
	if err != nil {
		return err
	}

	var dedupeKey *string
	if payload.DedupeKey != "" {
		dedupeKey = &payload.DedupeKey
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO signals (
			id, received_at, actor_token_id, source,
			signal_kind, dedupe_key, fingerprint, work_spec,
			work_item_id, created_work_item
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			received_at = EXCLUDED.received_at,
			actor_token_id = EXCLUDED.actor_token_id,
			source = EXCLUDED.source,
			signal_kind = EXCLUDED.signal_kind,
			dedupe_key = EXCLUDED.dedupe_key,
			fingerprint = EXCLUDED.fingerprint,
			work_spec = EXCLUDED.work_spec,
			work_item_id = EXCLUDED.work_item_id,
			created_work_item = EXCLUDED.created_work_item
	`,
		event.SubjectID, event.OccurredAt, event.ActorTokenID, string(event.Source),
		payload.SignalKind, dedupeKey, payload.fingerprintBytes, []byte(payload.WorkSpec),
		payload.WorkItemID, payload.CreatedWorkItem,
	)
	if err != nil {
		return fmt.Errorf("signal.received: insert projection: %w", err)
	}
	return nil
}

// signalPayload mirrors the documented signal.received payload shape (see
// docs/signals.md "Event contract"). Service.Receive passes a matching
// map[string]any to events.Writer.Append; this is the projector's typed
// view for decode + validate.
type signalPayload struct {
	SignalKind      string          `json:"signal_kind"`
	DedupeKey       string          `json:"dedupe_key,omitempty"`
	Fingerprint     string          `json:"fingerprint"`
	WorkSpec        json.RawMessage `json:"work_spec"`
	WorkItemID      uuid.UUID       `json:"work_item_id"`
	CreatedWorkItem bool            `json:"created_work_item"`

	fingerprintBytes []byte // populated by decodeSignalPayload after hex decode
}

// decodeSignalPayload JSON-roundtrips raw into a signalPayload and validates
// the fields the projector relies on. Roundtrip handles both call paths
// uniformly: the in-process map[string]any from the handler and the
// map[string]any-shaped JSONB the rebuild tool reads back from `events`.
func decodeSignalPayload(raw any) (signalPayload, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return signalPayload{}, fmt.Errorf("signal.received: marshal payload: %w", err)
	}
	var p signalPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return signalPayload{}, fmt.Errorf("signal.received: unmarshal payload: %w", err)
	}
	if p.SignalKind == "" {
		return signalPayload{}, fmt.Errorf("signal.received: signal_kind is required")
	}
	if p.Fingerprint == "" {
		return signalPayload{}, fmt.Errorf("signal.received: fingerprint is required")
	}
	fp, err := hex.DecodeString(p.Fingerprint)
	if err != nil {
		return signalPayload{}, fmt.Errorf("signal.received: fingerprint hex decode: %w", err)
	}
	p.fingerprintBytes = fp
	if len(p.WorkSpec) == 0 || !json.Valid(p.WorkSpec) {
		return signalPayload{}, fmt.Errorf("signal.received: work_spec must be valid JSON")
	}
	if p.WorkItemID == uuid.Nil {
		return signalPayload{}, fmt.Errorf("signal.received: work_item_id is required (service must resolve before append)")
	}
	return p, nil
}
