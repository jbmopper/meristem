package projectiondefs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(definedProjector{})
}

type definedProjector struct{}

func (definedProjector) Kind() string { return domain.EventProjectionDefined }

func (definedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectProjection {
		return fmt.Errorf("projection.defined: expected subject_kind %q, got %q", domain.SubjectProjection, event.SubjectKind)
	}
	payload, err := decodePayload(event.Payload)
	if err != nil {
		return err
	}
	filter, err := json.Marshal(payload.Filter)
	if err != nil {
		return fmt.Errorf("projection.defined: marshal filter: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO projections (
			name, version, projection_type, rootstock, filter, description,
			event_id, defined_at, defined_by, source
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10)
		ON CONFLICT (name) DO UPDATE SET
			version = EXCLUDED.version,
			projection_type = EXCLUDED.projection_type,
			rootstock = EXCLUDED.rootstock,
			filter = EXCLUDED.filter,
			description = EXCLUDED.description,
			event_id = EXCLUDED.event_id,
			defined_at = EXCLUDED.defined_at,
			defined_by = EXCLUDED.defined_by,
			source = EXCLUDED.source
	`, payload.Name, payload.Version, payload.Type, payload.Rootstock, filter,
		payload.Description, event.ID, event.OccurredAt, event.ActorTokenID, string(event.Source))
	if err != nil {
		return fmt.Errorf("projection.defined: upsert projection: %w", err)
	}
	return nil
}

func decodePayload(raw any) (DefineInput, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return DefineInput{}, fmt.Errorf("projection.defined: marshal payload: %w", err)
	}
	var p DefineInput
	if err := json.Unmarshal(b, &p); err != nil {
		return DefineInput{}, fmt.Errorf("projection.defined: unmarshal payload: %w", err)
	}
	p, _, err = normalizeInput(p)
	if err != nil {
		return DefineInput{}, fmt.Errorf("projection.defined: %w", err)
	}
	return p, nil
}
