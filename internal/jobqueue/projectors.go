package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

const KindDispatch = "dispatch"

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(dispatchRequestedProjector{})
}

type dispatchRequestedProjector struct{}

func (dispatchRequestedProjector) Kind() string { return domain.EventDispatchRequested }

func (dispatchRequestedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("dispatch.requested: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("dispatch.requested: marshal payload: %w", err)
	}
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte(`{}`)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO job_queue (id, kind, work_item_id, state, payload, created_at, updated_at)
		VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $5)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, KindDispatch, event.SubjectID, payload, event.OccurredAt)
	return err
}
