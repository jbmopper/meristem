package spoke

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(cursorAdvancedProjector{})
}

type cursorAdvancedProjector struct{}

func (cursorAdvancedProjector) Kind() string { return domain.EventSpokeCursorAdvanced }

func (cursorAdvancedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectSpokeCursor {
		return fmt.Errorf("spoke_cursor.advanced: expected subject_kind %q, got %q", domain.SubjectSpokeCursor, event.SubjectKind)
	}
	b, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("spoke_cursor.advanced: marshal payload: %w", err)
	}
	var p cursorAdvancedPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("spoke_cursor.advanced: decode payload: %w", err)
	}
	if p.PayloadVersion != 0 && p.PayloadVersion != 1 {
		return fmt.Errorf("spoke_cursor.advanced: unknown payload_version %d", p.PayloadVersion)
	}
	if p.Key == "" || p.Value == "" || CursorSubjectID(p.Key) != event.SubjectID {
		return fmt.Errorf("spoke_cursor.advanced: valid matching key and value are required")
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO spoke_state (key, value, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
	`, p.Key, p.Value, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("spoke_cursor.advanced: upsert projection: %w", err)
	}
	return nil
}
