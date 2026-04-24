package inbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(messageCapturedProjector{})
}

type messageCapturedProjector struct{}

func (messageCapturedProjector) Kind() string { return domain.EventMessageCaptured }

func (messageCapturedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		WorkItemID uuid.UUID `json:"work_item_id"`
		Text       string    `json:"text"`
	}
	b, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return err
	}
	if payload.Text == "" {
		return fmt.Errorf("message.captured: text is required")
	}
	// DO NOTHING on conflict for the same reason as
	// internal/auth/projectors.go: the events writer fires projectors only
	// on a fresh event-row insert, so a duplicate hit here means a real
	// bug (two distinct message.captured events with the same subject id)
	// or a rebuild-time replay where the projection table is empty.
	// DO UPDATE would silently mask the bug case.
	_, err = tx.Exec(ctx, `
		INSERT INTO messages (id, source, actor_token_id, work_item_id, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, string(event.Source), event.ActorTokenID, payload.WorkItemID, event.OccurredAt)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO message_parts (id, message_id, ordinal, part_type, content_text)
		VALUES ($1, $2, 0, 'text', $3)
		ON CONFLICT (message_id, ordinal) DO NOTHING
	`, uuid.NewSHA1(uuid.NameSpaceURL, []byte(event.SubjectID.String()+":0")), event.SubjectID, payload.Text)
	return err
}
