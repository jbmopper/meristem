package inbox

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type CaptureResult struct {
	MessageID  uuid.UUID
	WorkItemID uuid.UUID
}

func (s *Service) CaptureText(ctx context.Context, actor domain.Token, text string) (CaptureResult, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return CaptureResult{}, fmt.Errorf("inbox: text is required")
	}
	messageID := newSubjectID(ctx, "inbox.message")
	workItemID := newSubjectID(ctx, "inbox.work_item")
	title := text
	if len(title) > 80 {
		title = title[:80]
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CaptureResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    workItemID,
		Kind:         domain.EventWorkItemCreated,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title":                        title,
			"body":                         text,
			"state":                        domain.WorkItemCaptured,
			"suggested_convergence_checks": []string{},
			"human_review_status":          domain.HumanReviewWavedThrough,
		},
	}); err != nil {
		return CaptureResult{}, err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectMessage,
		SubjectID:    messageID,
		Kind:         domain.EventMessageCaptured,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"work_item_id": workItemID,
			"text":         text,
		},
	}); err != nil {
		return CaptureResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CaptureResult{}, err
	}
	return CaptureResult{MessageID: messageID, WorkItemID: workItemID}, nil
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}
