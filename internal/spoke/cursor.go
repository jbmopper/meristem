package spoke

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var cursorSubjectNamespace = uuid.MustParse("2c75f9bd-cb33-551e-9ca8-9965f376c730")

// CursorSubjectID derives the aggregate id for one opaque spoke cursor key.
func CursorSubjectID(key string) uuid.UUID {
	return uuid.NewSHA1(cursorSubjectNamespace, []byte("spoke-cursor|"+key))
}

type cursorAdvancedPayload struct {
	PayloadVersion int    `json:"payload_version,omitempty"`
	Key            string `json:"key"`
	Value          string `json:"value"`
}

type AdvanceCursorInput struct {
	Key          string
	Value        string
	ActorTokenID uuid.UUID
	Source       domain.Source
}

type AdvanceCursorResult struct {
	EventID uuid.UUID
	Fresh   bool
}

var ErrInvalidCursorAdvance = errors.New("spoke: cursor key, value, actor, and source are required")

// CursorService owns the only event-backed write path for spoke_state.
type CursorService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewCursorService(pool *pgxpool.Pool, writer *events.Writer) *CursorService {
	return &CursorService{pool: pool, writer: writer}
}

// Advance appends spoke_cursor.advanced and lets its projector update the
// bookmark in the same transaction. Repeating one value for one key collapses;
// cursors are monotonic and may not legitimately cycle back to an older value.
func (s *CursorService) Advance(ctx context.Context, in AdvanceCursorInput) (AdvanceCursorResult, error) {
	if s == nil || s.pool == nil || s.writer == nil {
		return AdvanceCursorResult{}, ErrCursorWriterNotConfigured
	}
	if in.Key == "" || in.Value == "" || in.ActorTokenID == uuid.Nil || !in.Source.Valid() {
		return AdvanceCursorResult{}, ErrInvalidCursorAdvance
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AdvanceCursorResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectSpokeCursor,
		SubjectID:    CursorSubjectID(in.Key),
		Kind:         domain.EventSpokeCursorAdvanced,
		Source:       in.Source,
		ActorTokenID: &in.ActorTokenID,
		Payload:      cursorAdvancedPayload{Key: in.Key, Value: in.Value},
	})
	if err != nil {
		return AdvanceCursorResult{}, fmt.Errorf("spoke: append cursor advance: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceCursorResult{}, err
	}
	return AdvanceCursorResult{EventID: id, Fresh: fresh}, nil
}
