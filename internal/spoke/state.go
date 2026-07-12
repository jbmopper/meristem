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

// CursorStore persists the spoke's hub-feed poll bookmark so a restart resumes
// where it left off. The bookmark is projected from spoke_cursor.advanced
// events (migration 0027), not written to spoke_state directly.
type CursorStore interface {
	// Load returns the persisted cursor, or "" when none has been stored yet.
	Load(ctx context.Context) (string, error)
	// Save persists cursor, overwriting any previous value.
	Save(ctx context.Context, cursor string) error
}

// pgCursorStore is the Postgres-backed CursorStore. It keys the bookmark by the
// hub base URL so re-pointing a spoke at a different hub starts a fresh cursor
// rather than replaying the old hub's seq against the new one.
type pgCursorStore struct {
	pool    *pgxpool.Pool
	key     string
	service *CursorService
	actorID uuid.UUID
	source  domain.Source
}

// NewCursorStore preserves the old constructor for compile-time compatibility,
// but its Save fails closed. New wiring must use NewEventCursorStore with a
// resolved local actor so every cursor mutation is fully attributed.
func NewCursorStore(pool *pgxpool.Pool, hubBaseURL string) CursorStore {
	return &pgCursorStore{pool: pool, key: "hub_feed_cursor:" + hubBaseURL}
}

// NewEventCursorStore constructs an event-backed CursorStore. Root wiring must
// resolve actorID/source from the local spoke token and pass the full app event
// writer; no remote metadata may supply this attribution.
func NewEventCursorStore(pool *pgxpool.Pool, writer *events.Writer, hubBaseURL string, actorID uuid.UUID, source domain.Source) CursorStore {
	return &pgCursorStore{
		pool:    pool,
		key:     "hub_feed_cursor:" + hubBaseURL,
		service: NewCursorService(pool, writer),
		actorID: actorID,
		source:  source,
	}
}

// ErrCursorWriterNotConfigured makes legacy wiring fail closed rather than
// directly mutating spoke_state outside the event log.
var ErrCursorWriterNotConfigured = errors.New("spoke: event-backed cursor writer is not configured")

func (s *pgCursorStore) Load(ctx context.Context) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx, `SELECT value FROM spoke_state WHERE key = $1`, s.key).Scan(&value)
	if err != nil {
		// pgx returns ErrNoRows when the bookmark has never been written; treat
		// that as "no cursor yet" so the first tick bootstraps from the hub head.
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("spoke: load cursor: %w", err)
	}
	return value, nil
}

func (s *pgCursorStore) Save(ctx context.Context, cursor string) error {
	if s.service == nil {
		return ErrCursorWriterNotConfigured
	}
	_, err := s.service.Advance(ctx, AdvanceCursorInput{
		Key:          s.key,
		Value:        cursor,
		ActorTokenID: s.actorID,
		Source:       s.source,
	})
	return err
}
