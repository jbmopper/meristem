package spoke

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CursorStore persists the spoke's hub-feed poll bookmark so a restart resumes
// where it left off. It is deliberately not on the events path: the cursor is
// derived, best-effort operational state (see migration 0024_spoke_state).
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
	pool *pgxpool.Pool
	key  string
}

// NewCursorStore constructs a Postgres-backed CursorStore for hubBaseURL over
// the spoke's local pool.
func NewCursorStore(pool *pgxpool.Pool, hubBaseURL string) CursorStore {
	return &pgCursorStore{pool: pool, key: "hub_feed_cursor:" + hubBaseURL}
}

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
	_, err := s.pool.Exec(ctx, `
		INSERT INTO spoke_state (key, value, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
	`, s.key, cursor)
	if err != nil {
		return fmt.Errorf("spoke: save cursor: %w", err)
	}
	return nil
}
