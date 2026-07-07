package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestFoldEventsUsesSeqOrder(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "rebuild_seq_order")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	occurredAt := time.Date(2026, 7, 7, 1, 0, 0, 0, time.UTC)
	firstID := uuid.MustParse("ffffffff-ffff-5fff-8fff-ffffffffffff")
	secondID := uuid.MustParse("00000000-0000-5000-8000-000000000000")
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, occurred_at, source, subject_kind, subject_id, kind, payload)
		VALUES
		  ($1, $2, 'system', 'work_item', $3, 'test.seq_order', '{"ordinal":1}'::jsonb),
		  ($4, $2, 'system', 'work_item', $5, 'test.seq_order', '{"ordinal":2}'::jsonb)
	`, firstID, occurredAt, uuid.New(), secondID, uuid.New()); err != nil {
		t.Fatalf("insert same-timestamp events: %v", err)
	}

	var seen []uuid.UUID
	reg := projections.NewRegistry()
	reg.Register(seqOrderRecorder{seen: &seen})
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := foldEvents(ctx, tx, reg, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("fold events: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("seen %d events, want 2: %v", len(seen), seen)
	}
	if seen[0] != firstID || seen[1] != secondID {
		t.Fatalf("fold order = %v, want seq insertion order [%s %s]", seen, firstID, secondID)
	}
}

type seqOrderRecorder struct {
	seen *[]uuid.UUID
}

func (r seqOrderRecorder) Kind() string { return "test.seq_order" }

func (r seqOrderRecorder) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	*r.seen = append(*r.seen, event.ID)
	return nil
}
