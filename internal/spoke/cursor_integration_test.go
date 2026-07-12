package spoke

import (
	"context"
	"testing"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestEventCursorStoreIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_spoke_cursor_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)
	created, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "spoke-cursor", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	actor := created.Token
	store := NewEventCursorStore(pool, writer, "https://hub.example", actor.ID, actor.Source)

	if got, err := store.Load(ctx); err != nil || got != "" {
		t.Fatalf("initial load = (%q, %v), want empty", got, err)
	}
	if err := store.Save(ctx, "cursor-a"); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := store.Save(ctx, "cursor-a"); err != nil {
		t.Fatalf("replay a: %v", err)
	}
	if err := store.Save(ctx, "cursor-b"); err != nil {
		t.Fatalf("save b: %v", err)
	}
	if got, err := store.Load(ctx); err != nil || got != "cursor-b" {
		t.Fatalf("final load = (%q, %v), want cursor-b", got, err)
	}
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, domain.EventSpokeCursorAdvanced).Scan(&eventCount); err != nil {
		t.Fatalf("count cursor events: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("cursor event count = %d, want 2", eventCount)
	}

	legacy := NewCursorStore(pool, "https://legacy.example")
	if err := legacy.Save(ctx, "cursor-x"); err != ErrCursorWriterNotConfigured {
		t.Fatalf("legacy direct save err = %v, want ErrCursorWriterNotConfigured", err)
	}
}
