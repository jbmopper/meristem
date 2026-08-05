package spoke_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/spoke"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestPeerCursorsPersistIndependently proves the isolation against real
// storage, not just at the key-construction level. The failure this guards is
// quiet: if two peers shared a row, each tick would move both to whichever
// peer advanced last, and the resulting skip looks exactly like normal forward
// progress.
func TestPeerCursorsPersistIndependently(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_spoke_cursors_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actor := createSpokeCursorActor(t, ctx, pool, writer)

	hubFeed, err := spoke.NewPeerCursorStore(pool, writer, spoke.PurposeFeedObservation, "hub", actor.ID, actor.Source)
	if err != nil {
		t.Fatalf("hub feed store: %v", err)
	}
	denFeed, err := spoke.NewPeerCursorStore(pool, writer, spoke.PurposeFeedObservation, "den", actor.ID, actor.Source)
	if err != nil {
		t.Fatalf("den feed store: %v", err)
	}
	hubDrain, err := spoke.NewPeerCursorStore(pool, writer, spoke.PurposeQueueDrain, "hub", actor.ID, actor.Source)
	if err != nil {
		t.Fatalf("hub drain store: %v", err)
	}

	if err := hubFeed.Save(ctx, "hub-feed-100"); err != nil {
		t.Fatalf("save hub feed: %v", err)
	}
	if err := denFeed.Save(ctx, "den-feed-7"); err != nil {
		t.Fatalf("save den feed: %v", err)
	}
	if err := hubDrain.Save(ctx, "hub-drain-42"); err != nil {
		t.Fatalf("save hub drain: %v", err)
	}

	for _, tc := range []struct {
		name  string
		store spoke.CursorStore
		want  string
	}{
		{"hub feed", hubFeed, "hub-feed-100"},
		{"den feed", denFeed, "den-feed-7"},
		{"hub drain", hubDrain, "hub-drain-42"},
	} {
		got, err := tc.store.Load(ctx)
		if err != nil {
			t.Fatalf("%s load: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s = %q, want %q — a cursor took another's value", tc.name, got, tc.want)
		}
	}

	// A peer that has never been polled starts empty rather than inheriting a
	// neighbour's position, which would skip its entire backlog on first sight.
	fresh, err := spoke.NewPeerCursorStore(pool, writer, spoke.PurposeFeedObservation, "m4", actor.ID, actor.Source)
	if err != nil {
		t.Fatalf("fresh store: %v", err)
	}
	got, err := fresh.Load(ctx)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	if got != "" {
		t.Fatalf("an unpolled peer loaded %q, want empty", got)
	}
}

// TestLegacyHubCursorSurvivesAlongsidePeerCursors is the upgrade path: an
// existing single-hub deployment's bookmark must still be readable after the
// peer-keyed scheme lands, or the first tick post-upgrade replays the whole
// feed.
func TestLegacyHubCursorSurvivesAlongsidePeerCursors(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_spoke_legacy_cursor_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actor := createSpokeCursorActor(t, ctx, pool, writer)

	legacy := spoke.NewEventCursorStore(pool, writer, "https://hub.example", actor.ID, actor.Source)
	if err := legacy.Save(ctx, "legacy-900"); err != nil {
		t.Fatalf("save legacy: %v", err)
	}
	peer, err := spoke.NewPeerCursorStore(pool, writer, spoke.PurposeFeedObservation, "hub", actor.ID, actor.Source)
	if err != nil {
		t.Fatalf("peer store: %v", err)
	}
	if err := peer.Save(ctx, "peer-5"); err != nil {
		t.Fatalf("save peer: %v", err)
	}

	got, err := legacy.Load(ctx)
	if err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if got != "legacy-900" {
		t.Fatalf("legacy cursor = %q, want legacy-900 — the peer-keyed store clobbered it", got)
	}
}

// createSpokeCursorActor mints the local agent token that attributes cursor
// advances. Cursor mutations are event-backed and fully attributed, so a real
// token is required rather than a zero value.
func createSpokeCursorActor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer) domain.Token {
	t.Helper()
	authService := auth.NewService(pool, writer)
	root, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "cursor-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	agent, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "cursor-agent", Source: domain.SourceAgent, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create agent token: %v", err)
	}
	return agent.Token
}
