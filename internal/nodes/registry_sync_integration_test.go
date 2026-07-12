package nodes_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/peerhttp"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestRegistrySyncTwoDatabaseReplayAndOutageRetention(t *testing.T) {
	ctx := context.Background()
	home := pgtest.NewPool(t, "registry_sync_home")
	consumer := pgtest.NewPool(t, "registry_sync_consumer")
	migrateRegistrySyncDB(t, ctx, home)
	migrateRegistrySyncDB(t, ctx, consumer)

	homeWriter := app.NewEventWriter()
	homeAuth := auth.NewService(home, homeWriter)
	homeRoot, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "home-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	homeRegistryWriter, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "registry-writer", Source: domain.SourceSystem, Actor: &homeRoot.Token})
	if err != nil {
		t.Fatal(err)
	}
	homeRead, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "consumer-registry-read", Source: domain.SourceAgent, Scopes: []string{nodes.SnapshotReadScope("hub")}, Actor: &homeRoot.Token})
	if err != nil {
		t.Fatal(err)
	}
	appendRegistryNodeEvent(t, ctx, home, homeWriter, homeRegistryWriter.Token, "hub", "https://hub.example")
	appendRegistryNodeEvent(t, ctx, home, homeWriter, homeRegistryWriter.Token, "spoke", "")

	t.Setenv(api.EnvNodeID, "hub")
	t.Setenv(api.EnvRegistryHomeNodeID, "")
	homeHTTP := httptest.NewServer(api.New(home, nil).Handler())

	consumerWriter := app.NewEventWriter()
	consumerAuth := auth.NewService(consumer, consumerWriter)
	consumerRoot, err := consumerAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "consumer-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := consumerAuth.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "registry-observer",
		Source: domain.SourceSystem,
		Scopes: []string{nodes.SnapshotObserveScope("hub")},
		Actor:  &consumerRoot.Token,
	})
	if err != nil {
		t.Fatal(err)
	}

	syncer, err := nodes.NewRegistrySyncService(nodes.NewSnapshotService(consumer, consumerWriter), nodes.RegistrySyncConfig{
		RegistryHomeOrigin: homeHTTP.URL,
		ExpectedSource:     "hub",
		RegistryHomeToken:  homeRead.Secret,
		LocalActor:         observer.Token,
		RequestTimeout:     2 * time.Second,
	}, peerhttp.Options{})
	if err != nil {
		t.Fatalf("new sync service: %v", err)
	}
	first, err := syncer.Tick(ctx)
	if err != nil || !first.Observed {
		t.Fatalf("first tick = %+v, %v", first, err)
	}
	assertRegistryConsumerState(t, ctx, consumer, 2, 1, first.SourceRevision)

	replay, err := syncer.Tick(ctx)
	if err != nil || replay.Observed {
		t.Fatalf("replay tick = %+v, %v", replay, err)
	}
	assertRegistryConsumerState(t, ctx, consumer, 2, 1, first.SourceRevision)

	appendRegistryNodeEvent(t, ctx, home, homeWriter, homeRegistryWriter.Token, "den", "https://den.example")
	second, err := syncer.Tick(ctx)
	if err != nil || !second.Observed || second.SourceRevision <= first.SourceRevision {
		t.Fatalf("new revision tick = %+v, %v", second, err)
	}
	assertRegistryConsumerState(t, ctx, consumer, 3, 2, second.SourceRevision)

	homeHTTP.Close()
	if _, err := syncer.Tick(ctx); err == nil {
		t.Fatal("outage tick unexpectedly succeeded")
	}
	assertRegistryConsumerState(t, ctx, consumer, 3, 2, second.SourceRevision)

	var payload []byte
	if err := consumer.QueryRow(ctx, `SELECT payload FROM events WHERE kind = $1 ORDER BY seq DESC LIMIT 1`, domain.EventRegistrySnapshotObserved).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), homeRead.Secret) || strings.Contains(string(payload), observer.Secret) {
		t.Fatal("registry snapshot event contains bearer material")
	}
}

func migrateRegistrySyncDB(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := storage.Migrate(ctx, pool, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func appendRegistryNodeEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, nodeID, baseURL string) {
	t.Helper()
	var base *string
	if baseURL != "" {
		base = &baseURL
	}
	payload, err := nodes.BuildRegisteredPayload(nodes.RegisterParams{NodeID: nodeID, BaseURL: base, Status: string(domain.NodeStatusActive)})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectNode,
		SubjectID:    nodes.NodeSubjectID(nodeID),
		Kind:         domain.EventNodeRegistered,
		Source:       actor.Source,
		ActorTokenID: &actor.ID,
		Payload:      payload,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertRegistryConsumerState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, wantNodes, wantEvents int, wantRevision int64) {
	t.Helper()
	var nodesCount, eventsCount int
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nodes`).Scan(&nodesCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, domain.EventRegistrySnapshotObserved).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT source_revision FROM registry_snapshot_state WHERE singleton`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if nodesCount != wantNodes || eventsCount != wantEvents || revision != wantRevision {
		t.Fatalf("consumer state nodes=%d events=%d revision=%d; want %d/%d/%d", nodesCount, eventsCount, revision, wantNodes, wantEvents, wantRevision)
	}
}
