package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestR8SeedV1SecondRunAppendsZeroFreshEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok := createCmdSystemToken(t, ctx, pool, writer, "r8-seed-idempotency")
	baselineEvents := countCmdAllEvents(t, ctx, pool)

	first := runSeedV1FixturesForTest(t, ctx, pool, writer, systemTok)
	if first.workItemsCreated != len(v1SubstrateItems) || first.workItemsReplayed != 0 {
		t.Fatalf("first work item seed created/replayed = %d/%d, want %d/0", first.workItemsCreated, first.workItemsReplayed, len(v1SubstrateItems))
	}
	if first.registryCreated != registrySeedTotal() || first.registryReplayed != 0 {
		t.Fatalf("first registry seed created/replayed = %d/%d, want %d/0", first.registryCreated, first.registryReplayed, registrySeedTotal())
	}
	if first.projectionsCreated != projectionSeedTotal() || first.projectionsReplayed != 0 {
		t.Fatalf("first projection seed created/replayed = %d/%d, want %d/0", first.projectionsCreated, first.projectionsReplayed, projectionSeedTotal())
	}

	afterFirstEvents := countCmdAllEvents(t, ctx, pool)
	wantSeedEvents := len(v1SubstrateItems) + registrySeedTotal() + projectionSeedTotal()
	if got := afterFirstEvents - baselineEvents; got != wantSeedEvents {
		t.Fatalf("first seed appended %d events, want %d", got, wantSeedEvents)
	}
	assertSeedProjectionCounts(t, ctx, pool)

	second := runSeedV1FixturesForTest(t, ctx, pool, writer, systemTok)
	if second.workItemsCreated != 0 || second.workItemsReplayed != len(v1SubstrateItems) {
		t.Fatalf("second work item seed created/replayed = %d/%d, want 0/%d", second.workItemsCreated, second.workItemsReplayed, len(v1SubstrateItems))
	}
	if second.registryCreated != 0 || second.registryReplayed != registrySeedTotal() {
		t.Fatalf("second registry seed created/replayed = %d/%d, want 0/%d", second.registryCreated, second.registryReplayed, registrySeedTotal())
	}
	if second.projectionsCreated != 0 || second.projectionsReplayed != projectionSeedTotal() {
		t.Fatalf("second projection seed created/replayed = %d/%d, want 0/%d", second.projectionsCreated, second.projectionsReplayed, projectionSeedTotal())
	}
	if afterSecondEvents := countCmdAllEvents(t, ctx, pool); afterSecondEvents != afterFirstEvents {
		t.Fatalf("second seed changed event count: before=%d after=%d", afterFirstEvents, afterSecondEvents)
	}
}

type seedV1FixtureCounts struct {
	workItemsCreated    int
	workItemsReplayed   int
	registryCreated     int
	registryReplayed    int
	projectionsCreated  int
	projectionsReplayed int
}

func runSeedV1FixturesForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, systemTok domain.Token) seedV1FixtureCounts {
	t.Helper()
	workItemsCreated, workItemsReplayed, err := seedV1Items(ctx, pool, writer, systemTok)
	if err != nil {
		t.Fatalf("seed v1 items: %v", err)
	}
	registryCreated, registryReplayed, err := seedRegistryFixtures(ctx, pool, writer, systemTok)
	if err != nil {
		t.Fatalf("seed registry fixtures: %v", err)
	}
	projectionsCreated, projectionsReplayed, err := seedProjectionFixtures(ctx, pool, writer, systemTok)
	if err != nil {
		t.Fatalf("seed projection fixtures: %v", err)
	}
	return seedV1FixtureCounts{
		workItemsCreated:    workItemsCreated,
		workItemsReplayed:   workItemsReplayed,
		registryCreated:     registryCreated,
		registryReplayed:    registryReplayed,
		projectionsCreated:  projectionsCreated,
		projectionsReplayed: projectionsReplayed,
	}
}

func countCmdAllEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return count
}

func assertSeedProjectionCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	assertCmdProjectionCount(t, ctx, pool, "seeded work_items", `
		SELECT count(*)
		FROM work_items
		WHERE id = ANY($1)
	`, seededSubstrateIDs(), len(v1SubstrateItems))
	assertCmdProjectionCount(t, ctx, pool, "seeded tropisms", `SELECT count(*) FROM tropisms`, nil, len(registrySeedTropisms))
	assertCmdProjectionCount(t, ctx, pool, "seeded cultivars", `SELECT count(*) FROM cultivars`, nil, len(registrySeedCultivars))
	assertCmdProjectionCount(t, ctx, pool, "seeded projections", `SELECT count(*) FROM projections`, nil, projectionSeedTotal())
}

func assertCmdProjectionCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string, query string, arg any, want int) {
	t.Helper()
	var count int
	var err error
	if arg == nil {
		err = pool.QueryRow(ctx, query).Scan(&count)
	} else {
		err = pool.QueryRow(ctx, query, arg).Scan(&count)
	}
	if err != nil {
		t.Fatalf("count %s: %v", label, err)
	}
	if count != want {
		t.Fatalf("%s count = %d, want %d", label, count, want)
	}
}
