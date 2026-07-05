package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/worker"
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

func TestR8BootstrapToFirstWorkerDispatch(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	root, seedTok, operator := createR8BootstrapTokens(t, ctx, pool, writer)
	if !root.IsRoot || root.Source != domain.SourceHuman {
		t.Fatalf("root token shape = root:%t source:%s, want root human", root.IsRoot, root.Source)
	}
	if seedTok.IsRoot || seedTok.Source != domain.SourceSystem {
		t.Fatalf("seed token shape = root:%t source:%s, want non-root system", seedTok.IsRoot, seedTok.Source)
	}
	if operator.IsRoot || operator.Source != domain.SourceHuman {
		t.Fatalf("operator token shape = root:%t source:%s, want non-root human", operator.IsRoot, operator.Source)
	}
	if !access.ToolVisible(operator, "policy_profile.switch") {
		t.Fatal("operator token should be able to switch policy profiles")
	}
	if access.ToolVisible(root, "policy_profile.switch") {
		t.Fatal("root token must not be able to switch policy profiles")
	}
	if access.ToolVisible(seedTok, "policy_profile.switch") {
		t.Fatal("system seed token must not be able to switch policy profiles")
	}
	if got := countCmdEventsForSubjectActor(t, ctx, pool, domain.EventTokenCreated, operator.ID, root.ID); got != 1 {
		t.Fatalf("operator token grant event count = %d, want 1 root-attributed grant", got)
	}

	runSeedV1FixturesForTest(t, ctx, pool, writer, seedTok)
	if got := countSeededWorkItemCreateEventsByActor(t, ctx, pool, seedTok.ID); got != len(v1SubstrateItems) {
		t.Fatalf("system-attributed seeded work_item.created events = %d, want %d", got, len(v1SubstrateItems))
	}
	if got := countSeededWorkItemCreateEventsByActor(t, ctx, pool, root.ID); got != 0 {
		t.Fatalf("root-attributed seeded work_item.created events = %d, want 0", got)
	}

	profileSvc := policyprofile.NewService(pool, writer)
	active, switched, err := profileSvc.Switch(ctx, policyprofile.SwitchInput{
		To:    safety.ProfileBringUp,
		Actor: operator,
	})
	if err != nil {
		t.Fatalf("switch bring-up: %v", err)
	}
	if !switched {
		t.Fatal("switch bring-up should append the first profile event")
	}
	if active.Name != safety.ProfileBringUp {
		t.Fatalf("active profile = %s, want %s", active.Name, safety.ProfileBringUp)
	}
	if got := countCmdEventsForSubjectActor(t, ctx, pool, domain.EventPolicyProfileSwitched, policyprofile.SubjectID, operator.ID); got != 1 {
		t.Fatalf("operator-attributed policy_profile.switched events = %d, want 1", got)
	}
	resolved, err := profileSvc.Active(ctx)
	if err != nil {
		t.Fatalf("resolve active profile: %v", err)
	}
	if resolved.Name != safety.ProfileBringUp {
		t.Fatalf("resolved active profile = %s, want %s", resolved.Name, safety.ProfileBringUp)
	}

	w, err := worker.New(pool, writer, worker.Budgets{ByState: resolved.Policy.PatienceBudgets}, &seedTok.ID, nil)
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("worker.ScanOnce: %v", err)
	}
	if result.ScribeChildrenSpawned != len(v1SubstrateItems) {
		t.Fatalf("scribe children spawned = %d, want %d", result.ScribeChildrenSpawned, len(v1SubstrateItems))
	}
	if result.DispatchesRequested != len(v1SubstrateItems) {
		t.Fatalf("dispatches requested = %d, want %d", result.DispatchesRequested, len(v1SubstrateItems))
	}
	if got := countCmdEventsByKind(t, ctx, pool, domain.EventDispatchRequested); got != len(v1SubstrateItems) {
		t.Fatalf("dispatch.requested events = %d, want %d", got, len(v1SubstrateItems))
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

func createR8BootstrapTokens(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer) (domain.Token, domain.Token, domain.Token) {
	t.Helper()
	service := auth.NewService(pool, writer)
	root, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	seed, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "seed",
		Source: domain.SourceSystem,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create seed token: %v", err)
	}
	operator, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "operator",
		Source: domain.SourceHuman,
		Scopes: []string{access.ScopePolicyProfileSwitch},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create operator token: %v", err)
	}
	return root.Token, seed.Token, operator.Token
}

func countCmdEventsForSubjectActor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string, subjectID any, actorID any) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events
		WHERE kind = $1
		  AND subject_id = $2
		  AND actor_token_id = $3
	`, kind, subjectID, actorID).Scan(&count); err != nil {
		t.Fatalf("count %s for subject/actor: %v", kind, err)
	}
	return count
}

func countSeededWorkItemCreateEventsByActor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, actorID any) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events
		WHERE kind = $1
		  AND subject_kind = $2
		  AND subject_id = ANY($3)
		  AND actor_token_id = $4
	`, domain.EventWorkItemCreated, domain.SubjectWorkItem, seededSubstrateIDs(), actorID).Scan(&count); err != nil {
		t.Fatalf("count seeded work_item.created events by actor: %v", err)
	}
	return count
}

func countCmdEventsByKind(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&count); err != nil {
		t.Fatalf("count events by kind %s: %v", kind, err)
	}
	return count
}
