package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/worker"
)

const (
	envIntegrationEnabled = "MERISTEM_INTEGRATION"
	envTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
)

func TestR3SeededBacklogSoakDispatchesScribeChildren(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok := createCmdSystemToken(t, ctx, pool, writer, "r3-soak-worker")
	if created, replayed, err := seedV1Items(ctx, pool, writer, systemTok); err != nil {
		t.Fatalf("seed v1 items: %v", err)
	} else if created != len(v1SubstrateItems) || replayed != 0 {
		t.Fatalf("seed v1 items created/replayed = %d/%d, want %d/0", created, replayed, len(v1SubstrateItems))
	}
	if _, _, err := seedRegistryFixtures(ctx, pool, writer, systemTok); err != nil {
		t.Fatalf("seed registry fixtures: %v", err)
	}
	if _, _, err := seedProjectionFixtures(ctx, pool, writer, systemTok); err != nil {
		t.Fatalf("seed projection fixtures: %v", err)
	}

	profile, err := safety.ProfileByName(safety.ProfileBringUp)
	if err != nil {
		t.Fatalf("bring-up profile: %v", err)
	}
	w, err := worker.New(pool, writer, worker.Budgets{ByState: profile.PatienceBudgets}, &systemTok.ID, func() time.Time {
		return time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.ScribeChildrenSpawned != len(v1SubstrateItems) {
		t.Fatalf("scribe children spawned = %d, want %d", result.ScribeChildrenSpawned, len(v1SubstrateItems))
	}
	if result.DispatchesRequested != len(v1SubstrateItems) {
		t.Fatalf("dispatches requested = %d, want %d", result.DispatchesRequested, len(v1SubstrateItems))
	}
	if result.BreachesEmitted != 0 || result.PatienceEscalationsRequested != 0 {
		t.Fatalf("bring-up soak should not breach fresh seed: breaches=%d escalations=%d", result.BreachesEmitted, result.PatienceEscalationsRequested)
	}

	seedIDs := seededSubstrateIDs()
	if missing := seededParentsWithoutForwardPath(t, ctx, pool, seedIDs); len(missing) > 0 {
		t.Fatalf("seeded non-terminal parents without scribe child or dispatch: %v", missing)
	}
	if missing := scribeChildrenWithoutDispatch(t, ctx, pool, seedIDs); len(missing) > 0 {
		t.Fatalf("scribe children without dispatch request: %v", missing)
	}
}

func seededSubstrateIDs() []uuid.UUID {
	out := make([]uuid.UUID, 0, len(v1SubstrateItems))
	for _, item := range v1SubstrateItems {
		out = append(out, seedSubjectID(item.Title))
	}
	return out
}

func seededParentsWithoutForwardPath(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ids []uuid.UUID) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT wi.id
		FROM work_items wi
		WHERE wi.id = ANY($1)
		  AND wi.state <> ALL($2::text[])
		  AND NOT EXISTS (
		      SELECT 1 FROM work_item_relations rel WHERE rel.parent_id = wi.id
		  )
		  AND NOT EXISTS (
		      SELECT 1 FROM events evt
		      WHERE evt.subject_kind = $3
		        AND evt.subject_id = wi.id
		        AND evt.kind = $4
		  )
		ORDER BY wi.id
	`, ids, terminalStateStrings(), domain.SubjectWorkItem, domain.EventDispatchRequested)
	if err != nil {
		t.Fatalf("query seeded parent forward paths: %v", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan missing parent path: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate missing parent paths: %v", err)
	}
	return out
}

func scribeChildrenWithoutDispatch(t *testing.T, ctx context.Context, pool *pgxpool.Pool, parentIDs []uuid.UUID) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT rel.child_id
		FROM work_item_relations rel
		JOIN work_items child ON child.id = rel.child_id
		WHERE rel.parent_id = ANY($1)
		  AND child.state <> ALL($2::text[])
		  AND NOT EXISTS (
		      SELECT 1 FROM events evt
		      WHERE evt.subject_kind = $3
		        AND evt.subject_id = rel.child_id
		        AND evt.kind = $4
		  )
		ORDER BY rel.child_id
	`, parentIDs, terminalStateStrings(), domain.SubjectWorkItem, domain.EventDispatchRequested)
	if err != nil {
		t.Fatalf("query scribe child dispatches: %v", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan missing child dispatch: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate missing child dispatches: %v", err)
	}
	return out
}

func terminalStateStrings() []string {
	return []string{
		string(domain.WorkItemDone),
		string(domain.WorkItemFailed),
		string(domain.WorkItemCanceled),
	}
}

func createCmdSystemToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, name string) domain.Token {
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
	system, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceSystem,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	return system.Token
}

func newCmdIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	baseURL := os.Getenv(envTestDatabaseURL)
	if baseURL == "" {
		if os.Getenv(envIntegrationEnabled) != "1" {
			t.Skipf("set %s=1 and %s (or %s) to run Postgres integration tests", envIntegrationEnabled, storage.EnvDatabaseURL, envTestDatabaseURL)
		}
		baseURL = os.Getenv(storage.EnvDatabaseURL)
	}
	if baseURL == "" {
		t.Skipf("%s is required for integration tests", storage.EnvDatabaseURL)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Fatalf("integration tests require postgres URL DSN, got scheme %q", parsed.Scheme)
	}

	dbName := "meristem_itest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminURL := *parsed
	adminURL.Path = "/postgres"
	testURL := *parsed
	testURL.Path = "/" + dbName

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, adminURL.String())
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteCmdIntegrationIdentifier(dbName)); err != nil {
		admin.Close()
		t.Fatalf("create temp database %s: %v", dbName, err)
	}
	admin.Close()

	pool, err := storage.Open(ctx, storage.Config{
		DatabaseURL:    testURL.String(),
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		dropCmdIntegrationDatabase(t, adminURL.String(), dbName)
		t.Fatalf("open temp database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropCmdIntegrationDatabase(t, adminURL.String(), dbName)
	})
	return pool
}

func dropCmdIntegrationDatabase(t *testing.T, adminDSN string, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Logf("open admin database for cleanup: %v", err)
		return
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteCmdIntegrationIdentifier(dbName)+` WITH (FORCE)`); err != nil {
		t.Logf("drop temp database %s: %v", dbName, err)
	}
}

func quoteCmdIntegrationIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
