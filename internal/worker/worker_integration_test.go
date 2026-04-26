package worker

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
)

const (
	envIntegrationEnabled = "MERISTEM_INTEGRATION"
	envTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
)

// TestScanOnceEmitsBreachAndIsIdempotent is the end-to-end pin: stand up a
// fresh DB, seed three work_items at controlled ages, run ScanOnce twice,
// and verify (1) the breaches we expect appear in the events table after
// the first pass, (2) the second pass is a clean no-op on the wire (no
// new event rows) and reports the same observations as already-recorded.
//
// This is the test that catches drift between the pure breach logic and
// the SQL/event-emit composition. The fixed clock + deterministic ids
// make every run reproducible.
func TestScanOnceEmitsBreachAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-integration")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}

	// Anchor "now" at a fixed instant so dwell times are exact and the
	// emitted event_id is the same on every CI run.
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	type seed struct {
		state     domain.WorkItemState
		dwell     time.Duration
		expectHit bool
	}
	seeds := []seed{
		// 30m dwell against a 1h budget: under, no breach.
		{state: domain.WorkItemCaptured, dwell: 30 * time.Minute, expectHit: false},
		// 90m dwell against a 1h budget: over, breach.
		{state: domain.WorkItemCaptured, dwell: 90 * time.Minute, expectHit: true},
		// 5h dwell against a 4h budget: over, breach.
		{state: domain.WorkItemRunning, dwell: 5 * time.Hour, expectHit: true},
	}

	type seeded struct {
		id    uuid.UUID
		state domain.WorkItemState
		dwell time.Duration
	}
	planted := make([]seeded, len(seeds))
	for i, s := range seeds {
		id := uuid.New()
		planted[i] = seeded{id: id, state: s.state, dwell: s.dwell}
		seedWorkItemAt(t, ctx, pool, writer, systemTok.Token, id, s.state, now.Add(-s.dwell))
	}

	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
		domain.WorkItemRunning:  4 * time.Hour,
	}}
	w, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	wantBreaches := 0
	for _, s := range seeds {
		if s.expectHit {
			wantBreaches++
		}
	}
	if first.Scanned != len(seeds) {
		t.Errorf("first.Scanned = %d, want %d", first.Scanned, len(seeds))
	}
	if first.BreachesEmitted != wantBreaches {
		t.Errorf("first.BreachesEmitted = %d, want %d", first.BreachesEmitted, wantBreaches)
	}
	if first.BreachesAlreadyRecorded != 0 {
		t.Errorf("first.BreachesAlreadyRecorded = %d, want 0", first.BreachesAlreadyRecorded)
	}

	if got := countEventsByKind(t, ctx, pool, domain.EventPatienceBreached); got != wantBreaches {
		t.Errorf("events count for %s = %d after first pass, want %d", domain.EventPatienceBreached, got, wantBreaches)
	}

	// Per-row check: the under-budget seed must not have a breach event.
	for i, s := range seeds {
		got := countEventsForSubject(t, ctx, pool, planted[i].id, domain.EventPatienceBreached)
		want := 0
		if s.expectHit {
			want = 1
		}
		if got != want {
			t.Errorf("planted[%d] (state=%s, dwell=%v): events for subject = %d, want %d",
				i, s.state, s.dwell, got, want)
		}
	}

	// Second pass: same clock, same data; nothing new should land.
	second, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.Scanned != len(seeds) {
		t.Errorf("second.Scanned = %d, want %d", second.Scanned, len(seeds))
	}
	if second.BreachesEmitted != 0 {
		t.Errorf("second.BreachesEmitted = %d, want 0 (idempotency)", second.BreachesEmitted)
	}
	if second.BreachesAlreadyRecorded != wantBreaches {
		t.Errorf("second.BreachesAlreadyRecorded = %d, want %d", second.BreachesAlreadyRecorded, wantBreaches)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventPatienceBreached); got != wantBreaches {
		t.Errorf("events count for %s = %d after second pass, want %d (no new rows)", domain.EventPatienceBreached, got, wantBreaches)
	}
}

// TestScanOnceReBreachesAfterStateRotation pins the "epoch" property: when
// a work_item leaves a breached state and re-enters it, the next breach is
// recorded as a distinct event, not collapsed against the prior one. This
// is what the state_entered_at_unix payload field exists for.
func TestScanOnceReBreachesAfterStateRotation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-rotation")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}

	id := uuid.New()
	t0 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	// Phase 1: planted in captured at t0-2h. Worker scans at t0 with a 1h
	// budget, observes one breach.
	seedWorkItemAt(t, ctx, pool, writer, systemTok.Token, id, domain.WorkItemCaptured, t0.Add(-2*time.Hour))
	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
	}}
	w, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return t0 })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := w.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce phase 1: %v", err)
	}
	if got := countEventsForSubject(t, ctx, pool, id, domain.EventPatienceBreached); got != 1 {
		t.Fatalf("phase 1: want 1 breach for subject, got %d", got)
	}

	// Phase 2: transition out (captured -> triaged) and back in (triaged ->
	// captured), each with a fresh updated_at. The "captured" re-entry is
	// at t0+1h (so the work_item's updated_at is later than the original
	// observation's state_entered_at).
	transitionWorkItem(t, ctx, pool, writer, systemTok.Token, id, domain.WorkItemTriaged, t0.Add(30*time.Minute))
	transitionWorkItem(t, ctx, pool, writer, systemTok.Token, id, domain.WorkItemCaptured, t0.Add(time.Hour))

	// Phase 3: scan at t0+3h. Captured for 2h again, over the 1h budget.
	wRot, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return t0.Add(3 * time.Hour) })
	if err != nil {
		t.Fatalf("New phase 3: %v", err)
	}
	r, err := wRot.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce phase 3: %v", err)
	}
	if r.BreachesEmitted != 1 {
		t.Errorf("phase 3: expected one fresh breach after rotation, got BreachesEmitted=%d already=%d",
			r.BreachesEmitted, r.BreachesAlreadyRecorded)
	}
	if got := countEventsForSubject(t, ctx, pool, id, domain.EventPatienceBreached); got != 2 {
		t.Errorf("phase 3: want 2 distinct breach events for subject, got %d", got)
	}
}

// seedWorkItemAt appends one work_item.created event with a deterministic
// id and then back-dates the projected updated_at column so the dwell time
// the worker sees is exactly what the test wants. Direct UPDATE of the
// projection is fine in tests; production code must never do this.
func seedWorkItemAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, id uuid.UUID, state domain.WorkItemState, enteredAt time.Time) {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx for seed: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    id,
		Kind:         domain.EventWorkItemCreated,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title": "worker-integration seed " + id.String(),
			"body":  "synthesized by worker integration test",
			"state": string(state),
		},
	})
	if err != nil {
		t.Fatalf("seed append: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2, created_at = $2 WHERE id = $1`, id, enteredAt); err != nil {
		t.Fatalf("seed back-date: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}

// transitionWorkItem appends a work_item.transitioned event and back-dates
// the projected updated_at to enteredAt. Mirrors seedWorkItemAt but uses
// the transition projector.
func transitionWorkItem(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, id uuid.UUID, to domain.WorkItemState, enteredAt time.Time) {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx for transition: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    id,
		Kind:         domain.EventWorkItemTransitioned,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"to":     string(to),
			"reason": "rotation test",
		},
	})
	if err != nil {
		t.Fatalf("transition append: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2 WHERE id = $1`, id, enteredAt); err != nil {
		t.Fatalf("transition back-date: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("transition commit: %v", err)
	}
}

func countEventsByKind(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE kind = $1`, kind).Scan(&n); err != nil {
		t.Fatalf("count events kind=%s: %v", kind, err)
	}
	return n
}

func countEventsForSubject(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, kind string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE subject_id = $1 AND kind = $2`, id, kind).Scan(&n); err != nil {
		t.Fatalf("count events subject=%s kind=%s: %v", id, kind, err)
	}
	return n
}

func createSystemToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, name string) (auth.CreateTokenResult, error) {
	t.Helper()

	service := auth.NewService(pool, writer)
	root, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		return auth.CreateTokenResult{}, err
	}
	return service.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceSystem,
		Actor:  &root.Token,
	})
}

// newIntegrationPool mirrors the helper in internal/api/signals_integration_test.go;
// duplicated rather than promoted to a shared package so the test layout
// stays self-contained per package. If a third caller needs it, factor.
func newIntegrationPool(t *testing.T) *pgxpool.Pool {
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
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdentifier(dbName)); err != nil {
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
		dropIntegrationDatabase(t, adminURL.String(), dbName)
		t.Fatalf("open temp database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropIntegrationDatabase(t, adminURL.String(), dbName)
	})
	return pool
}

func dropIntegrationDatabase(t *testing.T, adminDSN string, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Logf("open admin database for cleanup: %v", err)
		return
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdentifier(dbName)+` WITH (FORCE)`); err != nil {
		t.Logf("drop temp database %s: %v", dbName, err)
	}
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
