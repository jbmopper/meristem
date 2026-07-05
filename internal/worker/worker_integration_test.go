package worker

import (
	"context"
	"encoding/json"
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
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	envIntegrationEnabled = "MERISTEM_INTEGRATION"
	envTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
)

// TestScanOnceEmitsBreachAndEscalates is the end-to-end pin: stand up a
// fresh DB, seed three work_items at controlled ages, run ScanOnce, and
// verify that breached state epochs are both recorded and mechanically routed
// to human escalation. The fixed clock + deterministic ids make every run
// reproducible.
func TestScanOnceEmitsBreachAndEscalates(t *testing.T) {
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

	beforeKinds := eventKindCounts(t, ctx, pool)
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
	if first.PatienceEscalationsRequested != wantBreaches {
		t.Errorf("first.PatienceEscalationsRequested = %d, want %d", first.PatienceEscalationsRequested, wantBreaches)
	}
	if first.PatienceEscalationsAlreadyRequested != 0 {
		t.Errorf("first.PatienceEscalationsAlreadyRequested = %d, want 0", first.PatienceEscalationsAlreadyRequested)
	}

	if got := countEventsByKind(t, ctx, pool, domain.EventPatienceBreached); got != wantBreaches {
		t.Errorf("events count for %s = %d after first pass, want %d", domain.EventPatienceBreached, got, wantBreaches)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != wantBreaches {
		t.Errorf("events count for %s = %d after first pass, want %d", domain.EventEscalationRequested, got, wantBreaches)
	}
	assertEventKindDeltaAllowed(t, beforeKinds, eventKindCounts(t, ctx, pool), map[string]bool{
		domain.EventPatienceBreached:        true,
		domain.EventEscalationRequested:     true,
		domain.EventWorkItemCreated:         true,
		domain.EventWorkItemRelationAdded:   true,
		domain.EventWorkItemMetadataUpdated: true,
		domain.EventWorkItemTransitioned:    true,
	})

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
		gotItem, err := workitems.NewService(pool, writer).Get(ctx, planted[i].id)
		if err != nil {
			t.Fatalf("get planted[%d]: %v", i, err)
		}
		wantState := s.state
		wantReview := domain.HumanReviewWavedThrough
		if s.expectHit {
			wantState = domain.WorkItemBlocked
			wantReview = domain.HumanReviewBlocked
		}
		if gotItem.State != wantState {
			t.Errorf("planted[%d] state = %s, want %s", i, gotItem.State, wantState)
		}
		if gotItem.HumanReviewStatus != wantReview {
			t.Errorf("planted[%d] human_review_status = %s, want %s", i, gotItem.HumanReviewStatus, wantReview)
		}
	}

	// Second pass: the breached rows have moved to blocked, so the same
	// captured/running budgets produce no additional breach or escalation rows.
	second, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.BreachesEmitted != 0 {
		t.Errorf("second.BreachesEmitted = %d, want 0 (idempotency)", second.BreachesEmitted)
	}
	if second.PatienceEscalationsRequested != 0 {
		t.Errorf("second.PatienceEscalationsRequested = %d, want 0", second.PatienceEscalationsRequested)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventPatienceBreached); got != wantBreaches {
		t.Errorf("events count for %s = %d after second pass, want %d (no new rows)", domain.EventPatienceBreached, got, wantBreaches)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != wantBreaches {
		t.Errorf("events count for %s = %d after second pass, want %d (no new rows)", domain.EventEscalationRequested, got, wantBreaches)
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

func TestScanOnceProgressEventDoesNotResetPatienceEpoch(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-progress-does-not-reset-clock")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title: "chatty but stale",
		State: domain.WorkItemCaptured,
		Actor: systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	stateEnteredAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	setWorkItemTimestamps(t, ctx, pool, item.ID, stateEnteredAt)
	if err := service.AppendEvent(ctx, item.ID, "agent.progress", map[string]any{"summary": "still working"}, systemTok.Token); err != nil {
		t.Fatalf("append progress event: %v", err)
	}
	gotEntered, gotUpdated := workItemTimestamps(t, ctx, pool, item.ID)
	if !gotEntered.UTC().Equal(stateEnteredAt) {
		t.Fatalf("state_entered_at changed after progress event: got %s want %s", gotEntered, stateEnteredAt)
	}
	if !gotUpdated.After(gotEntered) {
		t.Fatalf("updated_at did not move after progress event: updated_at=%s state_entered_at=%s", gotUpdated, gotEntered)
	}

	// Make activity recent relative to the fixed scan time. If the worker still
	// used updated_at as the patience clock, this item would not breach.
	scanNow := stateEnteredAt.Add(2 * time.Hour)
	recentActivity := scanNow.Add(-5 * time.Minute)
	if _, err := pool.Exec(ctx, `UPDATE work_items SET updated_at = $2 WHERE id = $1`, item.ID, recentActivity); err != nil {
		t.Fatalf("set recent activity timestamp: %v", err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
	}}, &systemTok.Token.ID, func() time.Time { return scanNow })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.BreachesEmitted != 1 {
		t.Fatalf("breaches emitted = %d, want 1", result.BreachesEmitted)
	}
	if result.PatienceEscalationsRequested != 1 {
		t.Fatalf("patience escalations requested = %d, want 1", result.PatienceEscalationsRequested)
	}
	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemBlocked {
		t.Fatalf("state = %s, want blocked", got.State)
	}
}

func TestScanOnceBlockedHumanReviewBreachesButDoesNotEscalate(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-human-review-fixed-point")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:             "waiting on human",
		State:             domain.WorkItemCaptured,
		HumanReviewStatus: domain.HumanReviewBlocked,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	setWorkItemTimestamps(t, ctx, pool, item.ID, now.Add(-2*time.Hour))
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
	}}, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.BreachesEmitted != 1 {
		t.Fatalf("breaches emitted = %d, want 1", result.BreachesEmitted)
	}
	if result.PatienceEscalationsSkippedAwaitingHuman != 1 {
		t.Fatalf("skipped awaiting human = %d, want 1", result.PatienceEscalationsSkippedAwaitingHuman)
	}
	if result.PatienceEscalationsRequested != 0 {
		t.Fatalf("patience escalations requested = %d, want 0", result.PatienceEscalationsRequested)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != 0 {
		t.Fatalf("escalation requests = %d, want 0", got)
	}
	if got := countRelationsForParent(t, ctx, pool, item.ID); got != 0 {
		t.Fatalf("relations for waiting item = %d, want 0", got)
	}
	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemCaptured {
		t.Fatalf("state = %s, want captured", got.State)
	}
	if got.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("human_review_status = %s, want blocked", got.HumanReviewStatus)
	}
}

func TestScanOnceUsesExplicitLaunchPatienceRule(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-explicit-patience-rule")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                 "explicit launch budget",
		State:                 domain.WorkItemCaptured,
		PatienceBudgetSeconds: 60,
		EscalationRule:        domain.EscalationRuleHandToHuman,
		Actor:                 systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	stateEnteredAt := now.Add(-2 * time.Minute)
	setWorkItemTimestamps(t, ctx, pool, item.ID, stateEnteredAt)
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: 24 * time.Hour,
	}}, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.BreachesEmitted != 1 {
		t.Fatalf("breaches emitted = %d, want 1", result.BreachesEmitted)
	}
	if result.PatienceEscalationsRequested != 1 {
		t.Fatalf("patience escalations requested = %d, want 1", result.PatienceEscalationsRequested)
	}
	payload := patiencePayloadForSubject(t, ctx, pool, item.ID)
	if payload.BudgetSeconds != 60 || payload.BudgetSource != budgetSourceItemMetadata {
		t.Fatalf("payload budget = %ds/%s, want 60s/%s", payload.BudgetSeconds, payload.BudgetSource, budgetSourceItemMetadata)
	}
	if payload.EscalationRule != string(domain.EscalationRuleHandToHuman) {
		t.Fatalf("escalation_rule = %q, want %q", payload.EscalationRule, domain.EscalationRuleHandToHuman)
	}
	if payload.StateEnteredAtUnix != stateEnteredAt.Unix() {
		t.Fatalf("state_entered_at_unix = %d, want %d", payload.StateEnteredAtUnix, stateEnteredAt.Unix())
	}
}

func TestScanOnceUsesCultivarXylemPatienceBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-cultivar-patience-rule")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedFastWorkerCultivar(t, ctx, pool, writer, systemTok.Token, 60)
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:    "cultivar launch budget",
		State:    domain.WorkItemCaptured,
		Cultivar: "fast-worker@1",
		Actor:    systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	now := time.Date(2026, 6, 7, 13, 0, 0, 0, time.UTC)
	setWorkItemTimestamps(t, ctx, pool, item.ID, now.Add(-2*time.Minute))
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: 24 * time.Hour,
	}}, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.BreachesEmitted != 1 {
		t.Fatalf("breaches emitted = %d, want 1", result.BreachesEmitted)
	}
	payload := patiencePayloadForSubject(t, ctx, pool, item.ID)
	if payload.BudgetSeconds != 60 || payload.BudgetSource != budgetSourceCultivar {
		t.Fatalf("payload budget = %ds/%s, want 60s/%s", payload.BudgetSeconds, payload.BudgetSource, budgetSourceCultivar)
	}
	if payload.Cultivar != "fast-worker@1" {
		t.Fatalf("cultivar = %q, want fast-worker@1", payload.Cultivar)
	}
	if payload.EscalationRule != string(domain.EscalationRuleHandToHuman) {
		t.Fatalf("escalation_rule = %q, want %q", payload.EscalationRule, domain.EscalationRuleHandToHuman)
	}
}

func TestScanOnceRecordsPolicyFallbackPatienceRule(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-policy-patience-rule")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title: "policy fallback budget",
		State: domain.WorkItemCaptured,
		Actor: systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	now := time.Date(2026, 6, 7, 14, 0, 0, 0, time.UTC)
	setWorkItemTimestamps(t, ctx, pool, item.ID, now.Add(-2*time.Minute))
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Minute,
	}}, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.BreachesEmitted != 1 {
		t.Fatalf("breaches emitted = %d, want 1", result.BreachesEmitted)
	}
	payload := patiencePayloadForSubject(t, ctx, pool, item.ID)
	if payload.BudgetSeconds != 60 || payload.BudgetSource != budgetSourcePolicy {
		t.Fatalf("payload budget = %ds/%s, want 60s/%s", payload.BudgetSeconds, payload.BudgetSource, budgetSourcePolicy)
	}
	if payload.EscalationRule != string(domain.EscalationRuleHandToHuman) {
		t.Fatalf("escalation_rule = %q, want %q", payload.EscalationRule, domain.EscalationRuleHandToHuman)
	}
}

func TestScanOnceSpawnsDeterministicScribeChildForChecklessItem(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-scribe-spawn")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedScribeCultivar(t, ctx, pool, writer, systemTok.Token)
	service := workitems.NewService(pool, writer)
	parent, err := service.Create(ctx, workitems.CreateInput{
		Title:             "needs convergence definition",
		State:             domain.WorkItemCaptured,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	if first.ScribeCandidatesScanned != 1 || first.ScribeChildrenSpawned != 1 {
		t.Fatalf("scribe first = candidates %d spawned %d already %d, want 1/1/0",
			first.ScribeCandidatesScanned, first.ScribeChildrenSpawned, first.ScribeChildrenAlreadyPresent)
	}
	childID := singleChildForParent(t, ctx, pool, parent.ID)
	if want := convergence.ScribeChildID(parent.ID); childID != want {
		t.Fatalf("scribe child id = %s, want deterministic %s", childID, want)
	}
	child, err := service.Get(ctx, childID)
	if err != nil {
		t.Fatalf("get child: %v", err)
	}
	if child.State != domain.WorkItemTriaged {
		t.Fatalf("child state = %s, want triaged", child.State)
	}
	if len(child.SuggestedConvergenceChecks) != 1 || child.SuggestedConvergenceChecks[0] != convergence.ScribeChildCheck {
		t.Fatalf("child checks = %v, want [%s]", child.SuggestedConvergenceChecks, convergence.ScribeChildCheck)
	}

	second, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.ScribeChildrenSpawned != 0 {
		t.Fatalf("second spawned = %d, want 0", second.ScribeChildrenSpawned)
	}
	if got := countRelationsForParent(t, ctx, pool, parent.ID); got != 1 {
		t.Fatalf("parent child count after repeat = %d, want 1", got)
	}
	if got := countRelationsForParent(t, ctx, pool, childID); got != 0 {
		t.Fatalf("scribe child should not recurse; child count = %d", got)
	}
}

func TestScanOnceRefusesMissingScribeCultivar(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-scribe-missing-cultivar")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	parent, err := service.Create(ctx, workitems.CreateInput{
		Title:             "needs convergence definition",
		State:             domain.WorkItemCaptured,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce must not abort on a missing scribe cultivar (skip-with-observation): %v", err)
	}
	if result.ScribePassSkippedMissingCultivar != 1 {
		t.Fatalf("scribe pass skip not recorded: %+v", result)
	}
	if result.ScribeCandidatesScanned != 1 || result.ScribeChildrenSpawned != 0 {
		t.Fatalf("scribe result = candidates %d spawned %d, want 1/0", result.ScribeCandidatesScanned, result.ScribeChildrenSpawned)
	}
	if got := countRelationsForParent(t, ctx, pool, parent.ID); got != 0 {
		t.Fatalf("missing cultivar should not spawn child; got %d relations", got)
	}
}

func TestScanDispatchRequestsEligibleItems(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-dispatch")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedChecklistWorkerCultivar(t, ctx, pool, writer, systemTok.Token)
	seedScribeCultivar(t, ctx, pool, writer, systemTok.Token)
	service := workitems.NewService(pool, writer)
	eligibleDefault, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "eligible default dispatch",
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create eligible default: %v", err)
	}
	explicitParent, err := service.Create(ctx, workitems.CreateInput{
		Title:             "explicit dispatch parent",
		State:             domain.WorkItemDone,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create explicit parent: %v", err)
	}
	eligibleExplicit, _, err := service.SpawnChildWithID(ctx, explicitParent.ID, uuid.New(), workitems.CreateInput{
		Title:                      "eligible explicit dispatch",
		State:                      domain.WorkItemPlanned,
		SuggestedConvergenceChecks: []string{"human-ack:owner accepts"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   "convergence-scribe@1",
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create eligible explicit: %v", err)
	}
	checkless, err := service.Create(ctx, workitems.CreateInput{
		Title:             "checkless skip",
		State:             domain.WorkItemTriaged,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create checkless: %v", err)
	}
	blocked, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "blocked skip",
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewBlocked,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create blocked: %v", err)
	}
	running, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "running skip",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create running: %v", err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first, err := w.scanDispatch(ctx)
	if err != nil {
		t.Fatalf("scanDispatch first: %v", err)
	}
	if first.DispatchCandidatesScanned != 2 || first.DispatchesRequested != 2 || first.DispatchesAlreadyRequested != 0 {
		t.Fatalf("first dispatch result = %+v, want scanned=2 requested=2 already=0", first)
	}
	second, err := w.scanDispatch(ctx)
	if err != nil {
		t.Fatalf("scanDispatch second: %v", err)
	}
	if second.DispatchCandidatesScanned != 2 || second.DispatchesRequested != 0 || second.DispatchesAlreadyRequested != 2 {
		t.Fatalf("second dispatch result = %+v, want scanned=2 requested=0 already=2", second)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventDispatchRequested); got != 2 {
		t.Fatalf("dispatch events = %d, want 2", got)
	}
	defaultPayload := dispatchPayloadForSubject(t, ctx, pool, eligibleDefault.ID)
	if defaultPayload.WorkItemID != eligibleDefault.ID || defaultPayload.Cultivar != "checklist-worker@1" || defaultPayload.State != string(domain.WorkItemTriaged) {
		t.Fatalf("default dispatch payload = %+v", defaultPayload)
	}
	explicitPayload := dispatchPayloadForSubject(t, ctx, pool, eligibleExplicit.ID)
	if explicitPayload.WorkItemID != eligibleExplicit.ID || explicitPayload.Cultivar != "convergence-scribe@1" || explicitPayload.State != string(domain.WorkItemPlanned) {
		t.Fatalf("explicit dispatch payload = %+v", explicitPayload)
	}
	for _, skipped := range []uuid.UUID{checkless.ID, blocked.ID, running.ID} {
		if got := countEventsForSubject(t, ctx, pool, skipped, domain.EventDispatchRequested); got != 0 {
			t.Fatalf("dispatch events for skipped %s = %d, want 0", skipped, got)
		}
	}
}

func TestScanOnceDoesNotSpawnScribeForHumanReviewBlockedItem(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-scribe-human-blocked")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:             "owner-owned checkless item",
		State:             domain.WorkItemTriaged,
		HumanReviewStatus: domain.HumanReviewBlocked,
		Actor:             systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 2; i++ {
		result, err := w.ScanOnce(ctx)
		if err != nil {
			t.Fatalf("ScanOnce %d: %v", i+1, err)
		}
		if result.ScribeChildrenSpawned != 0 {
			t.Fatalf("ScanOnce %d spawned = %d, want 0", i+1, result.ScribeChildrenSpawned)
		}
	}
	if got := countRelationsForParent(t, ctx, pool, item.ID); got != 0 {
		t.Fatalf("blocked item child count = %d, want 0", got)
	}
}

func TestScanOnceEscalationChildrenDoNotBreed(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-escalation-child-fixed-point")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "fertility check",
		State:                      domain.WorkItemCaptured,
		SuggestedConvergenceChecks: []string{"event:patience_probe"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	setWorkItemTimestamps(t, ctx, pool, item.ID, now.Add(-2*time.Hour))
	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemCaptured: time.Hour,
		domain.WorkItemBlocked:  time.Hour,
	}}
	firstWorker, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New first: %v", err)
	}
	first, err := firstWorker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	if first.PatienceEscalationsRequested != 1 {
		t.Fatalf("first patience escalations = %d, want 1", first.PatienceEscalationsRequested)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != 1 {
		t.Fatalf("escalation requests after first = %d, want 1", got)
	}
	childID := singleChildForParent(t, ctx, pool, item.ID)

	secondNow := now.Add(2 * time.Hour)
	setWorkItemTimestamps(t, ctx, pool, item.ID, secondNow.Add(-2*time.Hour))
	setWorkItemTimestamps(t, ctx, pool, childID, secondNow.Add(-2*time.Hour))
	secondWorker, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return secondNow })
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	second, err := secondWorker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.BreachesEmitted != 2 {
		t.Fatalf("second breaches emitted = %d, want 2", second.BreachesEmitted)
	}
	if second.PatienceEscalationsSkippedAwaitingHuman != 2 {
		t.Fatalf("second skipped awaiting human = %d, want 2", second.PatienceEscalationsSkippedAwaitingHuman)
	}
	if second.PatienceEscalationsRequested != 0 {
		t.Fatalf("second patience escalations = %d, want 0", second.PatienceEscalationsRequested)
	}

	thirdNow := now.Add(4 * time.Hour)
	thirdWorker, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return thirdNow })
	if err != nil {
		t.Fatalf("New third: %v", err)
	}
	third, err := thirdWorker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce third: %v", err)
	}
	if third.PatienceEscalationsRequested != 0 {
		t.Fatalf("third patience escalations = %d, want 0", third.PatienceEscalationsRequested)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != 1 {
		t.Fatalf("escalation requests after later scans = %d, want 1", got)
	}
	if got := countRelationsForParent(t, ctx, pool, item.ID); got != 1 {
		t.Fatalf("children of original = %d, want 1", got)
	}
	if got := countRelationsForParent(t, ctx, pool, childID); got != 0 {
		t.Fatalf("children of human attention item = %d, want 0", got)
	}
	if got := countHumanAttentionItems(t, ctx, pool); got != 1 {
		t.Fatalf("human attention items = %d, want 1", got)
	}
}

func TestScanOnceConvergenceAcceptsAndPersistsVerdict(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-convergence-accept")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "convergence accept",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"tests_green"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	if err := service.AppendEvent(ctx, item.ID, "checklist.item:tests_green", map[string]any{
		"pass": true,
		"raw":  "unit suite passed",
	}, systemTok.Token); err != nil {
		t.Fatalf("append checklist signal: %v", err)
	}

	w, err := New(pool, writer, DefaultBudgets(), &systemTok.Token.ID, func() time.Time {
		return time.Date(2026, 6, 6, 0, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	result, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}
	if result.ConvergenceCandidatesScanned != 1 {
		t.Fatalf("candidates = %d, want 1", result.ConvergenceCandidatesScanned)
	}
	if result.ConvergenceVerdictsRecorded != 1 {
		t.Fatalf("verdicts recorded = %d, want 1", result.ConvergenceVerdictsRecorded)
	}
	if result.ConvergenceAccepts != 1 {
		t.Fatalf("accepts = %d, want 1", result.ConvergenceAccepts)
	}
	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemDone {
		t.Fatalf("state = %q, want done", got.State)
	}
	if countVerdictsForWorkItem(t, ctx, pool, item.ID) != 1 {
		t.Fatal("expected one convergence_verdicts projection row")
	}
}

func TestScanOnceConvergenceSkipsUnchangedRejectedInputs(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-convergence-stale")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "convergence stale reject",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"missing_check"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	w, err := New(pool, writer, DefaultBudgets(), &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	if first.ConvergenceVerdictsRecorded != 1 || first.ConvergenceRetries != 1 {
		t.Fatalf("first result = %+v, want one recorded retry", first)
	}

	second, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.ConvergenceVerdictsRecorded != 0 {
		t.Fatalf("second verdicts recorded = %d, want 0", second.ConvergenceVerdictsRecorded)
	}
	if second.ConvergenceStaleInputsSkipped != 1 {
		t.Fatalf("stale skips = %d, want 1", second.ConvergenceStaleInputsSkipped)
	}
	if countVerdictsForWorkItem(t, ctx, pool, item.ID) != 1 {
		t.Fatal("unchanged rejected inputs should not create a second verdict row")
	}
	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemRunning {
		t.Fatalf("state = %q, want running", got.State)
	}
}

func TestScanOnceEscalatesStaleRejectedInputsAfterPatienceBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-convergence-stale-budget")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "convergence stale reject over patience",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"missing_check"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	stateEnteredAt := time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC)
	setWorkItemTimestamps(t, ctx, pool, item.ID, stateEnteredAt)
	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{
		domain.WorkItemRunning: time.Hour,
	}}

	underBudgetWorker, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time {
		return stateEnteredAt.Add(30 * time.Minute)
	})
	if err != nil {
		t.Fatalf("New under budget: %v", err)
	}
	first, err := underBudgetWorker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce first: %v", err)
	}
	if first.ConvergenceVerdictsRecorded != 1 || first.ConvergenceRetries != 1 {
		t.Fatalf("first result = %+v, want one rejected verdict and retry", first)
	}
	if first.BreachesEmitted != 0 || first.PatienceEscalationsRequested != 0 {
		t.Fatalf("under-budget pass should not breach/escalate, got %+v", first)
	}

	overBudgetWorker, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time {
		return stateEnteredAt.Add(2 * time.Hour)
	})
	if err != nil {
		t.Fatalf("New over budget: %v", err)
	}
	second, err := overBudgetWorker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce second: %v", err)
	}
	if second.ConvergenceStaleInputsSkipped != 1 {
		t.Fatalf("stale skips = %d, want 1", second.ConvergenceStaleInputsSkipped)
	}
	if second.ConvergenceVerdictsRecorded != 0 {
		t.Fatalf("second verdicts recorded = %d, want 0", second.ConvergenceVerdictsRecorded)
	}
	if second.BreachesEmitted != 1 {
		t.Fatalf("second breaches emitted = %d, want 1", second.BreachesEmitted)
	}
	if second.PatienceEscalationsRequested != 1 {
		t.Fatalf("second patience escalations = %d, want 1", second.PatienceEscalationsRequested)
	}
	if countVerdictsForWorkItem(t, ctx, pool, item.ID) != 1 {
		t.Fatal("unchanged rejected inputs should not create a duplicate verdict row")
	}
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventPatienceBreached); got != 1 {
		t.Fatalf("patience breaches for item = %d, want 1", got)
	}
	if got := countEventsByKind(t, ctx, pool, domain.EventEscalationRequested); got != 1 {
		t.Fatalf("escalation requests = %d, want 1", got)
	}
	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemBlocked {
		t.Fatalf("state = %q, want blocked", got.State)
	}
	if got.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("human review status = %q, want blocked", got.HumanReviewStatus)
	}
}

func TestScanOnceConvergenceBlocksAfterFreshFailedAttempts(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "worker-convergence-exhaust")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "convergence exhausted reject",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"tests_green"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	w, err := New(pool, writer, DefaultBudgets(), &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for attempt := 1; attempt <= defaultConvergenceMaxAttempts; attempt++ {
		if err := service.AppendEvent(ctx, item.ID, "checklist.item:tests_green", map[string]any{
			"pass": false,
			"raw":  "attempt " + string(rune('0'+attempt)),
		}, systemTok.Token); err != nil {
			t.Fatalf("append failing signal %d: %v", attempt, err)
		}
		result, err := w.ScanOnce(ctx)
		if err != nil {
			t.Fatalf("ScanOnce attempt %d: %v", attempt, err)
		}
		if result.ConvergenceVerdictsRecorded != 1 {
			t.Fatalf("attempt %d verdicts recorded = %d, want 1", attempt, result.ConvergenceVerdictsRecorded)
		}
	}

	got, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.State != domain.WorkItemBlocked {
		t.Fatalf("state = %q, want blocked", got.State)
	}
	if got.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("human review status = %q, want blocked", got.HumanReviewStatus)
	}
	if countVerdictsForWorkItem(t, ctx, pool, item.ID) != defaultConvergenceMaxAttempts {
		t.Fatalf("expected %d verdict rows", defaultConvergenceMaxAttempts)
	}
}

// seedWorkItemAt appends one work_item.created event with a deterministic
// id and then back-dates the projected lifecycle timestamps so the dwell time
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
	if _, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2, created_at = $2, state_entered_at = $2 WHERE id = $1`, id, enteredAt); err != nil {
		t.Fatalf("seed back-date: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}

// transitionWorkItem appends a work_item.transitioned event and back-dates
// the projected state_entered_at/updated_at to enteredAt. Mirrors
// seedWorkItemAt but uses the transition projector.
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
	if _, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2, state_entered_at = $2 WHERE id = $1`, id, enteredAt); err != nil {
		t.Fatalf("transition back-date: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("transition commit: %v", err)
	}
}

func setWorkItemTimestamps(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, ts time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE work_items SET created_at = $2, updated_at = $2, state_entered_at = $2 WHERE id = $1`, id, ts); err != nil {
		t.Fatalf("set work_item timestamps: %v", err)
	}
}

func workItemTimestamps(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (stateEnteredAt, updatedAt time.Time) {
	t.Helper()
	if err := pool.QueryRow(ctx, `SELECT state_entered_at, updated_at FROM work_items WHERE id = $1`, id).Scan(&stateEnteredAt, &updatedAt); err != nil {
		t.Fatalf("read work_item timestamps: %v", err)
	}
	return stateEnteredAt, updatedAt
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

func eventKindCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT kind, COUNT(*) FROM events GROUP BY kind`)
	if err != nil {
		t.Fatalf("event kind counts: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			t.Fatalf("scan event kind counts: %v", err)
		}
		out[kind] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event kind counts: %v", err)
	}
	return out
}

func assertEventKindDeltaAllowed(t *testing.T, before, after map[string]int, allowed map[string]bool) {
	t.Helper()
	for kind, afterCount := range after {
		if afterCount <= before[kind] {
			continue
		}
		if !allowed[kind] {
			t.Fatalf("unexpected event kind emitted by worker: %s delta=%d", kind, afterCount-before[kind])
		}
	}
}

func countVerdictsForWorkItem(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM convergence_verdicts WHERE work_item_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count convergence_verdicts for %s: %v", id, err)
	}
	return n
}

func countRelationsForParent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, parentID uuid.UUID) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM work_item_relations WHERE parent_id = $1`, parentID).Scan(&n); err != nil {
		t.Fatalf("count relations for parent %s: %v", parentID, err)
	}
	return n
}

func singleChildForParent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, parentID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT child_id FROM work_item_relations WHERE parent_id = $1`, parentID).Scan(&id); err != nil {
		t.Fatalf("single child for parent %s: %v", parentID, err)
	}
	return id
}

func countHumanAttentionItems(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM work_items WHERE title LIKE 'Human attention:%'`).Scan(&n); err != nil {
		t.Fatalf("count human attention items: %v", err)
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

func seedScribeCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "checklist-all",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "all checklist items pass",
	})
	if err != nil {
		t.Fatalf("define scribe tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "convergence-scribe",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/convergence-scribe.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: 1800, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "scribe rootstock",
	})
	if err != nil {
		t.Fatalf("define scribe cultivar: %v", err)
	}
}

func seedChecklistWorkerCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "checklist-all",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "all checklist items pass",
	})
	if err != nil {
		t.Fatalf("define checklist tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "checklist-worker",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/checklist-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: 3600, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "checklist worker rootstock",
	})
	if err != nil {
		t.Fatalf("define checklist worker cultivar: %v", err)
	}
}

func seedFastWorkerCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, maxWallSeconds int) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "fast-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "fast checklist test tropism",
	})
	if err != nil {
		t.Fatalf("define fast tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "fast-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "fast-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/fast-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: maxWallSeconds, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "fast worker test cultivar",
	})
	if err != nil {
		t.Fatalf("define fast cultivar: %v", err)
	}
}

type patiencePayload struct {
	State              string `json:"state"`
	BudgetSeconds      int64  `json:"budget_seconds"`
	BudgetSource       string `json:"budget_source"`
	EscalationRule     string `json:"escalation_rule"`
	StateEnteredAtUnix int64  `json:"state_entered_at_unix"`
	Cultivar           string `json:"cultivar"`
}

func patiencePayloadForSubject(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) patiencePayload {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectWorkItem, id, domain.EventPatienceBreached).Scan(&raw); err != nil {
		t.Fatalf("patience payload for %s: %v", id, err)
	}
	var payload patiencePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode patience payload: %v", err)
	}
	return payload
}

type dispatchPayload struct {
	WorkItemID           uuid.UUID `json:"work_item_id"`
	State                string    `json:"state"`
	StateEnteredAtUnix   int64     `json:"state_entered_at_unix"`
	Cultivar             string    `json:"cultivar"`
	Reason               string    `json:"reason"`
	SourceReconcilerPass string    `json:"source_reconciler_pass"`
}

func dispatchPayloadForSubject(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) dispatchPayload {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectWorkItem, id, domain.EventDispatchRequested).Scan(&raw); err != nil {
		t.Fatalf("dispatch payload for %s: %v", id, err)
	}
	var payload dispatchPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode dispatch payload: %v", err)
	}
	return payload
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
