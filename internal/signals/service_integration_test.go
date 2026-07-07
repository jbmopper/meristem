package signals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestReceiveConcurrentSemanticDedupeCreatesOneLiveWorkItem(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "signals_dedupe")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)

	root, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "signals-dedupe-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	svc := NewService(pool, writer)

	const requests = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]ReceiveResult, requests)
	errs := make([]error, requests)
	for i := 0; i < requests; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.Receive(ctx, root.Token, validInput())
		}()
	}
	close(start)
	wg.Wait()

	var workItemID uuid.UUID
	createdCount := 0
	for i := 0; i < requests; i++ {
		if errs[i] != nil {
			t.Fatalf("receive %d: %v", i, errs[i])
		}
		if results[i].WorkItemID == uuid.Nil {
			t.Fatalf("receive %d returned nil work item", i)
		}
		if workItemID == uuid.Nil {
			workItemID = results[i].WorkItemID
		}
		if results[i].WorkItemID != workItemID {
			t.Fatalf("receive %d linked to work item %s, want %s", i, results[i].WorkItemID, workItemID)
		}
		if results[i].CreatedWorkItem {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent dedupe should create exactly one work item, got %d", createdCount)
	}
	assertSignalEventCount(t, pool, domain.EventSignalReceived, requests)
	assertSignalEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertSignalTableCount(t, pool, "signals", requests)
	assertSignalTableCount(t, pool, "work_items", 1)
}

// budgetTestStack migrates a fresh database, registers every projector the
// admission-budget path touches, and returns the wired signals Service.
func budgetTestStack(t *testing.T, dbName string) (*pgxpool.Pool, *events.Writer, *Service) {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, dbName)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	policyprofile.RegisterProjectors(reg)
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)
	return pool, writer, NewService(pool, writer)
}

// budgetHumanReporter mints a non-root human token (via a freshly minted root
// actor, per spec principle 7) that both switches the profile and reports
// signals. The admission budget meters per source token.
func budgetHumanReporter(t *testing.T, pool *pgxpool.Pool, writer *events.Writer, name string) domain.Token {
	t.Helper()
	ctx := context.Background()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name + "-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	reporter, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceHuman,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create reporter token: %v", err)
	}
	return reporter.Token
}

// budgetSignalInput is a valid work_spec signal with a caller-chosen dedupe key
// and title, so each distinct key drives a fresh work_item creation.
func budgetSignalInput(dedupeKey, title string) ReceiveInput {
	return ReceiveInput{
		SignalKind: "repairable_failure",
		DedupeKey:  dedupeKey,
		WorkSpec: json.RawMessage(fmt.Sprintf(`{
			"schema_version": "meristem.work_spec.v1",
			"kind": "repair",
			"title": %q,
			"priority": "P1",
			"objective": "Retry transient failures per configuration.",
			"acceptance_criteria": ["Transient failures retried."]
		}`, title)),
	}
}

func TestReceiveMetersNewItemCreationPerToken(t *testing.T) {
	ctx := context.Background()
	pool, writer, svc := budgetTestStack(t, "signals_budget_meter")

	// Switch to bring-up so the tighter budget (10) governs; this also proves
	// the enforcement resolves the active profile rather than a hard-coded
	// default.
	human := budgetHumanReporter(t, pool, writer, "signals-budget-reporter")
	if _, _, err := policyprofile.NewService(pool, writer).Switch(ctx, policyprofile.SwitchInput{To: safety.ProfileBringUp, Actor: human}); err != nil {
		t.Fatalf("switch to bring-up: %v", err)
	}
	budget := safety.Profiles()[safety.ProfileBringUp].MaxSignalItemsPerTokenPerHour

	// Under budget: every distinct dedupe key creates its own work_item.
	for i := 0; i < budget; i++ {
		res, err := svc.Receive(ctx, human, budgetSignalInput(fmt.Sprintf("repo:jay:repair:item-%d", i), fmt.Sprintf("Item %d", i)))
		if err != nil {
			t.Fatalf("under-budget receive %d: %v", i, err)
		}
		if !res.CreatedWorkItem {
			t.Fatalf("under-budget receive %d should create a work_item", i)
		}
	}
	assertSignalEventCount(t, pool, domain.EventXylemExhausted, 0)
	assertSignalEventCount(t, pool, domain.EventEscalationRequested, 0)

	// Over budget: creation refused, structured error, signal still recorded.
	over := budgetSignalInput("repo:jay:repair:over-1", "Over budget one")
	res, err := svc.Receive(ctx, human, over)
	if !errors.Is(err, ErrSignalItemBudgetExhausted) {
		t.Fatalf("expected ErrSignalItemBudgetExhausted, got %v", err)
	}
	if res.CreatedWorkItem || res.WorkItemID != uuid.Nil {
		t.Fatalf("refused signal must not create/link a work_item: created=%v id=%s", res.CreatedWorkItem, res.WorkItemID)
	}
	if res.SignalID == uuid.Nil {
		t.Fatalf("refused signal must still carry a recorded signal id")
	}
	assertRefusedSignalRow(t, pool, res.SignalID)
	assertSignalEventCount(t, pool, domain.EventEscalationRequested, 1)
	assertSignalEventCount(t, pool, domain.EventXylemExhausted, 1)
	// budget reporter items + exactly one escalation human-attention item.
	assertSignalTableCount(t, pool, "work_items", budget+1)

	// A second over-budget signal from the same token in the same window keeps
	// refusing and records its own audit + exhaustion, but the escalation is
	// idempotent: still exactly one.
	res2, err := svc.Receive(ctx, human, budgetSignalInput("repo:jay:repair:over-2", "Over budget two"))
	if !errors.Is(err, ErrSignalItemBudgetExhausted) {
		t.Fatalf("expected second refusal, got %v", err)
	}
	assertRefusedSignalRow(t, pool, res2.SignalID)
	assertSignalEventCount(t, pool, domain.EventEscalationRequested, 1)
	assertSignalEventCount(t, pool, domain.EventXylemExhausted, 2)
	assertSignalTableCount(t, pool, "work_items", budget+1)
}

func TestReceiveDoesNotMeterDedupeLinkedSignals(t *testing.T) {
	ctx := context.Background()
	pool, writer, svc := budgetTestStack(t, "signals_budget_dedupe_unmetered")

	human := budgetHumanReporter(t, pool, writer, "signals-budget-linker")
	if _, _, err := policyprofile.NewService(pool, writer).Switch(ctx, policyprofile.SwitchInput{To: safety.ProfileBringUp, Actor: human}); err != nil {
		t.Fatalf("switch to bring-up: %v", err)
	}
	budget := safety.Profiles()[safety.ProfileBringUp].MaxSignalItemsPerTokenPerHour

	const dedupe = "repo:jay:repair:sticky"
	first, err := svc.Receive(ctx, human, budgetSignalInput(dedupe, "Sticky"))
	if err != nil || !first.CreatedWorkItem {
		t.Fatalf("first sticky signal should create a work_item: created=%v err=%v", first.CreatedWorkItem, err)
	}

	// Far past the budget, but every one dedupe-links to the single live item —
	// so none is metered and none is refused.
	total := budget*3 + 5
	for i := 0; i < total; i++ {
		res, err := svc.Receive(ctx, human, budgetSignalInput(dedupe, "Sticky"))
		if err != nil {
			t.Fatalf("dedupe-linked receive %d refused unexpectedly: %v", i, err)
		}
		if res.CreatedWorkItem {
			t.Fatalf("dedupe-linked receive %d should link, not create", i)
		}
		if res.WorkItemID != first.WorkItemID {
			t.Fatalf("dedupe-linked receive %d attached to %s, want %s", i, res.WorkItemID, first.WorkItemID)
		}
	}
	assertSignalEventCount(t, pool, domain.EventXylemExhausted, 0)
	assertSignalEventCount(t, pool, domain.EventEscalationRequested, 0)
	assertSignalTableCount(t, pool, "work_items", 1)
	assertSignalTableCount(t, pool, "signals", total+1)
}

func assertRefusedSignalRow(t *testing.T, pool *pgxpool.Pool, signalID uuid.UUID) {
	t.Helper()
	var createdWorkItem bool
	var workItemID *uuid.UUID
	err := pool.QueryRow(context.Background(),
		`SELECT created_work_item, work_item_id FROM signals WHERE id = $1`, signalID).
		Scan(&createdWorkItem, &workItemID)
	if err != nil {
		t.Fatalf("refused signal row not recorded for audit: %v", err)
	}
	if createdWorkItem {
		t.Fatalf("refused signal row must have created_work_item = false")
	}
	if workItemID != nil {
		t.Fatalf("refused signal row must have NULL work_item_id, got %s", *workItemID)
	}
}

func assertSignalEventCount(t *testing.T, pool *pgxpool.Pool, kind string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&got); err != nil {
		t.Fatalf("count events %s: %v", kind, err)
	}
	if got != want {
		t.Fatalf("events %s: want %d, got %d", kind, want, got)
	}
}

func assertSignalTableCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+quoteSignalIdentifier(table)).Scan(&got); err != nil {
		t.Fatalf("count table %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("table %s: want %d, got %d", table, want, got)
	}
}

func quoteSignalIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
