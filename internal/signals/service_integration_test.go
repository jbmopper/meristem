package signals

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
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
