package workitems

import (
	"context"
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestTransitionFailsLoudlyOnUnexpectedDedupe is the regression for work
// item 87edd2dd: a first-attempt transition OUTSIDE the idempotency
// contract (empty discriminator) whose event collides with an existing row
// must return ErrUnexpectedEventDedupe instead of committing a silent
// no-op — the 2026-07-03 silent-swallow bug class, made loud.
func TestTransitionFailsLoudlyOnUnexpectedDedupe(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_dedupe")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "dedupe-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	root := rootResult.Token
	svc := NewService(pool, writer)

	item, err := svc.Create(ctx, CreateInput{
		Title:                      "dedupe probe",
		State:                      domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"human-ack: probe"},
		Actor:                      root,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Cycle: running -> blocked ("same reason") -> running -> blocked
	// ("same reason") again. Payloads of the first and third transition are
	// identical; without a discriminator the third must fail loudly.
	if _, err := svc.Transition(ctx, item.ID, domain.WorkItemBlocked, "same reason", root); err != nil {
		t.Fatalf("first block: %v", err)
	}
	if _, err := svc.Transition(ctx, item.ID, domain.WorkItemRunning, "resume", root); err != nil {
		t.Fatalf("resume: %v", err)
	}
	_, err = svc.Transition(ctx, item.ID, domain.WorkItemBlocked, "same reason", root)
	if !errors.Is(err, ErrUnexpectedEventDedupe) {
		t.Fatalf("repeated identical transition without discriminator: want ErrUnexpectedEventDedupe, got %v", err)
	}

	// The item must be untouched by the failed attempt: still running.
	got, err := svc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != domain.WorkItemRunning {
		t.Fatalf("failed transition mutated state: %s", got.State)
	}
}
