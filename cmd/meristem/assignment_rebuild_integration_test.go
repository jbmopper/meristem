package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestAssignmentReleaseReclaimRebuildHonesty(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "assignment_rebuild")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-rebuild-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	actorA, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-rebuild-a", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	actorB, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-rebuild-b", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	svc := workitems.NewService(pool, writer)
	item, err := svc.Create(ctx, workitems.CreateInput{
		Title: "assignment rebuild", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"rebuild exact"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: actorA.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.Claim(ctx, item.ID, actorA.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Yield(ctx, item.ID, actorA.Token); err != nil {
		t.Fatal(err)
	}
	second, err := svc.Claim(ctx, item.ID, actorB.Token)
	if err != nil {
		t.Fatal(err)
	}
	if first.AssignmentEventID == second.AssignmentEventID {
		t.Fatal("reclaim reused assignment event identity")
	}

	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "assignment_rebuild_check", slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("assignment rebuild mismatches: %+v", report.mismatches)
	}
}
