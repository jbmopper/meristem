package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestApprovalsRebuildFromEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-rebuild-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-rebuild-requester",
		Source: domain.SourceAgent,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-rebuild-decider",
		Source: domain.SourceHuman,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create decider token: %v", err)
	}
	systemResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "approval-rebuild-system",
		Source: domain.SourceSystem,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	decidedItem, err := workSvc.Create(ctx, workitems.CreateInput{Title: "approval rebuild decided", Actor: root})
	if err != nil {
		t.Fatalf("create decided item: %v", err)
	}
	expiredItem, err := workSvc.Create(ctx, workitems.CreateInput{Title: "approval rebuild expired", Actor: root})
	if err != nil {
		t.Fatalf("create expired item: %v", err)
	}

	approvalSvc := approvals.NewService(pool, writer)
	decided, err := approvalSvc.Create(ctx, approvals.CreateInput{
		WorkItemID: decidedItem.ID,
		Summary:    "rebuild decision",
		Request:    map[string]any{"action": "write"},
		Actor:      requester.Token,
	})
	if err != nil {
		t.Fatalf("create decided approval: %v", err)
	}
	if _, err := approvalSvc.Decide(ctx, approvals.DecisionInput{
		ApprovalID: decided.Approval.ID,
		Decision:   approvals.DecisionApproved,
		Reason:     "rebuild approved",
		Actor:      decider.Token,
	}); err != nil {
		t.Fatalf("decide approval: %v", err)
	}
	expiring, err := approvalSvc.Create(ctx, approvals.CreateInput{
		WorkItemID: expiredItem.ID,
		Summary:    "rebuild expiry",
		Actor:      requester.Token,
	})
	if err != nil {
		t.Fatalf("create expiring approval: %v", err)
	}
	if _, err := approvalSvc.Expire(ctx, approvals.ExpireInput{
		ApprovalID: expiring.Approval.ID,
		Reason:     "rebuild expired",
		Actor:      systemResult.Token,
	}); err != nil {
		t.Fatalf("expire approval: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "approvals_rebuild", logger, false)
	if err != nil {
		t.Fatalf("rebuild approvals projection: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("approval rebuild had mismatches: %+v", report.mismatches)
	}
}
