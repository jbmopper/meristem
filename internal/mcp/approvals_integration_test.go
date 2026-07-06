package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestApprovalsMCPParityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-approval-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-approval-decider",
		Source: domain.SourceHuman,
		Scopes: []string{access.ScopeApprovalsDecide},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create decider token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "mcp approval target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	requester, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-approval-requester",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create requester token: %v", err)
	}

	approvalSvc := approvals.NewService(pool, writer)
	s := New(Deps{
		Auth:        authSvc,
		Access:      access.NewService(pool),
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
		Approvals:   approvalSvc,
		Feed:        feed.NewService(pool),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	if err := s.Authenticate(ctx, requester.Secret); err != nil {
		t.Fatalf("authenticate requester: %v", err)
	}

	assertToolCallOK(t, s, "approvals.request", map[string]any{
		"work_item_id":       item.ID.String(),
		"summary":            "mcp side effect",
		"request":            map[string]any{"connector": "http"},
		"expires_in_seconds": 60,
		"idempotency_key":    "mcp-approval-request",
	})
	assertToolCallOK(t, s, "approvals.list_for_work_item", map[string]any{"work_item_id": item.ID.String()})
	current, err := workSvc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get awaiting item: %v", err)
	}
	if current.State != domain.WorkItemAwaitingApproval {
		t.Fatalf("MCP approval request did not park work item: state=%s", current.State)
	}
	if got := eventCount(t, pool, domain.EventApprovalCreated); got != 1 {
		t.Fatalf("approval.created count = %d, want 1", got)
	}
	var approvalID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM approvals WHERE work_item_id = $1`, item.ID).Scan(&approvalID); err != nil {
		t.Fatalf("read approval id: %v", err)
	}
	assertToolCallOK(t, s, "approvals.get", map[string]any{"id": approvalID.String()})

	if isError, text := callToolForTest(t, s, "approvals.decide", map[string]any{
		"id":              approvalID.String(),
		"decision":        "approved",
		"idempotency_key": "mcp-agent-decide-denied",
	}); !isError || !strings.Contains(text, "insufficient_scope") {
		t.Fatalf("agent decision should be denied by tool policy, isError=%t text=%q", isError, text)
	}
	if got := eventCount(t, pool, domain.EventApprovalDecided); got != 0 {
		t.Fatalf("denied MCP decision appended event: %d", got)
	}

	if err := s.Authenticate(ctx, decider.Secret); err != nil {
		t.Fatalf("authenticate decider: %v", err)
	}
	assertToolCallOK(t, s, "approvals.decide", map[string]any{
		"id":              approvalID.String(),
		"decision":        "denied",
		"reason":          "not now",
		"idempotency_key": "mcp-approval-decide",
	})
	decided, err := approvalSvc.Get(ctx, approvalID)
	if err != nil {
		t.Fatalf("get decided approval: %v", err)
	}
	if decided.Status != approvals.StatusDenied {
		t.Fatalf("approval status = %s, want denied", decided.Status)
	}
	current, err = workSvc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get denied item: %v", err)
	}
	if current.State != domain.WorkItemFailed {
		t.Fatalf("denied approval should fail work item, got %s", current.State)
	}
}
