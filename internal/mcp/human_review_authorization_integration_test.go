package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestMCPHumanReviewDecisionUsesDomainAuthority(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-review-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "mcp-review-gated", HumanReviewStatus: domain.HumanReviewBlocked, Actor: rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create blocked item: %v", err)
	}
	mint := func(name string, source domain.Source) auth.CreateTokenResult {
		t.Helper()
		result, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
			Name: name, Source: source,
			Scopes: []string{access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReviewDecide}, Actor: &rootResult.Token,
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		return result
	}
	newServer := func(result auth.CreateTokenResult) *Server {
		t.Helper()
		s := New(Deps{
			Auth: authSvc, Access: access.NewService(pool),
			Idempotency: idempotency.NewMiddleware(pool, writer), WorkItems: workSvc,
		}, ServerInfo{Name: "meristem-review-test", Version: "test"}, nil)
		if err := s.Authenticate(ctx, result.Secret); err != nil {
			t.Fatalf("authenticate %s: %v", result.Token.Name, err)
		}
		return s
	}
	args := func(key string) map[string]any {
		return map[string]any{
			"id": item.ID.String(), "suggested_convergence_checks": []string{"cmd:go test ./..."},
			"human_review_status": "waved_through", "idempotency_key": key,
		}
	}

	deniedServer := newServer(mint("mcp-review-agent", domain.SourceAgent))
	isError, text := callToolForTest(t, deniedServer, "work_items.update_metadata", args(uuid.NewString()))
	if !isError || !strings.Contains(text, "work_items.review_decide") {
		t.Fatalf("agent review decision = isError:%t text:%q", isError, text)
	}

	allowedServer := newServer(mint("mcp-review-human", domain.SourceHuman))
	isError, text = callToolForTest(t, allowedServer, "work_items.update_metadata", args(uuid.NewString()))
	if isError {
		t.Fatalf("scoped human review decision failed: %s", text)
	}
}
