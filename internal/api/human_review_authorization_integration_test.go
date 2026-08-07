package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestRESTHumanReviewDecisionUsesDomainAuthority(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "rest-review-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "rest-review-gated", HumanReviewStatus: domain.HumanReviewBlocked, Actor: rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create blocked item: %v", err)
	}
	writerResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "rest-review-writer", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReviewDecide}, Actor: &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	reviewerResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "rest-review-human", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReviewDecide}, Actor: &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	body := []byte(`{"suggested_convergence_checks":["cmd:go test ./..."],"human_review_status":"waved_through"}`)
	path := "/v1/work-items/" + item.ID.String() + "/metadata"

	denied := doREST(t, New(pool, nil).Handler(), http.MethodPost, path, writerResult.Secret, "rest-review-denied", body)
	assertRESTStatus(t, denied, http.StatusForbidden)
	assertErrorCode(t, denied, "human_review_decision_denied")

	allowed := doREST(t, New(pool, nil).Handler(), http.MethodPost, path, reviewerResult.Secret, "rest-review-allowed", body)
	assertRESTStatus(t, allowed, http.StatusOK)
}
