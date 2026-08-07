package workitems

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestHumanReviewDecisionsRequireExplicitScopedHumanBeforeAppend(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "human_review_authority")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "review-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	root := rootResult.Token
	mint := func(name string, source domain.Source, scopes ...string) domain.Token {
		t.Helper()
		result, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name, Source: source, Scopes: scopes, Actor: &root})
		if err != nil {
			t.Fatalf("mint %s: %v", name, err)
		}
		return result.Token
	}

	agent := mint("review-agent", domain.SourceAgent, access.ScopeWorkItemsReviewDecide)
	system := mint("review-system", domain.SourceSystem, access.ScopeWorkItemsReviewDecide)
	legacy := mint("review-legacy-human", domain.SourceHuman)
	tracker := mint("review-tracker", domain.SourceHuman, access.ScopeWorkItemsTrackerWriteAll)
	revoked := mint("review-revoked", domain.SourceHuman, access.ScopeWorkItemsReviewDecide)
	if err := authSvc.Revoke(ctx, revoked.ID, root); err != nil {
		t.Fatalf("revoke human reviewer: %v", err)
	}
	revoked, err = authSvc.Get(ctx, revoked.ID)
	if err != nil {
		t.Fatalf("reload revoked reviewer: %v", err)
	}
	rootWithScope := root
	rootWithScope.Scopes = []string{access.ScopeWorkItemsReviewDecide}

	svc := NewService(pool, writer)
	beforeCreate := countWorkItemKind(t, ctx, pool, uuid.Nil, domain.EventWorkItemCreated)
	if _, err := svc.Create(ctx, CreateInput{
		Title: "blocked terminal creation", State: domain.WorkItemDone,
		HumanReviewStatus: domain.HumanReviewBlocked, Actor: root,
	}); !errors.Is(err, ErrHumanReviewBlocked) {
		t.Fatalf("blocked terminal creation error = %v, want ErrHumanReviewBlocked", err)
	}
	if after := countWorkItemKind(t, ctx, pool, uuid.Nil, domain.EventWorkItemCreated); after != beforeCreate {
		t.Fatalf("blocked terminal creation appended event: before=%d after=%d", beforeCreate, after)
	}
	item, err := svc.Create(ctx, CreateInput{Title: "review-gated", HumanReviewStatus: domain.HumanReviewBlocked, Actor: root})
	if err != nil {
		t.Fatalf("create blocked item: %v", err)
	}
	denied := []struct {
		name  string
		actor domain.Token
	}{
		{"agent", agent},
		{"system", system},
		{"root", rootWithScope},
		{"revoked human", revoked},
		{"legacy unscoped human", legacy},
		{"tracker-only human", tracker},
	}
	for _, tc := range denied {
		for _, target := range []domain.HumanReviewStatus{domain.HumanReviewWavedThrough, domain.HumanReviewApproved} {
			t.Run(tc.name+" to "+string(target), func(t *testing.T) {
				before := countHumanReviewEvents(t, ctx, pool, item.ID)
				_, err := svc.UpdateMetadata(ctx, item.ID, UpdateMetadataInput{
					SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
					HumanReviewStatus:          target,
					Actor:                      tc.actor,
				})
				if !errors.Is(err, ErrHumanReviewDecisionDenied) {
					t.Fatalf("UpdateMetadata error = %v, want ErrHumanReviewDecisionDenied", err)
				}
				if after := countHumanReviewEvents(t, ctx, pool, item.ID); after != before {
					t.Fatalf("denied decision appended metadata event: before=%d after=%d", before, after)
				}
			})
		}
	}

	reviewer := mint("review-owner-client", domain.SourceHuman, access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReviewDecide)
	item, err = svc.UpdateMetadata(ctx, item.ID, UpdateMetadataInput{
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      reviewer,
	})
	if err != nil {
		t.Fatalf("authorized wave-through: %v", err)
	}
	if item.HumanReviewStatus != domain.HumanReviewWavedThrough {
		t.Fatalf("review status = %s, want waved_through", item.HumanReviewStatus)
	}
	if _, err := svc.UpdateMetadata(ctx, item.ID, UpdateMetadataInput{
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          domain.HumanReviewBlocked,
		Actor:                      agent,
	}); err != nil {
		t.Fatalf("agent should be able to make review more conservative: %v", err)
	}
	item, err = svc.UpdateMetadata(ctx, item.ID, UpdateMetadataInput{
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          domain.HumanReviewApproved,
		Actor:                      reviewer,
	})
	if err != nil {
		t.Fatalf("authorized approval: %v", err)
	}
	if item.HumanReviewStatus != domain.HumanReviewApproved {
		t.Fatalf("review status = %s, want approved", item.HumanReviewStatus)
	}
	before := countHumanReviewEvents(t, ctx, pool, item.ID)
	if _, err := svc.UpdateMetadata(ctx, item.ID, UpdateMetadataInput{
		SuggestedConvergenceChecks: []string{"cmd:changed after approval"},
		HumanReviewStatus:          domain.HumanReviewApproved,
		Actor:                      agent,
	}); !errors.Is(err, ErrHumanReviewDecisionDenied) {
		t.Fatalf("agent retained approval while changing checks: %v", err)
	}
	if after := countHumanReviewEvents(t, ctx, pool, item.ID); after != before {
		t.Fatalf("denied approval retention appended event: before=%d after=%d", before, after)
	}
}

func TestHistoricalUnauthorizedReviewEventRemainsButProjectsBlocked(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "human_review_history")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "history-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	agentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "history-agent", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReviewDecide}, Actor: &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	svc := NewService(pool, writer)
	item, err := svc.Create(ctx, CreateInput{Title: "historical gate", HumanReviewStatus: domain.HumanReviewBlocked, Actor: rootResult.Token})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin historical append: %v", err)
	}
	badEventID, fresh, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    item.ID,
		Kind:         domain.EventWorkItemMetadataUpdated,
		Source:       domain.SourceAgent,
		ActorTokenID: &agentResult.Token.ID,
		Payload: map[string]any{
			"from": map[string]any{"suggested_convergence_checks": []string{}, "human_review_status": domain.HumanReviewBlocked},
			"to":   map[string]any{"suggested_convergence_checks": []string{"cmd:historical"}, "human_review_status": domain.HumanReviewWavedThrough},
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append historical unauthorized event: %v", err)
	}
	if !fresh {
		_ = tx.Rollback(ctx)
		t.Fatal("historical unauthorized fixture unexpectedly deduped")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit historical fixture: %v", err)
	}

	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id=$1`, badEventID).Scan(&stored); err != nil {
		t.Fatalf("read immutable bad event: %v", err)
	}
	if stored != 1 {
		t.Fatalf("historical unauthorized event count = %d, want 1", stored)
	}
	current, err := svc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get projected item: %v", err)
	}
	if current.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("unauthorized historical event projected review=%s, want blocked", current.HumanReviewStatus)
	}
	beforeTransitions := countWorkItemKind(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned)
	if _, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "unauthorized convergence", agentResult.Token); !errors.Is(err, ErrHumanReviewBlocked) {
		t.Fatalf("blocked completion error = %v, want ErrHumanReviewBlocked", err)
	}
	if after := countWorkItemKind(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); after != beforeTransitions {
		t.Fatalf("blocked completion appended event: before=%d after=%d", beforeTransitions, after)
	}

	tx, err = pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin direct transition append: %v", err)
	}
	_, _, err = writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    item.ID,
		Kind:         domain.EventWorkItemTransitioned,
		Source:       domain.SourceSystem,
		ActorTokenID: &agentResult.Token.ID,
		Payload: map[string]any{
			"from":   item.State,
			"to":     domain.WorkItemDone,
			"reason": "internal producer bypass attempt",
		},
	})
	if err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("direct transition append completed blocked work item")
	}
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
		t.Fatalf("roll back rejected direct transition: %v", rollbackErr)
	}
	if after := countWorkItemKind(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); after != beforeTransitions {
		t.Fatalf("rejected direct completion left an event: before=%d after=%d", beforeTransitions, after)
	}
}

func countHumanReviewEvents(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id uuid.UUID) int {
	t.Helper()
	return countWorkItemKind(t, ctx, pool, id, domain.EventWorkItemMetadataUpdated)
}

func countWorkItemKind(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id uuid.UUID, kind string) int {
	t.Helper()
	var count int
	query := `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`
	args := []any{id, kind}
	if id == uuid.Nil {
		query = `SELECT count(*) FROM events WHERE kind=$1`
		args = []any{kind}
	}
	if err := pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	return count
}
