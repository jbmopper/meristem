package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestCultivarActivationEscalatesWithoutApprovedReview(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "activation-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineActivationTropism(t, ctx, root, pool, writer)
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "activation proposal",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create proposal item: %v", err)
	}
	agentSecret := createActivationAgent(t, ctx, pool, writer, root, item.ID)
	beforeCultivars := eventCount(t, pool, domain.EventCultivarDefined)
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)

	rec := doREST(t, New(pool, nil).Handler(), http.MethodPost, "/v1/work-items/"+item.ID.String()+"/cultivar-activations", agentSecret, "activation-escalate", activationBody("activation-worker", false))
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp cultivarActivationResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate: %s", resp.Disposition, resp.Reason)
	}
	if !strings.Contains(resp.Reason, "explicit human approval") {
		t.Fatalf("reason %q does not mention approval", resp.Reason)
	}
	if resp.Cultivar != nil {
		t.Fatalf("escalated activation returned cultivar: %+v", resp.Cultivar)
	}
	if resp.Escalation == nil || resp.Escalation.ID == uuid.Nil || resp.Escalation.HumanWorkItemID == uuid.Nil {
		t.Fatalf("escalation response missing durable ids: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventCultivarActivationRequested, 1)
	assertEventCount(t, pool, domain.EventCultivarActivationEscalated, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
	if afterCultivars := eventCount(t, pool, domain.EventCultivarDefined); afterCultivars != beforeCultivars {
		t.Fatalf("escalated activation appended cultivar.defined: before=%d after=%d", beforeCultivars, afterCultivars)
	}
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("escalated activation minted token: before=%d after=%d", beforeTokens, afterTokens)
	}
}

func TestCultivarActivationGrantsApprovedSameTreeProposal(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "activation-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineActivationTropism(t, ctx, root, pool, writer)
	reviewer, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "activation-reviewer", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeWorkItemsReviewDecide}, Actor: &root,
	})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:             "approved activation proposal",
		Actor:             reviewer.Token,
		HumanReviewStatus: domain.HumanReviewApproved,
	})
	if err != nil {
		t.Fatalf("create approved proposal item: %v", err)
	}
	agentSecret := createActivationAgent(t, ctx, pool, writer, root, item.ID)
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)

	rec := doREST(t, New(pool, nil).Handler(), http.MethodPost, "/v1/work-items/"+item.ID.String()+"/cultivar-activations", agentSecret, "activation-grant", activationBody("activation-worker", false))
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp cultivarActivationResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionGrant {
		t.Fatalf("disposition = %s, want grant: %s", resp.Disposition, resp.Reason)
	}
	if resp.Cultivar == nil || resp.Cultivar.Name != "activation-worker" || resp.Cultivar.Version != 1 {
		t.Fatalf("activation response missing cultivar: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventCultivarActivationRequested, 1)
	assertEventCount(t, pool, domain.EventCultivarActivationGranted, 1)
	assertEventCount(t, pool, domain.EventCultivarDefined, 1)
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("activation minted token: before=%d after=%d", beforeTokens, afterTokens)
	}
	current, err := registry.NewService(pool, writer).GetCultivar(ctx, "activation-worker")
	if err != nil {
		t.Fatalf("get activated cultivar: %v", err)
	}
	if current.Profile.Briefing != "briefings/activation-worker.md" || current.Rootstock {
		t.Fatalf("unexpected activated cultivar: %+v", current)
	}
}

func TestCultivarActivationDeniesRootstockSelfModification(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "activation-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineActivationTropism(t, ctx, root, pool, writer)
	reviewer, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "rootstock-activation-reviewer", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeWorkItemsReviewDecide}, Actor: &root,
	})
	if err != nil {
		t.Fatalf("create reviewer: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title:             "rootstock activation proposal",
		Actor:             reviewer.Token,
		HumanReviewStatus: domain.HumanReviewApproved,
	})
	if err != nil {
		t.Fatalf("create approved proposal item: %v", err)
	}
	agentSecret := createActivationAgent(t, ctx, pool, writer, root, item.ID)
	beforeCultivars := eventCount(t, pool, domain.EventCultivarDefined)

	rec := doREST(t, New(pool, nil).Handler(), http.MethodPost, "/v1/work-items/"+item.ID.String()+"/cultivar-activations", agentSecret, "activation-rootstock-deny", activationBody("activation-rootstock", true))
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp cultivarActivationResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionDeny {
		t.Fatalf("disposition = %s, want deny: %s", resp.Disposition, resp.Reason)
	}
	if resp.Cultivar != nil || resp.Escalation != nil {
		t.Fatalf("rootstock denial should not activate or escalate: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventCultivarActivationRequested, 1)
	assertEventCount(t, pool, domain.EventCultivarActivationDenied, 1)
	if afterCultivars := eventCount(t, pool, domain.EventCultivarDefined); afterCultivars != beforeCultivars {
		t.Fatalf("rootstock denial appended cultivar.defined: before=%d after=%d", beforeCultivars, afterCultivars)
	}
}

func defineActivationTropism(t *testing.T, ctx context.Context, actor domain.Token, pool *pgxpool.Pool, writer *events.Writer) {
	t.Helper()
	registrySvc := registry.NewService(pool, writer)
	_, _, err := registrySvc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "activation-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":2,"escalation":"hand_to_human"}}`),
		Description: "activation gate test tropism",
	})
	if err != nil {
		t.Fatalf("define activation tropism: %v", err)
	}
}

func createActivationAgent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, root domain.Token, workItemID uuid.UUID) string {
	t.Helper()
	result, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "activation-agent-" + workItemID.String()[:8],
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + workItemID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create activation agent: %v", err)
	}
	return result.Secret
}

func activationBody(name string, rootstock bool) []byte {
	body := map[string]any{
		"name":      name,
		"version":   1,
		"rootstock": rootstock,
		"tropism": map[string]any{
			"name":    "activation-checklist",
			"version": 1,
		},
		"profile": map[string]any{
			"briefing": "briefings/" + name + ".md",
			"scopes_template": []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		"xylem": map[string]any{
			"max_attempts":     2,
			"max_wall_seconds": 1200,
			"max_depth":        1,
		},
		"phloem":      "projection:work-item-brief",
		"description": "activation test worker",
	}
	out, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return out
}
