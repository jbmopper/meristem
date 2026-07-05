package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestSpawnChildEndpointBlocksOverChildrenBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "child-budget-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineSingleChildCultivar(t, ctx, pool, writer, root)

	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:    "budgeted parent",
		Actor:    root,
		Cultivar: "single-child-worker@1",
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	server := New(pool, nil)

	firstBody := []byte(`{"title":"first child"}`)
	first := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parent.ID.String()+"/children", rootResult.Secret, "first-child", firstBody)
	assertRESTStatus(t, first, http.StatusCreated)
	var firstResp struct {
		WorkItemID uuid.UUID `json:"work_item_id"`
	}
	decodeResponse(t, first, &firstResp)
	if firstResp.WorkItemID == uuid.Nil {
		t.Fatalf("first child response missing id: %s", first.Body.String())
	}

	replayed, fresh, err := workSvc.SpawnChildWithID(ctx, parent.ID, firstResp.WorkItemID, workitems.CreateInput{
		Title: "first child",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("service replay of already-related child should bypass budget exhaustion: %v", err)
	}
	if fresh {
		t.Fatalf("service replay returned fresh=true for existing child")
	}
	if replayed.ID != firstResp.WorkItemID {
		t.Fatalf("service replay id = %s, want %s", replayed.ID, firstResp.WorkItemID)
	}
	assertEventCount(t, pool, domain.EventXylemExhausted, 0)

	secondBody := []byte(`{"title":"second child"}`)
	second := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+parent.ID.String()+"/children", rootResult.Secret, "second-child", secondBody)
	assertRESTStatus(t, second, http.StatusConflict)
	assertErrorCode(t, second, "xylem_budget_exhausted")

	assertEventCount(t, pool, domain.EventXylemExhausted, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE kind = $1 AND subject_kind = $2 AND subject_id = $3
	`, domain.EventXylemExhausted, domain.SubjectWorkItem, parent.ID).Scan(&raw); err != nil {
		t.Fatalf("read xylem.exhausted payload: %v", err)
	}
	var payload struct {
		Budget             string    `json:"budget"`
		CurrentChildren    int       `json:"current_children"`
		MaxChildrenPerItem int       `json:"max_children_per_item"`
		BudgetSource       string    `json:"budget_source"`
		Cultivar           string    `json:"cultivar"`
		AttemptedChildID   uuid.UUID `json:"attempted_child_id"`
		AttemptedTitle     string    `json:"attempted_child_title"`
		EscalationRule     string    `json:"escalation_rule"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode xylem payload: %v", err)
	}
	if payload.Budget != "max_children_per_item" ||
		payload.CurrentChildren != 1 ||
		payload.MaxChildrenPerItem != 1 ||
		payload.BudgetSource != "cultivar:single-child-worker@1" ||
		payload.Cultivar != "single-child-worker@1" ||
		payload.AttemptedChildID == uuid.Nil ||
		payload.AttemptedTitle != "second child" ||
		payload.EscalationRule != string(domain.EscalationRuleHandToHuman) {
		t.Fatalf("unexpected xylem payload: %+v", payload)
	}
	var attemptedRelations int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM work_item_relations
		WHERE parent_id = $1 AND child_id = $2
	`, parent.ID, payload.AttemptedChildID).Scan(&attemptedRelations); err != nil {
		t.Fatalf("count attempted child relation: %v", err)
	}
	if attemptedRelations != 0 {
		t.Fatalf("over-budget attempted child relation was created")
	}
	var secondChildren int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_items WHERE title = 'second child'`).Scan(&secondChildren); err != nil {
		t.Fatalf("count second child rows: %v", err)
	}
	if secondChildren != 0 {
		t.Fatalf("over-budget child row was created")
	}
	updated, err := workSvc.Get(ctx, parent.ID)
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if updated.State != domain.WorkItemBlocked || updated.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("parent not blocked by child budget exhaustion: state=%s review=%s", updated.State, updated.HumanReviewStatus)
	}
	if updated.StateReason == nil || !strings.Contains(*updated.StateReason, "max_children_per_item") {
		t.Fatalf("parent state reason should name exhausted budget, got %v", updated.StateReason)
	}
}

func defineSingleChildCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "single-child-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "single child budget test tropism",
	})
	if err != nil {
		t.Fatalf("define single-child tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "single-child-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "single-child-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/single-child-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:        3,
			MaxWallSeconds:     1800,
			MaxDepth:           1,
			MaxChildrenPerItem: 1,
		},
		Phloem:      "projection:work-item-brief",
		Description: "single child budget test cultivar",
	})
	if err != nil {
		t.Fatalf("define single-child cultivar: %v", err)
	}
}
