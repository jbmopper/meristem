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
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
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

func TestTransitionEndpointBlocksOverConcurrentRunningBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "running-budget-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineSingleRunningCultivar(t, ctx, pool, writer, root)

	agentA, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "running-budget-agent-a",
		Source: domain.SourceAgent,
		Actor:  &root,
		Scopes: []string{access.ScopeWorkItemsReadAll, access.ScopeWorkItemsWriteAll},
	})
	if err != nil {
		t.Fatalf("create agent a token: %v", err)
	}
	agentB, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "running-budget-agent-b",
		Source: domain.SourceAgent,
		Actor:  &root,
		Scopes: []string{access.ScopeWorkItemsReadAll, access.ScopeWorkItemsWriteAll},
	})
	if err != nil {
		t.Fatalf("create agent b token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	first := createBudgetedRunningItem(t, ctx, workSvc, root, "first running item")
	second := createBudgetedRunningItem(t, ctx, workSvc, root, "second running item")
	third := createBudgetedRunningItem(t, ctx, workSvc, root, "third running item")
	server := New(pool, nil)

	firstPath := "/v1/work-items/" + first.ID.String() + "/transition"
	firstStart := doREST(t, server.Handler(), http.MethodPost, firstPath, agentA.Secret, "running-budget-first", []byte(`{"to":"running","reason":"start first"}`))
	assertRESTStatus(t, firstStart, http.StatusOK)

	sameState := doREST(t, server.Handler(), http.MethodPost, firstPath, agentA.Secret, "running-budget-first-noop", []byte(`{"to":"running","reason":"same-state heartbeat"}`))
	assertRESTStatus(t, sameState, http.StatusOK)
	assertEventCount(t, pool, domain.EventXylemExhausted, 0)

	secondPath := "/v1/work-items/" + second.ID.String() + "/transition"
	blocked := doREST(t, server.Handler(), http.MethodPost, secondPath, agentA.Secret, "running-budget-second", []byte(`{"to":"running","reason":"start second"}`))
	assertRESTStatus(t, blocked, http.StatusConflict)
	assertErrorCode(t, blocked, "xylem_budget_exhausted")

	assertEventCount(t, pool, domain.EventXylemExhausted, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
	var runningTransitions int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND kind = $3
		  AND payload->>'to' = $4
	`, domain.SubjectWorkItem, second.ID, domain.EventWorkItemTransitioned, domain.WorkItemRunning).Scan(&runningTransitions); err != nil {
		t.Fatalf("count second running transitions: %v", err)
	}
	if runningTransitions != 0 {
		t.Fatalf("over-budget item received a running transition")
	}

	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE kind = $1 AND subject_kind = $2 AND subject_id = $3
	`, domain.EventXylemExhausted, domain.SubjectWorkItem, second.ID).Scan(&raw); err != nil {
		t.Fatalf("read xylem.exhausted payload: %v", err)
	}
	var payload struct {
		Budget                       string    `json:"budget"`
		CurrentRunning               int       `json:"current_running"`
		MaxConcurrentRunningPerToken int       `json:"max_concurrent_running_items_per_token"`
		BudgetSource                 string    `json:"budget_source"`
		Cultivar                     string    `json:"cultivar"`
		ActorTokenID                 uuid.UUID `json:"actor_token_id"`
		AttemptedState               string    `json:"attempted_state"`
		EscalationRule               string    `json:"escalation_rule"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode xylem payload: %v", err)
	}
	if payload.Budget != "max_concurrent_running_items_per_token" ||
		payload.CurrentRunning != 1 ||
		payload.MaxConcurrentRunningPerToken != 1 ||
		payload.BudgetSource != "cultivar:single-running-worker@1" ||
		payload.Cultivar != "single-running-worker@1" ||
		payload.ActorTokenID != agentA.Token.ID ||
		payload.AttemptedState != string(domain.WorkItemRunning) ||
		payload.EscalationRule != string(domain.EscalationRuleHandToHuman) {
		t.Fatalf("unexpected xylem payload: %+v", payload)
	}
	updatedSecond, err := workSvc.Get(ctx, second.ID)
	if err != nil {
		t.Fatalf("get second item: %v", err)
	}
	if updatedSecond.State != domain.WorkItemBlocked || updatedSecond.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("second item not blocked by running budget exhaustion: state=%s review=%s", updatedSecond.State, updatedSecond.HumanReviewStatus)
	}
	if updatedSecond.StateReason == nil || !strings.Contains(*updatedSecond.StateReason, "max_concurrent_running_items_per_token") {
		t.Fatalf("second item state reason should name exhausted budget, got %v", updatedSecond.StateReason)
	}

	thirdPath := "/v1/work-items/" + third.ID.String() + "/transition"
	thirdStart := doREST(t, server.Handler(), http.MethodPost, thirdPath, agentB.Secret, "running-budget-third", []byte(`{"to":"running","reason":"start third as another token"}`))
	assertRESTStatus(t, thirdStart, http.StatusOK)
	updatedThird, err := workSvc.Get(ctx, third.ID)
	if err != nil {
		t.Fatalf("get third item: %v", err)
	}
	if updatedThird.State != domain.WorkItemRunning {
		t.Fatalf("third item should run for separate token, got %s", updatedThird.State)
	}
}

func TestTransitionEndpointUsesSafetyFallbackForZeroConcurrentRunningBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "running-budget-fallback-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineFallbackRunningCultivar(t, ctx, pool, writer, root)

	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "running-budget-fallback-agent",
		Source: domain.SourceAgent,
		Actor:  &root,
		Scopes: []string{access.ScopeWorkItemsReadAll, access.ScopeWorkItemsWriteAll},
	})
	if err != nil {
		t.Fatalf("create agent token: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	server := New(pool, nil)
	max := safety.DefaultPolicy().MaxConcurrentRunningPerToken
	for i := 0; i < max; i++ {
		item := createBudgetedRunningItemWithCultivar(t, ctx, workSvc, root, "fallback running item", "fallback-running-worker@1")
		path := "/v1/work-items/" + item.ID.String() + "/transition"
		rec := doREST(t, server.Handler(), http.MethodPost, path, agent.Secret, "fallback-running-start-"+item.ID.String(), []byte(`{"to":"running","reason":"fill fallback budget"}`))
		assertRESTStatus(t, rec, http.StatusOK)
	}

	over := createBudgetedRunningItemWithCultivar(t, ctx, workSvc, root, "fallback over-budget item", "fallback-running-worker@1")
	overPath := "/v1/work-items/" + over.ID.String() + "/transition"
	rec := doREST(t, server.Handler(), http.MethodPost, overPath, agent.Secret, "fallback-running-over", []byte(`{"to":"running","reason":"exceed fallback budget"}`))
	assertRESTStatus(t, rec, http.StatusConflict)
	assertErrorCode(t, rec, "xylem_budget_exhausted")

	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE kind = $1 AND subject_kind = $2 AND subject_id = $3
	`, domain.EventXylemExhausted, domain.SubjectWorkItem, over.ID).Scan(&raw); err != nil {
		t.Fatalf("read fallback xylem.exhausted payload: %v", err)
	}
	var payload struct {
		CurrentRunning               int    `json:"current_running"`
		MaxConcurrentRunningPerToken int    `json:"max_concurrent_running_items_per_token"`
		BudgetSource                 string `json:"budget_source"`
		Cultivar                     string `json:"cultivar"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode fallback xylem payload: %v", err)
	}
	if payload.CurrentRunning != max ||
		payload.MaxConcurrentRunningPerToken != max ||
		payload.BudgetSource != "safety_policy" ||
		payload.Cultivar != "fallback-running-worker@1" {
		t.Fatalf("unexpected fallback xylem payload: %+v", payload)
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

func defineSingleRunningCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "single-running-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "single concurrent running budget test tropism",
	})
	if err != nil {
		t.Fatalf("define single-running tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "single-running-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "single-running-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/single-running-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"work_items.write",
				"feed.read_assigned",
			},
		},
		Xylem: registry.Xylem{
			MaxAttempts:                  3,
			MaxWallSeconds:               1800,
			MaxDepth:                     1,
			MaxConcurrentRunningPerToken: 1,
		},
		Phloem:      "projection:work-item-brief",
		Description: "single concurrent running budget test cultivar",
	})
	if err != nil {
		t.Fatalf("define single-running cultivar: %v", err)
	}
}

func defineFallbackRunningCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "fallback-running-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "fallback concurrent running budget test tropism",
	})
	if err != nil {
		t.Fatalf("define fallback-running tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "fallback-running-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "fallback-running-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/fallback-running-worker.md",
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
			MaxChildrenPerItem: 0,
		},
		Phloem:      "projection:work-item-brief",
		Description: "fallback concurrent running budget test cultivar",
	})
	if err != nil {
		t.Fatalf("define fallback-running cultivar: %v", err)
	}
}

func createBudgetedRunningItem(t *testing.T, ctx context.Context, svc *workitems.Service, actor domain.Token, title string) domain.WorkItem {
	t.Helper()
	return createBudgetedRunningItemWithCultivar(t, ctx, svc, actor, title, "single-running-worker@1")
}

func createBudgetedRunningItemWithCultivar(t *testing.T, ctx context.Context, svc *workitems.Service, actor domain.Token, title string, cultivar string) domain.WorkItem {
	t.Helper()
	item, err := svc.Create(ctx, workitems.CreateInput{
		Title:                      title,
		Actor:                      actor,
		Cultivar:                   cultivar,
		SuggestedConvergenceChecks: []string{"running_budget_test_done"},
	})
	if err != nil {
		t.Fatalf("create %s: %v", title, err)
	}
	return item
}
