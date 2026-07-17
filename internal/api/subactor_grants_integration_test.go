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
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestSubactorGrantAdmittedSecretResponseSurvivesPinAdvanceAfterCommit(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "subactor-cutover-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "grant cutover target", Actor: rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "subactor-cutover-parent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create parent token: %v", err)
	}

	provider := buildguard.ProviderFunc(func() buildguard.Status {
		var committed bool
		err := pool.QueryRow(context.Background(), `
			SELECT EXISTS(SELECT 1 FROM events WHERE kind = $1)
		`, domain.EventSubactorGrantGranted).Scan(&committed)
		if err == nil && committed {
			return mismatchedBuildStatus()
		}
		return currentBuildStatus()
	})
	server := NewWithPolicyAndBuildGuard(pool, discardLogger(), safety.DefaultPolicy(), provider)
	body := []byte(`{"template":"same_tree_read_progress","work_item_id":"` + item.ID.String() + `"}`)
	rec := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-cutover", body)

	assertRESTStatus(t, rec, http.StatusCreated)
	var response subactorGrantResponse
	decodeResponse(t, rec, &response)
	if response.Token == nil || response.TokenSecret == "" {
		t.Fatalf("admitted grant lost its one-time token response: %+v", response)
	}
	if !provider.Status().Blocking() {
		t.Fatal("test did not advance the build pin after the grant committed")
	}
	if _, err := authSvc.Authenticate(ctx, response.TokenSecret); err != nil {
		t.Fatalf("returned one-time token is not active: %v", err)
	}
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 0)
	assertNoEventSecret(t, pool, response.TokenSecret)
}

func TestSubactorGrantEndpointGrantsReadOnlyAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "subactor-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "grant target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "parent-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create parent token: %v", err)
	}
	server := New(pool, nil)
	body := []byte(`{"template":"same_tree_read_progress","work_item_id":"` + item.ID.String() + `"}`)

	first := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-read", body)
	assertRESTStatus(t, first, http.StatusCreated)
	var firstResp subactorGrantResponse
	decodeResponse(t, first, &firstResp)
	if firstResp.Disposition != grants.DispositionGrant {
		t.Fatalf("disposition = %s, want grant: %s", firstResp.Disposition, firstResp.Reason)
	}
	if firstResp.Token == nil || firstResp.TokenSecret == "" {
		t.Fatalf("grant response missing token/secret: %+v", firstResp)
	}
	if strings.Contains(firstResp.TokenSecret, "\n") {
		t.Fatalf("token secret contains newline")
	}
	if _, err := authSvc.Authenticate(ctx, firstResp.TokenSecret); err != nil {
		t.Fatalf("authenticate granted token: %v", err)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantGranted, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantDenied, 0)
	assertEventCount(t, pool, domain.EventSubactorGrantEscalated, 0)
	assertNoEventSecret(t, pool, firstResp.TokenSecret)

	replay := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-read", body)
	assertRESTStatus(t, replay, http.StatusCreated)
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("expected replay header, got headers=%v", replay.Header())
	}
	var replayResp subactorGrantResponse
	decodeResponse(t, replay, &replayResp)
	if replayResp.Token == nil || replayResp.Token.ID != firstResp.Token.ID {
		t.Fatalf("replay did not return cached token metadata: first=%+v replay=%+v", firstResp, replayResp)
	}
	if replayResp.TokenSecret != "" {
		t.Fatalf("replay exposed token secret: %+v", replayResp)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantGranted, 1)
}

func TestSubactorGrantEndpointEscalatesWriteGrantWithoutApproval(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "subactor-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "write grant target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "writer-parent-agent",
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
		t.Fatalf("create parent token: %v", err)
	}
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)
	server := New(pool, nil)
	body := []byte(`{"template":"same_tree_worker","work_item_id":"` + item.ID.String() + `"}`)

	rec := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-write", body)
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp subactorGrantResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate: %s", resp.Disposition, resp.Reason)
	}
	if resp.Token != nil || resp.TokenSecret != "" {
		t.Fatalf("escalation returned token material: %+v", resp)
	}
	if resp.Escalation == nil || resp.Escalation.ID == uuid.Nil {
		t.Fatalf("escalation response missing durable escalation: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantEscalated, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("escalated request created token: before=%d after=%d", beforeTokens, afterTokens)
	}
	updated, err := workSvc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get updated work item: %v", err)
	}
	if updated.State != domain.WorkItemBlocked || updated.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("work item not blocked by escalation: state=%s review=%s", updated.State, updated.HumanReviewStatus)
	}
}

func TestSubactorGrantEndpointEscalatesOverDepthBudget(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "depth-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	defineDepthOneCultivar(t, ctx, pool, writer, root)
	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "depth root",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	child, err := workSvc.SpawnChild(ctx, parent.ID, workitems.CreateInput{
		Title: "depth child",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	grandchild, err := workSvc.SpawnChild(ctx, child.ID, workitems.CreateInput{
		Title:    "depth grandchild",
		Actor:    root,
		Cultivar: "depth-one-worker@1",
	})
	if err != nil {
		t.Fatalf("create grandchild: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "depth-parent-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + parent.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create parent token: %v", err)
	}
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)
	server := New(pool, nil)
	body := []byte(`{"template":"same_tree_read_progress","work_item_id":"` + grandchild.ID.String() + `"}`)

	rec := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-depth", body)
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp subactorGrantResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate: %s", resp.Disposition, resp.Reason)
	}
	for _, want := range []string{"delegation_depth_exceeded", "2", "1", "cultivar:depth-one-worker@1"} {
		if !strings.Contains(resp.Reason, want) {
			t.Fatalf("reason %q missing %q", resp.Reason, want)
		}
	}
	if resp.Token != nil || resp.TokenSecret != "" {
		t.Fatalf("over-depth escalation returned token material: %+v", resp)
	}
	if resp.Escalation == nil || resp.Escalation.ID == uuid.Nil {
		t.Fatalf("escalation response missing durable escalation: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantEscalated, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("over-depth request created token: before=%d after=%d", beforeTokens, afterTokens)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM events WHERE id = $1`, resp.Events.Requested).Scan(&raw); err != nil {
		t.Fatalf("read grant request payload: %v", err)
	}
	var payload struct {
		DelegationDepthKnown bool   `json:"delegation_depth_known"`
		DelegationDepth      int    `json:"delegation_depth"`
		MaxDelegationDepth   int    `json:"max_delegation_depth"`
		DepthBudgetSource    string `json:"depth_budget_source"`
		Cultivar             string `json:"cultivar"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode grant request payload: %v", err)
	}
	if !payload.DelegationDepthKnown || payload.DelegationDepth != 2 || payload.MaxDelegationDepth != 1 {
		t.Fatalf("unexpected depth payload: %+v", payload)
	}
	if payload.DepthBudgetSource != "cultivar:depth-one-worker@1" || payload.Cultivar != "depth-one-worker@1" {
		t.Fatalf("unexpected budget source payload: %+v", payload)
	}
	updated, err := workSvc.Get(ctx, grandchild.ID)
	if err != nil {
		t.Fatalf("get updated grandchild: %v", err)
	}
	if updated.State != domain.WorkItemBlocked || updated.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("work item not blocked by depth escalation: state=%s review=%s", updated.State, updated.HumanReviewStatus)
	}
}

func TestSubactorGrantEndpointDeniesUnknownTemplate(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "subactor-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "deny target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "deny-parent-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create parent token: %v", err)
	}
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)
	server := New(pool, nil)
	body := []byte(`{"template":"unknown","work_item_id":"` + item.ID.String() + `"}`)

	rec := doREST(t, server.Handler(), http.MethodPost, "/v1/subactor-grants", parentResult.Secret, "grant-deny", body)
	assertRESTStatus(t, rec, http.StatusCreated)
	var resp subactorGrantResponse
	decodeResponse(t, rec, &resp)
	if resp.Disposition != grants.DispositionDeny {
		t.Fatalf("disposition = %s, want deny: %s", resp.Disposition, resp.Reason)
	}
	if resp.Token != nil || resp.TokenSecret != "" {
		t.Fatalf("denial returned token material: %+v", resp)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantDenied, 1)
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("denied request created token: before=%d after=%d", beforeTokens, afterTokens)
	}
}

func TestSubactorGrantIssueReturnsExistingOutcomeWithoutDuplicateToken(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "durable-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "durable grant target",
		Actor: root,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	parentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "durable-parent-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + item.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create parent token: %v", err)
	}

	issueCtx := idempotency.WithRequest(ctx, idempotency.Request{
		TokenID:     parentResult.Token.ID,
		Scope:       "POST /v1/subactor-grants",
		Key:         "durable-retry",
		RequestHash: []byte("same-request"),
	})
	svc := grants.NewIssuanceService(pool, writer, authSvc, nil)
	first, err := svc.Issue(issueCtx, grants.IssueInput{
		Parent:     parentResult.Token,
		WorkItemID: item.ID,
		Template:   grants.TemplateSameTreeReadProgress,
	})
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	if first.Token == nil || first.TokenSecret == "" {
		t.Fatalf("first issue missing token material: %+v", first)
	}
	beforeTokens := eventCount(t, pool, domain.EventTokenCreated)
	second, err := svc.Issue(issueCtx, grants.IssueInput{
		Parent:     parentResult.Token,
		WorkItemID: item.ID,
		Template:   grants.TemplateSameTreeReadProgress,
	})
	if err != nil {
		t.Fatalf("second issue: %v", err)
	}
	if second.Token == nil || second.Token.ID != first.Token.ID {
		t.Fatalf("second issue did not return existing token metadata: first=%+v second=%+v", first, second)
	}
	if second.TokenSecret != "" {
		t.Fatalf("second durable issue returned a secret: %+v", second)
	}
	if afterTokens := eventCount(t, pool, domain.EventTokenCreated); afterTokens != beforeTokens {
		t.Fatalf("durable retry created duplicate token: before=%d after=%d", beforeTokens, afterTokens)
	}
	assertEventCount(t, pool, domain.EventSubactorGrantRequested, 1)
	assertEventCount(t, pool, domain.EventSubactorGrantGranted, 1)
}

func eventCount(t *testing.T, pool *pgxpool.Pool, kind string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&count); err != nil {
		t.Fatalf("count events for %s: %v", kind, err)
	}
	return count
}

func assertNoEventSecret(t *testing.T, pool *pgxpool.Pool, secret string) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM events
		WHERE payload::text LIKE '%' || $1 || '%'
	`, secret).Scan(&count); err != nil {
		t.Fatalf("scan event payloads: %v", err)
	}
	if count != 0 {
		t.Fatalf("event payload leaked token secret")
	}
}

func defineDepthOneCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) {
	t.Helper()
	svc := registry.NewService(pool, writer)
	_, _, err := svc.DefineTropism(ctx, actor, registry.DefineTropismInput{
		Name:    "depth-one-checklist",
		Version: 1,
		Reducer: registry.ReducerRef{
			Identity: "all_pass_checklist",
			Version:  1,
		},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "depth budget test tropism",
	})
	if err != nil {
		t.Fatalf("define depth-one tropism: %v", err)
	}
	_, _, err = svc.DefineCultivar(ctx, actor, registry.DefineCultivarInput{
		Name:      "depth-one-worker",
		Version:   1,
		Rootstock: false,
		Tropism:   registry.TropismRef{Name: "depth-one-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/depth-one-worker.md",
			ScopesTemplate: []string{
				"work_items.tree:{root}",
				"work_items.read",
				"feed.read_assigned",
			},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: 1800, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "depth budget test cultivar",
	})
	if err != nil {
		t.Fatalf("define depth-one cultivar: %v", err)
	}
}
