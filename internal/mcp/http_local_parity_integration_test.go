package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// TestLocalAgentHTTPReadSurfaceParityGuardIntegration pins the current
// local-agent HTTP MCP surface for item 4473e765. A local agent token (no
// sealed provider.profile marker) is routed by internal/api/mcp.go's
// providerHTTPProfile onto ProviderSafeReadHTTPProfile, so its HTTP surface is
// capped at the four provider-safe reads even though stdio advertises the full
// read surface. This guard makes the parity gap explicit: if a future slice
// widens the local HTTP surface (the parent item), it must consciously update
// this test rather than drift silently. It also proves that tools denied at the
// HTTP profile boundary append no events. See docs/mcp-parity.md, section
// "Local-Agent HTTP MCP Parity Audit".
func TestLocalAgentHTTPReadSurfaceParityGuardIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "local-parity-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	a, err := workSvc.Create(ctx, workitems.CreateInput{Title: "A", Actor: rootResult.Token})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	a1, err := workSvc.SpawnChild(ctx, a.ID, workitems.CreateInput{
		Title:                      "A1",
		SuggestedConvergenceChecks: []string{"event:local_parity"},
		Actor:                      rootResult.Token,
	})
	if err != nil {
		t.Fatalf("spawn A1: %v", err)
	}

	// Legacy scope-less broad local agent token: source=agent, no scopes, not
	// root -> access.legacyUnscoped grants the full stdio surface.
	broadResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "local-broad-agent",
		Source: domain.SourceAgent,
		Actor:  &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create broad agent token: %v", err)
	}
	broad := broadResult.Token

	s := New(Deps{
		Auth:        authSvc,
		Access:      access.NewService(pool),
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
		Feed:        feed.NewService(pool),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	// stdio: the broad token sees the full read surface plus its permitted
	// mutations.
	if err := s.Authenticate(ctx, broadResult.Secret); err != nil {
		t.Fatalf("authenticate broad agent: %v", err)
	}
	stdioNames := toolListNamesStdio(t, s)
	for _, name := range []string{
		"feed.read", "backlog.readiness", "work_items.list", "work_items.get",
		"registry.list", "registry.get", "projections.list", "projections.get",
		"approvals.get", "approvals.list_for_work_item",
		"deterministic_errors.list", "deterministic_errors.get",
		"work_items.create", "work_items.transition", "convergence.propose_checks",
	} {
		if !stdioNames[name] {
			t.Fatalf("stdio broad surface missing %q: %v", name, stdioNames)
		}
	}

	// HTTP: the same local token gets ProviderSafeReadHTTPProfile (the API's
	// fallback for an unmarked token) and is capped at exactly four reads.
	localProfile := ProviderSafeReadHTTPProfile()
	httpNames := toolListNamesHTTP(t, s, broad, localProfile)
	wantHTTP := map[string]bool{
		"feed.read": true, "backlog.readiness": true,
		"work_items.list": true, "work_items.get": true,
	}
	if len(httpNames) != len(wantHTTP) {
		t.Fatalf("HTTP local surface = %v, want exactly %v", httpNames, wantHTTP)
	}
	for name := range wantHTTP {
		if !httpNames[name] {
			t.Fatalf("HTTP local surface missing %q: %v", name, httpNames)
		}
	}
	for _, denied := range []string{
		"registry.list", "registry.get", "projections.list", "projections.get",
		"approvals.get", "approvals.list_for_work_item",
		"deterministic_errors.list", "deterministic_errors.get",
		"work_items.create", "work_items.spawn_child", "work_items.transition",
		"work_items.append_event", "work_items.update_metadata",
		"convergence.propose_checks", "inbox.capture",
	} {
		if httpNames[denied] {
			t.Fatalf("HTTP local surface leaked %q that stdio-only should keep: %v", denied, httpNames)
		}
	}

	// Denied tools append no events. registry.list is a stdio-visible read;
	// work_items.transition is a mutation. Both must be rejected at the HTTP
	// profile boundary before any handler or event append.
	before := durableEffectCounts(t, ctx, pool)

	deniedRead := callHTTPTool(t, s, broad, localProfile, "registry.list", map[string]any{})
	if deniedRead.TransportError == "" {
		t.Fatalf("registry.list should be rejected at the HTTP profile boundary, got %+v", deniedRead)
	}

	deniedWrite := callHTTPTool(t, s, broad, localProfile, "work_items.transition", map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemRunning),
		"reason":          "should never run over the local HTTP fallback",
		"idempotency_key": "local-http-transition-denied",
	})
	if deniedWrite.TransportError == "" {
		t.Fatalf("work_items.transition should be rejected at the HTTP profile boundary, got %+v", deniedWrite)
	}

	after := durableEffectCounts(t, ctx, pool)
	if after != before {
		t.Fatalf("denied HTTP local-profile calls changed durable state: before=%+v after=%+v", before, after)
	}
}

// TestHTTPPermittedWriteAttributesPerAgentActorIntegration proves item
// 4473e765(2c): events created over an already-permitted HTTP write path carry
// actor_token_id equal to the calling bearer, and two distinct bearers produce
// two distinct actor attributions. The provider-tracker profile is the only
// already-permitted HTTP write path today; local-token writes wait on 95c24a80.
// This locks the per-agent attribution property local agents must inherit once
// HTTP writes are enabled.
func TestHTTPPermittedWriteAttributesPerAgentActorIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "http-attr-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	actorAResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-attr-a", Source: domain.SourceAgent, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor A: %v", err)
	}
	actorBResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-attr-b", Source: domain.SourceAgent, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor B: %v", err)
	}
	actorA := actorAResult.Token
	actorB := actorBResult.Token

	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	profile := ProviderTrackerHTTPProfile()

	baseArgs := func(key string) map[string]any {
		return map[string]any{
			"title":                        "HTTP attribution item",
			"body":                         "safe coordination only",
			"human_review_status":          "blocked",
			"suggested_convergence_checks": []string{"event:owner_tracker_reviewed"},
			"idempotency_key":              key,
		}
	}

	aResult := callHTTPTool(t, s, actorA, profile, "work_items.create", baseArgs("attr-a-key"))
	if aResult.IsError || aResult.TransportError != "" {
		t.Fatalf("actor A create failed: %+v", aResult)
	}
	aItem := createdWorkItemID(t, aResult.Text)
	if got := actorForSubject(t, pool, domain.EventWorkItemCreated, aItem); got != actorA.ID {
		t.Fatalf("actor A create actor_token_id = %s, want %s", got, actorA.ID)
	}

	bResult := callHTTPTool(t, s, actorB, profile, "work_items.create", baseArgs("attr-b-key"))
	if bResult.IsError || bResult.TransportError != "" {
		t.Fatalf("actor B create failed: %+v", bResult)
	}
	bItem := createdWorkItemID(t, bResult.Text)
	if got := actorForSubject(t, pool, domain.EventWorkItemCreated, bItem); got != actorB.ID {
		t.Fatalf("actor B create actor_token_id = %s, want %s", got, actorB.ID)
	}

	if aItem == bItem {
		t.Fatalf("distinct bearers collapsed to one work item %s", aItem)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 2 {
		t.Fatalf("per-agent creates produced %d work_item.created events, want 2", got)
	}
}

func toolListNamesStdio(t *testing.T, s *Server) map[string]bool {
	t.Helper()
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if resp.Error != nil {
		t.Fatalf("stdio tools/list error: %+v", resp.Error)
	}
	return decodeToolListNames(t, resp.Result)
}

func toolListNamesHTTP(t *testing.T, s *Server, actor domain.Token, profile *HTTPToolProfile) map[string]bool {
	t.Helper()
	resp := s.HandleHTTPMessageWithOptions(
		context.Background(),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`),
		actor,
		HTTPOptions{Profile: profile},
	)
	var envelope struct {
		Error  *rpcError       `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &envelope); err != nil {
		t.Fatalf("decode HTTP tools/list: %v body=%s", err, resp.Body)
	}
	if envelope.Error != nil {
		t.Fatalf("HTTP tools/list error: %+v", envelope.Error)
	}
	return decodeToolListNames(t, envelope.Result)
}

func decodeToolListNames(t *testing.T, raw json.RawMessage) map[string]bool {
	t.Helper()
	var body struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode tools list: %v raw=%s", err, raw)
	}
	names := make(map[string]bool, len(body.Tools))
	for _, tool := range body.Tools {
		names[tool.Name] = true
	}
	return names
}

func createdWorkItemID(t *testing.T, text string) uuid.UUID {
	t.Helper()
	var payload struct {
		WorkItemID uuid.UUID `json:"work_item_id"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("decode create result %q: %v", text, err)
	}
	if payload.WorkItemID == uuid.Nil {
		t.Fatalf("create result missing work_item_id: %q", text)
	}
	return payload.WorkItemID
}

func actorForSubject(t *testing.T, pool *pgxpool.Pool, kind string, subject uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT actor_token_id FROM events WHERE kind = $1 AND subject_id = $2
	`, kind, subject).Scan(&id); err != nil {
		t.Fatalf("actor for %s subject %s: %v", kind, subject, err)
	}
	return id
}
