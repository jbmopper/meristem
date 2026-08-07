package mcp

import (
	"context"
	"encoding/json"
	"strings"
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

// TestLocalAgentHTTPSurfaceAndIdempotencyParityIntegration proves that an
// explicitly local-profiled actor receives the same scope-derived tools and
// ordinary DTOs over stdio and HTTP. Canonical and cursor aliases share one
// canonical idempotency identity; changed-body reuse conflicts without a
// second authoritative event.
func TestLocalAgentHTTPSurfaceAndIdempotencyParityIntegration(t *testing.T) {
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
	b, err := workSvc.Create(ctx, workitems.CreateInput{Title: "B", Actor: rootResult.Token})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	b1, err := workSvc.SpawnChild(ctx, b.ID, workitems.CreateInput{Title: "B1", Actor: rootResult.Token})
	if err != nil {
		t.Fatalf("spawn B1: %v", err)
	}

	// Explicit local profile plus explicit business scopes: the profile changes
	// presentation only and cannot revive the legacy empty-scope shortcut.
	broadResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "local-broad-agent",
		Scopes: []string{
			access.ScopeMCPLocalAgentProfileV1,
			access.ScopeFeedRead,
			access.ScopeWorkItemsReadAll,
			access.ScopeWorkItemsWriteAll,
			access.ScopeWorkItemsCreate,
		},
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

	// Stdio and HTTP both derive the surface from the same explicit scopes.
	if err := s.Authenticate(ctx, broadResult.Secret); err != nil {
		t.Fatalf("authenticate broad agent: %v", err)
	}
	stdioNames := toolListNamesStdio(t, s)
	for _, name := range []string{
		"feed.read", "backlog.readiness", "work_items.list", "work_items.get",
		"registry.list", "registry.get", "projections.list", "projections.get",
		"approvals.get", "approvals.list_for_work_item",
		"work_items.create", "work_items.transition", "convergence.propose_checks",
	} {
		if !stdioNames[name] {
			t.Fatalf("stdio broad surface missing %q: %v", name, stdioNames)
		}
	}

	localProfile := LocalAgentHTTPProfile()
	httpNames := toolListNamesHTTP(t, s, broad, localProfile)
	if len(httpNames) != len(stdioNames) {
		t.Fatalf("HTTP local surface count=%d, stdio=%d; HTTP=%v stdio=%v", len(httpNames), len(stdioNames), httpNames, stdioNames)
	}
	for name := range stdioNames {
		if !httpNames[name] {
			t.Fatalf("HTTP local surface missing stdio tool %q: %v", name, httpNames)
		}
	}

	ordinary := callHTTPTool(t, s, broad, localProfile, "work_items.get", map[string]any{"id": a1.ID.String()})
	if ordinary.IsError || ordinary.TransportError != "" || !strings.Contains(ordinary.Text, "created_by") || strings.Contains(ordinary.Text, ProviderSafeWorkItemsContract) {
		t.Fatalf("local HTTP did not retain the ordinary work-item DTO: %+v", ordinary)
	}

	args := map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemTriaged),
		"reason":          "local HTTP canonical mutation",
		"idempotency_key": "local-http-transition",
	}
	first := callHTTPTool(t, s, broad, localProfile, "work_items.transition", args)
	aliasReplay := callHTTPTool(t, s, broad, localProfile, "work_items_transition", args)
	if first.IsError || first.TransportError != "" || aliasReplay.IsError || aliasReplay.TransportError != "" || first.Text != aliasReplay.Text {
		t.Fatalf("canonical/alias replay mismatch: first=%+v replay=%+v", first, aliasReplay)
	}
	changed := map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemTriaged),
		"reason":          "changed body under reused key",
		"idempotency_key": "local-http-transition",
	}
	conflict := callHTTPTool(t, s, broad, localProfile, "work_items.transition", changed)
	if !conflict.IsError || !strings.Contains(conflict.Text, "idempotency") {
		t.Fatalf("changed-body reuse did not conflict: %+v", conflict)
	}
	if got := eventCount(t, pool, domain.EventWorkItemTransitioned); got != 1 {
		t.Fatalf("canonical/alias replay plus changed-body conflict appended %d transition events, want 1", got)
	}

	// Concurrent first-seen calls through the two spellings serialize on the
	// same canonical identity. Exactly one handler may append the transition.
	concurrentArgs := map[string]any{
		"id":              b1.ID.String(),
		"to":              string(domain.WorkItemTriaged),
		"reason":          "local HTTP concurrent mutation",
		"idempotency_key": "local-http-concurrent-transition",
	}
	type concurrentCallResult struct {
		name   string
		result httpToolCallResult
		err    error
	}
	results := make(chan concurrentCallResult, 2)
	for _, name := range []string{"work_items.transition", "work_items_transition"} {
		name := name
		go func() {
			result, err := doHTTPToolCall(s, broad, localProfile, name, concurrentArgs)
			results <- concurrentCallResult{name: name, result: result, err: err}
		}()
	}
	concurrentFirst, concurrentSecond := <-results, <-results
	if concurrentFirst.err != nil || concurrentSecond.err != nil {
		t.Fatalf("concurrent HTTP MCP helper failure: first %s: %v; second %s: %v", concurrentFirst.name, concurrentFirst.err, concurrentSecond.name, concurrentSecond.err)
	}
	if concurrentFirst.result.IsError || concurrentFirst.result.TransportError != "" ||
		concurrentSecond.result.IsError || concurrentSecond.result.TransportError != "" ||
		concurrentFirst.result.Text != concurrentSecond.result.Text {
		t.Fatalf("concurrent canonical/alias replay mismatch: first=%+v second=%+v", concurrentFirst, concurrentSecond)
	}
	if got := eventCount(t, pool, domain.EventWorkItemTransitioned); got != 2 {
		t.Fatalf("concurrent canonical/alias mutation produced %d total transition events, want 2", got)
	}

	treeResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "local-tree-agent",
		Scopes: []string{
			access.ScopeMCPLocalAgentProfileV1,
			access.ScopeFeedReadAssigned,
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.WorkItemTreeScope(a.ID),
		},
		Source: domain.SourceAgent,
		Actor:  &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create tree local agent: %v", err)
	}
	if err := s.Authenticate(ctx, treeResult.Secret); err != nil {
		t.Fatalf("authenticate tree local agent: %v", err)
	}
	treeStdio := toolListNamesStdio(t, s)
	treeHTTP := toolListNamesHTTP(t, s, treeResult.Token, localProfile)
	if len(treeHTTP) != len(treeStdio) {
		t.Fatalf("tree-scoped HTTP/stdin surface count differs: HTTP=%v stdio=%v", treeHTTP, treeStdio)
	}
	for name := range treeStdio {
		if !treeHTTP[name] {
			t.Fatalf("tree-scoped HTTP missing stdio tool %q: %v", name, treeHTTP)
		}
	}
	if treeHTTP["work_items.create"] || !treeHTTP["work_items.get"] || !treeHTTP["work_items.transition"] {
		t.Fatalf("tree-scoped surface did not remain narrowed: %v", treeHTTP)
	}
	inside := callHTTPTool(t, s, treeResult.Token, localProfile, "work_items.get", map[string]any{"id": a1.ID.String()})
	outside := callHTTPTool(t, s, treeResult.Token, localProfile, "work_items.get", map[string]any{"id": b1.ID.String()})
	if inside.IsError || inside.TransportError != "" || !outside.IsError {
		t.Fatalf("tree object boundary failed: inside=%+v outside=%+v", inside, outside)
	}
	beforeDenied := eventCount(t, pool, domain.EventWorkItemTransitioned)
	deniedWrite := callHTTPTool(t, s, treeResult.Token, localProfile, "work_items.transition", map[string]any{
		"id":              b1.ID.String(),
		"to":              string(domain.WorkItemPlanned),
		"reason":          "must remain outside tree",
		"idempotency_key": "local-http-outside-tree",
	})
	if !deniedWrite.IsError || deniedWrite.TransportError != "" {
		t.Fatalf("tree-scoped HTTP mutation escaped object boundary: %+v", deniedWrite)
	}
	if afterDenied := eventCount(t, pool, domain.EventWorkItemTransitioned); afterDenied != beforeDenied {
		t.Fatalf("denied tree-scoped HTTP mutation appended an event: before=%d after=%d", beforeDenied, afterDenied)
	}
}

// TestHTTPPermittedWriteAttributesPerAgentActorIntegration proves that events
// created over the local HTTP write path carry actor_token_id equal to the
// calling bearer and two distinct bearers retain distinct attribution.
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
	localScopes := []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeWorkItemsWriteAll, access.ScopeWorkItemsReadAll, access.ScopeWorkItemsCreate}
	actorAResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-attr-a", Source: domain.SourceAgent, Scopes: localScopes, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor A: %v", err)
	}
	actorBResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-attr-b", Source: domain.SourceAgent, Scopes: localScopes, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor B: %v", err)
	}
	actorA := actorAResult.Token
	actorB := actorBResult.Token

	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		Access:      access.NewService(pool),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	profile := LocalAgentHTTPProfile()

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
