package mcp

import (
	"context"
	"encoding/json"
	"reflect"
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

// TestLocalAgentHTTPMatchesStdioSurfaceAndScopedAccessIntegration proves that
// an unmarked local bearer uses the ordinary MCP surface over HTTP. Tool
// visibility and object access remain the same deterministic reducers used by
// stdio; only sealed provider profiles select the provider-safe boundary.
func TestLocalAgentHTTPMatchesStdioSurfaceAndScopedAccessIntegration(t *testing.T) {
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

	broadResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "local-broad-agent",
		Source: domain.SourceAgent,
		Actor:  &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create broad agent token: %v", err)
	}
	broad := broadResult.Token
	scopedResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "local-scoped-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + a.ID.String(),
		},
		Actor: &rootResult.Token,
	})
	if err != nil {
		t.Fatalf("create scoped agent token: %v", err)
	}
	scoped := scopedResult.Token

	s := New(Deps{
		Auth:        authSvc,
		Access:      access.NewService(pool),
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
		Feed:        feed.NewService(pool),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	for _, tc := range []struct {
		name   string
		secret string
		actor  domain.Token
	}{
		{name: "broad", secret: broadResult.Secret, actor: broad},
		{name: "scoped", secret: scopedResult.Secret, actor: scoped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.Authenticate(ctx, tc.secret); err != nil {
				t.Fatalf("authenticate %s agent: %v", tc.name, err)
			}
			stdioNames := toolListNamesStdio(t, s)
			httpNames := toolListNamesHTTP(t, s, tc.actor, nil)
			if !reflect.DeepEqual(httpNames, stdioNames) {
				t.Fatalf("local HTTP/stdio tool mismatch:\nHTTP  %v\nstdio %v", httpNames, stdioNames)
			}
		})
	}

	broadHTTP := toolListNamesHTTP(t, s, broad, nil)
	for _, name := range []string{"registry.list", "approvals.get", "work_items.create", "work_items.transition"} {
		if !broadHTTP[name] {
			t.Fatalf("broad local HTTP surface missing stdio-visible tool %q: %v", name, broadHTTP)
		}
	}
	scopedHTTP := toolListNamesHTTP(t, s, scoped, nil)
	if !scopedHTTP["work_items.get"] || !scopedHTTP["work_items.transition"] || scopedHTTP["work_items.create"] {
		t.Fatalf("scoped local HTTP surface did not preserve scope policy: %v", scopedHTTP)
	}
	inTree := callHTTPTool(t, s, scoped, nil, "work_items.get", map[string]any{"id": a1.ID.String()})
	if inTree.IsError || inTree.TransportError != "" {
		t.Fatalf("scoped local HTTP could not read its assigned child: %+v", inTree)
	}

	// Local HTTP returns the same ordinary operator DTO as stdio, not the
	// provider-safe structural projection.
	if err := s.Authenticate(ctx, broadResult.Secret); err != nil {
		t.Fatalf("reauthenticate broad agent: %v", err)
	}
	stdioErr, stdioText := callToolForTest(t, s, "work_items.get", map[string]any{"id": a.ID.String()})
	httpGet := callHTTPTool(t, s, broad, nil, "work_items.get", map[string]any{"id": a.ID.String()})
	if stdioErr || httpGet.IsError || httpGet.TransportError != "" {
		t.Fatalf("ordinary get failed: stdio_error=%v http=%+v", stdioErr, httpGet)
	}
	if httpGet.Text != stdioText || !strings.Contains(httpGet.Text, `"created_by"`) || strings.Contains(httpGet.Text, ProviderSafeWorkItemsContract) {
		t.Fatalf("local HTTP did not preserve stdio DTO:\nHTTP  %s\nstdio %s", httpGet.Text, stdioText)
	}

	// A tool hidden by the scoped policy is refused before idempotency or a
	// handler can append anything.
	before := durableEffectCounts(t, ctx, pool)
	deniedByScope := callHTTPTool(t, s, scoped, nil, "work_items.create", map[string]any{
		"title":           "must not be created",
		"idempotency_key": "local-http-create-denied",
	})
	if !deniedByScope.IsError || !strings.Contains(deniedByScope.Text, "insufficient_scope") {
		t.Fatalf("scoped create was not denied by ordinary tool policy: %+v", deniedByScope)
	}
	after := durableEffectCounts(t, ctx, pool)
	if after != before {
		t.Fatalf("scope-denied local HTTP call changed durable state: before=%+v after=%+v", before, after)
	}

	// An advertised tree-scoped mutation still rechecks object access. Its
	// replayable refusal may be cached, but no domain transition event occurs.
	beforeTransitions := eventCount(t, pool, domain.EventWorkItemTransitioned)
	outOfTree := callHTTPTool(t, s, scoped, nil, "work_items.transition", map[string]any{
		"id":              b.ID.String(),
		"to":              string(domain.WorkItemRunning),
		"reason":          "outside assigned tree",
		"idempotency_key": "local-http-out-of-tree",
	})
	if !outOfTree.IsError || !strings.Contains(outOfTree.Text, "not found") {
		t.Fatalf("out-of-tree local HTTP transition was not hidden: %+v", outOfTree)
	}
	if got := eventCount(t, pool, domain.EventWorkItemTransitioned); got != beforeTransitions {
		t.Fatalf("out-of-tree local HTTP call appended transition event: before=%d after=%d", beforeTransitions, got)
	}
}

// TestLocalHTTPMutationIdempotencyAndPerBearerAttributionIntegration proves
// that ordinary HTTP mutations use the stdio idempotency executor and the
// authenticated request actor. Replays collapse per bearer while the same key
// used by two distinct local sessions remains two distinctly attributed acts.
func TestLocalHTTPMutationIdempotencyAndPerBearerAttributionIntegration(t *testing.T) {
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

	baseArgs := map[string]any{
		"title":                        "Local HTTP attribution item",
		"body":                         "ordinary local coordination",
		"human_review_status":          "blocked",
		"suggested_convergence_checks": []string{"event:local_http_reviewed"},
		"idempotency_key":              "local-http-same-key",
	}

	first := callHTTPTool(t, s, actorA, nil, "work_items.create", baseArgs)
	if first.IsError || first.TransportError != "" {
		t.Fatalf("actor A create failed: %+v", first)
	}
	replay := callHTTPTool(t, s, actorA, nil, "work_items.create", baseArgs)
	if replay.IsError || replay.TransportError != "" || replay.Text != first.Text {
		t.Fatalf("actor A replay did not return the recorded result: first=%+v replay=%+v", first, replay)
	}
	if !strings.Contains(first.Text, `"created_by"`) || strings.Contains(first.Text, ProviderSafeWorkItemsContract) {
		t.Fatalf("local mutation returned provider-safe rather than ordinary DTO: %s", first.Text)
	}
	aItem := createdWorkItemID(t, first.Text)
	if got := actorForSubject(t, pool, domain.EventWorkItemCreated, aItem); got != actorA.ID {
		t.Fatalf("actor A create actor_token_id = %s, want %s", got, actorA.ID)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 1 {
		t.Fatalf("actor A replay produced %d create events, want 1", got)
	}

	conflicting := cloneHTTPArgs(baseArgs)
	conflicting["title"] = "different local HTTP arguments"
	conflict := callHTTPTool(t, s, actorA, nil, "work_items.create", conflicting)
	if !conflict.IsError || !strings.Contains(conflict.Text, "idempotency_key_conflict") {
		t.Fatalf("same local actor/key with different args did not conflict: %+v", conflict)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 1 {
		t.Fatalf("conflicting local request changed create count: %d", got)
	}

	second := callHTTPTool(t, s, actorB, nil, "work_items.create", baseArgs)
	if second.IsError || second.TransportError != "" {
		t.Fatalf("actor B create failed: %+v", second)
	}
	bItem := createdWorkItemID(t, second.Text)
	if got := actorForSubject(t, pool, domain.EventWorkItemCreated, bItem); got != actorB.ID {
		t.Fatalf("actor B create actor_token_id = %s, want %s", got, actorB.ID)
	}
	if aItem == bItem {
		t.Fatalf("distinct local bearers collapsed to one work item %s", aItem)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 2 {
		t.Fatalf("distinct local bearers produced %d create events, want 2", got)
	}
	if first.Text == second.Text {
		t.Fatalf("distinct local bearers received the same work item response: %s", first.Text)
	}
	if actorA.ID == actorB.ID {
		t.Fatal("test setup reused one actor token for distinct sessions")
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
