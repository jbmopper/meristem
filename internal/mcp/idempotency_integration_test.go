package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func currentMCPBuildStatus() buildguard.Status {
	const commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return buildguard.Status{
		State:            buildguard.StateCurrent,
		CompiledCommit:   commit,
		ExpectedCommit:   commit,
		CompiledMetadata: buildguard.CompiledValid,
		Reason:           "compiled commit matches the reviewed v1 pin",
	}
}

func mismatchedMCPBuildStatus() buildguard.Status {
	return buildguard.Status{
		State:            buildguard.StateMismatch,
		CompiledCommit:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpectedCommit:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CompiledMetadata: buildguard.CompiledValid,
		Reason:           "compiled commit does not match the reviewed v1 pin",
	}
}

func TestMCPAdmittedMutationResponseSurvivesPinAdvance(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const (
		compiled = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		advanced = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	status := buildguard.Status{
		State:            buildguard.StateCurrent,
		CompiledCommit:   compiled,
		ExpectedCommit:   compiled,
		CompiledMetadata: buildguard.CompiledValid,
		Reason:           "compiled commit matches the reviewed v1 pin",
	}
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	writer := app.NewGuardedEventWriter(provider)
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-build-post-dispatch")
	s := New(Deps{
		Idempotency: idempotency.NewMiddlewareWithGuard(pool, writer, func() error {
			return buildguard.RequireNonBlocking(provider)
		}),
	}, ServerInfo{Name: "meristem-test", Version: "test", BuildStatus: provider}, nil)
	s.actor = actor
	addTestMutationTool(s, Tool{
		Name:    "test.build_flip",
		Mutates: true,
		Handler: func(context.Context, domain.Token, json.RawMessage) (any, error) {
			status = buildguard.Status{
				State:            buildguard.StateMismatch,
				CompiledCommit:   compiled,
				ExpectedCommit:   advanced,
				CompiledMetadata: buildguard.CompiledValid,
				Reason:           "compiled commit does not match the reviewed v1 pin",
			}
			return map[string]any{"admitted_payload": true}, nil
		},
	})

	isError, text := callToolForTest(t, s, "test.build_flip", map[string]any{"idempotency_key": "flip-in-flight"})
	if isError || text != `{"admitted_payload":true}` {
		t.Fatalf("admitted mutation response was stranded: isError=%t text=%q", isError, text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.build_flip", "flip-in-flight"); got != 0 {
		t.Fatalf("advanced pin allowed an idempotency record: got %d rows", got)
	}
}

func TestMCPSemanticMutationErrorBecomesBuildPinDuringIdempotencyRecord(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	statusCalls := 0
	provider := buildguard.ProviderFunc(func() buildguard.Status {
		statusCalls++
		// Entry, idempotency boundaries, and the post-handler semantic-error
		// check are current. The guarded idempotency event append observes the
		// newly advanced pin.
		if statusCalls <= 5 {
			return currentMCPBuildStatus()
		}
		return mismatchedMCPBuildStatus()
	})
	writer := app.NewGuardedEventWriter(provider)
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-build-error")
	s := New(Deps{
		Idempotency: idempotency.NewMiddlewareWithGuard(pool, writer, func() error {
			return buildguard.RequireNonBlocking(provider)
		}),
	}, ServerInfo{Name: "meristem-test", Version: "test", BuildStatus: provider}, nil)
	s.actor = actor
	addTestMutationTool(s, Tool{
		Name:    "test.build_error",
		Mutates: true,
		Handler: func(context.Context, domain.Token, json.RawMessage) (any, error) {
			return nil, replayableToolErr(errors.New("must-not-escape"))
		},
	})

	isError, text := callToolForTest(t, s, "test.build_error", map[string]any{"idempotency_key": "error-in-flight"})
	if !isError || !strings.Contains(text, "build_pin") {
		t.Fatalf("pin change did not replace mutation error: isError=%t text=%q", isError, text)
	}
	if strings.Contains(text, "must-not-escape") {
		t.Fatalf("stale mutation error escaped after pin change: %q", text)
	}
	if statusCalls < 6 {
		t.Fatalf("test did not advance the pin at the idempotency-record boundary: calls=%d", statusCalls)
	}
}

func TestMCPMutationInfrastructureErrorIsNotCached(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-infra-error")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actor

	calls := 0
	addTestMutationTool(s, Tool{
		Name:    "test.mutate",
		Mutates: true,
		Handler: func(context.Context, domain.Token, json.RawMessage) (any, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("projector failed: missing state_entered_at")
			}
			return map[string]any{"calls": calls}, nil
		},
	})

	args := map[string]any{"idempotency_key": "infra-retry"}
	if isError, text := callToolForTest(t, s, "test.mutate", args); !isError || text != "projector failed: missing state_entered_at" {
		t.Fatalf("first call should return uncached infrastructure tool error, isError=%t text=%q", isError, text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.mutate", "infra-retry"); got != 0 {
		t.Fatalf("infrastructure failure was recorded in idempotency_keys: got %d rows", got)
	}

	if isError, text := callToolForTest(t, s, "test.mutate", args); isError || text != `{"calls":2}` {
		t.Fatalf("retry should re-execute and succeed, isError=%t text=%q", isError, text)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.mutate", "infra-retry"); got != 1 {
		t.Fatalf("successful retry should record one idempotency row, got %d", got)
	}
}

func TestMCPMutationInfrastructureNotFoundErrorIsNotCached(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-infra-not-found")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actor

	calls := 0
	addTestMutationTool(s, Tool{
		Name:    "test.not_found_infra",
		Mutates: true,
		Handler: func(context.Context, domain.Token, json.RawMessage) (any, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("pgx: prepared statement not found")
			}
			return map[string]any{"calls": calls}, nil
		},
	})

	args := map[string]any{"idempotency_key": "infra-not-found-retry"}
	if isError, text := callToolForTest(t, s, "test.not_found_infra", args); !isError || text != "pgx: prepared statement not found" {
		t.Fatalf("first call should return uncached infrastructure not-found error, isError=%t text=%q", isError, text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.not_found_infra", "infra-not-found-retry"); got != 0 {
		t.Fatalf("infrastructure not-found failure was recorded in idempotency_keys: got %d rows", got)
	}

	if isError, text := callToolForTest(t, s, "test.not_found_infra", args); isError || text != `{"calls":2}` {
		t.Fatalf("retry should re-execute and succeed, isError=%t text=%q", isError, text)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestMCPMutationSemanticToolErrorDoesNotConsumeKey(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-semantic-error")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actor

	// The rejection re-derives on every same-body retry instead of pinning.
	invalid := map[string]any{"idempotency_key": "semantic-keep-key", "title": ""}
	for i := 0; i < 2; i++ {
		if isError, text := callToolForTest(t, s, "work_items.create", invalid); !isError || text != "workitems: title is required" {
			t.Fatalf("call %d should return the semantic tool error, isError=%t text=%q", i+1, isError, text)
		}
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.create", "semantic-keep-key"); got != 0 {
		t.Fatalf("semantic error must not record an idempotency row, got %d", got)
	}

	// The defect scenario: the corrected body reuses the SAME key and must
	// execute, not surface a same-key/different-body conflict.
	corrected := map[string]any{"idempotency_key": "semantic-keep-key", "title": "recovered after rejection"}
	if isError, text := callToolForTest(t, s, "work_items.create", corrected); isError {
		t.Fatalf("corrected retry under the same key should execute, got tool error: %q", text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.create", "semantic-keep-key"); got != 1 {
		t.Fatalf("corrected retry should record one idempotency row, got %d", got)
	}

	// The committed conclusion still replays byte-for-byte...
	_, first := callToolForTest(t, s, "work_items.create", corrected)
	if isError, text := callToolForTest(t, s, "work_items.create", corrected); isError || text != first {
		t.Fatalf("committed success should replay identically, isError=%t text=%q want=%q", isError, text, first)
	}
	// ...and the original invalid body now conflicts with the recorded one.
	if isError, text := callToolForTest(t, s, "work_items.create", invalid); !isError || !strings.Contains(text, "idempotency_key_conflict") {
		t.Fatalf("invalid body after commit should conflict, isError=%t text=%q", isError, text)
	}
}

func TestMCPMutationSemanticWorkItemNotFoundDoesNotConsumeKey(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-semantic-not-found")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actor

	// The fleet's recurring shape of this defect: a fabricated/mistyped item
	// UUID rejects as not-found, and the corrected retry must be able to
	// reuse the burned key.
	missingID := uuid.New()
	args := map[string]any{
		"idempotency_key": "semantic-not-found-keep-key",
		"id":              missingID.String(),
		"to":              string(domain.WorkItemTriaged),
	}
	want := "work item " + missingID.String() + " not found"
	for i := 0; i < 2; i++ {
		if isError, text := callToolForTest(t, s, "work_items.transition", args); !isError || text != want {
			t.Fatalf("call %d should return the semantic not-found tool error, isError=%t text=%q", i+1, isError, text)
		}
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.transition", "semantic-not-found-keep-key"); got != 0 {
		t.Fatalf("semantic not-found error must not record an idempotency row, got %d", got)
	}

	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "real transition target",
		Actor: actor,
	})
	if err != nil {
		t.Fatalf("create work item: %v", err)
	}
	corrected := map[string]any{
		"idempotency_key": "semantic-not-found-keep-key",
		"id":              item.ID.String(),
		"to":              string(domain.WorkItemTriaged),
	}
	if isError, text := callToolForTest(t, s, "work_items.transition", corrected); isError {
		t.Fatalf("corrected transition under the same key should execute, got tool error: %q", text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.transition", "semantic-not-found-keep-key"); got != 1 {
		t.Fatalf("corrected transition should record one idempotency row, got %d", got)
	}
}

func createMCPTestActor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, name string) domain.Token {
	t.Helper()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name + "-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	result, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   name,
		Source: domain.SourceAgent,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	return result.Token
}

func addTestMutationTool(s *Server, tool Tool) {
	s.tools = append(s.tools, tool)
	s.toolsByName[tool.Name] = tool
	s.toolsByName[cursorToolName(tool.Name)] = tool
}

func idempotencyKeyCount(t *testing.T, pool *pgxpool.Pool, tokenID uuid.UUID, scope, key string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM idempotency_keys
		WHERE token_id = $1 AND scope = $2 AND key = $3
	`, tokenID, scope, key).Scan(&count); err != nil {
		t.Fatalf("count idempotency keys: %v", err)
	}
	return count
}

// TestMCPStatefulRefusalConsumesKey is the MCP twin of the REST
// TestStatefulRefusalConsumesKeyAndReplays (review finding IDEM-B1): a xylem
// budget refusal commits xylem.exhausted + escalation before returning, so
// its 422-class tool error must be recorded — the same key replays the
// refusal without a second append, and changed arguments conflict.
func TestMCPStatefulRefusalConsumesKey(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actorRoot, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "mcp-stateful-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	regSvc := registry.NewService(pool, writer)
	if _, _, err := regSvc.DefineTropism(ctx, actorRoot.Token, registry.DefineTropismInput{
		Name: "mcp-single-child-checklist", Version: 1,
		Reducer:     registry.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "mcp single child budget tropism",
	}); err != nil {
		t.Fatalf("define tropism: %v", err)
	}
	if _, _, err := regSvc.DefineCultivar(ctx, actorRoot.Token, registry.DefineCultivarInput{
		Name: "mcp-single-child-worker", Version: 1,
		Tropism: registry.TropismRef{Name: "mcp-single-child-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing:       "briefings/mcp-single-child-worker.md",
			ScopesTemplate: []string{"work_items.tree:{root}", "work_items.read", "work_items.write", "feed.read_assigned"},
		},
		Xylem:       registry.Xylem{MaxAttempts: 3, MaxWallSeconds: 1800, MaxDepth: 1, MaxChildrenPerItem: 1},
		Phloem:      "projection:work-item-brief",
		Description: "mcp single child budget cultivar",
	}); err != nil {
		t.Fatalf("define cultivar: %v", err)
	}

	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "mcp stateful parent", Actor: actorRoot.Token, Cultivar: "mcp-single-child-worker@1",
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actorRoot.Token

	first := map[string]any{"idempotency_key": "mcp-first-child", "parent_id": parent.ID.String(), "title": "first child"}
	if isError, text := callToolForTest(t, s, "work_items.spawn_child", first); isError {
		t.Fatalf("first spawn errored: %s", text)
	}

	countEvents := func(kind string) int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", kind, err)
		}
		return n
	}

	overBudget := map[string]any{"idempotency_key": "mcp-stateful-409", "parent_id": parent.ID.String(), "title": "second child"}
	isError, text := callToolForTest(t, s, "work_items.spawn_child", overBudget)
	if !isError || !strings.Contains(text, "xylem budget exhausted") {
		t.Fatalf("over-budget spawn should refuse with the budget error, isError=%t text=%q", isError, text)
	}
	if got := countEvents(string(domain.EventXylemExhausted)); got != 1 {
		t.Fatalf("expected one xylem.exhausted after refusal, got %d", got)
	}
	if got := idempotencyKeyCount(t, pool, actorRoot.Token.ID, "MCP:work_items.spawn_child", "mcp-stateful-409"); got != 1 {
		t.Fatalf("stateful refusal must consume its key, got %d rows", got)
	}

	// Same key, same args: replay the recorded refusal, append nothing new.
	if isError, text = callToolForTest(t, s, "work_items.spawn_child", overBudget); !isError || !strings.Contains(text, "xylem budget exhausted") {
		t.Fatalf("stateful refusal retry should replay, isError=%t text=%q", isError, text)
	}
	if got := countEvents(string(domain.EventXylemExhausted)); got != 1 {
		t.Fatalf("replayed refusal appended a second xylem.exhausted: %d", got)
	}

	// Same key, changed args: conflict, still exactly one refusal set.
	renamed := map[string]any{"idempotency_key": "mcp-stateful-409", "parent_id": parent.ID.String(), "title": "second child renamed"}
	if isError, text = callToolForTest(t, s, "work_items.spawn_child", renamed); !isError || !strings.Contains(text, "idempotency_key_conflict") {
		t.Fatalf("changed args under a consumed key should conflict, isError=%t text=%q", isError, text)
	}
	if got := countEvents(string(domain.EventXylemExhausted)); got != 1 {
		t.Fatalf("conflicting reuse appended a second xylem.exhausted: %d", got)
	}
}

// TestMCPUnmarkedSemanticLookingRefusalIsRecorded pins review finding
// IDEM-B2: cache disposition depends only on the explicit typed pure
// markers, never on message text. An unmarked replayable refusal whose text
// LOOKS like validation ("payload: ...") must be conservatively recorded —
// same args replay it, changed args conflict — because for all the
// middleware knows it committed state before refusing.
func TestMCPUnmarkedSemanticLookingRefusalIsRecorded(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-unmarked-semantic")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = actor

	calls := 0
	addTestMutationTool(s, Tool{
		Name:    "test.unmarked_semantic",
		Mutates: true,
		Handler: func(context.Context, domain.Token, json.RawMessage) (any, error) {
			calls++
			// The review's probe: semantic-looking text, no pure marker.
			return nil, replayableToolErr(errors.New("payload: stateful refusal after commit"))
		},
	})

	args := map[string]any{"idempotency_key": "unmarked-semantic", "note": "a"}
	if isError, text := callToolForTest(t, s, "test.unmarked_semantic", args); !isError || text != "payload: stateful refusal after commit" {
		t.Fatalf("first call should return the refusal, isError=%t text=%q", isError, text)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.unmarked_semantic", "unmarked-semantic"); got != 1 {
		t.Fatalf("unmarked semantic-looking refusal must be recorded, got %d rows", got)
	}

	// Same args: replayed from the cache, handler NOT re-executed.
	if isError, text := callToolForTest(t, s, "test.unmarked_semantic", args); !isError || text != "payload: stateful refusal after commit" {
		t.Fatalf("retry should replay the refusal, isError=%t text=%q", isError, text)
	}
	if calls != 1 {
		t.Fatalf("recorded refusal was re-executed: %d handler calls", calls)
	}

	// Changed args under the consumed key: conflict, not execution.
	changed := map[string]any{"idempotency_key": "unmarked-semantic", "note": "b"}
	if isError, text := callToolForTest(t, s, "test.unmarked_semantic", changed); !isError || !strings.Contains(text, "idempotency_key_conflict") {
		t.Fatalf("changed args should conflict, isError=%t text=%q", isError, text)
	}
	if calls != 1 {
		t.Fatalf("conflicting reuse re-executed the handler: %d calls", calls)
	}
}

// TestMCPStatefulRefusalRecordSurvivesPinAdvance pins review finding
// IDEM-B4 on the Execute path: a stateful refusal that committed
// authoritative events while admitted/current must get its idempotency
// record durably written even when the reviewed pin advances before the
// record — otherwise, after cutover, the same key with changed arguments
// executes again and appends a second authoritative event set. The stale
// process still refuses outward with build_pin.
func TestMCPStatefulRefusalRecordSurvivesPinAdvance(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	status := currentMCPBuildStatus()
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	writer := app.NewGuardedEventWriter(provider)
	actor := createMCPTestActor(t, ctx, pool, writer, "mcp-cutover-stateful")
	s := New(Deps{
		Idempotency: idempotency.NewMiddlewareWithGuard(pool, writer, func() error {
			return buildguard.RequireNonBlocking(provider)
		}),
	}, ServerInfo{Name: "meristem-test", Version: "test", BuildStatus: provider}, nil)
	s.actor = actor

	subject := uuid.New()
	calls := 0
	addTestMutationTool(s, Tool{
		Name:    "test.cutover_stateful",
		Mutates: true,
		Handler: func(callCtx context.Context, _ domain.Token, _ json.RawMessage) (any, error) {
			calls++
			// Commit an authoritative event while the build is current...
			tx, err := pool.Begin(callCtx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() { _ = tx.Rollback(callCtx) }()
			actorID := actor.ID
			if _, _, err := writer.Append(callCtx, tx, events.Spec{
				SubjectKind: domain.SubjectWorkItem, SubjectID: subject,
				Kind: domain.EventWorkItemEventAppended, Source: domain.SourceAgent,
				ActorTokenID: &actorID,
				Payload:      map[string]any{"inner_kind": "test.cutover_refusal", "inner": map[string]any{"attempt": calls}},
			}); err != nil {
				t.Fatalf("append while current: %v", err)
			}
			if err := tx.Commit(callCtx); err != nil {
				t.Fatalf("commit: %v", err)
			}
			// ...then the pin advances before the refusal returns.
			status = mismatchedMCPBuildStatus()
			return nil, replayableToolErr(errors.New("stateful refusal after commit"))
		},
	})

	countEvents := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id = $1`, subject).Scan(&n); err != nil {
			t.Fatalf("count events: %v", err)
		}
		return n
	}

	args := map[string]any{"idempotency_key": "cutover-stateful", "note": "a"}
	isError, text := callToolForTest(t, s, "test.cutover_stateful", args)
	if !isError || !strings.Contains(text, "build_pin") {
		t.Fatalf("stale process should refuse outward with build_pin, isError=%t text=%q", isError, text)
	}
	if got := countEvents(); got != 1 {
		t.Fatalf("expected exactly one committed event, got %d", got)
	}
	// The heart of IDEM-B4: the record survived the pin advance.
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:test.cutover_stateful", "cutover-stateful"); got != 1 {
		t.Fatalf("admitted stateful refusal must be recorded across the pin advance, got %d rows", got)
	}

	// A current build returns: same key+args replays the recorded refusal
	// without re-executing; changed args conflict; still one event set.
	status = currentMCPBuildStatus()
	if isError, text = callToolForTest(t, s, "test.cutover_stateful", args); !isError || !strings.Contains(text, "stateful refusal after commit") {
		t.Fatalf("post-cutover same-args retry should replay the refusal, isError=%t text=%q", isError, text)
	}
	if calls != 1 {
		t.Fatalf("recorded refusal was re-executed after cutover: %d calls", calls)
	}
	changed := map[string]any{"idempotency_key": "cutover-stateful", "note": "b"}
	if isError, text = callToolForTest(t, s, "test.cutover_stateful", changed); !isError || !strings.Contains(text, "idempotency_key_conflict") {
		t.Fatalf("post-cutover changed args should conflict, isError=%t text=%q", isError, text)
	}
	if got := countEvents(); got != 1 {
		t.Fatalf("cutover allowed a second authoritative event set: %d", got)
	}
	if calls != 1 {
		t.Fatalf("conflicting reuse re-executed the handler: %d calls", calls)
	}
}
