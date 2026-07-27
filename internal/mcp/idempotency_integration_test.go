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
