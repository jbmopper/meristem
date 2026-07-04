package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

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

func TestMCPMutationSemanticToolErrorIsCached(t *testing.T) {
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

	args := map[string]any{"idempotency_key": "semantic-replay", "title": ""}
	for i := 0; i < 2; i++ {
		if isError, text := callToolForTest(t, s, "work_items.create", args); !isError || text != "workitems: title is required" {
			t.Fatalf("call %d should replay semantic tool error, isError=%t text=%q", i+1, isError, text)
		}
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.create", "semantic-replay"); got != 1 {
		t.Fatalf("semantic error should record one idempotency row, got %d", got)
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
