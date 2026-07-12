package mcp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestScopedMCPWorkItemTreeAccessIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token

	workSvc := workitems.NewService(pool, writer)
	a, err := workSvc.Create(ctx, workitems.CreateInput{Title: "A", Actor: root})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	a1, err := workSvc.SpawnChild(ctx, a.ID, workitems.CreateInput{
		Title:                      "A1",
		SuggestedConvergenceChecks: []string{"event:mcp_scoped_access"},
		Actor:                      root,
	})
	if err != nil {
		t.Fatalf("spawn A1: %v", err)
	}
	b, err := workSvc.Create(ctx, workitems.CreateInput{Title: "B", Actor: root})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}

	agentResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "scoped-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + a.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create scoped agent token: %v", err)
	}

	s := New(Deps{
		Auth:        authSvc,
		Access:      access.NewService(pool),
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
		Feed:        feed.NewService(pool),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	if err := s.Authenticate(ctx, agentResult.Secret); err != nil {
		t.Fatalf("authenticate scoped agent: %v", err)
	}

	assertToolCallOK(t, s, "work_items.get", map[string]any{"id": a.ID.String()})
	assertToolCallOK(t, s, "work_items.get", map[string]any{"id": a1.ID.String()})

	if isError, text := callToolForTest(t, s, "work_items.get", map[string]any{"id": b.ID.String()}); !isError || !strings.Contains(text, "not found") {
		t.Fatalf("out-of-tree get should be hidden as not found, isError=%t text=%q", isError, text)
	}

	beforeDenied := eventCount(t, pool, domain.EventWorkItemTransitioned)
	if isError, text := callToolForTest(t, s, "work_items.transition", map[string]any{
		"id":              b.ID.String(),
		"to":              string(domain.WorkItemRunning),
		"reason":          "should not happen",
		"idempotency_key": "deny-out-of-tree",
	}); !isError || !strings.Contains(text, "not found") {
		t.Fatalf("out-of-tree transition should be denied as not found, isError=%t text=%q", isError, text)
	}
	if after := eventCount(t, pool, domain.EventWorkItemTransitioned); after != beforeDenied {
		t.Fatalf("denied write appended transition event: before=%d after=%d", beforeDenied, after)
	}

	beforeAllowed := eventCount(t, pool, domain.EventWorkItemTransitioned)
	assertToolCallOK(t, s, "work_items.transition", map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemRunning),
		"reason":          "inside assigned tree",
		"idempotency_key": "transition-a1-running",
	})
	afterAllowed := eventCount(t, pool, domain.EventWorkItemTransitioned)
	if afterAllowed != beforeAllowed+1 {
		t.Fatalf("allowed transition event count = %d, want %d", afterAllowed, beforeAllowed+1)
	}
	if got := lastActorForKind(t, pool, domain.EventWorkItemTransitioned); got != agentResult.Token.ID {
		t.Fatalf("allowed write actor = %s, want scoped agent %s", got, agentResult.Token.ID)
	}

	assertToolCallOK(t, s, "work_items.transition", map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemRunning),
		"reason":          "inside assigned tree",
		"idempotency_key": "transition-a1-running",
	})
	if afterReplay := eventCount(t, pool, domain.EventWorkItemTransitioned); afterReplay != afterAllowed {
		t.Fatalf("replayed transition appended another event: before=%d after=%d", afterAllowed, afterReplay)
	}
	if isError, text := callToolForTest(t, s, "work_items.transition", map[string]any{
		"id":              a1.ID.String(),
		"to":              string(domain.WorkItemDone),
		"reason":          "same key different args",
		"idempotency_key": "transition-a1-running",
	}); !isError || !strings.Contains(text, "idempotency_key_conflict") {
		t.Fatalf("same key/different args should conflict, isError=%t text=%q", isError, text)
	}
	if afterConflict := eventCount(t, pool, domain.EventWorkItemTransitioned); afterConflict != afterAllowed {
		t.Fatalf("conflicting transition appended event: before=%d after=%d", afterAllowed, afterConflict)
	}

	if isError, text := callToolForTest(t, s, "feed.read", map[string]any{"limit": 20}); isError {
		t.Fatalf("feed.read returned tool error: %s", text)
	} else {
		if strings.Contains(text, b.ID.String()) {
			t.Fatalf("assigned feed included out-of-tree work item B %s: %s", b.ID, text)
		}
		if !strings.Contains(text, a1.ID.String()) {
			t.Fatalf("assigned feed did not include in-tree child transition %s: %s", a1.ID, text)
		}
	}

	if isError, text := callToolForTest(t, s, "backlog.readiness", map[string]any{"limit": 1}); isError {
		t.Fatalf("backlog.readiness returned tool error: %s", text)
	} else {
		if !strings.Contains(text, a.ID.String()) || !strings.Contains(text, a1.ID.String()) {
			t.Fatalf("scoped readiness omitted assigned tree items: %s", text)
		}
		if strings.Contains(text, b.ID.String()) || strings.Contains(text, `"title":"B"`) {
			t.Fatalf("scoped readiness leaked out-of-tree B: %s", text)
		}
		if !strings.Contains(text, `"limit":0`) {
			t.Fatalf("scoped readiness did not report an unbounded scan: %s", text)
		}
	}
}

func callToolForTest(t *testing.T, s *Server, name string, args map[string]any) (bool, string) {
	t.Helper()
	argBytes, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	req := `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":` + quoteJSON(t, name) + `,"arguments":` + string(argBytes) + `}}`
	resp := roundtrip(t, s, req)
	if resp.Error != nil {
		t.Fatalf("tools/call transport error: %+v", resp.Error)
	}
	var result struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	text := ""
	if len(result.Content) > 0 {
		text = result.Content[0].Text
	}
	return result.IsError, text
}

func assertToolCallOK(t *testing.T, s *Server, name string, args map[string]any) {
	t.Helper()
	if isError, text := callToolForTest(t, s, name, args); isError {
		t.Fatalf("%s returned tool error: %s", name, text)
	}
}

func quoteJSON(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func eventCount(t *testing.T, pool *pgxpool.Pool, kind string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&count); err != nil {
		t.Fatalf("count events %s: %v", kind, err)
	}
	return count
}

func lastActorForKind(t *testing.T, pool *pgxpool.Pool, kind string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT actor_token_id
		FROM events
		WHERE kind = $1
		ORDER BY occurred_at DESC, id DESC
		LIMIT 1
	`, kind).Scan(&id); err != nil {
		t.Fatalf("last actor for %s: %v", kind, err)
	}
	return id
}

func newMCPIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return pgtest.NewPool(t, "meristem_mcp_itest")
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
