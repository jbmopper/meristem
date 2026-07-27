package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// End-to-end MCP proof of the append payload-shape boundary: objects land as
// jsonb objects, double-encoded strings are rejected with the precise message
// and append nothing, and the tool schema types the parameter so conformant
// clients marshal objects instead of strings (the 2026-07-22 defect origin).
func TestMCPAppendEventPayloadShapeBoundaryIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "shape-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	item, err := workSvc.Create(ctx, workitems.CreateInput{Title: "mcp-shape", Actor: root.Token})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "shape-agent", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite, "work_items.tree:" + item.ID.String()},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	s := New(Deps{Auth: authSvc, Access: access.NewService(pool), Idempotency: idempotency.NewMiddleware(pool, writer), WorkItems: workSvc}, ServerInfo{Name: "shape-test", Version: "test"}, nil)
	if err := s.Authenticate(ctx, agent.Secret); err != nil {
		t.Fatalf("authenticate: %v", err)
	}

	isErr, text := callToolForTest(t, s, "work_items.append_event", map[string]any{
		"id": item.ID.String(), "kind": "agent.mcp_shape_object",
		"payload":         map[string]any{"marker": "mcp-object", "nested": map[string]any{"n": 1}},
		"idempotency_key": "shape-object-1",
	})
	if isErr {
		t.Fatalf("object payload rejected: %s", text)
	}
	var innerType string
	if err := pool.QueryRow(ctx, `
		SELECT jsonb_typeof(payload->'inner') FROM events
		WHERE subject_id=$1 AND payload->>'inner_kind'='agent.mcp_shape_object'`, item.ID).Scan(&innerType); err != nil {
		t.Fatalf("read inner type: %v", err)
	}
	if innerType != "object" {
		t.Fatalf("MCP-written inner type = %s, want object", innerType)
	}

	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, item.ID, domain.EventWorkItemEventAppended).Scan(&before); err != nil {
		t.Fatalf("count before: %v", err)
	}
	isErr, text = callToolForTest(t, s, "work_items.append_event", map[string]any{
		"id": item.ID.String(), "kind": "agent.mcp_shape_double",
		"payload":         `{"marker":"double-encoded"}`,
		"idempotency_key": "shape-double-1",
	})
	if !isErr || !strings.Contains(text, "double-encoded") {
		t.Fatalf("double-encoded payload not rejected precisely: isErr=%t %s", isErr, text)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, item.ID, domain.EventWorkItemEventAppended).Scan(&after); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if after != before {
		t.Fatalf("rejected append wrote an event: before=%d after=%d", before, after)
	}
	for _, tc := range []struct {
		name    string
		kind    string
		payload map[string]any
		want    string
	}{
		{
			name: "reserved wrapper kind",
			kind: domain.EventWorkItemEventAppended,
			payload: map[string]any{
				"inner_kind": "agent.actual_kind",
				"inner":      map[string]any{"marker": "nested"},
			},
			want: "transport envelope",
		},
		{
			name: "envelope-shaped payload",
			kind: "agent.double_wrapped",
			payload: map[string]any{
				"inner_kind": "agent.actual_kind",
				"inner":      map[string]any{"marker": "nested"},
			},
			want: "inner_kind/inner wrapper",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isErr, text := callToolForTest(t, s, "work_items.append_event", map[string]any{
				"id": item.ID.String(), "kind": tc.kind, "payload": tc.payload,
				"idempotency_key": "shape-" + strings.ReplaceAll(tc.name, " ", "-"),
			})
			if !isErr || !strings.Contains(text, tc.want) {
				t.Fatalf("double wrapper not rejected precisely: isErr=%t text=%s, want %q", isErr, text, tc.want)
			}
		})
	}
	var afterDoubleWrap int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, item.ID, domain.EventWorkItemEventAppended).Scan(&afterDoubleWrap); err != nil {
		t.Fatalf("count after double-wrapper cases: %v", err)
	}
	if afterDoubleWrap != before {
		t.Fatalf("double-wrapper rejection wrote events: before=%d after=%d", before, afterDoubleWrap)
	}

	// Schema pin: the payload parameter must stay typed as object.
	tool := s.toolWorkItemsAppendEvent()
	props, _ := tool.InputSchema["properties"].(map[string]any)
	payloadSchema, _ := props["payload"].(map[string]any)
	if payloadSchema["type"] != "object" {
		t.Fatalf("payload parameter schema type = %v, want object (typeless parameters get client-stringified)", payloadSchema["type"])
	}
}
