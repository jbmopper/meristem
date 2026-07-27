package workitems

import (
	"context"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// The append seam is the one place REST and MCP share, so the object contract
// is enforced here: objects and nil pass, everything else fails closed, and
// the string-of-JSON case names double-encoding because it is always a client
// marshaling bug (2026-07-22 incident: an untyped MCP tool parameter caused a
// client to send the object's string form, silently minting non-signals).
func TestAppendEventPayloadShapeFailsClosed(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_payload_shape")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "payload-shape-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	service := NewService(pool, writer)
	item, err := service.Create(ctx, CreateInput{Title: "payload-shape", Actor: root.Token})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	if err := service.AppendEvent(ctx, item.ID, "agent.shape_ok", map[string]any{"marker": "object"}, root.Token); err != nil {
		t.Fatalf("object payload rejected: %v", err)
	}
	if err := service.AppendEvent(ctx, item.ID, "agent.shape_nil", nil, root.Token); err != nil {
		t.Fatalf("nil payload rejected: %v", err)
	}

	rejected := []struct {
		name     string
		payload  any
		fragment string
	}{
		{"double-encoded object", `{"pass": true}`, "double-encoded"},
		{"double-encoded array", `[1,2]`, "double-encoded"},
		{"prose string", "free prose, not an object", "must be a JSON object"},
		{"json-ish prose fails like prose", "{not valid json", "must be a JSON object"},
		{"array", []any{"a"}, "must be a JSON object"},
		{"number", 41.5, "must be a JSON object"},
		{"bool", true, "must be a JSON object"},
	}
	var eventsBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, item.ID, domain.EventWorkItemEventAppended).Scan(&eventsBefore); err != nil {
		t.Fatalf("count before: %v", err)
	}
	for _, tc := range rejected {
		err := service.AppendEvent(ctx, item.ID, "agent.shape_reject", tc.payload, root.Token)
		if err == nil || !strings.Contains(err.Error(), tc.fragment) {
			t.Fatalf("%s: err=%v, want fragment %q", tc.name, err, tc.fragment)
		}
	}
	// The typed-verdict path gets the precise shape error too, replacing the
	// old misleading rejection for double-encoded verdicts.
	if err := service.AppendEvent(ctx, item.ID, ReviewVerdictInnerKind, `{"verdict":"accepted"}`, root.Token); err == nil || !strings.Contains(err.Error(), "double-encoded") {
		t.Fatalf("double-encoded verdict: err=%v, want double-encoded message", err)
	}
	var eventsAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, item.ID, domain.EventWorkItemEventAppended).Scan(&eventsAfter); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("rejected payloads appended events: before=%d after=%d", eventsBefore, eventsAfter)
	}

	var innerType string
	if err := pool.QueryRow(ctx, `
		SELECT jsonb_typeof(payload->'inner') FROM events
		WHERE subject_id=$1 AND payload->>'inner_kind'='agent.shape_ok'`, item.ID).Scan(&innerType); err != nil {
		t.Fatalf("read stored inner type: %v", err)
	}
	if innerType != "object" {
		t.Fatalf("stored inner type = %s, want object", innerType)
	}
}
