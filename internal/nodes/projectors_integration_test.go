package nodes

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestNodeProjectorsIntegration folds a node.registered event and a following
// node.route_updated event through the real projectors against Postgres, then
// reads the `nodes` row back. It also asserts that a replay of the same
// registration is idempotent (created_at preserved). pgtest.NewPool skips the
// test unless the Postgres integration environment is configured.
func TestNodeProjectorsIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_nodes_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)

	base := "https://ingress.example/mcp"
	registeredPayload := map[string]any{
		"node_id":   "m4",
		"base_url":  base,
		"status":    string(domain.NodeStatusActive),
		"relay_via": []string{"den"},
	}

	appendEvent := func(kind string, payload any) {
		t.Helper()
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, _, err := writer.Append(ctx, tx, events.Spec{
			SubjectKind: domain.SubjectNode,
			SubjectID:   NodeSubjectID("m4"),
			Kind:        kind,
			Source:      domain.SourceSystem,
			Payload:     payload,
		}); err != nil {
			t.Fatalf("append %s: %v", kind, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %s: %v", kind, err)
		}
	}

	type row struct {
		baseURL   *string
		directURL *string
		relayVia  []string
		status    string
		createdAt time.Time
		updatedAt time.Time
	}
	read := func() row {
		t.Helper()
		var r row
		var relay []byte
		err := pool.QueryRow(ctx, `
			SELECT base_url, direct_url, relay_via, status, created_at, updated_at
			FROM nodes WHERE node_id = $1
		`, "m4").Scan(&r.baseURL, &r.directURL, &relay, &r.status, &r.createdAt, &r.updatedAt)
		if err != nil {
			t.Fatalf("read node row: %v", err)
		}
		if err := json.Unmarshal(relay, &r.relayVia); err != nil {
			t.Fatalf("decode relay_via %s: %v", relay, err)
		}
		return r
	}

	appendEvent(domain.EventNodeRegistered, registeredPayload)
	got := read()
	if got.baseURL == nil || *got.baseURL != base {
		t.Fatalf("base_url = %v, want %s", got.baseURL, base)
	}
	if got.status != string(domain.NodeStatusActive) {
		t.Fatalf("status = %s, want active", got.status)
	}
	if len(got.relayVia) != 1 || got.relayVia[0] != "den" {
		t.Fatalf("relay_via = %v, want [den]", got.relayVia)
	}
	if got.directURL != nil {
		t.Fatalf("direct_url = %v, want nil", got.directURL)
	}
	created := got.createdAt

	// Replay of the identical registration event is a no-op: the deterministic
	// id collides, so the projector never re-fires and created_at is unchanged.
	appendEvent(domain.EventNodeRegistered, registeredPayload)
	if again := read(); !again.createdAt.Equal(created) {
		t.Fatalf("replay changed created_at: %s -> %s", created, again.createdAt)
	}

	// A route update rewrites direct_url/relay_via/status, leaves base_url and
	// created_at intact.
	direct := "https://m4.peer.example/mcp"
	appendEvent(domain.EventNodeRouteUpdated, map[string]any{
		"node_id":    "m4",
		"direct_url": direct,
		"status":     string(domain.NodeStatusUnreachable),
		"relay_via":  []string{},
	})
	after := read()
	if after.directURL == nil || *after.directURL != direct {
		t.Fatalf("direct_url = %v, want %s", after.directURL, direct)
	}
	if after.status != string(domain.NodeStatusUnreachable) {
		t.Fatalf("status = %s, want unreachable", after.status)
	}
	if len(after.relayVia) != 0 {
		t.Fatalf("relay_via = %v, want []", after.relayVia)
	}
	if after.baseURL == nil || *after.baseURL != base {
		t.Fatalf("route update clobbered base_url: %v", after.baseURL)
	}
	if !after.createdAt.Equal(created) {
		t.Fatalf("route update changed created_at: %s -> %s", created, after.createdAt)
	}
}
