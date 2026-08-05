package nodes

import (
	"context"
	"encoding/json"
	"slices"
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

	base := "https://ingress.example"
	registeredPayload := map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"base_url":        base,
		"status":          string(domain.NodeStatusActive),
		"relay_via":       []string{"den"},
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
		queueVia  []string
		status    string
		createdAt time.Time
		updatedAt time.Time
	}
	read := func() row {
		t.Helper()
		var r row
		var qv []byte
		err := pool.QueryRow(ctx, `
			SELECT base_url, direct_url, queue_via, status, created_at, updated_at
			FROM nodes WHERE node_id = $1
		`, "m4").Scan(&r.baseURL, &r.directURL, &qv, &r.status, &r.createdAt, &r.updatedAt)
		if err != nil {
			t.Fatalf("read node row: %v", err)
		}
		if err := json.Unmarshal(qv, &r.queueVia); err != nil {
			t.Fatalf("decode queue_via %s: %v", qv, err)
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
	if len(got.queueVia) != 1 || got.queueVia[0] != "den" {
		t.Fatalf("queue_via = %v, want [den]", got.queueVia)
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

	// A route update rewrites direct_url/queue_via/status, leaves base_url and
	// created_at intact.
	direct := "https://m4.peer.example"
	appendEvent(domain.EventNodeRouteUpdated, map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"direct_url":      direct,
		"status":          string(domain.NodeStatusUnreachable),
		"relay_via":       []string{},
	})
	after := read()
	if after.directURL == nil || *after.directURL != direct {
		t.Fatalf("direct_url = %v, want %s", after.directURL, direct)
	}
	if after.status != string(domain.NodeStatusUnreachable) {
		t.Fatalf("status = %s, want unreachable", after.status)
	}
	if len(after.queueVia) != 0 {
		t.Fatalf("queue_via = %v, want []", after.queueVia)
	}
	if after.baseURL == nil || *after.baseURL != base {
		t.Fatalf("route update clobbered base_url: %v", after.baseURL)
	}
	if !after.createdAt.Equal(created) {
		t.Fatalf("route update changed created_at: %s -> %s", created, after.createdAt)
	}
}

// TestQueueViaExpandMirrorsLegacyColumn pins the drift contract of migration
// 0037. queue_via is added alongside relay_via rather than renaming it, because
// a stale meristem binary on the same database still selects the legacy column
// (docs/network-operations.md, binary drift) and a rename would break it on
// every node read. The projectors must therefore keep relay_via current on
// every write path until the contract migration drops it.
func TestQueueViaExpandMirrorsLegacyColumn(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_nodes_expand_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)

	append := func(kind string, payload any) {
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

	// assertMirrored reads both columns so the assertion compares the two sides
	// rather than trusting the one the writer names.
	assertMirrored := func(want []string) {
		t.Helper()
		var qv, rv []byte
		if err := pool.QueryRow(ctx, `SELECT queue_via, relay_via FROM nodes WHERE node_id = 'm4'`).Scan(&qv, &rv); err != nil {
			t.Fatalf("read node row: %v", err)
		}
		var queue, legacy []string
		if err := json.Unmarshal(qv, &queue); err != nil {
			t.Fatalf("decode queue_via %s: %v", qv, err)
		}
		if err := json.Unmarshal(rv, &legacy); err != nil {
			t.Fatalf("decode relay_via %s: %v", rv, err)
		}
		if !slices.Equal(queue, want) {
			t.Fatalf("queue_via = %v, want %v", queue, want)
		}
		if !slices.Equal(legacy, want) {
			t.Fatalf("relay_via = %v, want %v (legacy column is stale — a drifted binary reads this)", legacy, want)
		}
	}

	append(domain.EventNodeRegistered, map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"base_url":        "https://ingress.example",
		"status":          string(domain.NodeStatusActive),
		"queue_via":       []string{"den"},
	})
	assertMirrored([]string{"den"})

	append(domain.EventNodeRouteUpdated, map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"status":          string(domain.NodeStatusActive),
		"queue_via":       []string{"den", "hub"},
	})
	assertMirrored([]string{"den", "hub"})

	// Clearing the allowlist must propagate too, or a drifted reader keeps
	// routing through a queue host the operator removed.
	append(domain.EventNodeRouteUpdated, map[string]any{
		"payload_version": routePayloadVersion,
		"node_id":         "m4",
		"status":          string(domain.NodeStatusActive),
		"queue_via":       []string{},
	})
	assertMirrored([]string{})

	// The other direction, and the one the previous revision got wrong: a
	// drifted pre-0037 binary writes ONLY relay_via, because queue_via did not
	// exist when it was built. The current reader must see that write. Reading
	// queue_via here would serve the backfilled value forever and route through
	// a queue host the operator had already moved off of.
	if _, err := pool.Exec(ctx, `UPDATE nodes SET relay_via = $1::jsonb, updated_at = now() WHERE node_id = 'm4'`, `["hub"]`); err != nil {
		t.Fatalf("simulate stale-binary write: %v", err)
	}
	listed, err := List(ctx, pool)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	var m4 *domain.Node
	for i := range listed {
		if listed[i].NodeID == "m4" {
			m4 = &listed[i]
		}
	}
	if m4 == nil {
		t.Fatal("node m4 missing from List")
	}
	if !slices.Equal(m4.QueueVia, []string{"hub"}) {
		t.Fatalf("List read QueueVia = %v, want [hub] — the reader is not seeing a drifted binary's relay_via write", m4.QueueVia)
	}

	// Same column, read directly. This is not a test of SnapshotService.Build —
	// it re-reads relay_via rather than calling the builder, so it pins the
	// column's value after a drifted write and nothing more. The builder's own
	// coverage lives in the snapshot tests; what matters here is that the
	// column a stale binary writes is the column the snapshot path later reads,
	// so the two cannot silently disagree about this node's topology.
	var snapQueueVia []string
	var snapRelay []byte
	if err := pool.QueryRow(ctx, `SELECT relay_via FROM nodes WHERE node_id = 'm4'`).Scan(&snapRelay); err != nil {
		t.Fatalf("read relay_via: %v", err)
	}
	if err := json.Unmarshal(snapRelay, &snapQueueVia); err != nil {
		t.Fatalf("decode relay_via: %v", err)
	}
	if !slices.Equal(snapQueueVia, []string{"hub"}) {
		t.Fatalf("relay_via = %v, want [hub]", snapQueueVia)
	}
}

func TestLegacyNodeOriginsRemainReplayable(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_nodes_legacy_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	legacy := "http://10.0.0.63:8080"
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectNode,
		SubjectID:   NodeSubjectID("m4"),
		Kind:        domain.EventNodeRegistered,
		Source:      domain.SourceSystem,
		Payload: map[string]any{
			"node_id": "m4", "base_url": legacy, "status": "active",
		},
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append legacy registration: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := pool.QueryRow(ctx, `SELECT base_url FROM nodes WHERE node_id='m4'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != legacy {
		t.Fatalf("legacy base_url = %q, want %q", got, legacy)
	}
}
