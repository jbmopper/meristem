package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestNodeCommandPaths drives the register -> list -> update-route -> list
// command core against a real Postgres through the pgtest harness, plus the
// re-register idempotency contract. It calls the injected-pool variants
// (registerNode/updateNodeRoute/listNodes) so it exercises the real append and
// projection path without env/pool wiring. pgtest.NewPool skips unless the
// integration environment is configured.
func TestNodeCommandPaths(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actor := createCmdSystemToken(t, ctx, pool, writer, "node-itest")

	// register: hub with an ingress base_url and a relay hop, no direct route.
	if err := registerNode(ctx, pool, writer, actor, []string{
		"--node-id", "m4",
		"--base-url", "https://ingress.example",
		"--relay-via", "den",
		"--status", "active",
	}); err != nil {
		t.Fatalf("registerNode: %v", err)
	}

	// list shows the row.
	row := listRow(t, ctx, pool, "m4")
	if row[1] != "https://ingress.example" {
		t.Fatalf("base_url column = %q", row[1])
	}
	if row[2] != "-" {
		t.Fatalf("direct_url column = %q, want -", row[2])
	}
	if row[3] != "den" {
		t.Fatalf("relay_via column = %q, want den", row[3])
	}
	if row[4] != "active" {
		t.Fatalf("status column = %q, want active", row[4])
	}

	// re-register with identical fields collapses: no new event appended.
	before := countNodeEvents(t, ctx, pool)
	if err := registerNode(ctx, pool, writer, actor, []string{
		"--node-id", "m4",
		"--base-url", "https://ingress.example",
		"--relay-via", "den",
		"--status", "active",
	}); err != nil {
		t.Fatalf("registerNode replay: %v", err)
	}
	if after := countNodeEvents(t, ctx, pool); after != before {
		t.Fatalf("identical re-register appended events: before=%d after=%d", before, after)
	}

	// a changed field (status) appends a fresh event and updates the row.
	if err := registerNode(ctx, pool, writer, actor, []string{
		"--node-id", "m4",
		"--base-url", "https://ingress.example",
		"--relay-via", "den",
		"--status", "unreachable",
	}); err != nil {
		t.Fatalf("registerNode changed: %v", err)
	}
	if after := countNodeEvents(t, ctx, pool); after != before+1 {
		t.Fatalf("changed re-register event count: before=%d after=%d, want +1", before, after)
	}
	if got := listRow(t, ctx, pool, "m4")[4]; got != "unreachable" {
		t.Fatalf("status after changed re-register = %q, want unreachable", got)
	}

	// update-route fully replaces the route state: set a direct route, clear
	// the relay chain, and flip status back to active. base_url is untouched.
	if err := updateNodeRoute(ctx, pool, writer, actor, []string{
		"--node-id", "m4",
		"--direct-url", "https://m4.peer.example",
		"--status", "active",
	}); err != nil {
		t.Fatalf("updateNodeRoute: %v", err)
	}
	after := listRow(t, ctx, pool, "m4")
	if after[1] != "https://ingress.example" {
		t.Fatalf("update-route clobbered base_url: %q", after[1])
	}
	if after[2] != "https://m4.peer.example" {
		t.Fatalf("direct_url after update-route = %q", after[2])
	}
	if after[3] != "-" {
		t.Fatalf("relay_via after update-route = %q, want cleared (-)", after[3])
	}
	if after[4] != "active" {
		t.Fatalf("status after update-route = %q, want active", after[4])
	}

	// Repeating the current declaration is an immediate retry and must not
	// append. A later A -> B -> A cycle is two new logical actions even though
	// the final A payload matches the first route update byte-for-byte.
	routeA := []string{
		"--node-id", "m4",
		"--direct-url", "https://m4.peer.example",
		"--status", "active",
	}
	routeCount := countNodeEvents(t, ctx, pool)
	if err := updateNodeRoute(ctx, pool, writer, actor, routeA); err != nil {
		t.Fatalf("updateNodeRoute immediate retry: %v", err)
	}
	if got := countNodeEvents(t, ctx, pool); got != routeCount {
		t.Fatalf("immediate route retry appended: before=%d after=%d", routeCount, got)
	}
	if err := updateNodeRoute(ctx, pool, writer, actor, []string{
		"--node-id", "m4",
		"--relay-via", "den",
		"--status", "unreachable",
	}); err != nil {
		t.Fatalf("updateNodeRoute B: %v", err)
	}
	if err := updateNodeRoute(ctx, pool, writer, actor, routeA); err != nil {
		t.Fatalf("updateNodeRoute return to A: %v", err)
	}
	if got := countNodeEvents(t, ctx, pool); got != routeCount+2 {
		t.Fatalf("A -> B -> A appended %d events, want 2", got-routeCount)
	}
	final := listRow(t, ctx, pool, "m4")
	if final[2] != "https://m4.peer.example" || final[3] != "-" || final[4] != "active" {
		t.Fatalf("final route A not restored: %v", final)
	}
}

// listRow renders the node list and returns the tab-split columns for nodeID.
func listRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, nodeID string) []string {
	t.Helper()
	var buf bytes.Buffer
	if err := listNodes(ctx, pool, &buf, nil); err != nil {
		t.Fatalf("listNodes: %v", err)
	}
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		cols := strings.Split(line, "\t")
		if len(cols) > 0 && cols[0] == nodeID {
			return cols
		}
	}
	t.Fatalf("node %q not found in list output:\n%s", nodeID, buf.String())
	return nil
}

func countNodeEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_kind = $1`, domain.SubjectNode).Scan(&count); err != nil {
		t.Fatalf("count node events: %v", err)
	}
	return count
}
