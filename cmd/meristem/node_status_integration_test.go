package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// `node status` must answer the parent item's check #4 end to end: route
// plan, queue state, last failure, and next retry/expiry, read back from the
// same event-backed projections the delivery path writes — no psql, no
// mutation, no network.
func TestNodeStatusIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_node_status_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	crossnode.RegisterProjectors(reg)
	nodes.RegisterProjectors(reg)
	writer := events.NewWriter(reg)

	created, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "node-status", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	actor := created.Token

	// Registry: a hub with a direct URL and a pull-only target that queues
	// through it.
	registerTestNode(t, ctx, pool, writer, actor, "hub", "https://hub.example", nil)
	registerTestNode(t, ctx, pool, writer, actor, "m4", "", []string{"hub"})

	// Queue: two commands for m4 — one attempted twice and still pending, one
	// acked as a failed local execution (the "last failure" the operator must
	// be able to see).
	svc := crossnode.NewQueueService(pool, writer)
	pending, err := svc.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID: "m4", OriginNodeID: "hub",
		CommandPath: "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition",
		CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: "status-pending",
	})
	if err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}
	var queuedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT queued_at FROM command_queue WHERE id = $1`, pending.EventID).Scan(&queuedAt); err != nil {
		t.Fatalf("read queued_at: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := svc.RecordAttempt(ctx, crossnode.RecordAttemptInput{
			CommandQueueID: pending.EventID,
			AttemptKey:     string(rune('a' + i - 1)),
			Now:            queuedAt.Add(time.Duration(i) * time.Minute),
			ActorTokenID:   actor.ID,
			Source:         actor.Source,
		}); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	failed, err := svc.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID: "m4", OriginNodeID: "hub",
		CommandPath: "/v1/work-items/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/transition",
		CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: "status-failed",
	})
	if err != nil {
		t.Fatalf("enqueue failed-case: %v", err)
	}
	if _, err := svc.Ack(ctx, crossnode.AckInput{
		CommandQueueID: failed.EventID, StatusCode: 502, OK: false,
		ActorTokenID: &actor.ID, Source: actor.Source,
	}); err != nil {
		t.Fatalf("ack failed-case: %v", err)
	}

	now := queuedAt.Add(10 * time.Minute)

	// JSON: the machine-readable shape carries every fact.
	var jsonOut bytes.Buffer
	if err := statusNodes(ctx, pool, &jsonOut, []string{"--json"}, now); err != nil {
		t.Fatalf("status --json: %v", err)
	}
	var report nodeStatusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, jsonOut.String())
	}

	routes := map[string]routePlanReport{}
	for _, r := range report.Routes {
		routes[r.TargetNodeID] = r
	}
	if got := routes["m4"].Plan; len(got) != 1 || got[0] != "queue via hub https://hub.example" {
		t.Fatalf("m4 route plan = %v", got)
	}
	if got := routes["hub"].Plan; len(got) != 1 || got[0] != "direct https://hub.example" {
		t.Fatalf("hub route plan = %v", got)
	}

	if len(report.Queue) != 1 {
		t.Fatalf("queue targets = %d, want 1 (m4)", len(report.Queue))
	}
	q := report.Queue[0]
	if q.TargetNodeID != "m4" || q.Pending != 1 || q.MaxAttempts != 2 || q.Failed != 1 {
		t.Fatalf("queue status = %+v, want m4 pending=1 attempts=2 failed=1", q)
	}
	if q.OldestQueuedAt == nil || q.LastAttemptAt == nil || q.NextExpiresAt == nil {
		t.Fatalf("queue status missing pending timestamps: %+v", q)
	}
	if want := queuedAt.Add(crossnode.CommandQueuePatience); !q.NextExpiresAt.Equal(want) {
		t.Fatalf("next_expires_at = %s, want queued_at + patience = %s", q.NextExpiresAt, want)
	}
	if q.LastTerminal == nil || q.LastTerminal.State != "failed" ||
		q.LastTerminal.StatusCode == nil || *q.LastTerminal.StatusCode != 502 ||
		q.LastTerminal.CommandQueueID != failed.EventID {
		t.Fatalf("last terminal = %+v, want the 502-failed command", q.LastTerminal)
	}

	// Text: the human-readable rendering shows the same facts.
	var textOut bytes.Buffer
	if err := statusNodes(ctx, pool, &textOut, nil, now); err != nil {
		t.Fatalf("status: %v", err)
	}
	for _, want := range []string{
		"queue via hub https://hub.example",
		"pending=1",
		"attempts=2/5",
		"last_terminal=failed status=502",
	} {
		if !strings.Contains(textOut.String(), want) {
			t.Fatalf("text status missing %q:\n%s", want, textOut.String())
		}
	}

	// --target filters to one node's routes and queue.
	var filtered bytes.Buffer
	if err := statusNodes(ctx, pool, &filtered, []string{"--target", "hub", "--json"}, now); err != nil {
		t.Fatalf("status --target: %v", err)
	}
	var filteredReport nodeStatusReport
	if err := json.Unmarshal(filtered.Bytes(), &filteredReport); err != nil {
		t.Fatalf("decode filtered report: %v", err)
	}
	if len(filteredReport.Routes) != 1 || filteredReport.Routes[0].TargetNodeID != "hub" || len(filteredReport.Queue) != 0 {
		t.Fatalf("filtered report = routes %v queue %v, want hub only", filteredReport.Routes, filteredReport.Queue)
	}

	// Read-only: the diagnostics run appended no events.
	var before, after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&before); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := statusNodes(ctx, pool, io.Discard, []string{"--json"}, now); err != nil {
		t.Fatalf("status rerun: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&after); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if before != after {
		t.Fatalf("status appended %d events; the surface must be read-only", after-before)
	}
}

// registerTestNode appends one node.registered event through the production
// payload builder and projectors, mirroring `meristem node register`.
func registerTestNode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, nodeID, directURL string, relayVia []string) {
	t.Helper()
	params := nodes.RegisterParams{NodeID: nodeID, Status: string(domain.NodeStatusActive), RelayVia: relayVia}
	if directURL != "" {
		params.DirectURL = &directURL
	}
	payload, err := nodes.BuildRegisteredPayload(params)
	if err != nil {
		t.Fatalf("build payload for %s: %v", nodeID, err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectNode,
		SubjectID:    nodes.NodeSubjectID(nodeID),
		Kind:         domain.EventNodeRegistered,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload:      payload,
	}); err != nil {
		t.Fatalf("append node.registered %s: %v", nodeID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
