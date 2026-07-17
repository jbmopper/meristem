package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
// plan, queue state in its honest retry split, last failure retained past a
// later success, and expiry eligibility — read back from the same
// event-backed projections the delivery path writes. No psql, no mutation,
// no network.
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

	// Queue three commands for m4, in event order:
	//   A — acked as a failed local execution (502): the last FAILURE.
	//   B — acked done (201) after A: the last TERMINAL. A later success must
	//       not hide A from last_failure.
	//   C — still pending with two recorded attempts: the retryable row.
	svc := crossnode.NewQueueService(pool, writer)
	enqueue := func(key, path string) crossnode.EnqueueResult {
		t.Helper()
		res, err := svc.Enqueue(ctx, crossnode.EnqueueInput{
			TargetNodeID: "m4", OriginNodeID: "hub",
			CommandPath: path,
			CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: key,
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", key, err)
		}
		return res
	}
	failedCmd := enqueue("status-a-failed", "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition")
	var failedQueuedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT queued_at FROM command_queue WHERE id = $1`, failedCmd.EventID).Scan(&failedQueuedAt); err != nil {
		t.Fatalf("read failed queued_at: %v", err)
	}
	if _, err := svc.RecordAttempt(ctx, crossnode.RecordAttemptInput{
		CommandQueueID: failedCmd.EventID,
		AttemptKey:     "failed-attempt-1",
		Now:            failedQueuedAt.Add(time.Minute),
		ActorTokenID:   actor.ID,
		Source:         actor.Source,
	}); err != nil {
		t.Fatalf("attempt on failed-case: %v", err)
	}
	if _, err := svc.Ack(ctx, crossnode.AckInput{
		CommandQueueID: failedCmd.EventID, StatusCode: 502, OK: false,
		ActorTokenID: &actor.ID, Source: actor.Source,
	}); err != nil {
		t.Fatalf("ack failed-case: %v", err)
	}
	// The projection stamps last_attempt_at from the attempt event's clock
	// (never the caller's Now), so the truth to assert the report against is
	// the folded row itself.
	var failedAttemptAt time.Time
	if err := pool.QueryRow(ctx, `SELECT last_attempt_at FROM command_queue WHERE id = $1`, failedCmd.EventID).Scan(&failedAttemptAt); err != nil {
		t.Fatalf("read failed last_attempt_at: %v", err)
	}
	doneCmd := enqueue("status-b-done", "/v1/work-items/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/transition")
	if _, err := svc.Ack(ctx, crossnode.AckInput{
		CommandQueueID: doneCmd.EventID, StatusCode: 201, OK: true,
		ActorTokenID: &actor.ID, Source: actor.Source,
	}); err != nil {
		t.Fatalf("ack done-case: %v", err)
	}
	pendingCmd := enqueue("status-c-pending", "/v1/work-items/cccccccc-cccc-4ccc-8ccc-cccccccccccc/transition")
	var queuedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT queued_at FROM command_queue WHERE id = $1`, pendingCmd.EventID).Scan(&queuedAt); err != nil {
		t.Fatalf("read queued_at: %v", err)
	}
	recordAttempts := func(from, to int) {
		t.Helper()
		for i := from; i <= to; i++ {
			if _, err := svc.RecordAttempt(ctx, crossnode.RecordAttemptInput{
				CommandQueueID: pendingCmd.EventID,
				AttemptKey:     fmt.Sprintf("attempt-%d", i),
				Now:            queuedAt.Add(time.Duration(i) * time.Minute),
				ActorTokenID:   actor.ID,
				Source:         actor.Source,
			}); err != nil {
				t.Fatalf("attempt %d: %v", i, err)
			}
		}
	}
	recordAttempts(1, 2)

	now := queuedAt.Add(10 * time.Minute)

	readReport := func(args []string, at time.Time) nodeStatusReport {
		t.Helper()
		var buf bytes.Buffer
		if err := statusNodes(ctx, pool, &buf, append(args, "--json"), at); err != nil {
			t.Fatalf("status %v: %v", args, err)
		}
		var report nodeStatusReport
		if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
			t.Fatalf("decode report: %v\n%s", err, buf.String())
		}
		return report
	}

	report := readReport(nil, now)

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
	if q.TargetNodeID != "m4" || q.Pending != 1 || q.PendingRetryable != 1 || q.PendingExhausted != 0 || q.PendingDue != 0 {
		t.Fatalf("queue split = %+v, want one retryable pending row", q)
	}
	if q.MaxAttempts != 2 || q.Done != 1 || q.Failed != 1 {
		t.Fatalf("queue tallies = %+v, want attempts=2 done=1 failed=1", q)
	}
	if q.OldestQueuedAt == nil || q.LastAttemptAt == nil || q.EarliestDeadlineAt == nil {
		t.Fatalf("queue status missing pending timestamps: %+v", q)
	}
	if want := queuedAt.Add(crossnode.CommandQueuePatience); !q.EarliestDeadlineAt.Equal(want) {
		t.Fatalf("earliest_deadline_at = %s, want queued_at + patience = %s", q.EarliestDeadlineAt, want)
	}

	// The regression codex demanded: B (done) is the last terminal, but A
	// (failed 502) must remain the last failure — carrying its final
	// recorded local attempt time.
	if q.LastTerminal == nil || q.LastTerminal.State != "done" || q.LastTerminal.CommandQueueID != doneCmd.EventID {
		t.Fatalf("last terminal = %+v, want the later done command", q.LastTerminal)
	}
	if q.LastFailure == nil || q.LastFailure.State != "failed" ||
		q.LastFailure.StatusCode == nil || *q.LastFailure.StatusCode != 502 ||
		q.LastFailure.CommandQueueID != failedCmd.EventID {
		t.Fatalf("last failure = %+v, want the earlier 502-failed command retained past the later done", q.LastFailure)
	}
	if q.LastFailure.LastAttemptAt == nil || !q.LastFailure.LastAttemptAt.Equal(failedAttemptAt) {
		t.Fatalf("last failure attempt time = %v, want the recorded attempt at %s", q.LastFailure.LastAttemptAt, failedAttemptAt)
	}

	// Attempt exhaustion: at 5/5 the row stops being retryable and is
	// expiry-eligible IMMEDIATELY — the report must say exhausted, and the
	// rendered deadline must stay what it is (the future 24h deadline), never
	// dressed up as an eligibility instant.
	recordAttempts(3, 5)
	q = readReport(nil, now).Queue[0]
	if q.Pending != 1 || q.PendingRetryable != 0 || q.PendingExhausted != 1 || q.PendingDue != 0 {
		t.Fatalf("post-exhaustion split = %+v, want one exhausted pending row", q)
	}
	if q.EarliestDeadlineAt == nil || !q.EarliestDeadlineAt.Equal(queuedAt.Add(crossnode.CommandQueuePatience)) {
		t.Fatalf("post-exhaustion deadline = %v, want the unchanged 24h deadline fact", q.EarliestDeadlineAt)
	}

	// Past the 24h deadline the same row becomes due for the expiry worker's
	// next pass — an eligibility statement, not an invented timestamp.
	q = readReport(nil, queuedAt.Add(25*time.Hour)).Queue[0]
	if q.Pending != 1 || q.PendingRetryable != 0 || q.PendingExhausted != 0 || q.PendingDue != 1 {
		t.Fatalf("past-deadline split = %+v, want one due pending row", q)
	}

	// Text rendering shows the same facts (the pending row is exhausted by
	// now: attempts 3-5 were recorded above). The exhausted regression: the
	// timestamp renders as a deadline fact, never as an eligibility claim.
	var textOut bytes.Buffer
	if err := statusNodes(ctx, pool, &textOut, nil, now); err != nil {
		t.Fatalf("status: %v", err)
	}
	for _, want := range []string{
		"queue via hub https://hub.example",
		"pending=1 (retryable=0 exhausted=1 due=0)",
		"deadline=",
		"last_terminal=done status=201",
		"last_failure=failed status=502",
		"last_attempt=" + failedAttemptAt.Format(time.RFC3339),
	} {
		if !strings.Contains(textOut.String(), want) {
			t.Fatalf("text status missing %q:\n%s", want, textOut.String())
		}
	}
	if strings.Contains(textOut.String(), "expiry_eligible") {
		t.Fatalf("text status still claims expiry eligibility for an exhausted row:\n%s", textOut.String())
	}

	// --target for an unknown node surfaces the selection refusal.
	ghost := readReport([]string{"--target", "ghost"}, now)
	if len(ghost.Routes) != 1 || ghost.Routes[0].TargetNodeID != "ghost" ||
		ghost.Routes[0].Error != crossnode.ErrUnknownTarget.Error() {
		t.Fatalf("ghost target routes = %v, want the unknown-target refusal", ghost.Routes)
	}
	if len(ghost.Queue) != 0 {
		t.Fatalf("ghost target queue = %v, want empty", ghost.Queue)
	}

	// --target filters to one known node's routes and queue.
	hubOnly := readReport([]string{"--target", "hub"}, now)
	if len(hubOnly.Routes) != 1 || hubOnly.Routes[0].TargetNodeID != "hub" || len(hubOnly.Queue) != 0 {
		t.Fatalf("filtered report = routes %v queue %v, want hub only", hubOnly.Routes, hubOnly.Queue)
	}

	// Read-only, twice enforced: the production path runs inside a
	// repeatable-read READ ONLY transaction (the database refuses writes),
	// and the event log length is unchanged by a status run.
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
