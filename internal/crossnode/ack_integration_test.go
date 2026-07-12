package crossnode

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestAckProjectorIntegration drives the full hub-side drain projection through
// Postgres: enqueue two commands, read them back via PendingForTarget, ack one
// success and one failure, and assert the command_queue rows transition
// pending -> done/failed with the outcome recorded and drop out of the pending
// read. pgtest.NewPool skips unless the integration environment is set.
func TestAckProjectorIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_crossnode_ack_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	svc := NewQueueService(pool, events.NewWriter(reg))

	enqueue := func(key string) uuid.UUID {
		res, err := svc.Enqueue(ctx, EnqueueInput{
			TargetNodeID:         "m4",
			CommandPath:          "/v1/work-items/abc/transition",
			CommandBody:          json.RawMessage(`{"to":"running"}`),
			OriginIdempotencyKey: key,
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", key, err)
		}
		return res.EventID
	}
	id1 := enqueue("k1")
	id2 := enqueue("k2")

	// A different target's command must not appear in m4's pending read.
	if _, err := svc.Enqueue(ctx, EnqueueInput{
		TargetNodeID: "laptop", CommandPath: "/v1/x", CommandBody: json.RawMessage(`{}`), OriginIdempotencyKey: "kx",
	}); err != nil {
		t.Fatalf("enqueue other target: %v", err)
	}

	pending, err := svc.PendingForTarget(ctx, "m4", 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(pending))
	}
	if pending[0].EventID != id1 || pending[0].OriginIdempotencyKey != "k1" {
		t.Fatalf("oldest-first ordering broken: %+v", pending[0])
	}

	// Ack id1 success -> done; ack id2 failure -> failed.
	firstAck, err := svc.Ack(ctx, AckInput{CommandQueueID: id1, StatusCode: 200, OK: true, Source: domain.SourceAgent})
	if err != nil {
		t.Fatalf("ack success: %v", err)
	}
	if _, err := svc.Ack(ctx, AckInput{CommandQueueID: id2, StatusCode: 500, OK: false}); err != nil {
		t.Fatalf("ack failure: %v", err)
	}

	assertRow(t, pool, id1, "done", 200, true)
	assertRow(t, pool, id2, "failed", 500, false)

	// An identical later acknowledgement is idempotent and returns the winning
	// event. A contradictory acknowledgement conflicts and appends nothing.
	replayAck, err := svc.Ack(ctx, AckInput{CommandQueueID: id1, StatusCode: 200, OK: true, Source: domain.SourceHuman})
	if err != nil || replayAck.EventID != firstAck.EventID {
		t.Fatalf("identical ack replay = (%+v, %v), want event %s", replayAck, err, firstAck.EventID)
	}
	if _, err := svc.Ack(ctx, AckInput{CommandQueueID: id1, StatusCode: 503, OK: false, Source: domain.SourceHuman}); err != ErrCommandTerminalConflict {
		t.Fatalf("contradictory ack err = %v, want ErrCommandTerminalConflict", err)
	}
	assertRow(t, pool, id1, "done", 200, true)
	var firstSource string
	if err := pool.QueryRow(ctx, `SELECT source FROM events WHERE id = $1`, firstAck.EventID).Scan(&firstSource); err != nil {
		t.Fatalf("read first ack source: %v", err)
	}
	if firstSource != string(domain.SourceAgent) {
		t.Fatalf("ack source = %q, want agent", firstSource)
	}

	// Concurrent contradictory acks serialize before their event append. The
	// earliest event in seq order must therefore be the live projection winner,
	// which is also what a rebuild will deterministically reproduce.
	id3 := enqueue("k3")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, ack := range []AckInput{
		{CommandQueueID: id3, StatusCode: 202, OK: true},
		{CommandQueueID: id3, StatusCode: 409, OK: false},
	} {
		wg.Add(1)
		go func(in AckInput) {
			defer wg.Done()
			<-start
			_, err := svc.Ack(ctx, in)
			errs <- err
		}(ack)
	}
	close(start)
	wg.Wait()
	close(errs)
	var successes, conflicts int
	for err := range errs {
		switch err {
		case nil:
			successes++
		case ErrCommandTerminalConflict:
			conflicts++
		default:
			t.Fatalf("concurrent ack: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent ack results successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	var wantStatus int
	var wantOK bool
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'status_code')::integer, (payload->>'ok')::boolean
		FROM events
		WHERE kind = $1 AND payload->>'command_queue_id' = $2
		ORDER BY seq
		LIMIT 1
	`, domain.EventCommandAcked, id3.String()).Scan(&wantStatus, &wantOK); err != nil {
		t.Fatalf("read first concurrent ack event: %v", err)
	}
	wantState := "failed"
	if wantOK {
		wantState = "done"
	}
	assertRow(t, pool, id3, wantState, wantStatus, wantOK)

	// The expanded terminal vocabulary represents deterministic refusal
	// separately from retryable/execution failure.
	id4 := enqueue("k4")
	if _, err := svc.Ack(ctx, AckInput{CommandQueueID: id4, StatusCode: 403, Outcome: CommandRefused}); err != nil {
		t.Fatalf("ack refused: %v", err)
	}
	assertRow(t, pool, id4, "refused", 403, false)

	// Both acked: m4's pending read is empty.
	pending, err = svc.PendingForTarget(ctx, "m4", 10)
	if err != nil {
		t.Fatalf("pending after ack: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after ack = %d, want 0", len(pending))
	}

	// Ack of an unknown command id fails cleanly.
	if _, err := svc.Ack(ctx, AckInput{CommandQueueID: uuid.New(), StatusCode: 200, OK: true}); err != ErrUnknownCommand {
		t.Fatalf("ack unknown: err = %v, want ErrUnknownCommand", err)
	}
}

func assertRow(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, wantState string, wantStatus int, wantOK bool) {
	t.Helper()
	var state string
	var status *int
	var ok *bool
	if err := pool.QueryRow(context.Background(), `
		SELECT state, outcome_status_code, outcome_ok FROM command_queue WHERE id = $1
	`, id).Scan(&state, &status, &ok); err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	if state != wantState {
		t.Fatalf("state = %q, want %q", state, wantState)
	}
	if status == nil || *status != wantStatus {
		t.Fatalf("outcome_status_code = %v, want %d", status, wantStatus)
	}
	if ok == nil || *ok != wantOK {
		t.Fatalf("outcome_ok = %v, want %v", ok, wantOK)
	}
}
