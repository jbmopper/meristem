package crossnode

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestCommandQueuedProjectorIntegration enqueues a command.queued through the
// real QueueService and projector against Postgres, reads the command_queue
// row back, and asserts a replay of the same logical request folds to one row.
// pgtest.NewPool skips unless the Postgres integration environment is set.
func TestCommandQueuedProjectorIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_crossnode_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// A writer carrying this package's projector so command_queue folds on
	// append. Building the registry locally keeps the test off internal/app,
	// which imports this package.
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	svc := NewQueueService(pool, events.NewWriter(reg))

	// OriginActorTokenID is left nil here: events.actor_token_id carries a
	// foreign key to tokens, and this test exercises the projector in
	// isolation without seeding a token. The api integration test covers the
	// attributed path with a real token.
	in := EnqueueInput{
		TargetNodeID:         "m4",
		CommandPath:          "/v1/work-items/abc/transition",
		CommandBody:          json.RawMessage(`{"to":"running"}`),
		OriginIdempotencyKey: "idem-xyz",
	}

	res, err := svc.Enqueue(ctx, in)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if res.EventID == uuid.Nil {
		t.Fatal("expected a command.queued event id")
	}

	type row struct {
		target  string
		path    string
		body    []byte
		idemKey string
		actorID *uuid.UUID
	}
	read := func() (row, int) {
		var r row
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_queue`).Scan(&count); err != nil {
			t.Fatalf("count command_queue: %v", err)
		}
		err := pool.QueryRow(ctx, `
			SELECT target_node_id, command_path, command_body, origin_idempotency_key, origin_actor_token_id
			FROM command_queue WHERE id = $1
		`, res.EventID).Scan(&r.target, &r.path, &r.body, &r.idemKey, &r.actorID)
		if err != nil {
			t.Fatalf("read command_queue row: %v", err)
		}
		return r, count
	}

	got, count := read()
	if count != 1 {
		t.Fatalf("command_queue rows = %d, want 1", count)
	}
	if got.target != "m4" || got.path != in.CommandPath || got.idemKey != "idem-xyz" {
		t.Fatalf("row mismatch: %+v", got)
	}
	if got.actorID != nil {
		t.Fatalf("origin_actor_token_id = %v, want nil", got.actorID)
	}
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil || body["to"] != "running" {
		t.Fatalf("command_body = %s (err %v)", got.body, err)
	}

	// A replay of the identical logical request (no idempotency discriminator
	// in context, so identity is payload-only) collides on the deterministic
	// event id and folds to the same single row.
	replay, err := svc.Enqueue(ctx, in)
	if err != nil {
		t.Fatalf("replay enqueue: %v", err)
	}
	if replay.EventID != res.EventID {
		t.Fatalf("replay produced a new event id %s (want %s)", replay.EventID, res.EventID)
	}
	if _, count := read(); count != 1 {
		t.Fatalf("replay grew command_queue to %d rows, want 1", count)
	}
}
