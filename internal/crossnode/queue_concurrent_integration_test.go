package crossnode

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestConcurrentDuplicateEnqueueFoldsToOneRow pins the duplicate-delivery
// cell of the fault matrix under real concurrency (work item 17ce2faf, audit
// finding G4c). Two racing enqueues of the same originating request — same
// target, path, body, and origin idempotency key — must fold to one
// command.queued event and one command_queue row, with both callers observing
// the same deterministic event id. Sequential replay already collapses via
// the deterministic event identity; this proves the same holds when the
// replay races the original in flight, i.e. the second transaction blocks on
// the first and folds instead of erroring or duplicating.
func TestConcurrentDuplicateEnqueueFoldsToOneRow(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_crossnode_concurrent_enqueue_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)
	svc := NewQueueService(pool, writer)

	input := EnqueueInput{
		TargetNodeID: "m4", OriginNodeID: "hub",
		CommandPath:          "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition",
		CommandBody:          json.RawMessage(`{"to":"running"}`),
		OriginIdempotencyKey: "concurrent-duplicate-1",
	}

	const racers = 8
	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		gate    = make(chan struct{})
		results = make([]EnqueueResult, racers)
		errs    = make([]error, racers)
	)
	start.Add(racers)
	done.Add(racers)
	for i := 0; i < racers; i++ {
		go func(i int) {
			defer done.Done()
			start.Done()
			<-gate // maximize overlap: all goroutines enqueue together
			results[i], errs[i] = svc.Enqueue(ctx, input)
		}(i)
	}
	start.Wait()
	close(gate)
	done.Wait()

	var wantID uuid.UUID
	for i := 0; i < racers; i++ {
		if errs[i] != nil {
			t.Fatalf("racer %d: concurrent duplicate enqueue must fold, not error: %v", i, errs[i])
		}
		if i == 0 {
			wantID = results[i].EventID
			continue
		}
		if results[i].EventID != wantID {
			t.Fatalf("racer %d event id = %s, want the one deterministic id %s", i, results[i].EventID, wantID)
		}
	}

	var eventCount, rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, domain.EventCommandQueued).Scan(&eventCount); err != nil {
		t.Fatalf("count command.queued events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("command.queued events = %d, want exactly 1 across %d racers", eventCount, racers)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM command_queue`).Scan(&rowCount); err != nil {
		t.Fatalf("count queue rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("command_queue rows = %d, want exactly 1", rowCount)
	}

	// The surviving row is intact: pending, correctly keyed, correct target.
	var state, target, originKey string
	if err := pool.QueryRow(ctx, `SELECT state, target_node_id, origin_idempotency_key FROM command_queue WHERE id = $1`, wantID).Scan(&state, &target, &originKey); err != nil {
		t.Fatalf("read surviving row: %v", err)
	}
	if state != "pending" || target != "m4" || originKey != input.OriginIdempotencyKey {
		t.Fatalf("surviving row = (%s, %s, %s), want (pending, m4, %s)", state, target, originKey, input.OriginIdempotencyKey)
	}
}
