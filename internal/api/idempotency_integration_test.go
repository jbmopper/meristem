package api

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestIdempotencyAdvisoryLockSerializesConcurrentRequests fires N
// concurrent POSTs that share the same (token, scope, key, body) at
// the live Postgres-backed API. The advisory lock in
// internal/idempotency/middleware.go must serialize them: exactly one
// runs the inner handler (one signal row, one work_item, one
// idempotency.recorded event), and all N responses must be
// byte-identical so callers converge regardless of which one their
// retry latched onto.
//
// Pre-lock this would intermittently produce two cache rows worth of
// races; today the events layer absorbs the duplicate event id but
// the loser used to send back its own buffered response which can
// differ in any non-deterministic field. With pg_advisory_lock all N
// callers get the winner's bytes.
func TestIdempotencyAdvisoryLockSerializesConcurrentRequests(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "idempotency-concurrent",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	body := signalRequestBody(t, "integration:idem-lock:retry")
	const concurrency = 8
	const sharedKey = "concurrent-key"

	type resp struct {
		status int
		body   []byte
	}
	results := make([]resp, concurrency)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			r := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, body)
			results[i] = resp{status: r.Code, body: append([]byte(nil), r.Body.Bytes()...)}
		}()
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.status != http.StatusCreated {
			t.Fatalf("worker %d: want 201, got %d body=%s", i, r.status, string(r.body))
		}
		if !bytes.Equal(r.body, results[0].body) {
			t.Fatalf("worker %d body diverged from worker 0:\nworker 0=%s\nworker %d=%s", i, string(results[0].body), i, string(r.body))
		}
	}

	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "signals", 1)
	assertTableCount(t, pool, "work_items", 1)
}

// TestIdempotencyAdvisoryLockRejectsConflictingBodies confirms that
// two same-key requests with different bodies serialize on the lock
// and the second is rejected as a 422 conflict before its handler
// runs (so it produces no events / projection rows of its own).
func TestIdempotencyAdvisoryLockRejectsConflictingBodies(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "idempotency-conflict",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	bodyA := signalRequestBody(t, "integration:idem-lock:body-a")
	bodyB := signalRequestBody(t, "integration:idem-lock:body-b")
	const sharedKey = "conflict-key"

	first := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, bodyA)
	if first.Code != http.StatusCreated {
		t.Fatalf("first body: want 201, got %d body=%s", first.Code, first.Body.String())
	}

	second := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, bodyB)
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("second body (conflict): want 422, got %d body=%s", second.Code, second.Body.String())
	}

	// The conflict must be rejected before the inner handler runs —
	// no extra signal/work_item rows, no extra events.
	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "signals", 1)
	assertTableCount(t, pool, "work_items", 1)
}
