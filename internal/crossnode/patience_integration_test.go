package crossnode

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestQueuePatienceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_crossnode_patience_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	writer := events.NewWriter(reg)
	created, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{
		Name: "queue-patience", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	actor := created.Token
	svc := NewQueueService(pool, writer)

	enqueue := func(key string) EnqueueResult {
		res, err := svc.Enqueue(ctx, EnqueueInput{
			TargetNodeID: "m4", OriginNodeID: "hub", CommandPath: "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition",
			CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: key,
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", key, err)
		}
		return res
	}

	attemptBound := enqueue("attempt-bound")
	var queuedAt, expiresAt time.Time
	if err := pool.QueryRow(ctx, `SELECT queued_at, expires_at FROM command_queue WHERE id = $1`, attemptBound.EventID).Scan(&queuedAt, &expiresAt); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	if !expiresAt.Equal(queuedAt.Add(CommandQueuePatience)) {
		t.Fatalf("expires_at - queued_at = %s, want %s", expiresAt.Sub(queuedAt), CommandQueuePatience)
	}

	for i := 1; i <= MaxCommandAttempts; i++ {
		in := RecordAttemptInput{
			CommandQueueID: attemptBound.EventID,
			AttemptKey:     string(rune('a' + i - 1)),
			Now:            queuedAt.Add(time.Duration(i) * time.Minute),
			ActorTokenID:   actor.ID,
			Source:         actor.Source,
		}
		got, err := svc.RecordAttempt(ctx, in)
		if err != nil || got.AttemptCount != i || !got.Fresh {
			t.Fatalf("attempt %d = (%+v, %v)", i, got, err)
		}
		if i == 1 {
			replay, err := svc.RecordAttempt(ctx, in)
			if err != nil || replay.Fresh || replay.EventID != got.EventID || replay.AttemptCount != 1 {
				t.Fatalf("attempt replay = (%+v, %v), want same non-fresh event", replay, err)
			}
		}
	}
	if _, err := svc.RecordAttempt(ctx, RecordAttemptInput{
		CommandQueueID: attemptBound.EventID, AttemptKey: "sixth", Now: queuedAt.Add(time.Hour),
		ActorTokenID: actor.ID, Source: actor.Source,
	}); err != ErrCommandPatienceExhausted {
		t.Fatalf("sixth attempt err = %v, want ErrCommandPatienceExhausted", err)
	}

	expired, err := svc.ExpireDue(ctx, ExpireDueInput{
		Now: queuedAt.Add(time.Hour), Limit: 10, ActorTokenID: actor.ID, Source: actor.Source,
	})
	if err != nil || len(expired) != 1 || expired[0].Reason != ExpiryAttemptsExhausted {
		t.Fatalf("expire exhausted = (%+v, %v)", expired, err)
	}
	assertPatienceRow(t, pool, attemptBound.EventID, "expired", MaxCommandAttempts, ExpiryAttemptsExhausted)
	if _, err := svc.Ack(ctx, AckInput{CommandQueueID: attemptBound.EventID, StatusCode: 200, OK: true}); err != ErrCommandTerminalConflict {
		t.Fatalf("ack after expiry err = %v, want conflict", err)
	}

	deadlineBound := enqueue("deadline-bound")
	var deadline time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM command_queue WHERE id = $1`, deadlineBound.EventID).Scan(&deadline); err != nil {
		t.Fatalf("read deadline-bound expiry: %v", err)
	}
	expired, err = svc.ExpireDue(ctx, ExpireDueInput{
		Now: deadline, Limit: 10, ActorTokenID: actor.ID, Source: actor.Source,
	})
	if err != nil || len(expired) != 1 || expired[0].Reason != ExpiryDeadline {
		t.Fatalf("expire deadline = (%+v, %v)", expired, err)
	}
	assertPatienceRow(t, pool, deadlineBound.EventID, "expired", 0, ExpiryDeadline)

	causing, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "owns remote delivery", Actor: actor, State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"remote_write_applied"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
	})
	if err != nil {
		t.Fatalf("create causing work item: %v", err)
	}
	owned, err := svc.Enqueue(ctx, EnqueueInput{
		TargetNodeID: "m4", OriginNodeID: "hub",
		CommandPath: "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition",
		CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: "owned-expiry",
		OriginActorTokenID: &actor.ID, Source: actor.Source, CausingWorkItemID: &causing.ID,
	})
	if err != nil {
		t.Fatalf("enqueue owned command: %v", err)
	}
	var ownedDeadline time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM command_queue WHERE id=$1`, owned.EventID).Scan(&ownedDeadline); err != nil {
		t.Fatal(err)
	}
	resolved, err := svc.ExpireDue(ctx, ExpireDueInput{Now: ownedDeadline, ActorTokenID: actor.ID, Source: actor.Source, LocalNodeID: "hub"})
	if err != nil || len(resolved) != 1 || resolved[0].CauseResolution != CauseLocalFailed {
		t.Fatalf("expire owned command = (%+v, %v)", resolved, err)
	}
	var state domain.WorkItemState
	var reason *string
	if err := pool.QueryRow(ctx, `SELECT state, state_reason FROM work_items WHERE id=$1`, causing.ID).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != domain.WorkItemFailed || reason == nil || *reason != CrossNodeDeliveryExpiredReason {
		t.Fatalf("causing item state=%s reason=%v", state, reason)
	}
	var resolution string
	if err := pool.QueryRow(ctx, `SELECT payload->>'cause_resolution' FROM events WHERE id=$1`, resolved[0].EventID).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if resolution != string(CauseLocalFailed) {
		t.Fatalf("expiry cause_resolution=%q", resolution)
	}

	noIdentity, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "must retain its home", Actor: actor, State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"remote_write_applied"}, HumanReviewStatus: domain.HumanReviewWavedThrough,
	})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := svc.Enqueue(ctx, EnqueueInput{
		TargetNodeID: "m4", OriginNodeID: "hub", CommandPath: "/v1/work-items/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/transition",
		CommandBody: json.RawMessage(`{"to":"running"}`), OriginIdempotencyKey: "no-local-identity",
		OriginActorTokenID: &actor.ID, Source: actor.Source, CausingWorkItemID: &noIdentity.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var remoteDeadline time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM command_queue WHERE id=$1`, remote.EventID).Scan(&remoteDeadline); err != nil {
		t.Fatal(err)
	}
	remoteResult, err := svc.ExpireDue(ctx, ExpireDueInput{Now: remoteDeadline, ActorTokenID: actor.ID, Source: actor.Source})
	if err != nil || len(remoteResult) != 1 || remoteResult[0].CauseResolution != CauseRemoteNotification {
		t.Fatalf("expiry without local node identity = (%+v, %v)", remoteResult, err)
	}
	var retained domain.WorkItemState
	if err := pool.QueryRow(ctx, `SELECT state FROM work_items WHERE id=$1`, noIdentity.ID).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != domain.WorkItemRunning {
		t.Fatalf("coincident local UUID mutated without proven home: %s", retained)
	}
}

func assertPatienceRow(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, wantState string, wantAttempts int, wantReason ExpiryReason) {
	t.Helper()
	var state string
	var attempts int
	var reason *string
	var terminalID *uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT state, attempt_count, terminal_reason, terminal_event_id
		FROM command_queue WHERE id = $1
	`, id).Scan(&state, &attempts, &reason, &terminalID); err != nil {
		t.Fatalf("read patience row: %v", err)
	}
	if state != wantState || attempts != wantAttempts || reason == nil || *reason != string(wantReason) || terminalID == nil {
		t.Fatalf("patience row state=%s attempts=%d reason=%v terminal=%v", state, attempts, reason, terminalID)
	}
}
