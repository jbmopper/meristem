package crossnode_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/peerhttp"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

// TestOutcomeReturnTwoNodeAcceptance proves the Stage 1 return path across
// independent event logs: remote expiry remains durable through an origin
// polling outage, then one authenticated outbound poll records one local
// observation and one causing-item transition. A stale replay is a no-op.
func TestOutcomeReturnTwoNodeAcceptance(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hostPool, hostActor, hostReadSecret := newOutcomeNode(t, ctx, "queue-host", logger,
		[]string{crossnode.QueueOutcomeReadScope("origin-a")})
	originPool, originActor, _ := newOutcomeNode(t, ctx, "origin-a", logger,
		[]string{crossnode.OutcomeObserveScope("queue-host", "origin-a")})

	cause, err := workitems.NewService(originPool, app.NewEventWriter()).Create(ctx, workitems.CreateInput{
		Title: "origin-owned delivery", Actor: originActor,
	})
	if err != nil {
		t.Fatalf("create origin cause: %v", err)
	}
	hostQueue := crossnode.NewQueueService(hostPool, app.NewEventWriter())
	queued, err := hostQueue.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID: "target-b", OriginNodeID: "origin-a",
		CommandPath: "/v1/work-items", CommandBody: []byte(`{"title":"remote"}`),
		OriginIdempotencyKey: "outcome-acceptance-1", OriginActorTokenID: &hostActor.ID,
		Source: hostActor.Source, CausingWorkItemID: &cause.ID,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	expired, err := hostQueue.ExpireDue(ctx, crossnode.ExpireDueInput{
		Now: time.Now().UTC().Add(25 * time.Hour), ActorTokenID: hostActor.ID,
		Source: hostActor.Source, LocalNodeID: "queue-host",
	})
	if err != nil || len(expired) != 1 || expired[0].CauseResolution != crossnode.CauseRemoteNotification {
		t.Fatalf("expire remote cause: results=%+v err=%v", expired, err)
	}

	t.Setenv(api.EnvNodeID, "queue-host")
	hostHandler := api.New(hostPool, logger).Handler()
	var outage atomic.Bool
	outage.Store(true)
	hostHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if outage.Load() {
			http.Error(w, "injected queue-host outage", http.StatusServiceUnavailable)
			return
		}
		hostHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(hostHTTP.Close)

	syncer, err := crossnode.NewOutcomeSyncService(
		crossnode.NewOutcomeObserver(originPool, app.NewEventWriter()),
		crossnode.OutcomeSyncConfig{
			QueueHostNodeID: "queue-host", QueueHostOrigin: hostHTTP.URL,
			OriginNodeID: "origin-a", QueueHostToken: hostReadSecret,
			LocalActor: originActor, RequestTimeout: 2 * time.Second,
		}, peerhttp.Options{})
	if err != nil {
		t.Fatalf("construct syncer: %v", err)
	}

	if _, err := syncer.Tick(ctx); err == nil {
		t.Fatal("outage tick unexpectedly succeeded")
	}
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM crossnode_outcome_observations", 0)
	assertOutcomeCount(t, hostPool, "SELECT count(*) FROM command_queue WHERE state='expired'", 1)
	assertWorkState(t, originPool, cause.ID, domain.WorkItemCaptured)

	outage.Store(false)
	first, err := syncer.Tick(ctx)
	if err != nil || first.Observed != 1 || first.CauseTransitions != 1 || first.Cursor <= 0 {
		t.Fatalf("first healthy tick=%+v err=%v", first, err)
	}
	assertWorkState(t, originPool, cause.ID, domain.WorkItemFailed)
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM events WHERE kind='command_outcome.observed'", 1)
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM events WHERE kind='work_item.transitioned' AND subject_id='"+cause.ID.String()+"'", 1)
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM crossnode_outcome_observations WHERE command_queue_id='"+queued.EventID.String()+"' AND cause_resolution='local_work_item_failed'", 1)

	replay, err := syncer.Tick(ctx)
	if err != nil || replay.Observed != 0 || replay.CauseTransitions != 0 || replay.Cursor != first.Cursor {
		t.Fatalf("replay tick=%+v err=%v", replay, err)
	}
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM events WHERE kind='command_outcome.observed'", 1)
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM events WHERE kind='work_item.transitioned' AND subject_id='"+cause.ID.String()+"'", 1)

	// A stale page with the identical terminal fact is also a no-op, while a
	// conflicting terminal fact for the same command fails closed.
	remotePage, err := hostQueue.OutcomesForOrigin(ctx, "origin-a", 0, 10)
	if err != nil || len(remotePage) != 1 {
		t.Fatalf("read remote outcome page: len=%d err=%v", len(remotePage), err)
	}
	observer := crossnode.NewOutcomeObserver(originPool, app.NewEventWriter())
	stale, err := observer.Observe(ctx, crossnode.ObserveOutcomesInput{
		QueueHostNodeID: "queue-host", LocalNodeID: "origin-a", LocalActor: originActor, Outcomes: remotePage,
	})
	if err != nil || stale.Observed != 0 || stale.CauseTransitions != 0 || stale.Cursor != first.Cursor {
		t.Fatalf("stale page=%+v err=%v", stale, err)
	}
	conflict := remotePage[0]
	changedReason := string(crossnode.ExpiryAttemptsExhausted)
	conflict.TerminalReason = &changedReason
	if _, err := observer.Observe(ctx, crossnode.ObserveOutcomesInput{
		QueueHostNodeID: "queue-host", LocalNodeID: "origin-a", LocalActor: originActor, Outcomes: []crossnode.QueueOutcome{conflict},
	}); !errors.Is(err, crossnode.ErrOutcomeConflict) {
		t.Fatalf("conflicting page error=%v, want ErrOutcomeConflict", err)
	}

	// A terminal row for another origin is filtered at the queue-host query and
	// cannot advance or appear in origin-a's response.
	foreign, err := hostQueue.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID: "target-b", OriginNodeID: "origin-b",
		CommandPath: "/v1/work-items", CommandBody: []byte(`{"title":"foreign"}`),
		OriginIdempotencyKey: "outcome-acceptance-foreign", OriginActorTokenID: &hostActor.ID,
		Source: hostActor.Source,
	})
	if err != nil {
		t.Fatalf("enqueue foreign outcome: %v", err)
	}
	if _, err := hostQueue.Ack(ctx, crossnode.AckInput{
		CommandQueueID: foreign.EventID, StatusCode: http.StatusCreated,
		Outcome: crossnode.CommandDone, ActorTokenID: &hostActor.ID, Source: hostActor.Source,
	}); err != nil {
		t.Fatalf("ack foreign outcome: %v", err)
	}
	filtered, err := syncer.Tick(ctx)
	if err != nil || filtered.Observed != 0 || filtered.Cursor != first.Cursor {
		t.Fatalf("foreign-filter tick=%+v err=%v", filtered, err)
	}

	// A missing origin-local cause is still terminally audited and advances the
	// cursor; it cannot wedge the poller into an infinite replay loop.
	missing := uuid.New()
	_, err = hostQueue.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID: "target-b", OriginNodeID: "origin-a",
		CommandPath: "/v1/work-items", CommandBody: []byte(`{"title":"missing cause"}`),
		OriginIdempotencyKey: "outcome-acceptance-missing", OriginActorTokenID: &hostActor.ID,
		Source: hostActor.Source, CausingWorkItemID: &missing,
	})
	if err != nil {
		t.Fatalf("enqueue missing cause: %v", err)
	}
	if rows, err := hostQueue.ExpireDue(ctx, crossnode.ExpireDueInput{
		Now: time.Now().UTC().Add(25 * time.Hour), ActorTokenID: hostActor.ID,
		Source: hostActor.Source, LocalNodeID: "queue-host",
	}); err != nil || len(rows) != 1 {
		t.Fatalf("expire missing cause: rows=%+v err=%v", rows, err)
	}
	missingResult, err := syncer.Tick(ctx)
	if err != nil || missingResult.Observed != 1 || missingResult.CauseTransitions != 0 || missingResult.Cursor <= first.Cursor {
		t.Fatalf("missing-cause tick=%+v err=%v", missingResult, err)
	}
	assertOutcomeCount(t, originPool, "SELECT count(*) FROM crossnode_outcome_observations WHERE causing_work_item_id='"+missing.String()+"' AND cause_resolution='local_work_item_missing'", 1)
}

func newOutcomeNode(t *testing.T, ctx context.Context, nodeID string, logger *slog.Logger, scopes []string) (*pgxpool.Pool, domain.Token, string) {
	t.Helper()
	pool := pgtest.NewPool(t, "meristem_outcome_"+nodeID)
	if err := storage.Migrate(ctx, pool, logger); err != nil {
		t.Fatalf("migrate %s: %v", nodeID, err)
	}
	service := auth.NewService(pool, app.NewEventWriter())
	root, err := service.CreateToken(ctx, auth.CreateTokenInput{Name: nodeID + "-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create %s root: %v", nodeID, err)
	}
	agent, err := service.CreateToken(ctx, auth.CreateTokenInput{
		Name: nodeID + "-outcome-agent", Source: domain.SourceAgent, Scopes: scopes, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create %s agent: %v", nodeID, err)
	}
	return pool, agent.Token, agent.Secret
}

func assertOutcomeCount(t *testing.T, pool *pgxpool.Pool, query string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != want {
		t.Fatalf("count=%d want=%d query=%s", got, want, query)
	}
}

func assertWorkState(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want domain.WorkItemState) {
	t.Helper()
	var got domain.WorkItemState
	if err := pool.QueryRow(context.Background(), "SELECT state FROM work_items WHERE id=$1", id).Scan(&got); err != nil {
		t.Fatalf("read work item state: %v", err)
	}
	if got != want {
		t.Fatalf("work item state=%s want=%s", got, want)
	}
}
