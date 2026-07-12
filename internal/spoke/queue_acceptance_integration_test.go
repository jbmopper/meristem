package spoke_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/spoke"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestQueueFirstTwoNodeAcceptance exercises the stage-1 queue path across two
// independent event logs and two real HTTP API handlers:
//
//	hub enqueue -> spoke outbound pull -> local work-item creation -> hub ack.
//
// The hub rejects the first ack before it reaches the API. That leaves the
// command pending, forcing a second local execution with the original
// idempotency key. The local projection and event counts prove that retry
// collapses before the hub accepts the second ack. Finally, the hub listener is
// removed and the spoke's local readiness is checked to pin partition-local
// availability.
//
// pgtest.NewPool skips unless the Postgres integration environment is set.
func TestQueueFirstTwoNodeAcceptance(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	hubPool, hubToken := newAcceptanceNode(t, ctx, "hub", logger, []string{
		crossnode.QueueWriteScope("spoke-a", crossnode.OperationClassWorkItemsWrite),
		crossnode.OriginScope("hub"),
		crossnode.QueueDrainScope("spoke-a"),
		crossnode.QueueAckScope("spoke-a"),
		access.ScopeFeedRead,
	})
	localPool, localToken := newAcceptanceNode(t, ctx, "spoke-a", logger, []string{
		access.ScopeWorkItemsCreate,
		crossnode.TargetExecuteScope("spoke-a"),
		crossnode.OriginScope("hub"),
	})

	// Server constructors capture MERISTEM_NODE_ID, so the two handlers keep
	// independent identities even though this test process has one environment.
	t.Setenv(api.EnvNodeID, "hub")
	hubHandler := api.New(hubPool, logger).Handler()
	var ackAttempts atomic.Int32
	hubHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/crossnode/commands/") && strings.HasSuffix(r.URL.Path, "/ack") {
			if ackAttempts.Add(1) == 1 {
				http.Error(w, "injected transient ack failure", http.StatusServiceUnavailable)
				return
			}
		}
		hubHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(hubHTTP.Close)

	t.Setenv(api.EnvNodeID, "spoke-a")
	localHTTP := httptest.NewServer(api.New(localPool, logger).Handler())
	t.Cleanup(localHTTP.Close)

	const originKey = "acceptance-queue-create-1"
	enqueueBody := []byte(`{
		"command_path":"/v1/work-items",
		"command_body":{
			"title":"queue-first acceptance item",
			"body":"created on the home node through an outbound spoke poll"
		}
	}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hubHTTP.URL+"/v1/crossnode/commands", bytes.NewReader(enqueueBody))
	if err != nil {
		t.Fatalf("build enqueue request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+hubToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(crossnode.HeaderIdempotencyKey, originKey)
	req.Header.Set(crossnode.HeaderQueueFor, "spoke-a")
	req.Header.Set(crossnode.HeaderTargetNode, "hub")
	req.Header.Set(crossnode.HeaderOriginNode, "hub")
	resp, err := hubHTTP.Client().Do(req)
	if err != nil {
		t.Fatalf("enqueue request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue: status=%d body=%s", resp.StatusCode, body)
	}
	assertCount(t, hubPool, "SELECT count(*) FROM command_queue WHERE target_node_id = 'spoke-a' AND state = 'pending'", 1)

	poller := spoke.New(spoke.Config{
		HubBaseURL: hubHTTP.URL,
		NodeID:     "spoke-a",
		HubToken:   hubToken,
		LocalURL:   localHTTP.URL,
		LocalToken: localToken,
		DrainLimit: 10,
	}, &http.Client{Timeout: 2 * time.Second}, nil, logger)

	first := poller.Tick(ctx)
	if !first.HubReachable || first.Drained != 1 || first.Executed != 1 || first.Acked != 0 {
		t.Fatalf("first tick = %+v, want reachable drained=1 executed=1 acked=0", first)
	}
	assertCount(t, localPool, "SELECT count(*) FROM work_items", 1)
	assertEventCount(t, localPool, domain.EventWorkItemCreated, 1)
	assertRemoteProvenance(t, hubPool, localPool)
	assertLocalIdempotencyRecord(t, localPool, originKey)
	assertCount(t, hubPool, "SELECT count(*) FROM command_queue WHERE target_node_id = 'spoke-a' AND state = 'pending'", 1)

	second := poller.Tick(ctx)
	if !second.HubReachable || second.Drained != 1 || second.Executed != 1 || second.Acked != 1 {
		t.Fatalf("second tick = %+v, want reachable drained=1 executed=1 acked=1", second)
	}
	if ackAttempts.Load() != 2 {
		t.Fatalf("ack attempts = %d, want 2", ackAttempts.Load())
	}
	// The second local POST returned the cached 201 response; it did not append
	// a second work_item.created or idempotency.recorded event.
	assertCount(t, localPool, "SELECT count(*) FROM work_items", 1)
	assertEventCount(t, localPool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, localPool, domain.EventIdempotencyRecorded, 1)
	assertCount(t, hubPool, "SELECT count(*) FROM command_queue WHERE target_node_id = 'spoke-a' AND state = 'done' AND outcome_status_code = 201 AND outcome_ok", 1)

	// Remove only the hub listener. The poller degrades to a clean no-op while
	// the home node's own API and database remain ready.
	hubHTTP.Close()
	partitioned := poller.Tick(ctx)
	if partitioned.HubReachable || partitioned.Drained != 0 || partitioned.Executed != 0 || partitioned.Acked != 0 {
		t.Fatalf("partitioned tick = %+v, want unreachable clean no-op", partitioned)
	}
	readyResp, err := localHTTP.Client().Get(localHTTP.URL + "/readyz")
	if err != nil {
		t.Fatalf("local readiness during hub loss: %v", err)
	}
	defer func() { _ = readyResp.Body.Close() }()
	if readyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(readyResp.Body)
		t.Fatalf("local readiness during hub loss: status=%d body=%s", readyResp.StatusCode, body)
	}
}

func assertRemoteProvenance(t *testing.T, hubPool, localPool *pgxpool.Pool) {
	t.Helper()
	var queueID, originActor uuid.UUID
	if err := hubPool.QueryRow(context.Background(), `
		SELECT id, origin_actor_token_id FROM command_queue LIMIT 1
	`).Scan(&queueID, &originActor); err != nil {
		t.Fatalf("read queued provenance: %v", err)
	}
	var localActor, payloadOriginActor, payloadQueue uuid.UUID
	var localSource, originNode, originSource string
	if err := localPool.QueryRow(context.Background(), `
		SELECT actor_token_id, source,
		       payload->'remote_provenance'->>'origin_node_id',
		       (payload->'remote_provenance'->>'origin_actor_token_id')::uuid,
		       payload->'remote_provenance'->>'origin_actor_source',
		       (payload->'remote_provenance'->>'queue_command_id')::uuid
		FROM events WHERE kind=$1 LIMIT 1
	`, domain.EventWorkItemCreated).Scan(&localActor, &localSource, &originNode, &payloadOriginActor, &originSource, &payloadQueue); err != nil {
		t.Fatalf("read target event provenance: %v", err)
	}
	if localActor == originActor {
		t.Fatal("target event impersonated the remote actor")
	}
	if localSource != string(domain.SourceAgent) || originNode != "hub" || originSource != string(domain.SourceAgent) || payloadOriginActor != originActor || payloadQueue != queueID {
		t.Fatalf("target provenance local=(%s,%s) remote=(%s,%s,%s,%s)", localActor, localSource, originNode, payloadOriginActor, originSource, payloadQueue)
	}
}

func newAcceptanceNode(t *testing.T, ctx context.Context, nodeID string, logger *slog.Logger, scopes []string) (*pgxpool.Pool, string) {
	t.Helper()
	pool := pgtest.NewPool(t, "meristem_network_"+strings.ReplaceAll(nodeID, "-", "_"))
	if err := storage.Migrate(ctx, pool, logger); err != nil {
		t.Fatalf("migrate %s: %v", nodeID, err)
	}
	authService := auth.NewService(pool, app.NewEventWriter())
	root, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name:   nodeID + "-acceptance-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create %s root token: %v", nodeID, err)
	}
	agent, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name:   nodeID + "-acceptance-agent",
		Scopes: scopes,
		Source: domain.SourceAgent,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create %s agent token: %v", nodeID, err)
	}
	return pool, agent.Secret
}

func assertCount(t *testing.T, pool *pgxpool.Pool, query string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d (query %q)", got, want, query)
	}
}

func assertEventCount(t *testing.T, pool *pgxpool.Pool, kind string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE kind = $1`, kind).Scan(&got); err != nil {
		t.Fatalf("count %s events: %v", kind, err)
	}
	if got != want {
		t.Fatalf("%s events = %d, want %d", kind, got, want)
	}
}

func assertLocalIdempotencyRecord(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	var scope string
	var status int
	if err := pool.QueryRow(context.Background(), `
		SELECT scope, response_status
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(&scope, &status); err != nil {
		t.Fatalf("read local idempotency record: %v", err)
	}
	if scope != "POST /v1/work-items" || status != http.StatusCreated {
		t.Fatalf("local idempotency record = (%q, %d), want (POST /v1/work-items, 201)", scope, status)
	}
}
