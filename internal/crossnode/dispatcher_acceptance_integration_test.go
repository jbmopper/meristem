package crossnode_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/api"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

// TestDirectRouteTwoNodeAcceptance proves that the production Dispatcher, not
// test-only candidate construction, loads A's event-backed registry projection
// and mutates an independently stored B-homed work item through B's canonical
// REST API. It also pins idempotent replay and accelerated direct-to-queue
// ordering. pgtest.NewPool skips unless Postgres integration is enabled.
func TestDirectRouteTwoNodeAcceptance(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	aPool, aRoot := newDirectAcceptanceNode(t, ctx, "direct_a", logger)
	bPool, bRoot := newDirectAcceptanceNode(t, ctx, "direct_b", logger)

	bAuth := auth.NewService(bPool, app.NewEventWriter())
	bPeer, err := bAuth.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "node-a-direct-to-b",
		Scopes: []string{access.ScopeWorkItemsWriteAll},
		Source: domain.SourceAgent,
		Actor:  &bRoot.Token,
	})
	if err != nil {
		t.Fatalf("create B-minted scoped peer token: %v", err)
	}
	bItems := workitems.NewService(bPool, app.NewEventWriter())
	item, err := bItems.Create(ctx, workitems.CreateInput{
		Title: "B-homed direct acceptance item",
		Body:  "must be mutated only in B's event log",
		Actor: bRoot.Token,
	})
	if err != nil {
		t.Fatalf("create B work item: %v", err)
	}

	// The registered direct origin stays stable while the handler can be
	// switched to a retryable failure for the accelerated fallback proof.
	t.Setenv(api.EnvNodeID, "node-b")
	bHandler := api.New(bPool, logger).Handler()
	var failDirect atomic.Bool
	var directAttempts atomic.Int32
	bDirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		directAttempts.Add(1)
		if failDirect.Load() {
			http.Error(w, "injected unavailable peer", http.StatusServiceUnavailable)
			return
		}
		bHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(bDirect.Close)

	var queueHits atomic.Int32
	var queueTarget, queueAuth string
	queueHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueHits.Add(1)
		queueTarget = r.Header.Get(crossnode.HeaderQueueFor)
		queueAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"queued":true}`))
	}))
	t.Cleanup(queueHost.Close)

	appendDirectRegistryNode(t, ctx, aPool, aRoot.Token, "node-b", bDirect.URL, []string{"hub"})
	appendDirectRegistryNode(t, ctx, aPool, aRoot.Token, "hub", queueHost.URL, nil)

	// Keyed on the terminating peer: node-b's own bearer for a direct attempt,
	// the queue host's separate capability for a queue hop. The two are
	// deliberately different values — a resolver that returned one credential
	// for both would pass this test's assertions but prove nothing about
	// per-peer custody.
	resolver := func(_ context.Context, req crossnode.CredentialRequest) (string, error) {
		switch req.TerminatingPeer {
		case "node-b":
			return bPeer.Secret, nil
		case "hub":
			return "hub-queue-capability", nil
		default:
			return "", crossnode.ErrMissingCredential
		}
	}
	policy := crossnode.DeliveryPolicy{
		DirectAttempts: 3,
		AttemptTimeout: time.Second,
		DirectPatience: time.Second,
		DirectBackoff:  func(int) time.Duration { return 0 },
	}
	dispatcher := crossnode.NewDispatcher(aPool, bDirect.Client(), resolver, policy)
	command := crossnode.Command{
		OriginNodeID:   "node-a",
		TargetNodeID:   "node-b",
		IdempotencyKey: "direct-b-transition-1",
		Path:           "/v1/work-items/" + item.ID.String() + "/transition",
		Body:           json.RawMessage(`{"to":"triaged","reason":"direct route acceptance"}`),
	}

	first, err := dispatcher.DispatchMutation(ctx, command, nil)
	if err != nil {
		t.Fatalf("first direct dispatch: %v", err)
	}
	if !first.Delivered || first.Terminal.Kind != crossnode.KindDirect || first.StatusCode != http.StatusOK {
		t.Fatalf("first outcome = %+v, want direct 200", first)
	}
	second, err := dispatcher.DispatchMutation(ctx, command, first.Cooldowns)
	if err != nil {
		t.Fatalf("idempotent direct replay: %v", err)
	}
	if !second.Delivered || second.Terminal.Kind != crossnode.KindDirect || second.StatusCode != http.StatusOK {
		t.Fatalf("second outcome = %+v, want cached direct 200", second)
	}
	assertDirectEventCount(t, bPool, item.ID.String(), domain.EventWorkItemTransitioned, 1)
	assertDirectItemState(t, bPool, item.ID.String(), domain.WorkItemTriaged)
	assertDirectIdempotency(t, bPool, command.IdempotencyKey, command.Path)
	assertDirectEventCount(t, aPool, item.ID.String(), domain.EventWorkItemTransitioned, 0)

	read, err := dispatcher.ReadWorkItem(ctx, "node-a", domain.FormatQualifiedRef("node-b", item.ID))
	if err != nil {
		t.Fatalf("qualified direct read: %v", err)
	}
	if read.StatusCode != http.StatusOK || !strings.Contains(string(read.Body), item.ID.String()) {
		t.Fatalf("qualified read status/body = %d %s", read.StatusCode, read.Body)
	}

	// A retryable direct failure consumes exactly the accelerated direct budget
	// before the same production-selected candidate list reaches the queue host.
	beforeAttempts := directAttempts.Load()
	failDirect.Store(true)
	fallbackCommand := command
	fallbackCommand.IdempotencyKey = "direct-b-transition-queue-fallback-1"
	fallbackCommand.Body = json.RawMessage(`{"to":"planned","reason":"must be queued after direct patience"}`)
	fallback, err := dispatcher.DispatchMutation(ctx, fallbackCommand, nil)
	if err != nil {
		t.Fatalf("fallback dispatch: %v", err)
	}
	if !fallback.Delivered || fallback.Terminal.Kind != crossnode.KindQueue || fallback.StatusCode != http.StatusAccepted {
		t.Fatalf("fallback outcome = %+v, want queue 202", fallback)
	}
	if got := directAttempts.Load() - beforeAttempts; got != 3 {
		t.Fatalf("accelerated direct attempts = %d, want 3", got)
	}
	if len(fallback.Attempts) != 4 {
		t.Fatalf("fallback attempts = %d, want 3 direct + 1 queue", len(fallback.Attempts))
	}
	for i := 0; i < 3; i++ {
		if fallback.Attempts[i].Candidate.Kind != crossnode.KindDirect {
			t.Fatalf("fallback attempt[%d] = %s", i, fallback.Attempts[i].Candidate.Kind)
		}
	}
	if fallback.Attempts[3].Candidate.Kind != crossnode.KindQueue || queueHits.Load() != 1 {
		t.Fatalf("queue attempt/hits = %s/%d", fallback.Attempts[3].Candidate.Kind, queueHits.Load())
	}
	if queueTarget != "node-b" || queueAuth != "Bearer hub-queue-capability" {
		t.Fatalf("queue target/auth = %q %q", queueTarget, queueAuth)
	}
	assertDirectItemState(t, bPool, item.ID.String(), domain.WorkItemTriaged)
}

func newDirectAcceptanceNode(t *testing.T, ctx context.Context, name string, logger *slog.Logger) (*pgxpool.Pool, auth.CreateTokenResult) {
	t.Helper()
	pool := pgtest.NewPool(t, "meristem_network_"+name)
	if err := storage.Migrate(ctx, pool, logger); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	root, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: name + "-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root %s: %v", name, err)
	}
	return pool, root
}

func appendDirectRegistryNode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, actor domain.Token, nodeID, directURL string, queueVia []string) {
	t.Helper()
	payload, err := nodes.BuildRegisteredPayload(nodes.RegisterParams{
		NodeID: nodeID, DirectURL: &directURL, RelayVia: queueVia, Status: string(domain.NodeStatusActive),
	})
	if err != nil {
		t.Fatalf("build node %s: %v", nodeID, err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin node %s: %v", nodeID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := app.NewEventWriter().Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectNode, SubjectID: nodes.NodeSubjectID(nodeID),
		Kind: domain.EventNodeRegistered, Source: actor.Source, ActorTokenID: &actor.ID, Payload: payload,
	}); err != nil {
		t.Fatalf("append node %s: %v", nodeID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit node %s: %v", nodeID, err)
	}
}

func assertDirectEventCount(t *testing.T, pool *pgxpool.Pool, subjectID, kind string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE subject_id = $1 AND kind = $2`, subjectID, kind).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	if got != want {
		t.Fatalf("%s events for %s = %d, want %d", kind, subjectID, got, want)
	}
}

func assertDirectItemState(t *testing.T, pool *pgxpool.Pool, id string, want domain.WorkItemState) {
	t.Helper()
	var got string
	if err := pool.QueryRow(context.Background(), `SELECT state FROM work_items WHERE id = $1`, id).Scan(&got); err != nil {
		t.Fatalf("read work item state: %v", err)
	}
	if got != string(want) {
		t.Fatalf("work item state = %s, want %s", got, want)
	}
}

func assertDirectIdempotency(t *testing.T, pool *pgxpool.Pool, key, path string) {
	t.Helper()
	var gotScope string
	var gotStatus int
	if err := pool.QueryRow(context.Background(), `SELECT scope, response_status FROM idempotency_keys WHERE key = $1`, key).Scan(&gotScope, &gotStatus); err != nil {
		t.Fatalf("read direct idempotency record: %v", err)
	}
	if gotScope != "POST "+path || gotStatus != http.StatusOK {
		t.Fatalf("idempotency = %q/%d, want %q/200", gotScope, gotStatus, "POST "+path)
	}
}
