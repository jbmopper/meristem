package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestRegistrySnapshotTwoDatabaseAPI(t *testing.T) {
	ctx := context.Background()
	home := pgtest.NewPool(t, "registry_snapshot_home")
	consumer := pgtest.NewPool(t, "registry_snapshot_consumer")
	if err := storage.Migrate(ctx, home, discardLogger()); err != nil {
		t.Fatalf("migrate home: %v", err)
	}
	if err := storage.Migrate(ctx, consumer, discardLogger()); err != nil {
		t.Fatalf("migrate consumer: %v", err)
	}

	homeAuth := auth.NewService(home, app.NewEventWriter())
	homeRoot, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "home-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("home root: %v", err)
	}
	systemActor, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "registry-writer", Source: domain.SourceSystem, Actor: &homeRoot.Token})
	if err != nil {
		t.Fatalf("registry writer: %v", err)
	}
	appendRegistryNode(t, ctx, home, systemActor.Token, "hub", "https://HUB.example:443/", nil)
	appendRegistryNode(t, ctx, home, systemActor.Token, "spoke", "", []string{"hub"})
	readToken, err := homeAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "spoke-registry-reader", Source: domain.SourceAgent, Scopes: []string{nodes.SnapshotReadScope("hub")}, Actor: &homeRoot.Token})
	if err != nil {
		t.Fatalf("read token: %v", err)
	}

	t.Setenv(EnvNodeID, "hub")
	t.Setenv(EnvRegistryHomeNodeID, "")
	homeServer := New(home, nil)
	get := httptest.NewRequest(http.MethodGet, "/v1/nodes/registry-snapshot", nil)
	get.Header.Set("Authorization", "Bearer "+readToken.Secret)
	getRec := httptest.NewRecorder()
	homeServer.Handler().ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("snapshot GET: %d %s", getRec.Code, getRec.Body.String())
	}
	var snapshot nodes.RegistrySnapshot
	if err := json.Unmarshal(getRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.SourceNodeID != "hub" || len(snapshot.Nodes) != 2 || snapshot.Nodes[0].NodeID != "hub" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.Nodes[0].BaseURL == nil || *snapshot.Nodes[0].BaseURL != "https://hub.example" {
		t.Fatalf("snapshot origin not canonical: %v", snapshot.Nodes[0].BaseURL)
	}

	consumerAuth := auth.NewService(consumer, app.NewEventWriter())
	consumerRoot, err := consumerAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "consumer-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("consumer root: %v", err)
	}
	observeToken, err := consumerAuth.CreateToken(ctx, auth.CreateTokenInput{Name: "registry-observer", Source: domain.SourceSystem, Scopes: []string{nodes.SnapshotObserveScope("hub")}, Actor: &consumerRoot.Token})
	if err != nil {
		t.Fatalf("observe token: %v", err)
	}

	t.Setenv(EnvNodeID, "spoke")
	t.Setenv(EnvRegistryHomeNodeID, "hub")
	consumerServer := New(consumer, nil)
	postSnapshot := func(token, key string, value nodes.RegistrySnapshot) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(value)
		req := httptest.NewRequest(http.MethodPost, "/v1/nodes/registry-snapshot/observe", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		consumerServer.Handler().ServeHTTP(rec, req)
		return rec
	}

	if rec := postSnapshot(consumerRoot.Secret, "root-denied", snapshot); rec.Code != http.StatusForbidden {
		t.Fatalf("root observe = %d %s", rec.Code, rec.Body.String())
	}
	var deniedIdempotencyEvents int
	if err := consumer.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, domain.EventIdempotencyRecorded).Scan(&deniedIdempotencyEvents); err != nil {
		t.Fatal(err)
	}
	if deniedIdempotencyEvents != 0 {
		t.Fatalf("denied observation appended %d idempotency events", deniedIdempotencyEvents)
	}
	if rec := postSnapshot(observeToken.Secret, "observe-1", snapshot); rec.Code != http.StatusOK {
		t.Fatalf("observe = %d %s", rec.Code, rec.Body.String())
	}
	assertRegistrySnapshotState(t, ctx, consumer, snapshot.SourceRevision, 2, 1)

	// A new idempotency identity still reaches the service reducer; equal
	// source revision is a no-op and appends no second snapshot event.
	if rec := postSnapshot(observeToken.Secret, "observe-replay", snapshot); rec.Code != http.StatusOK {
		t.Fatalf("replay = %d %s", rec.Code, rec.Body.String())
	}
	assertRegistrySnapshotState(t, ctx, consumer, snapshot.SourceRevision, 2, 1)
	conflict := snapshot
	changed := "https://changed.example"
	conflict.Nodes = append([]nodes.SnapshotEntry(nil), snapshot.Nodes...)
	conflict.Nodes[0].BaseURL = &changed
	if rec := postSnapshot(observeToken.Secret, "observe-conflict", conflict); rec.Code != http.StatusConflict {
		t.Fatalf("same-revision conflict = %d %s", rec.Code, rec.Body.String())
	}

	stale := snapshot
	stale.SourceRevision--
	if rec := postSnapshot(observeToken.Secret, "observe-stale", stale); rec.Code != http.StatusConflict {
		t.Fatalf("stale = %d %s", rec.Code, rec.Body.String())
	}
	wrong := snapshot
	wrong.SourceNodeID = "other"
	if rec := postSnapshot(observeToken.Secret, "observe-wrong", wrong); rec.Code != http.StatusBadRequest {
		t.Fatalf("wrong source = %d %s", rec.Code, rec.Body.String())
	}
	malformed := snapshot
	bad := "https://hub.example/mcp"
	malformed.Nodes = append([]nodes.SnapshotEntry(nil), snapshot.Nodes...)
	malformed.Nodes[0].BaseURL = &bad
	if rec := postSnapshot(observeToken.Secret, "observe-bad", malformed); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed = %d %s", rec.Code, rec.Body.String())
	}
	assertRegistrySnapshotState(t, ctx, consumer, snapshot.SourceRevision, 2, 1)
}

func appendRegistryNode(t *testing.T, ctx context.Context, pool interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}, actor domain.Token, nodeID, baseURL string, relay []string) {
	t.Helper()
	var base *string
	if baseURL != "" {
		base = &baseURL
	}
	payload, err := nodes.BuildRegisteredPayload(nodes.RegisterParams{NodeID: nodeID, BaseURL: base, QueueVia: relay, Status: string(domain.NodeStatusActive)})
	if err != nil {
		t.Fatalf("build node %s: %v", nodeID, err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin node %s: %v", nodeID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := app.NewEventWriter().Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectNode, SubjectID: nodes.NodeSubjectID(nodeID), Kind: domain.EventNodeRegistered, Source: actor.Source, ActorTokenID: &actor.ID, Payload: payload}); err != nil {
		t.Fatalf("append node %s: %v", nodeID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit node %s: %v", nodeID, err)
	}
}

func assertRegistrySnapshotState(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, revision int64, wantNodes, wantEvents int) {
	t.Helper()
	var gotRevision int64
	if err := pool.QueryRow(ctx, `SELECT source_revision FROM registry_snapshot_state WHERE singleton`).Scan(&gotRevision); err != nil {
		t.Fatalf("snapshot state: %v", err)
	}
	if gotRevision != revision {
		t.Fatalf("source revision = %d, want %d", gotRevision, revision)
	}
	var nodesCount, eventsCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nodes`).Scan(&nodesCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = $1`, domain.EventRegistrySnapshotObserved).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if nodesCount != wantNodes || eventsCount != wantEvents {
		t.Fatalf("nodes/events = %d/%d, want %d/%d", nodesCount, eventsCount, wantNodes, wantEvents)
	}
}
