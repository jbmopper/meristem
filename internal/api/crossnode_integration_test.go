package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

type crossnodeQueueResponse struct {
	Queued             bool      `json:"queued"`
	Local              bool      `json:"local"`
	TargetNodeID       string    `json:"target_node_id"`
	CommandQueuedEvent uuid.UUID `json:"command_queued_event"`
}

func postCrossnode(t *testing.T, handler http.Handler, token, key string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, crossnode.CommandPath, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(crossnode.HeaderOriginNode, "sender")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestCrossnodeCommandEndpointIntegration exercises durable queueing, relay
// refusal, canonical-path validation, local-placeholder rejection, and
// idempotent replay with a narrow target-specific credential.
func TestCrossnodeCommandEndpointIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	_, queueToken := createCrossnodeToken(t, ctx, pool, "crossnode-queue-m4",
		[]string{crossnode.QueueWriteScope("m4", crossnode.OperationClassWorkItemsWrite), crossnode.OriginScope("sender")})

	t.Setenv(EnvNodeID, "hub")
	server := New(pool, nil)
	handler := server.Handler()
	body := []byte(`{"command_path":"/v1/work-items/7b1fc532-14f2-4be5-81a5-4719dd11d453/transition","command_body":{"to":"running"}}`)

	// 1. Queue path: X-Meristem-Queue-For names a peer → command.queued + row.
	queueHdr := map[string]string{
		crossnode.HeaderQueueFor:   "m4",
		crossnode.HeaderTargetNode: "hub",
	}
	rec := postCrossnode(t, handler, queueToken.Secret, "cmd-queue-1", body, queueHdr)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("queue: want 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	var queued crossnodeQueueResponse
	decodeResponse(t, rec, &queued)
	if !queued.Queued || queued.Local || queued.TargetNodeID != "m4" || queued.CommandQueuedEvent == uuid.Nil {
		t.Fatalf("unexpected queue response: %+v", queued)
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 1)
	assertTableCount(t, pool, "command_queue", 1)

	// Replay of the same key+body returns the cached 202 and appends nothing.
	replay := postCrossnode(t, handler, queueToken.Secret, "cmd-queue-1", body, queueHdr)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("queue replay: want 202, got %d", replay.Code)
	}
	if replay.Header().Get("Idempotency-Replayed") == "" {
		t.Fatalf("expected Idempotency-Replayed header on replay")
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 1)
	assertTableCount(t, pool, "command_queue", 1)

	// 2. Relay refusal: an already-relayed request that would need onward
	//    routing (queue-for a peer) is refused, never forwarded twice.
	refuseHdr := map[string]string{
		crossnode.HeaderQueueFor: "m4",
		crossnode.HeaderRelayed:  "true",
	}
	refused := postCrossnode(t, handler, queueToken.Secret, "cmd-refuse-1", body, refuseHdr)
	if refused.Code != http.StatusConflict {
		t.Fatalf("relay refusal: want 409, got %d body=%s", refused.Code, refused.Body.String())
	}
	var refuseBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	decodeResponse(t, refused, &refuseBody)
	if refuseBody.Error.Code != "relay_refused_already_relayed" {
		t.Fatalf("error code = %q, want relay_refused_already_relayed", refuseBody.Error.Code)
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 1) // unchanged

	// 3a. Missing queue target is not a local-execution surrogate. Direct
	// delivery calls the canonical REST endpoint.
	local := postCrossnode(t, handler, queueToken.Secret, "cmd-local-1", body, nil)
	if local.Code != http.StatusBadRequest {
		t.Fatalf("missing queue target: want 400, got %d body=%s", local.Code, local.Body.String())
	}

	// 3b. Queue-for naming this node is equally invalid.
	selfHdr := map[string]string{crossnode.HeaderQueueFor: "hub"}
	self := postCrossnode(t, handler, queueToken.Secret, "cmd-local-2", body, selfHdr)
	if self.Code != http.StatusBadRequest {
		t.Fatalf("self-queue: want 400, got %d", self.Code)
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 1) // still just the one real queue

	var actorID uuid.UUID
	var source string
	var payload []byte
	if err := pool.QueryRow(ctx, `
		SELECT actor_token_id, source, payload FROM events WHERE kind = $1
	`, domain.EventCommandQueued).Scan(&actorID, &source, &payload); err != nil {
		t.Fatalf("read queue attribution: %v", err)
	}
	if actorID != queueToken.Token.ID || source != string(domain.SourceAgent) {
		t.Fatalf("queue attribution = (%s,%s), want (%s,%s)", actorID, source, queueToken.Token.ID, domain.SourceAgent)
	}
	var provenance struct {
		OriginNodeID string `json:"origin_node_id"`
	}
	if err := json.Unmarshal(payload, &provenance); err != nil {
		t.Fatalf("decode queue provenance: %v", err)
	}
	if provenance.OriginNodeID != "sender" {
		t.Fatalf("origin_node_id = %q, want sender", provenance.OriginNodeID)
	}
}

func TestCrossnodeQueueAuthorizationDenialsAppendNoEvents(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	root, _ := createCrossnodeToken(t, ctx, pool, "crossnode-root", nil)
	_, wrongTarget := createCrossnodeToken(t, ctx, pool, "crossnode-queue-peer",
		[]string{crossnode.QueueWriteScope("peer", crossnode.OperationClassWorkItemsWrite), crossnode.OriginScope("sender")})
	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()
	body := []byte(`{"command_path":"/v1/work-items","command_body":{"title":"remote"}}`)
	headers := map[string]string{crossnode.HeaderQueueFor: "m4"}

	before := countAllEvents(t, pool)
	rootDenied := postCrossnode(t, handler, root.Secret, "root-denied", body, headers)
	if rootDenied.Code != http.StatusForbidden {
		t.Fatalf("root queue: want 403, got %d body=%s", rootDenied.Code, rootDenied.Body.String())
	}
	wrongDenied := postCrossnode(t, handler, wrongTarget.Secret, "wrong-target-denied", body, headers)
	if wrongDenied.Code != http.StatusForbidden {
		t.Fatalf("wrong-target queue: want 403, got %d body=%s", wrongDenied.Code, wrongDenied.Body.String())
	}
	targetMismatch := postCrossnode(t, handler, wrongTarget.Secret, "target-mismatch", body, map[string]string{
		crossnode.HeaderQueueFor:   "m4",
		crossnode.HeaderTargetNode: "other-hub",
	})
	if targetMismatch.Code != http.StatusConflict {
		t.Fatalf("target mismatch: want 409, got %d body=%s", targetMismatch.Code, targetMismatch.Body.String())
	}
	missingOrigin := postCrossnode(t, handler, wrongTarget.Secret, "missing-origin", body, map[string]string{
		crossnode.HeaderQueueFor:   "m4",
		crossnode.HeaderOriginNode: "",
	})
	if missingOrigin.Code != http.StatusBadRequest {
		t.Fatalf("missing origin: want 400, got %d body=%s", missingOrigin.Code, missingOrigin.Body.String())
	}
	if after := countAllEvents(t, pool); after != before {
		t.Fatalf("authorization denials appended %d events", after-before)
	}
}

// TestCrossnodeDrainEndpointsIntegration exercises the hub half of the spoke
// drain: GET /v1/crossnode/commands?target= returns a target's pending rows,
// POST /v1/crossnode/commands/{event_id}/ack folds the outcome onto the row
// (state pending -> done/failed), and a drained row disappears from the pending
// read. Unknown ids 404.
func TestCrossnodeDrainEndpointsIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, queueToken := createCrossnodeToken(t, ctx, pool, "crossnode-queue-m4-drain",
		[]string{crossnode.QueueWriteScope("m4", crossnode.OperationClassWorkItemsWrite), crossnode.OriginScope("sender")})
	_, drainToken := createCrossnodeToken(t, ctx, pool, "crossnode-drain-m4",
		[]string{crossnode.QueueDrainScope("m4")})
	_, ackToken := createCrossnodeToken(t, ctx, pool, "crossnode-ack-m4",
		[]string{crossnode.QueueAckScope("m4")})

	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()
	body := []byte(`{"command_path":"/v1/work-items/7b1fc532-14f2-4be5-81a5-4719dd11d453/transition","command_body":{"to":"running"}}`)

	// Enqueue two commands for peer "m4" with distinct idempotency keys so both
	// land as separate rows.
	queueHdr := map[string]string{crossnode.HeaderQueueFor: "m4"}
	var ids []uuid.UUID
	for _, key := range []string{"drain-1", "drain-2"} {
		rec := postCrossnode(t, handler, queueToken.Secret, key, body, queueHdr)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("enqueue %s: want 202, got %d body=%s", key, rec.Code, rec.Body.String())
		}
		var q crossnodeQueueResponse
		decodeResponse(t, rec, &q)
		ids = append(ids, q.CommandQueuedEvent)
	}

	// GET the pending queue for m4: both rows, oldest-first, with the replay
	// fields the spoke needs.
	list := getCrossnodeCommands(t, handler, drainToken.Secret, "m4", 10)
	if list.Code != http.StatusOK {
		t.Fatalf("GET commands: want 200, got %d body=%s", list.Code, list.Body.String())
	}
	var listResp struct {
		Commands []crossnode.QueuedCommand `json:"commands"`
	}
	decodeResponse(t, list, &listResp)
	if len(listResp.Commands) != 2 {
		t.Fatalf("pending commands = %d, want 2", len(listResp.Commands))
	}
	first := listResp.Commands[0]
	if first.TargetNodeID != "m4" || first.CommandPath != "/v1/work-items/7b1fc532-14f2-4be5-81a5-4719dd11d453/transition" || first.OriginIdempotencyKey != "drain-1" {
		t.Fatalf("unexpected first pending command: %+v", first)
	}
	if first.EventID != ids[0] {
		t.Fatalf("first pending event_id = %s, want %s", first.EventID, ids[0])
	}

	// Ack the first command as a success. The row transitions to done and drops
	// out of the pending read.
	ackOK := ackCrossnode(t, handler, ackToken.Secret, ids[0], "ack-1", []byte(`{"status_code":200,"ok":true}`))
	if ackOK.Code != http.StatusOK {
		t.Fatalf("ack ok: want 200, got %d body=%s", ackOK.Code, ackOK.Body.String())
	}
	assertEventCount(t, pool, domain.EventCommandAcked, 1)
	assertCommandQueueState(t, pool, ids[0], "done", 200, true)

	// Ack replay collapses at the idempotency middleware: no second event.
	ackReplay := ackCrossnode(t, handler, ackToken.Secret, ids[0], "ack-1", []byte(`{"status_code":200,"ok":true}`))
	if ackReplay.Code != http.StatusOK {
		t.Fatalf("ack replay: want 200, got %d", ackReplay.Code)
	}
	assertEventCount(t, pool, domain.EventCommandAcked, 1)

	// Ack the second as a failure: state failed, outcome recorded.
	ackFail := ackCrossnode(t, handler, ackToken.Secret, ids[1], "ack-2", []byte(`{"status_code":409,"ok":false}`))
	if ackFail.Code != http.StatusOK {
		t.Fatalf("ack fail: want 200, got %d body=%s", ackFail.Code, ackFail.Body.String())
	}
	assertCommandQueueState(t, pool, ids[1], "failed", 409, false)

	// Both drained: pending read is now empty.
	empty := getCrossnodeCommands(t, handler, drainToken.Secret, "m4", 10)
	var emptyResp struct {
		Commands []crossnode.QueuedCommand `json:"commands"`
	}
	decodeResponse(t, empty, &emptyResp)
	if len(emptyResp.Commands) != 0 {
		t.Fatalf("pending after drain = %d, want 0", len(emptyResp.Commands))
	}

	// Ack of an unknown command id is a 404.
	unknown := ackCrossnode(t, handler, ackToken.Secret, uuid.New(), "ack-unknown", []byte(`{"status_code":200,"ok":true}`))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("ack unknown: want 404, got %d body=%s", unknown.Code, unknown.Body.String())
	}
}

func TestCrossnodeDrainAndAckAuthorizationDenialsAppendNoEvents(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, queueToken := createCrossnodeToken(t, ctx, pool, "queue-authz-m4",
		[]string{crossnode.QueueWriteScope("m4", crossnode.OperationClassWorkItemsWrite), crossnode.OriginScope("sender")})
	_, wrongTarget := createCrossnodeToken(t, ctx, pool, "drain-authz-peer",
		[]string{crossnode.QueueDrainScope("peer"), crossnode.QueueAckScope("peer")})
	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()
	body := []byte(`{"command_path":"/v1/work-items","command_body":{"title":"remote"}}`)
	queued := postCrossnode(t, handler, queueToken.Secret, "authz-command", body,
		map[string]string{crossnode.HeaderQueueFor: "m4"})
	var response crossnodeQueueResponse
	decodeResponse(t, queued, &response)

	before := countAllEvents(t, pool)
	drainDenied := getCrossnodeCommands(t, handler, wrongTarget.Secret, "m4", 10)
	if drainDenied.Code != http.StatusForbidden {
		t.Fatalf("wrong-target drain: want 403, got %d body=%s", drainDenied.Code, drainDenied.Body.String())
	}
	ackDenied := ackCrossnode(t, handler, wrongTarget.Secret, response.CommandQueuedEvent, "wrong-ack",
		[]byte(`{"status_code":200,"ok":true}`))
	if ackDenied.Code != http.StatusForbidden {
		t.Fatalf("wrong-target ack: want 403, got %d body=%s", ackDenied.Code, ackDenied.Body.String())
	}
	if after := countAllEvents(t, pool); after != before {
		t.Fatalf("drain/ack authorization denials appended %d events", after-before)
	}
}

func getCrossnodeCommands(t *testing.T, handler http.Handler, token, target string, limit int) *httptest.ResponseRecorder {
	t.Helper()
	url := fmt.Sprintf("/v1/crossnode/commands?target=%s&limit=%d", target, limit)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func ackCrossnode(t *testing.T, handler http.Handler, token string, eventID uuid.UUID, key string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	url := "/v1/crossnode/commands/" + eventID.String() + "/ack"
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func assertCommandQueueState(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, wantState string, wantStatus int, wantOK bool) {
	t.Helper()
	var state string
	var status *int
	var ok *bool
	var ackedAt *time.Time
	err := pool.QueryRow(context.Background(), `
		SELECT state, outcome_status_code, outcome_ok, acked_at FROM command_queue WHERE id = $1
	`, id).Scan(&state, &status, &ok, &ackedAt)
	if err != nil {
		t.Fatalf("read command_queue row %s: %v", id, err)
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
	if ackedAt == nil {
		t.Fatalf("acked_at is nil, want a timestamp")
	}
}

// TestCrossnodeCommandRequiresCommandPath asserts the envelope validation
// rejects a missing command_path before any queue append.
func TestCrossnodeCommandRequiresCommandPath(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, queueToken := createCrossnodeToken(t, ctx, pool, "crossnode-path-m4",
		[]string{crossnode.QueueWriteScope("m4", crossnode.OperationClassWorkItemsWrite), crossnode.OriginScope("sender")})
	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()

	rec := postCrossnode(t, handler, queueToken.Secret, "cmd-bad-1",
		[]byte(`{"command_body":{"to":"running"}}`),
		map[string]string{crossnode.HeaderQueueFor: "m4"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 0)
}

func createCrossnodeToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string, scopes []string) (auth.CreateTokenResult, auth.CreateTokenResult) {
	t.Helper()
	authService := auth.NewService(pool, app.NewEventWriter())
	var root auth.CreateTokenResult
	var source string
	err := pool.QueryRow(ctx, `SELECT id, name, hash, is_root, source, created_at FROM tokens WHERE is_root AND revoked_at IS NULL LIMIT 1`).Scan(
		&root.Token.ID, &root.Token.Name, &root.Token.Hash, &root.Token.IsRoot, &source, &root.Token.CreatedAt,
	)
	if err != nil {
		root, err = authService.CreateToken(ctx, auth.CreateTokenInput{
			Name: "crossnode-test-root", IsRoot: true, Source: domain.SourceHuman,
		})
		if err != nil {
			t.Fatalf("create root token: %v", err)
		}
	} else {
		root.Token.Source = domain.Source(source)
	}
	if scopes == nil {
		return root, root
	}
	result, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: name, Source: domain.SourceAgent, Scopes: scopes, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create scoped token: %v", err)
	}
	return root, result
}

func countAllEvents(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return count
}
