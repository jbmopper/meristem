package api

import (
	"bytes"
	"context"
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
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestCrossnodeCommandEndpointIntegration exercises all three §2/§2b receiver
// paths — durable queue, relay refusal, and local placeholder — plus idempotent
// replay through the command middleware. This node is "hub"; commands
// queue-for "m4" (a peer) or target hub itself.
func TestCrossnodeCommandEndpointIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "crossnode-human",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	t.Setenv(EnvNodeID, "hub")
	server := New(pool, nil)
	handler := server.Handler()
	body := []byte(`{"command_path":"/v1/work-items/abc/transition","command_body":{"to":"running"}}`)

	// 1. Queue path: X-Meristem-Queue-For names a peer → command.queued + row.
	queueHdr := map[string]string{crossnode.HeaderQueueFor: "m4"}
	rec := postCrossnode(t, handler, tokenResult.Secret, "cmd-queue-1", body, queueHdr)
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
	replay := postCrossnode(t, handler, tokenResult.Secret, "cmd-queue-1", body, queueHdr)
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
	refused := postCrossnode(t, handler, tokenResult.Secret, "cmd-refuse-1", body, refuseHdr)
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

	// 3a. Local placeholder: no queue-for header → targets this node.
	local := postCrossnode(t, handler, tokenResult.Secret, "cmd-local-1", body, nil)
	if local.Code != http.StatusAccepted {
		t.Fatalf("local: want 202, got %d body=%s", local.Code, local.Body.String())
	}
	var localResp crossnodeQueueResponse
	decodeResponse(t, local, &localResp)
	if localResp.Queued || !localResp.Local {
		t.Fatalf("local response = %+v, want {queued:false, local:true}", localResp)
	}

	// 3b. Queue-for naming this node ("hub") is also local, not a queue.
	selfHdr := map[string]string{crossnode.HeaderQueueFor: "hub"}
	self := postCrossnode(t, handler, tokenResult.Secret, "cmd-local-2", body, selfHdr)
	if self.Code != http.StatusAccepted {
		t.Fatalf("self-queue: want 202, got %d", self.Code)
	}
	var selfResp crossnodeQueueResponse
	decodeResponse(t, self, &selfResp)
	if selfResp.Queued || !selfResp.Local {
		t.Fatalf("self response = %+v, want local placeholder", selfResp)
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 1) // still just the one real queue
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
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "crossnode-drain",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	tok := tokenResult.Secret

	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()
	body := []byte(`{"command_path":"/v1/work-items/abc/transition","command_body":{"to":"running"}}`)

	// Enqueue two commands for peer "m4" with distinct idempotency keys so both
	// land as separate rows.
	queueHdr := map[string]string{crossnode.HeaderQueueFor: "m4"}
	var ids []uuid.UUID
	for _, key := range []string{"drain-1", "drain-2"} {
		rec := postCrossnode(t, handler, tok, key, body, queueHdr)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("enqueue %s: want 202, got %d body=%s", key, rec.Code, rec.Body.String())
		}
		var q crossnodeQueueResponse
		decodeResponse(t, rec, &q)
		ids = append(ids, q.CommandQueuedEvent)
	}

	// GET the pending queue for m4: both rows, oldest-first, with the replay
	// fields the spoke needs.
	list := getCrossnodeCommands(t, handler, tok, "m4", 10)
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
	if first.TargetNodeID != "m4" || first.CommandPath != "/v1/work-items/abc/transition" || first.OriginIdempotencyKey != "drain-1" {
		t.Fatalf("unexpected first pending command: %+v", first)
	}
	if first.EventID != ids[0] {
		t.Fatalf("first pending event_id = %s, want %s", first.EventID, ids[0])
	}

	// Ack the first command as a success. The row transitions to done and drops
	// out of the pending read.
	ackOK := ackCrossnode(t, handler, tok, ids[0], "ack-1", []byte(`{"status_code":200,"ok":true}`))
	if ackOK.Code != http.StatusOK {
		t.Fatalf("ack ok: want 200, got %d body=%s", ackOK.Code, ackOK.Body.String())
	}
	assertEventCount(t, pool, domain.EventCommandAcked, 1)
	assertCommandQueueState(t, pool, ids[0], "done", 200, true)

	// Ack replay collapses at the idempotency middleware: no second event.
	ackReplay := ackCrossnode(t, handler, tok, ids[0], "ack-1", []byte(`{"status_code":200,"ok":true}`))
	if ackReplay.Code != http.StatusOK {
		t.Fatalf("ack replay: want 200, got %d", ackReplay.Code)
	}
	assertEventCount(t, pool, domain.EventCommandAcked, 1)

	// Ack the second as a failure: state failed, outcome recorded.
	ackFail := ackCrossnode(t, handler, tok, ids[1], "ack-2", []byte(`{"status_code":409,"ok":false}`))
	if ackFail.Code != http.StatusOK {
		t.Fatalf("ack fail: want 200, got %d body=%s", ackFail.Code, ackFail.Body.String())
	}
	assertCommandQueueState(t, pool, ids[1], "failed", 409, false)

	// Both drained: pending read is now empty.
	empty := getCrossnodeCommands(t, handler, tok, "m4", 10)
	var emptyResp struct {
		Commands []crossnode.QueuedCommand `json:"commands"`
	}
	decodeResponse(t, empty, &emptyResp)
	if len(emptyResp.Commands) != 0 {
		t.Fatalf("pending after drain = %d, want 0", len(emptyResp.Commands))
	}

	// Ack of an unknown command id is a 404.
	unknown := ackCrossnode(t, handler, tok, uuid.New(), "ack-unknown", []byte(`{"status_code":200,"ok":true}`))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("ack unknown: want 404, got %d body=%s", unknown.Code, unknown.Body.String())
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
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "crossnode-human-2",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	t.Setenv(EnvNodeID, "hub")
	handler := New(pool, nil).Handler()

	rec := postCrossnode(t, handler, tokenResult.Secret, "cmd-bad-1",
		[]byte(`{"command_body":{"to":"running"}}`),
		map[string]string{crossnode.HeaderQueueFor: "m4"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	assertEventCount(t, pool, domain.EventCommandQueued, 0)
}
