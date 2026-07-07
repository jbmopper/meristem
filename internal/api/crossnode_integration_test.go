package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

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
