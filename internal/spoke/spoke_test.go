package spoke

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jbmopper/meristem/internal/crossnode"
)

// memCursor is an in-memory CursorStore for unit tests.
type memCursor struct {
	mu    sync.Mutex
	value string
}

func (m *memCursor) Load(context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.value, nil
}

func (m *memCursor) Save(_ context.Context, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = cursor
	return nil
}

// localCall records one command replay the spoke posted to the local api.
type localCall struct {
	Path    string
	IdemKey string
	Body    string
}

// ackCall records one ack the spoke posted to the hub.
type ackCall struct {
	EventID string
	Payload ackPayload
}

type attemptCall struct {
	EventID string
	Payload attemptPayload
}

// fakeFleet is a hub + local api pair backed by httptest servers, recording the
// spoke's outbound calls so a test can assert order, headers, and payloads.
type fakeFleet struct {
	hub   *httptest.Server
	local *httptest.Server

	mu          sync.Mutex
	pending     []map[string]any // commands the hub GET returns
	localCalls  []localCall
	attempts    []attemptCall
	acks        []ackCall
	feedCursor  string
	feedItems   int
	localStatus int // status the local api returns (default 200)
}

func newFakeFleet(t *testing.T) *fakeFleet {
	t.Helper()
	f := &fakeFleet{localStatus: http.StatusOK, feedCursor: "cursor-1"}

	localMux := http.NewServeMux()
	localMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.localCalls = append(f.localCalls, localCall{
			Path:    r.URL.Path,
			IdemKey: r.Header.Get("Idempotency-Key"),
			Body:    string(body),
		})
		status := f.localStatus
		f.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{}`))
	})
	f.local = httptest.NewServer(localMux)

	hubMux := http.NewServeMux()
	hubMux.HandleFunc("GET /v1/crossnode/commands", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		out := map[string]any{"commands": f.pending}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	hubMux.HandleFunc("POST /v1/crossnode/commands/{event_id}/ack", func(w http.ResponseWriter, r *http.Request) {
		var p ackPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		f.mu.Lock()
		f.acks = append(f.acks, ackCall{EventID: r.PathValue("event_id"), Payload: p})
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"acked":true}`))
	})
	hubMux.HandleFunc("POST /v1/crossnode/commands/{event_id}/attempt", func(w http.ResponseWriter, r *http.Request) {
		var p attemptPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		f.mu.Lock()
		f.attempts = append(f.attempts, attemptCall{EventID: r.PathValue("event_id"), Payload: p})
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"recorded":true}`))
	})
	hubMux.HandleFunc("GET /v1/feed", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		items := make([]json.RawMessage, f.feedItems)
		for i := range items {
			items[i] = json.RawMessage(`{}`)
		}
		out := map[string]any{"items": items, "next_cursor": f.feedCursor, "has_more": false}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	f.hub = httptest.NewServer(hubMux)

	t.Cleanup(func() {
		f.hub.Close()
		f.local.Close()
	})
	return f
}

func (f *fakeFleet) config() Config {
	return Config{
		HubBaseURL: f.hub.URL,
		NodeID:     "m4",
		HubToken:   "hub-token",
		LocalURL:   f.local.URL,
		LocalToken: "local-token",
		DrainLimit: 100,
	}
}

func (f *fakeFleet) setPending(cmds ...map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = cmds
}

func cmd(eventID, path, key string, body any) map[string]any {
	return map[string]any{
		"event_id":               eventID,
		"target_node_id":         "m4",
		"command_path":           path,
		"command_body":           body,
		"origin_idempotency_key": key,
		"queued_at":              "2026-07-07T00:00:00Z",
		"expires_at":             "2026-07-08T00:00:00Z",
		"attempt_count":          0,
	}
}

func TestTickDrainExecuteAck(t *testing.T) {
	f := newFakeFleet(t)
	id1 := "11111111-1111-4111-8111-111111111111"
	id2 := "22222222-2222-4222-8222-222222222222"
	f.setPending(
		cmd(id1, "/v1/work-items/a/transition", "orig-key-1", map[string]any{"to": "running"}),
		cmd(id2, "/v1/work-items/b/transition", "orig-key-2", map[string]any{"to": "blocked"}),
	)

	p := New(f.config(), f.hub.Client(), &memCursor{}, nil)
	res := p.Tick(context.Background())

	if !res.HubReachable {
		t.Fatal("hub should be reachable")
	}
	if res.Drained != 2 || res.AttemptsRecorded != 2 || res.Executed != 2 || res.Acked != 2 || res.Failed != 0 {
		t.Fatalf("result = %+v, want drained/executed/acked=2 failed=0", res)
	}

	// Execute order matches queue order.
	if len(f.localCalls) != 2 {
		t.Fatalf("local calls = %d, want 2", len(f.localCalls))
	}
	if len(f.attempts) != 2 || f.attempts[0].Payload.AttemptKey != id1+":1" {
		t.Fatalf("attempt budget records = %+v", f.attempts)
	}
	if f.localCalls[0].Path != "/v1/work-items/a/transition" || f.localCalls[1].Path != "/v1/work-items/b/transition" {
		t.Fatalf("execute order wrong: %+v", f.localCalls)
	}
	// Original idempotency key is reused verbatim as the local Idempotency-Key.
	if f.localCalls[0].IdemKey != "orig-key-1" || f.localCalls[1].IdemKey != "orig-key-2" {
		t.Fatalf("idempotency key not reused: %+v", f.localCalls)
	}
	// Command body is replayed verbatim.
	if f.localCalls[0].Body != `{"to":"running"}` {
		t.Fatalf("body = %q, want the queued command_body", f.localCalls[0].Body)
	}
	// Ack payload carries the structural outcome and targets the right event id.
	if len(f.acks) != 2 {
		t.Fatalf("acks = %d, want 2", len(f.acks))
	}
	if f.acks[0].EventID != id1 || f.acks[0].Payload.StatusCode != 200 || !f.acks[0].Payload.OK || f.acks[0].Payload.Outcome != crossnode.CommandDone {
		t.Fatalf("ack[0] = %+v, want id1 status 200 ok true", f.acks[0])
	}
}

func TestTickFailureFailedAck(t *testing.T) {
	f := newFakeFleet(t)
	f.localStatus = http.StatusConflict // 409: definitive local rejection
	id := "33333333-3333-4333-8333-333333333333"
	f.setPending(cmd(id, "/v1/work-items/c/transition", "orig-key-3", map[string]any{"to": "done"}))

	p := New(f.config(), f.hub.Client(), &memCursor{}, nil)
	res := p.Tick(context.Background())

	if res.Executed != 1 || res.Failed != 1 || res.Acked != 1 {
		t.Fatalf("result = %+v, want executed/failed/acked=1", res)
	}
	if len(f.acks) != 1 {
		t.Fatalf("acks = %d, want 1", len(f.acks))
	}
	if f.acks[0].Payload.OK || f.acks[0].Payload.StatusCode != 409 || f.acks[0].Payload.Outcome != crossnode.CommandRefused {
		t.Fatalf("failed ack = %+v, want status 409 ok false", f.acks[0].Payload)
	}
}

func TestTickRetryable5xxConsumesAttemptWithoutAck(t *testing.T) {
	f := newFakeFleet(t)
	f.localStatus = http.StatusServiceUnavailable
	id := "77777777-7777-4777-8777-777777777777"
	f.setPending(cmd(id, "/v1/work-items/c/transition", "orig-key-7", map[string]any{"to": "done"}))

	p := New(f.config(), f.hub.Client(), &memCursor{}, nil)
	res := p.Tick(context.Background())

	if res.AttemptsRecorded != 1 || res.Executed != 1 || res.Failed != 1 || res.Acked != 0 {
		t.Fatalf("result = %+v, want one retryable unacked attempt", res)
	}
	if len(f.acks) != 0 {
		t.Fatalf("retryable 5xx was acknowledged: %+v", f.acks)
	}
}

func TestTickHubDownNoOp(t *testing.T) {
	f := newFakeFleet(t)
	f.setPending(cmd("44444444-4444-4444-8444-444444444444", "/v1/x", "k", map[string]any{}))
	cfg := f.config()
	f.hub.Close() // hub is now unreachable
	// Point at the closed hub; local stays up but must never be called.
	cfg.HubBaseURL = f.hub.URL

	p := New(cfg, f.hub.Client(), &memCursor{}, nil)
	res := p.Tick(context.Background())

	if res.HubReachable {
		t.Fatal("hub should be unreachable")
	}
	if res.Drained != 0 || res.Executed != 0 || res.Acked != 0 {
		t.Fatalf("result = %+v, want a clean no-op", res)
	}
	if len(f.localCalls) != 0 {
		t.Fatalf("local was called %d times during a partition, want 0", len(f.localCalls))
	}
}

func TestTickLocalDownLeavesPending(t *testing.T) {
	f := newFakeFleet(t)
	id := "55555555-5555-4555-8555-555555555555"
	f.setPending(cmd(id, "/v1/work-items/d/transition", "orig-key-5", map[string]any{"to": "running"}))
	cfg := f.config()
	f.local.Close() // local api unreachable: cannot determine an outcome

	p := New(cfg, f.hub.Client(), &memCursor{}, nil)
	res := p.Tick(context.Background())

	// Hub drain succeeded (command fetched), but local execution failed, so the
	// command is neither executed nor acked — it stays pending for a retry.
	if !res.HubReachable {
		t.Fatal("hub should be reachable (only local is down)")
	}
	if res.Drained != 1 || res.Executed != 0 || res.Acked != 0 {
		t.Fatalf("result = %+v, want drained=1 executed=acked=0", res)
	}
	if len(f.acks) != 0 {
		t.Fatalf("acks = %d during local outage, want 0 (row must stay pending)", len(f.acks))
	}
}

func TestTickFeedCursorAdvances(t *testing.T) {
	f := newFakeFleet(t)
	f.feedItems = 3
	f.feedCursor = "cursor-next"
	store := &memCursor{}

	p := New(f.config(), f.hub.Client(), store, nil)
	res := p.Tick(context.Background())

	if res.NewFeedEvents != 3 {
		t.Fatalf("new feed events = %d, want 3", res.NewFeedEvents)
	}
	if got, _ := store.Load(context.Background()); got != "cursor-next" {
		t.Fatalf("persisted cursor = %q, want cursor-next", got)
	}
}
