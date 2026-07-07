package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// capture records what one stub receiver saw, so tests assert on headers and
// the decoded envelope.
type capture struct {
	mu       sync.Mutex
	hits     int
	path     string
	relayed  string
	queueFor string
	idemKey  string
	body     wireCommand
}

func (c *capture) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits++
	c.path = r.URL.Path
	c.relayed = r.Header.Get(HeaderRelayed)
	c.queueFor = r.Header.Get(HeaderQueueFor)
	c.idemKey = r.Header.Get(HeaderIdempotencyKey)
	_ = json.NewDecoder(r.Body).Decode(&c.body)
}

func stub(t *testing.T, status int, cap *capture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cap != nil {
			cap.record(r)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func sampleCommand() Command {
	return Command{
		TargetNodeID:   "m4",
		IdempotencyKey: "idem-123",
		Path:           "/v1/work-items/abc/transition",
		Body:           json.RawMessage(`{"to":"running"}`),
	}
}

func TestDeliverDirectSuccess(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidates := []Candidate{{Kind: KindDirect, URL: srv.URL, RouteKey: "direct|m4"}}

	out, err := Deliver(context.Background(), srv.Client(), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.StatusCode != http.StatusAccepted {
		t.Fatalf("out = %+v, want delivered 202", out)
	}
	if out.Terminal.Kind != KindDirect {
		t.Fatalf("terminal kind = %s, want direct", out.Terminal.Kind)
	}
	if cap.path != CommandPath {
		t.Fatalf("path = %s, want %s", cap.path, CommandPath)
	}
	if cap.relayed != "" || cap.queueFor != "" {
		t.Fatalf("direct request set routing headers: relayed=%q queueFor=%q", cap.relayed, cap.queueFor)
	}
	if cap.idemKey != "idem-123" {
		t.Fatalf("idempotency key = %q, want idem-123", cap.idemKey)
	}
	if cap.body.CommandPath != "/v1/work-items/abc/transition" {
		t.Fatalf("body command_path = %q", cap.body.CommandPath)
	}
	if len(out.Cooldowns) != 0 {
		t.Fatalf("success recorded cooldowns: %v", out.Cooldowns)
	}
}

func TestDeliverRelaySetsRelayedHeader(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidates := []Candidate{{Kind: KindRelay, URL: srv.URL, Via: "hub", RouteKey: "relay|m4|hub"}}

	out, err := Deliver(context.Background(), srv.Client(), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered {
		t.Fatalf("relay not delivered: %+v", out)
	}
	if cap.relayed != "true" {
		t.Fatalf("relay header = %q, want true", cap.relayed)
	}
	if cap.queueFor != "" {
		t.Fatalf("relay set queue-for header: %q", cap.queueFor)
	}
}

func TestDeliverQueueSetsQueueForHeader(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidates := []Candidate{{Kind: KindQueue, URL: srv.URL, Via: "hub", RouteKey: "queue|m4|hub"}}

	out, err := Deliver(context.Background(), srv.Client(), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered {
		t.Fatalf("queue not delivered: %+v", out)
	}
	if cap.queueFor != "m4" {
		t.Fatalf("queue-for header = %q, want m4", cap.queueFor)
	}
	if cap.relayed != "" {
		t.Fatalf("queue set relayed header: %q", cap.relayed)
	}
}

func TestDeliverAdvancesPast5xxAndRecordsCooldown(t *testing.T) {
	failing := stub(t, http.StatusBadGateway, nil)
	good := &capture{}
	goodSrv := stub(t, http.StatusAccepted, good)
	now := time.Unix(1_700_000_000, 0).UTC()

	candidates := []Candidate{
		{Kind: KindDirect, URL: failing.URL, RouteKey: "direct|m4"},
		{Kind: KindRelay, URL: goodSrv.URL, Via: "hub", RouteKey: "relay|m4|hub"},
	}

	out, err := Deliver(context.Background(), http.DefaultClient, candidates, sampleCommand(), nil, now)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindRelay {
		t.Fatalf("expected relay to win, got %+v", out)
	}
	if ts, ok := out.Cooldowns["direct|m4"]; !ok || !ts.Equal(now) {
		t.Fatalf("failed direct route not cooled at now: %v", out.Cooldowns)
	}
	if _, ok := out.Cooldowns["relay|m4|hub"]; ok {
		t.Fatalf("winning relay route was cooled: %v", out.Cooldowns)
	}
	if good.relayed != "true" {
		t.Fatalf("second attempt was not marked relayed: %q", good.relayed)
	}
}

func TestDeliverAdvancesPastTransportFailure(t *testing.T) {
	// A closed server yields a transport error (connection refused).
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	good := &capture{}
	goodSrv := stub(t, http.StatusAccepted, good)
	now := time.Unix(42, 0).UTC()

	candidates := []Candidate{
		{Kind: KindDirect, URL: deadURL, RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: goodSrv.URL, Via: "hub", RouteKey: "queue|m4|hub"},
	}
	out, err := Deliver(context.Background(), http.DefaultClient, candidates, sampleCommand(), nil, now)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue {
		t.Fatalf("expected queue fallback to win, got %+v", out)
	}
	if ts, ok := out.Cooldowns["direct|m4"]; !ok || !ts.Equal(now) {
		t.Fatalf("dead route not cooled: %v", out.Cooldowns)
	}
	if out.Attempts[0].Err == nil {
		t.Fatalf("first attempt should carry a transport error")
	}
}

func TestDeliverStopsOn4xx(t *testing.T) {
	rejecting := &capture{}
	rejectSrv := stub(t, http.StatusConflict, rejecting)
	nextCap := &capture{}
	nextSrv := stub(t, http.StatusAccepted, nextCap)

	candidates := []Candidate{
		{Kind: KindRelay, URL: rejectSrv.URL, Via: "hub", RouteKey: "relay|m4|hub"},
		{Kind: KindQueue, URL: nextSrv.URL, Via: "hub", RouteKey: "queue|m4|hub"},
	}
	out, err := Deliver(context.Background(), http.DefaultClient, candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if out.Delivered {
		t.Fatalf("4xx must not count as delivered: %+v", out)
	}
	if out.StatusCode != http.StatusConflict || out.Terminal.Kind != KindRelay {
		t.Fatalf("expected terminal 409 on relay, got %+v", out)
	}
	if nextCap.hits != 0 {
		t.Fatalf("walk did not stop on 4xx; next route was tried %d times", nextCap.hits)
	}
	if len(out.Cooldowns) != 0 {
		t.Fatalf("4xx must not cool a route: %v", out.Cooldowns)
	}
}

func TestDeliverAllRoutesFail(t *testing.T) {
	a := stub(t, http.StatusInternalServerError, nil)
	b := stub(t, http.StatusServiceUnavailable, nil)
	now := time.Unix(7, 0).UTC()

	existing := map[string]time.Time{"stale|route": now.Add(-time.Hour)}
	candidates := []Candidate{
		{Kind: KindRelay, URL: a.URL, Via: "hub", RouteKey: "relay|m4|hub"},
		{Kind: KindQueue, URL: b.URL, Via: "hub", RouteKey: "queue|m4|hub"},
	}
	out, err := Deliver(context.Background(), http.DefaultClient, candidates, sampleCommand(), existing, now)
	if !errors.Is(err, ErrAllRoutesFailed) {
		t.Fatalf("err = %v, want ErrAllRoutesFailed", err)
	}
	if out.Delivered {
		t.Fatalf("nothing should be delivered: %+v", out)
	}
	for _, k := range []string{"relay|m4|hub", "queue|m4|hub"} {
		if ts, ok := out.Cooldowns[k]; !ok || !ts.Equal(now) {
			t.Fatalf("route %q not cooled at now: %v", k, out.Cooldowns)
		}
	}
	// Input cooldowns are preserved, not mutated.
	if out.Cooldowns["stale|route"] != existing["stale|route"] {
		t.Fatalf("input cooldown lost")
	}
	if _, ok := existing["relay|m4|hub"]; ok {
		t.Fatalf("Deliver mutated the caller's cooldown map")
	}
}
