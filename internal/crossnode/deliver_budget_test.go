package crossnode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// The attempt-vs-wall boundary tests (work item 17ce2faf, audit finding G4b)
// pin how the two direct-delivery budgets interact: the per-attempt timeout
// bounds each try and consumes exactly one of the finite attempts, while the
// wall-clock patience can end the direct walk early — before the attempt
// budget is spent — and hand over to the queue candidate. Both use a server
// that never answers (it blocks until the request context is canceled), so an
// attempt can only end by one of the two budgets; no sleep-tuned assertions.

// hangingStub counts hits and holds every request open past the client's
// attempt deadline. The handler releases on the request context OR on a
// test-scoped release channel: with an unread POST body the net/http server
// does not reliably detect the client's abort (no background read starts), so
// teardown must not depend on r.Context() ever canceling. Cleanup order is
// LIFO — the release close registered after NewServer runs before srv.Close,
// deterministically unblocking every held handler first.
func hangingStub(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv
}

// With a roomy wall budget, the per-attempt timeout must consume the direct
// attempts one by one — each try bounded, each recorded as a cooled retryable
// failure — and delivery must then land on the queue host. This pins that a
// hanging peer costs attempts*AttemptTimeout, never an unbounded stall.
func TestDeliverAttemptTimeoutConsumesEachDirectAttempt(t *testing.T) {
	var directHits atomic.Int32
	hanging := hangingStub(t, &directHits)
	queueCap := &capture{}
	queueSrv := stub(t, http.StatusAccepted, queueCap)
	now := time.Unix(1_700_000_000, 0).UTC()
	candidates := []Candidate{
		{Kind: KindDirect, URL: hanging.URL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: queueSrv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}
	policy := DeliveryPolicy{
		DirectAttempts: 3,
		AttemptTimeout: 25 * time.Millisecond,
		DirectPatience: 5 * time.Second, // roomy: only the attempt budget binds
		DirectBackoff:  func(int) time.Duration { return 0 },
	}

	out, err := DeliverWithPolicy(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), nil, now, policy)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue {
		t.Fatalf("expected queue to win after attempt-timeout exhaustion, got %+v", out)
	}
	if got := directHits.Load(); got != 3 {
		t.Fatalf("direct attempts against hanging peer = %d, want the full budget of 3", got)
	}
	if len(out.Attempts) != 4 {
		t.Fatalf("recorded attempts = %d, want 3 direct + 1 queue", len(out.Attempts))
	}
	for i, a := range out.Attempts[:3] {
		if a.Err == nil {
			t.Fatalf("direct attempt %d has no error; each hanging attempt must end in its per-attempt deadline", i+1)
		}
		if !a.CooledDown {
			t.Fatalf("direct attempt %d not cooled; a timed-out route is a retryable failure", i+1)
		}
	}
	if queueCap.queueFor != "m4" {
		t.Fatalf("fallback was not queued for target: %q", queueCap.queueFor)
	}
}

// When the wall-clock patience is smaller than a second try (attempt timeout
// plus backoff), the direct walk must stop early — with attempts left unspent
// — and fall to the queue candidate. This pins that DirectPatience is a hard
// wall, not advisory: 3 attempts is a cap, never an entitlement.
func TestDeliverWallClockTruncatesDirectAttempts(t *testing.T) {
	var directHits atomic.Int32
	hanging := hangingStub(t, &directHits)
	queueCap := &capture{}
	queueSrv := stub(t, http.StatusAccepted, queueCap)
	now := time.Unix(1_700_000_000, 0).UTC()
	candidates := []Candidate{
		{Kind: KindDirect, URL: hanging.URL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: queueSrv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}
	policy := DeliveryPolicy{
		DirectAttempts: 3,
		AttemptTimeout: 40 * time.Millisecond,
		DirectPatience: 50 * time.Millisecond,
		// Backoff exceeds the whole wall budget, so the retry wait can only
		// end via the wall expiring — deterministically one direct attempt.
		DirectBackoff: func(int) time.Duration { return time.Second },
	}

	out, err := DeliverWithPolicy(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), nil, now, policy)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue {
		t.Fatalf("expected queue to win after wall-clock truncation, got %+v", out)
	}
	if got := directHits.Load(); got != 1 {
		t.Fatalf("direct attempts = %d, want exactly 1: the wall budget must truncate the remaining attempts", got)
	}
	if len(out.Attempts) != 2 {
		t.Fatalf("recorded attempts = %d, want 1 truncated direct + 1 queue", len(out.Attempts))
	}
	if out.Attempts[0].Err == nil || !out.Attempts[0].CooledDown {
		t.Fatalf("truncated direct attempt = %+v, want a cooled retryable failure", out.Attempts[0])
	}
	if queueCap.queueFor != "m4" {
		t.Fatalf("fallback was not queued for target: %q", queueCap.queueFor)
	}
}
