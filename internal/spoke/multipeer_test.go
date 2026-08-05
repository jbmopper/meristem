package spoke

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// fakePeerSource is a fixed drain set.
type fakePeerSource struct {
	peers []DrainPeer
	err   error
}

func (f fakePeerSource) DrainPeers(context.Context) ([]DrainPeer, error) {
	return f.peers, f.err
}

// queueHost is a stub queue host that hands out one command and records the
// attempt and ack it receives, along with the bearer used on each call.
type queueHost struct {
	mu        sync.Mutex
	srv       *httptest.Server
	nodeID    string
	commandID uuid.UUID
	served    bool
	attempts  int
	acks      []uuid.UUID
	bearers   map[string]bool
}

func newQueueHost(t *testing.T, nodeID string, hasCommand bool) *queueHost {
	t.Helper()
	h := &queueHost{nodeID: nodeID, commandID: uuid.New(), bearers: map[string]bool{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.bearers[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")] = true
		h.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/crossnode/commands":
			w.Header().Set("Content-Type", "application/json")
			if !hasCommand {
				_, _ = w.Write([]byte(`{"commands":[]}`))
				return
			}
			h.mu.Lock()
			h.served = true
			h.mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"commands":[{"event_id":%q,"target_node_id":"m4","command_path":"/v1/work-items/%s/transition","command_body":{"to":"running"},"origin_idempotency_key":"k-%s","attempt_count":0}]}`,
				h.commandID, uuid.New(), h.nodeID)
		case strings.HasSuffix(r.URL.Path, "/attempt"):
			h.mu.Lock()
			h.attempts++
			h.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/ack"):
			parts := strings.Split(r.URL.Path, "/")
			id, _ := uuid.Parse(parts[len(parts)-2])
			h.mu.Lock()
			h.acks = append(h.acks, id)
			h.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *queueHost) peer() DrainPeer { return DrainPeer{NodeID: h.nodeID, DirectURL: h.srv.URL} }

func (h *queueHost) snapshot() (served bool, attempts int, acks []uuid.UUID, bearers []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for b := range h.bearers {
		bearers = append(bearers, b)
	}
	return h.served, h.attempts, append([]uuid.UUID(nil), h.acks...), bearers
}

// localAPI accepts every replayed command with 200.
func newLocalAPI(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func multiPeerConfig(local string) Config {
	return Config{
		HubBaseURL: "https://unused.example",
		NodeID:     "m4",
		HubToken:   "hub-token-must-not-leak",
		LocalURL:   local,
		LocalToken: "local-token",
		DrainLimit: 10,
	}
}

func perPeerBearers(tokens map[string]string) PeerBearerResolver {
	return func(_ context.Context, peerNodeID string) (string, error) {
		if tok := tokens[peerNodeID]; tok != "" {
			return tok, nil
		}
		return "", errors.New("no credential")
	}
}

// TestDrainVisitsEveryQueueHost is the core of convergence check 1 at the loop
// level: the drain iterates the resolved peer set rather than one configured
// host.
func TestDrainVisitsEveryQueueHost(t *testing.T) {
	local := newLocalAPI(t)
	a := newQueueHost(t, "hub", true)
	b := newQueueHost(t, "den", true)
	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{a.peer(), b.peer()}},
			perPeerBearers(map[string]string{"hub": "hub-cred", "den": "den-cred"}))

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if res.Drained != 2 || res.PeersDrained != 2 || res.PeersUnreachable != 0 {
		t.Fatalf("drained=%d peers=%d unreachable=%d, want 2/2/0", res.Drained, res.PeersDrained, res.PeersUnreachable)
	}
	for _, h := range []*queueHost{a, b} {
		served, attempts, acks, _ := h.snapshot()
		if !served || attempts != 1 || len(acks) != 1 {
			t.Errorf("%s: served=%v attempts=%d acks=%d, want true/1/1", h.nodeID, served, attempts, len(acks))
		}
	}
}

// TestUnreachablePeerDoesNotBlockOthers is convergence check 5. In a mesh this
// is the difference between degraded and stopped: one dead queue host must not
// starve every other host for as long as it stays dead.
func TestUnreachablePeerDoesNotBlockOthers(t *testing.T) {
	local := newLocalAPI(t)
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	live := newQueueHost(t, "den", true)

	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{
			{NodeID: "hub", DirectURL: deadURL},
			live.peer(),
		}}, perPeerBearers(map[string]string{"hub": "hub-cred", "den": "den-cred"}))

	res := TickResult{}
	err := p.drain(context.Background(), &res)
	if err == nil {
		t.Fatal("drain returned nil, want the dead peer's error surfaced")
	}
	if res.PeersDrained != 1 || res.PeersUnreachable != 1 {
		t.Fatalf("drained=%d unreachable=%d, want 1/1", res.PeersDrained, res.PeersUnreachable)
	}
	served, attempts, acks, _ := live.snapshot()
	if !served || attempts != 1 || len(acks) != 1 {
		t.Fatalf("live peer was starved by the dead one: served=%v attempts=%d acks=%d", served, attempts, len(acks))
	}
}

// TestAttemptAndAckReturnToTheOriginatingPeer is the correctness trap in going
// multi-peer. A command lives on the host that accepted it; acking a different
// host leaves the real one holding a command it still believes is in flight,
// and it would be retried until it expired.
func TestAttemptAndAckReturnToTheOriginatingPeer(t *testing.T) {
	local := newLocalAPI(t)
	empty := newQueueHost(t, "hub", false)
	holder := newQueueHost(t, "den", true)

	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{empty.peer(), holder.peer()}},
			perPeerBearers(map[string]string{"hub": "hub-cred", "den": "den-cred"}))

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err != nil {
		t.Fatalf("drain: %v", err)
	}
	_, emptyAttempts, emptyAcks, _ := empty.snapshot()
	if emptyAttempts != 0 || len(emptyAcks) != 0 {
		t.Fatalf("the host holding nothing received attempts=%d acks=%d, want 0/0", emptyAttempts, len(emptyAcks))
	}
	_, holderAttempts, holderAcks, _ := holder.snapshot()
	if holderAttempts != 1 || len(holderAcks) != 1 {
		t.Fatalf("holder attempts=%d acks=%d, want 1/1", holderAttempts, len(holderAcks))
	}
	if holderAcks[0] != holder.commandID {
		t.Fatalf("acked %s, want the command the holder served (%s)", holderAcks[0], holder.commandID)
	}
}

// TestEachPeerSeesOnlyItsOwnCredential is convergence check 3. Bearers are
// node-local: presenting one host's token to another both fails and hands that
// host a credential it was never meant to see.
func TestEachPeerSeesOnlyItsOwnCredential(t *testing.T) {
	local := newLocalAPI(t)
	a := newQueueHost(t, "hub", true)
	b := newQueueHost(t, "den", true)
	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{a.peer(), b.peer()}},
			perPeerBearers(map[string]string{"hub": "hub-cred", "den": "den-cred"}))

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err != nil {
		t.Fatalf("drain: %v", err)
	}
	for host, want := range map[*queueHost]string{a: "hub-cred", b: "den-cred"} {
		_, _, _, bearers := host.snapshot()
		for _, seen := range bearers {
			if seen != want {
				t.Errorf("%s saw bearer %q, want only %q", host.nodeID, seen, want)
			}
		}
	}
	// And the configured hub token must never reach a resolved peer.
	for _, host := range []*queueHost{a, b} {
		_, _, _, bearers := host.snapshot()
		for _, seen := range bearers {
			if seen == "hub-token-must-not-leak" {
				t.Errorf("%s received the configured hub token", host.nodeID)
			}
		}
	}
}

// TestPeerWithoutACredentialIsSkippedNotSubstituted pins the fail-closed edge.
// Falling back to some other host's bearer would leak it; failing the whole
// tick would let one unconfigured peer stop the rest.
func TestPeerWithoutACredentialIsSkippedNotSubstituted(t *testing.T) {
	local := newLocalAPI(t)
	unconfigured := newQueueHost(t, "hub", true)
	configured := newQueueHost(t, "den", true)

	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{unconfigured.peer(), configured.peer()}},
			perPeerBearers(map[string]string{"den": "den-cred"}))

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err != nil {
		t.Fatalf("drain: %v", err)
	}
	served, attempts, _, bearers := unconfigured.snapshot()
	if served || attempts != 0 {
		t.Errorf("uncredentialed peer was contacted: served=%v attempts=%d", served, attempts)
	}
	if len(bearers) != 0 {
		t.Errorf("uncredentialed peer received bearers %v, want none", bearers)
	}
	if res.PeersDrained != 1 {
		t.Errorf("PeersDrained = %d, want 1 (the credentialed peer still drains)", res.PeersDrained)
	}
}

// TestMultiPeerRequiresAResolver refuses the configuration that would send the
// configured hub bearer to every resolved peer.
func TestMultiPeerRequiresAResolver(t *testing.T) {
	local := newLocalAPI(t)
	host := newQueueHost(t, "den", true)
	p := New(multiPeerConfig(local.URL), local.Client(), &memCursor{}, nil).
		WithPeers(fakePeerSource{peers: []DrainPeer{host.peer()}}, nil)

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err == nil {
		t.Fatal("drain succeeded with no credential resolver, want refusal")
	}
	if served, _, _, bearers := host.snapshot(); served || len(bearers) != 0 {
		t.Fatalf("peer was contacted despite the refusal: served=%v bearers=%v", served, bearers)
	}
}

// TestSingleHostBehaviorIsUnchanged is convergence check 6 at the loop level: a
// Poller with no PeerSource keeps draining exactly the configured host under
// the configured token, so existing deployments need no configuration change.
func TestSingleHostBehaviorIsUnchanged(t *testing.T) {
	local := newLocalAPI(t)
	hub := newQueueHost(t, "hub", true)
	cfg := multiPeerConfig(local.URL)
	cfg.HubBaseURL = hub.srv.URL
	cfg.HubToken = "legacy-hub-token"
	p := New(cfg, local.Client(), &memCursor{}, nil)

	res := TickResult{}
	if err := p.drain(context.Background(), &res); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if res.Drained != 1 || res.PeersDrained != 1 {
		t.Fatalf("drained=%d peers=%d, want 1/1", res.Drained, res.PeersDrained)
	}
	_, attempts, acks, bearers := hub.snapshot()
	if attempts != 1 || len(acks) != 1 {
		t.Fatalf("attempts=%d acks=%d, want 1/1", attempts, len(acks))
	}
	for _, seen := range bearers {
		if seen != "legacy-hub-token" {
			t.Errorf("single-host path used bearer %q, want the configured token", seen)
		}
	}
}
