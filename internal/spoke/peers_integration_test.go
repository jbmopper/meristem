package spoke

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// peerFixture appends node route events through the real projectors so the
// nodes projection and the event history agree, which is the whole point:
// ResolveDrainPeers reads both.
type peerFixture struct {
	pool       *pgxpool.Pool
	writer     *events.Writer
	ctx        context.Context
	t          *testing.T
	registered map[string]bool
}

func newPeerFixture(t *testing.T, db string) *peerFixture {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, db)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	nodes.RegisterProjectors(reg)
	return &peerFixture{pool: pool, writer: events.NewWriter(reg), ctx: ctx, t: t, registered: map[string]bool{}}
}

// route declares a node's peer origin and queue-host allowlist. The first call
// for a node registers it: node.route_updated only UPDATEs an existing row, so
// a node that was never registered would silently project nothing.
func (f *peerFixture) route(nodeID string, directURL string, queueVia []string) {
	f.t.Helper()
	kind := domain.EventNodeRouteUpdated
	if !f.registered[nodeID] {
		kind = domain.EventNodeRegistered
		f.registered[nodeID] = true
	}
	payload := map[string]any{
		"node_id":    nodeID,
		"status":     string(domain.NodeStatusActive),
		"relay_via":  queueVia,
		"direct_url": directURL,
	}
	if directURL == "" {
		delete(payload, "direct_url")
	}
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{})
	if err != nil {
		f.t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	if _, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectNode,
		SubjectID:   nodes.NodeSubjectID(nodeID),
		Kind:        kind,
		Source:      domain.SourceSystem,
		Payload:     payload,
	}); err != nil {
		f.t.Fatalf("append route for %s: %v", nodeID, err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		f.t.Fatalf("commit: %v", err)
	}
}

func peerIDs(peers []DrainPeer) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.NodeID)
	}
	return out
}

func equalIDs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestDrainPeersComeFromTheProjectionInDeclaredOrder is convergence check 1:
// the drain set is the nodes projection, not one env var. Order is the
// operator's declared order, because that order is what makes per-host
// fairness testable downstream.
func TestDrainPeersComeFromTheProjectionInDeclaredOrder(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_itest")
	f.route("hub", "https://hub.example", nil)
	f.route("den", "https://den.example", nil)
	f.route("m4", "", []string{"den", "hub"})

	peers, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now(), DefaultDrainGrace)
	if err != nil {
		t.Fatalf("ResolveDrainPeers: %v", err)
	}
	if got := peerIDs(peers); !equalIDs(got, "den", "hub") {
		t.Fatalf("peers = %v, want [den hub] in declared order", got)
	}
	for _, p := range peers {
		if p.Retained {
			t.Errorf("%s marked retained while still in the allowlist", p.NodeID)
		}
		if p.DirectURL == "" {
			t.Errorf("%s has no peer origin", p.NodeID)
		}
	}
}

// TestRemovedQueueHostIsStillDrainedWithinGrace is convergence check 2, and the
// reason this resolver exists at all. A command accepted by "hub" stays on hub
// until its target collects it. If removing hub from the allowlist immediately
// stopped the polling, that command would sit there with nobody coming for it —
// not lost, stranded, and silent.
func TestRemovedQueueHostIsStillDrainedWithinGrace(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_grace_itest")
	f.route("hub", "https://hub.example", nil)
	f.route("den", "https://den.example", nil)
	f.route("m4", "", []string{"den", "hub"})
	// The operator moves m4 off hub.
	f.route("m4", "", []string{"den"})

	peers, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now(), DefaultDrainGrace)
	if err != nil {
		t.Fatalf("ResolveDrainPeers: %v", err)
	}
	if got := peerIDs(peers); !equalIDs(got, "den", "hub") {
		t.Fatalf("peers = %v, want [den hub] — hub must still be drained inside the grace window", got)
	}
	byID := map[string]DrainPeer{}
	for _, p := range peers {
		byID[p.NodeID] = p
	}
	if byID["hub"].Retained != true {
		t.Error("hub should be marked retained: it is drained only to collect already-enqueued commands")
	}
	if byID["den"].Retained != false {
		t.Error("den is still in the allowlist and must not be marked retained")
	}
}

// TestGraceWindowExpires pins the other end. The retained set is bounded, or a
// node accumulates every queue host it has ever used and polls all of them
// forever.
func TestGraceWindowExpires(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_expiry_itest")
	f.route("hub", "https://hub.example", nil)
	f.route("den", "https://den.example", nil)
	f.route("m4", "", []string{"den", "hub"})
	f.route("m4", "", []string{"den"})

	// A grace window that ends before any of this history was written.
	peers, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now().Add(48*time.Hour), 1*time.Hour)
	if err != nil {
		t.Fatalf("ResolveDrainPeers: %v", err)
	}
	if got := peerIDs(peers); !equalIDs(got, "den") {
		t.Fatalf("peers = %v, want [den] — hub's removal is outside the grace window", got)
	}
}

// TestSingleQueueHostStillWorks is convergence check 6. The degenerate
// spoke-hub topology has to keep working with no configuration change, or this
// migration breaks every existing deployment on the way to supporting more.
func TestSingleQueueHostStillWorks(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_degenerate_itest")
	f.route("hub", "https://hub.example", nil)
	f.route("m4", "", []string{"hub"})

	peers, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now(), DefaultDrainGrace)
	if err != nil {
		t.Fatalf("ResolveDrainPeers: %v", err)
	}
	if got := peerIDs(peers); !equalIDs(got, "hub") {
		t.Fatalf("peers = %v, want [hub]", got)
	}
}

// TestQueueHostWithoutPeerOriginIsOmitted pins that an unpollable host is left
// out rather than returned with an empty URL for the caller to trip over. A
// host that is merely unreachable right now is a different case and belongs to
// the drain loop's failure isolation.
func TestQueueHostWithoutPeerOriginIsOmitted(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_noorigin_itest")
	f.route("hub", "https://hub.example", nil)
	f.route("ghost", "", nil)
	f.route("m4", "", []string{"ghost", "hub"})

	peers, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now(), DefaultDrainGrace)
	if err != nil {
		t.Fatalf("ResolveDrainPeers: %v", err)
	}
	if got := peerIDs(peers); !equalIDs(got, "hub") {
		t.Fatalf("peers = %v, want [hub] — a host with no peer origin cannot be polled", got)
	}
}

// TestDrainPeersRejectsBadLocalNodeID keeps the entry point fail-closed.
func TestDrainPeersRejectsBadLocalNodeID(t *testing.T) {
	f := newPeerFixture(t, "meristem_spoke_peers_badid_itest")
	for _, id := range []string{"", "M4", "m4.example", "-m4"} {
		if _, err := ResolveDrainPeers(f.ctx, f.pool, id, time.Now(), DefaultDrainGrace); err == nil {
			t.Errorf("ResolveDrainPeers(%q) succeeded, want refusal", id)
		}
	}
	if _, err := ResolveDrainPeers(f.ctx, f.pool, "m4", time.Now(), -time.Second); err == nil {
		t.Error("negative grace succeeded, want refusal")
	}
}
