package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

type countingRegistry struct {
	loads int
	nodes []domain.Node
	err   error
}

func (r *countingRegistry) Load(context.Context) ([]domain.Node, error) {
	r.loads++
	return r.nodes, r.err
}

func TestDispatcherLoadsOneSnapshotAndUsesProductionSelection(t *testing.T) {
	direct := stub(t, http.StatusServiceUnavailable, nil)
	queued := &capture{}
	queue := stub(t, http.StatusAccepted, queued)
	registry := &countingRegistry{nodes: []domain.Node{
		node("m4", ptr(direct.URL), "hub"),
		node("hub", ptr(queue.URL)),
	}}
	dispatcher := NewDispatcherWithRegistry(registry, http.DefaultClient, resolver(map[string]string{
		"m4": "m4-token", "hub": "hub-token",
	}), fastDeliveryPolicy())
	dispatcher.now = func() time.Time { return time.Unix(123, 0).UTC() }

	out, err := dispatcher.DispatchMutation(context.Background(), sampleCommand(), nil)
	if err != nil {
		t.Fatalf("DispatchMutation: %v", err)
	}
	if registry.loads != 1 {
		t.Fatalf("registry loads = %d, want one immutable selection snapshot", registry.loads)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue || len(out.Attempts) != 4 {
		t.Fatalf("out = %+v, want three bounded direct attempts then queue", out)
	}
	for i := 0; i < 3; i++ {
		if out.Attempts[i].Candidate.Kind != KindDirect {
			t.Fatalf("attempt[%d] = %s, want direct", i, out.Attempts[i].Candidate.Kind)
		}
	}
	if out.Attempts[3].Candidate.Kind != KindQueue || queued.hits != 1 {
		t.Fatalf("fallback order = %+v queue hits=%d", out.Attempts, queued.hits)
	}
}

// TestCredentialRequestDistinguishesQueueHopFromTarget is the reason the
// resolver takes a relation instead of a node id. On a queue fallback the
// connection terminates at the queue host while the command is bound for a
// different home. A resolver handed only "hub" cannot tell which target it is
// being asked to act for, so it must either hand back a credential that gets
// denied or one broad enough to cover every target the hub serves — and the
// broad one is the dangerous outcome, because it works.
func TestCredentialRequestDistinguishesQueueHopFromTarget(t *testing.T) {
	direct := stub(t, http.StatusServiceUnavailable, nil)
	queue := stub(t, http.StatusAccepted, &capture{})
	registry := &countingRegistry{nodes: []domain.Node{
		node("m4", ptr(direct.URL), "hub"),
		node("hub", ptr(queue.URL)),
	}}
	var seen []CredentialRequest
	dispatcher := NewDispatcherWithRegistry(registry, http.DefaultClient,
		recordingResolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}, &seen),
		fastDeliveryPolicy())

	if _, err := dispatcher.DispatchMutation(context.Background(), sampleCommand(), nil); err != nil {
		t.Fatalf("DispatchMutation: %v", err)
	}
	if len(seen) != 4 {
		t.Fatalf("resolver called %d times, want 4 (three direct attempts then the queue hop)", len(seen))
	}

	transition := "/v1/work-items/" + sampleWorkItemID + "/transition"
	for i, got := range seen[:3] {
		if got.TerminatingPeer != "m4" || got.UltimateTarget != "m4" {
			t.Errorf("direct attempt %d: peer/target = %q/%q, want m4/m4", i, got.TerminatingPeer, got.UltimateTarget)
		}
		if got.Route != KindDirect || got.Purpose != PurposeRemoteMutation {
			t.Errorf("direct attempt %d: route/purpose = %q/%q, want direct/remote_mutation", i, got.Route, got.Purpose)
		}
		if got.Method != http.MethodPost || got.Path != transition {
			t.Errorf("direct attempt %d: operation = %s %s, want POST %s", i, got.Method, got.Path, transition)
		}
	}

	hop := seen[3]
	if hop.TerminatingPeer != "hub" {
		t.Errorf("queue hop: terminating peer = %q, want hub", hop.TerminatingPeer)
	}
	if hop.UltimateTarget != "m4" {
		t.Errorf("queue hop: ultimate target = %q, want m4 — the credential is for reaching m4 *through* hub, not for hub in general", hop.UltimateTarget)
	}
	if hop.Route != KindQueue {
		t.Errorf("queue hop: route = %q, want queue", hop.Route)
	}
	// The queue host is being asked to accept an enqueue, not to perform the
	// transition. Naming the carried command's path here would ask for
	// authority the hop does not need.
	if hop.Method != http.MethodPost || hop.Path != CommandPath {
		t.Errorf("queue hop: operation = %s %s, want POST %s", hop.Method, hop.Path, CommandPath)
	}
	for i, got := range seen {
		if got.OriginNodeID != "den" {
			t.Errorf("attempt %d: origin = %q, want den", i, got.OriginNodeID)
		}
	}
}

// TestCredentialRequestForRemoteRead pins the read side of the relation. A
// read has no queue hop, so peer and target coincide — but the purpose and
// method still differ from a mutation, which is what lets a peer mint
// read-only authority.
func TestCredentialRequestForRemoteRead(t *testing.T) {
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"work_item":{"id":"` + sampleWorkItemID + `"}}`))
	}))
	t.Cleanup(direct.Close)
	registry := &countingRegistry{nodes: []domain.Node{node("m4", ptr(direct.URL))}}
	var seen []CredentialRequest
	dispatcher := NewDispatcherWithRegistry(registry, direct.Client(),
		recordingResolver(map[string]string{"m4": "read-token"}, &seen), fastDeliveryPolicy())

	if _, err := dispatcher.ReadWorkItem(context.Background(), "den", "m4:"+sampleWorkItemID); err != nil {
		t.Fatalf("ReadWorkItem: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("resolver called %d times, want 1", len(seen))
	}
	got := seen[0]
	if got.Purpose != PurposeRemoteRead {
		t.Errorf("purpose = %q, want remote_read", got.Purpose)
	}
	if got.Method != http.MethodGet || got.Path != "/v1/work-items/"+sampleWorkItemID {
		t.Errorf("operation = %s %s, want GET /v1/work-items/%s", got.Method, got.Path, sampleWorkItemID)
	}
	if got.TerminatingPeer != "m4" || got.UltimateTarget != "m4" || got.OriginNodeID != "den" {
		t.Errorf("relation = %q -> %q (target %q), want den -> m4 (target m4)", got.OriginNodeID, got.TerminatingPeer, got.UltimateTarget)
	}
	if got.Route != KindDirect {
		t.Errorf("route = %q, want direct — a read must never claim a queue route", got.Route)
	}
}

func TestDispatcherRegistryFailureDoesNotAttemptNetwork(t *testing.T) {
	registry := &countingRegistry{err: errors.New("database unavailable")}
	dispatcher := NewDispatcherWithRegistry(registry, http.DefaultClient, resolver(nil), fastDeliveryPolicy())
	_, err := dispatcher.DispatchMutation(context.Background(), sampleCommand(), nil)
	if !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v, want ErrRegistryUnavailable", err)
	}
	if registry.loads != 1 {
		t.Fatalf("registry loads = %d", registry.loads)
	}
}

func TestDispatcherReadWorkItemUsesQualifiedHomeAndDirectRouteOnly(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotTarget, gotOrigin string
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotTarget = r.Header.Get(HeaderTargetNode)
		gotOrigin = r.Header.Get(HeaderOriginNode)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"work_item":{"id":"` + sampleWorkItemID + `"}}`))
	}))
	t.Cleanup(direct.Close)
	queue := stub(t, http.StatusAccepted, nil)
	registry := &countingRegistry{nodes: []domain.Node{
		node("m4", ptr(direct.URL), "hub"),
		node("hub", ptr(queue.URL)),
	}}
	dispatcher := NewDispatcherWithRegistry(registry, direct.Client(), resolver(map[string]string{"m4": "read-token"}), fastDeliveryPolicy())

	out, err := dispatcher.ReadWorkItem(context.Background(), "den", "m4:"+sampleWorkItemID)
	if err != nil {
		t.Fatalf("ReadWorkItem: %v", err)
	}
	if registry.loads != 1 || out.StatusCode != http.StatusOK || len(out.Attempts) != 1 {
		t.Fatalf("loads/out = %d %+v", registry.loads, out)
	}
	if gotMethod != http.MethodGet || gotPath != "/v1/work-items/"+sampleWorkItemID {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if gotAuth != "Bearer read-token" || gotTarget != "m4" || gotOrigin != "den" {
		t.Fatalf("auth/target/origin = %q %q %q", gotAuth, gotTarget, gotOrigin)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil || body["work_item"] == nil {
		t.Fatalf("body = %s err=%v", out.Body, err)
	}
}

// TestDispatcherReadWorkItemAcceptsCanonicalURI pins that the canonical
// reference form routes to exactly the same home and path as the compact alias.
// The dispatcher is the first production caller that takes a reference from
// outside, so if the two spellings diverged here the divergence would surface
// as a read dispatched to the wrong node rather than as a parse error.
func TestDispatcherReadWorkItemAcceptsCanonicalURI(t *testing.T) {
	var gotPath, gotTarget string
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotTarget = r.URL.Path, r.Header.Get(HeaderTargetNode)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"work_item":{"id":"` + sampleWorkItemID + `"}}`))
	}))
	t.Cleanup(direct.Close)
	registry := &countingRegistry{nodes: []domain.Node{node("m4", ptr(direct.URL))}}
	dispatcher := NewDispatcherWithRegistry(registry, direct.Client(), resolver(map[string]string{"m4": "read-token"}), fastDeliveryPolicy())

	out, err := dispatcher.ReadWorkItem(context.Background(), "den", "mrs://m4/work-items/"+sampleWorkItemID)
	if err != nil {
		t.Fatalf("ReadWorkItem(canonical): %v", err)
	}
	if out.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", out.StatusCode)
	}
	if gotPath != "/v1/work-items/"+sampleWorkItemID || gotTarget != "m4" {
		t.Fatalf("canonical ref routed to %s (target %q), want /v1/work-items/%s (target m4)", gotPath, gotTarget, sampleWorkItemID)
	}
}

// TestDispatcherReadWorkItemRejectsUnroutableRefs keeps the fail-closed edge
// honest. A bare UUID is local, so reaching a remote read with one means the
// caller lost the home; guessing a node would dispatch someone else's read.
func TestDispatcherReadWorkItemRejectsUnroutableRefs(t *testing.T) {
	registry := &countingRegistry{nodes: []domain.Node{node("m4", ptr("https://m4.example"))}}
	dispatcher := NewDispatcherWithRegistry(registry, http.DefaultClient, resolver(nil), fastDeliveryPolicy())
	for _, ref := range []string{
		sampleWorkItemID,                                   // bare uuid: local, not remote
		"mrs://m4/tropisms/" + sampleWorkItemID,            // wrong object kind
		"mrs://m4/work-items/" + sampleWorkItemID + "?x=1", // decorated
		"http://m4/work-items/" + sampleWorkItemID,         // wrong scheme
		"MRS://M4/work-items/" + sampleWorkItemID,          // uppercase host is not a node id
	} {
		if _, err := dispatcher.ReadWorkItem(context.Background(), "den", ref); !errors.Is(err, ErrInvalidQualifiedRef) {
			t.Errorf("ReadWorkItem(%q) err = %v, want ErrInvalidQualifiedRef", ref, err)
		}
	}
}

func TestDispatcherReadNeverUsesQueue(t *testing.T) {
	queueCapture := &capture{}
	queue := stub(t, http.StatusAccepted, queueCapture)
	registry := &countingRegistry{nodes: []domain.Node{
		node("m4", nil, "hub"),
		node("hub", ptr(queue.URL)),
	}}
	dispatcher := NewDispatcherWithRegistry(registry, queue.Client(), resolver(map[string]string{"hub": "queue-token"}), fastDeliveryPolicy())

	_, err := dispatcher.ReadWorkItem(context.Background(), "den", "m4:"+sampleWorkItemID)
	if !errors.Is(err, ErrNoDirectReadRoute) {
		t.Fatalf("err = %v, want ErrNoDirectReadRoute", err)
	}
	if queueCapture.hits != 0 {
		t.Fatalf("remote read was queued %d times", queueCapture.hits)
	}
}
