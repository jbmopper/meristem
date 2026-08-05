package nodes

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
)

// The fixtures in this file model a PRE-RENAME binary — one that knows only the
// relay_via spelling — decoding what the current binary produces. That is the
// axis the rest of the suite cannot see: every other test runs one binary
// against itself, so a wire-compatibility break is invisible to it no matter
// how green it is. Each type below is a frozen copy of the old wire shape and
// must not be "fixed" to match the current structs; changing them to keep a
// test passing defeats the entire point.

// priorNodeEventPayload is the pre-rename v2 decoder for node.registered and
// node.route_updated. It reads relay_via and has never heard of queue_via.
type priorNodeEventPayload struct {
	PayloadVersion int      `json:"payload_version,omitempty"`
	NodeID         string   `json:"node_id"`
	BaseURL        *string  `json:"base_url,omitempty"`
	DirectURL      *string  `json:"direct_url,omitempty"`
	RelayVia       []string `json:"relay_via,omitempty"`
	Status         string   `json:"status"`
}

// TestPriorBinaryDecodesCurrentNodeEvents is the regression for QV-B2. The
// payload version did not change, so an old projector will decode these events
// on replay or rebuild and must still see the route. Before the fix it folded
// an empty allowlist and silently dropped every queue host.
func TestPriorBinaryDecodesCurrentNodeEvents(t *testing.T) {
	hops := []string{"den", "hub"}

	registered, err := BuildRegisteredPayload(RegisterParams{
		NodeID:   "m4",
		BaseURL:  strPtr("https://ingress.example"),
		QueueVia: hops,
		Status:   string(domain.NodeStatusActive),
	})
	if err != nil {
		t.Fatalf("build registered: %v", err)
	}
	routed, err := BuildRouteUpdatedPayload(RouteParams{
		NodeID:   "m4",
		QueueVia: hops,
		Status:   string(domain.NodeStatusActive),
	})
	if err != nil {
		t.Fatalf("build route updated: %v", err)
	}

	for _, tc := range []struct {
		name    string
		payload any
	}{
		{"node.registered", registered},
		{"node.route_updated", routed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			// The wire must carry BOTH spellings, with equal values.
			var keys map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &keys); err != nil {
				t.Fatalf("unmarshal to keys: %v", err)
			}
			if _, ok := keys["queue_via"]; !ok {
				t.Errorf("payload omits queue_via: %s", encoded)
			}
			if _, ok := keys["relay_via"]; !ok {
				t.Errorf("payload omits relay_via, so a prior binary folds an empty route: %s", encoded)
			}

			var prior priorNodeEventPayload
			if err := json.Unmarshal(encoded, &prior); err != nil {
				t.Fatalf("prior binary failed to decode: %v", err)
			}
			if !slices.Equal(prior.RelayVia, hops) {
				t.Fatalf("prior binary read relay_via = %v, want %v", prior.RelayVia, hops)
			}
			if prior.PayloadVersion != routePayloadVersion {
				t.Fatalf("payload_version = %d, want %d — a version bump would make the prior binary fail closed instead", prior.PayloadVersion, routePayloadVersion)
			}
		})
	}
}

// priorSnapshotEntry / priorSnapshot are the pre-rename registry snapshot v1
// consumer. Crucially it decodes with DisallowUnknownFields, exactly as
// registry_sync does, so an additive key is a hard rejection rather than a
// tolerated extra.
type priorSnapshotEntry struct {
	NodeID           string            `json:"node_id"`
	BaseURL          *string           `json:"base_url,omitempty"`
	DirectURL        *string           `json:"direct_url,omitempty"`
	RelayVia         []string          `json:"relay_via"`
	Status           domain.NodeStatus `json:"status"`
	RegistryRevision int64             `json:"registry_revision"`
}

type priorSnapshot struct {
	PayloadVersion int                  `json:"payload_version"`
	SourceNodeID   string               `json:"source_node_id"`
	SourceRevision int64                `json:"source_revision"`
	Nodes          []priorSnapshotEntry `json:"nodes"`
}

// TestPriorBinaryDecodesCurrentSnapshot is the regression for QV-B3, the most
// severe of the four findings. Emitting queue_via on the v1 snapshot wire does
// not degrade gracefully — the old peer rejects the whole payload and cross-node
// registry sync stops, which is the mesh's own bootstrap path.
func TestPriorBinaryDecodesCurrentSnapshot(t *testing.T) {
	snapshot, err := NormalizeSnapshot(RegistrySnapshot{
		PayloadVersion: SnapshotPayloadVersion,
		SourceNodeID:   "hub",
		SourceRevision: 10,
		Nodes: []SnapshotEntry{
			{NodeID: "hub", BaseURL: strPtr("https://hub.example"), Status: domain.NodeStatusActive, RegistryRevision: 5},
			{NodeID: "m4", QueueVia: []string{"hub"}, Status: domain.NodeStatusActive, RegistryRevision: 6},
		},
	}, "hub")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if bytes.Contains(encoded, []byte("queue_via")) {
		t.Fatalf("snapshot v1 emitted queue_via; a prior peer using DisallowUnknownFields rejects the entire snapshot: %s", encoded)
	}

	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.DisallowUnknownFields()
	var prior priorSnapshot
	if err := dec.Decode(&prior); err != nil {
		t.Fatalf("prior binary rejected the snapshot: %v", err)
	}
	var m4 *priorSnapshotEntry
	for i := range prior.Nodes {
		if prior.Nodes[i].NodeID == "m4" {
			m4 = &prior.Nodes[i]
		}
	}
	if m4 == nil {
		t.Fatal("prior binary did not see node m4")
	}
	if !slices.Equal(m4.RelayVia, []string{"hub"}) {
		t.Fatalf("prior binary read relay_via = %v, want [hub]", m4.RelayVia)
	}
}

// TestCurrentBinaryDecodesPriorSnapshot is the other direction: a prior peer's
// relay_via-only v1 snapshot must still be understood here.
func TestCurrentBinaryDecodesPriorSnapshot(t *testing.T) {
	encoded, err := json.Marshal(priorSnapshot{
		PayloadVersion: SnapshotPayloadVersion,
		SourceNodeID:   "hub",
		SourceRevision: 10,
		Nodes: []priorSnapshotEntry{
			{NodeID: "hub", BaseURL: strPtr("https://hub.example"), RelayVia: []string{}, Status: domain.NodeStatusActive, RegistryRevision: 5},
			{NodeID: "m4", RelayVia: []string{"hub"}, Status: domain.NodeStatusActive, RegistryRevision: 6},
		},
	})
	if err != nil {
		t.Fatalf("marshal prior snapshot: %v", err)
	}
	var current RegistrySnapshot
	if err := json.Unmarshal(encoded, &current); err != nil {
		t.Fatalf("current binary rejected a prior snapshot: %v", err)
	}
	normalized, err := NormalizeSnapshot(current, "hub")
	if err != nil {
		t.Fatalf("normalize prior snapshot: %v", err)
	}
	for _, entry := range normalized.Nodes {
		if entry.NodeID == "m4" && !slices.Equal(entry.QueueVia, []string{"hub"}) {
			t.Fatalf("queue_via from prior relay_via = %v, want [hub]", entry.QueueVia)
		}
	}
}

// TestResolveQueueViaSelectsOnPresence is the regression for QV-B4. Selecting on
// slice length rather than field presence made an explicit empty allowlist
// indistinguishable from an absent one, so clearing the queue hosts silently
// reverted to the legacy value.
func TestResolveQueueViaSelectsOnPresence(t *testing.T) {
	present := func(v ...string) *[]string {
		if v == nil {
			v = []string{}
		}
		return &v
	}

	tests := []struct {
		name       string
		queueVia   *[]string
		relayVia   *[]string
		want       []string
		wantAmbig  bool
		wantAbsent bool
	}{
		{name: "both absent", wantAbsent: true},
		{name: "queue_via only", queueVia: present("den"), want: []string{"den"}},
		{name: "relay_via only", relayVia: present("hub"), want: []string{"hub"}},
		{name: "both equal", queueVia: present("den"), relayVia: present("den"), want: []string{"den"}},
		{
			name:     "explicit empty queue_via does not fall back to relay_via",
			queueVia: present(),
			relayVia: present("den"),
			// This is the QV-B4 failure: length-based selection returned [den],
			// resurrecting a queue host the operator had just removed.
			wantAmbig: true,
		},
		{name: "conflicting non-empty values are ambiguous", queueVia: present("den"), relayVia: present("hub"), wantAmbig: true},
		{name: "order matters", queueVia: present("den", "hub"), relayVia: present("hub", "den"), wantAmbig: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveQueueVia(tt.queueVia, tt.relayVia)
			if tt.wantAmbig {
				if !errors.Is(err, ErrAmbiguousQueueVia) {
					t.Fatalf("err = %v, want ErrAmbiguousQueueVia", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantAbsent {
				if got != nil {
					t.Fatalf("got %v, want nil for an absent field", got)
				}
				return
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestExplicitEmptyQueueViaIsPreserved pins the operator-facing consequence of
// presence-based selection: clearing the allowlist must survive a decode.
func TestExplicitEmptyQueueViaIsPreserved(t *testing.T) {
	var p routeUpdatedPayload
	if err := json.Unmarshal([]byte(`{
		"payload_version": 2,
		"node_id": "m4",
		"status": "active",
		"queue_via": [],
		"relay_via": []
	}`), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(p.QueueVia) != 0 {
		t.Fatalf("queue_via = %v, want empty", p.QueueVia)
	}
	if p.QueueVia == nil {
		t.Fatal("an explicitly empty queue_via decoded as absent, losing the operator's clear")
	}
}

// TestAmbiguousNodeEventIsRejected proves the conflict rule reaches the real
// decode path, not just the helper.
func TestAmbiguousNodeEventIsRejected(t *testing.T) {
	var p registeredPayload
	err := json.Unmarshal([]byte(`{
		"payload_version": 2,
		"node_id": "m4",
		"status": "active",
		"queue_via": ["den"],
		"relay_via": ["hub"]
	}`), &p)
	if !errors.Is(err, ErrAmbiguousQueueVia) {
		t.Fatalf("err = %v, want ErrAmbiguousQueueVia", err)
	}
}

func strPtr(s string) *string { return &s }
