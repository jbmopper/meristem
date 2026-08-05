package nodes

import (
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestNormalizeSnapshotCanonicalizesAndSorts(t *testing.T) {
	base := "HTTPS://Hub.Example:443/"
	direct := "https://Spoke.Example:8443/"
	got, err := NormalizeSnapshot(RegistrySnapshot{
		PayloadVersion: 1,
		SourceNodeID:   "hub",
		SourceRevision: 12,
		Nodes: []SnapshotEntry{
			{NodeID: "spoke", DirectURL: &direct, QueueVia: []string{"hub"}, Status: domain.NodeStatusActive, RegistryRevision: 11},
			{NodeID: "hub", BaseURL: &base, QueueVia: nil, Status: domain.NodeStatusActive, RegistryRevision: 10},
		},
	}, "hub")
	if err != nil {
		t.Fatalf("NormalizeSnapshot: %v", err)
	}
	if got.Nodes[0].NodeID != "hub" || got.Nodes[1].NodeID != "spoke" {
		t.Fatalf("nodes not sorted: %+v", got.Nodes)
	}
	if got.Nodes[0].BaseURL == nil || *got.Nodes[0].BaseURL != "https://hub.example" {
		t.Fatalf("base_url = %v", got.Nodes[0].BaseURL)
	}
	if got.Nodes[1].DirectURL == nil || *got.Nodes[1].DirectURL != "https://spoke.example:8443" {
		t.Fatalf("direct_url = %v", got.Nodes[1].DirectURL)
	}
	if got.Nodes[0].QueueVia == nil {
		t.Fatal("nil queue_via was not normalized")
	}
}

func TestNormalizeSnapshotRefusesMalformedTopology(t *testing.T) {
	httpsPath := "https://hub.example/mcp"
	base := RegistrySnapshot{
		PayloadVersion: 1,
		SourceNodeID:   "hub",
		SourceRevision: 2,
		Nodes:          []SnapshotEntry{{NodeID: "hub", Status: domain.NodeStatusActive, RegistryRevision: 1}},
	}
	tests := []RegistrySnapshot{
		{PayloadVersion: 1, SourceNodeID: "other", SourceRevision: 2, Nodes: base.Nodes},
		{PayloadVersion: 1, SourceNodeID: "hub", SourceRevision: 2, Nodes: []SnapshotEntry{{NodeID: "hub", BaseURL: &httpsPath, Status: domain.NodeStatusActive, RegistryRevision: 1}}},
		{PayloadVersion: 1, SourceNodeID: "hub", SourceRevision: 2, Nodes: []SnapshotEntry{{NodeID: "hub", QueueVia: []string{"missing"}, Status: domain.NodeStatusActive, RegistryRevision: 1}}},
		{PayloadVersion: 1, SourceNodeID: "hub", SourceRevision: 2, Nodes: []SnapshotEntry{{NodeID: "hub", Status: domain.NodeStatus("unreachable"), RegistryRevision: 1}}},
	}
	for i, snapshot := range tests {
		_, err := NormalizeSnapshot(snapshot, "hub")
		if err == nil || (!errors.Is(err, ErrInvalidSnapshot) && !errors.Is(err, ErrWrongSnapshotSource)) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
}

func TestSnapshotScopesAreExactAndNeverRoot(t *testing.T) {
	actor := domain.Token{Scopes: []string{SnapshotReadScope("hub")}, Source: domain.SourceAgent}
	if !hasExactScope(actor, SnapshotReadScope("hub")) || hasExactScope(actor, SnapshotReadScope("other")) {
		t.Fatal("source-bound scope reducer mismatch")
	}
	actor.IsRoot = true
	if hasExactScope(actor, SnapshotReadScope("hub")) {
		t.Fatal("root token must not participate in peer registry delivery")
	}
}
