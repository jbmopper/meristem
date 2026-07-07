package crossnode

import (
	"errors"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/domain"
)

func ptr(s string) *string { return &s }

func node(id string, direct *string, relay ...string) domain.Node {
	return domain.Node{
		NodeID:    id,
		DirectURL: direct,
		RelayVia:  relay,
		Status:    domain.NodeStatusActive,
	}
}

// candidateShape is the comparable projection of a Candidate the table asserts
// on, so tests read as (kind, url, via) tuples in the expected order.
type candidateShape struct {
	kind CandidateKind
	url  string
	via  string
}

func shapes(cs []Candidate) []candidateShape {
	out := make([]candidateShape, len(cs))
	for i, c := range cs {
		out[i] = candidateShape{c.Kind, c.URL, c.Via}
	}
	return out
}

func TestSelect(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	hub := "https://hub.example"
	m4direct := "https://m4.peer.example"

	tests := []struct {
		name      string
		nodes     []domain.Node
		target    string
		cooldowns map[string]time.Time
		want      []candidateShape
		wantErr   error
	}{
		{
			name:    "unknown target",
			nodes:   []domain.Node{node("hub", ptr(hub))},
			target:  "ghost",
			wantErr: ErrUnknownTarget,
		},
		{
			name:   "direct only",
			nodes:  []domain.Node{node("m4", ptr(m4direct))},
			target: "m4",
			want:   []candidateShape{{KindDirect, m4direct, ""}},
		},
		{
			name: "direct then relay then queue, relay order preserved",
			nodes: []domain.Node{
				node("m4", ptr(m4direct), "hub", "den"),
				node("hub", ptr(hub)),
				node("den", ptr("https://den.example")),
			},
			target: "m4",
			want: []candidateShape{
				{KindDirect, m4direct, ""},
				{KindRelay, hub, "hub"},
				{KindRelay, "https://den.example", "den"},
				{KindQueue, hub, "hub"},
				{KindQueue, "https://den.example", "den"},
			},
		},
		{
			name: "relay without direct_url is skipped for both relay and queue",
			nodes: []domain.Node{
				node("m4", nil, "isolated", "hub"),
				node("isolated", nil),
				node("hub", ptr(hub)),
			},
			target: "m4",
			want: []candidateShape{
				{KindRelay, hub, "hub"},
				{KindQueue, hub, "hub"},
			},
		},
		{
			name: "degenerate hub-and-spoke: exactly one node has a direct_url",
			nodes: []domain.Node{
				node("m4", nil, "hub"),
				node("laptop", nil, "hub"),
				node("hub", ptr(hub)),
			},
			target: "m4",
			want: []candidateShape{
				{KindRelay, hub, "hub"},
				{KindQueue, hub, "hub"},
			},
		},
		{
			name:    "known target with no direct_url and no relays returns no route",
			nodes:   []domain.Node{node("m4", nil)},
			target:  "m4",
			wantErr: ErrNoRoute,
		},
		{
			name: "unknown relay id in relay_via is ignored",
			nodes: []domain.Node{
				node("m4", nil, "gone", "hub"),
				node("hub", ptr(hub)),
			},
			target: "m4",
			want: []candidateShape{
				{KindRelay, hub, "hub"},
				{KindQueue, hub, "hub"},
			},
		},
		{
			name: "cooling direct route is skipped, relay survives",
			nodes: []domain.Node{
				node("m4", ptr(m4direct), "hub"),
				node("hub", ptr(hub)),
			},
			target: "m4",
			cooldowns: map[string]time.Time{
				routeKey(KindDirect, "m4", ""): now.Add(-30 * time.Second),
			},
			want: []candidateShape{
				{KindRelay, hub, "hub"},
				{KindQueue, hub, "hub"},
			},
		},
		{
			name: "expired cooldown no longer skips the route",
			nodes: []domain.Node{
				node("m4", ptr(m4direct), "hub"),
				node("hub", ptr(hub)),
			},
			target: "m4",
			cooldowns: map[string]time.Time{
				routeKey(KindDirect, "m4", ""): now.Add(-(CooldownWindow + time.Second)),
			},
			want: []candidateShape{
				{KindDirect, m4direct, ""},
				{KindRelay, hub, "hub"},
				{KindQueue, hub, "hub"},
			},
		},
		{
			name: "all routes cooling returns no route",
			nodes: []domain.Node{
				node("m4", ptr(m4direct), "hub"),
				node("hub", ptr(hub)),
			},
			target: "m4",
			cooldowns: map[string]time.Time{
				routeKey(KindDirect, "m4", ""):   now,
				routeKey(KindRelay, "m4", "hub"): now,
				routeKey(KindQueue, "m4", "hub"): now,
			},
			wantErr: ErrNoRoute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Select(tt.nodes, tt.target, tt.cooldowns, now)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			gotShapes := shapes(got)
			if len(gotShapes) != len(tt.want) {
				t.Fatalf("got %d candidates %v, want %d %v", len(gotShapes), gotShapes, len(tt.want), tt.want)
			}
			for i := range tt.want {
				if gotShapes[i] != tt.want[i] {
					t.Fatalf("candidate[%d] = %+v, want %+v (full: %v)", i, gotShapes[i], tt.want[i], gotShapes)
				}
			}
		})
	}
}

// TestSelectRouteKeysAreStableAndDistinct guards the cooldown contract: the
// three candidates through one relay node must have distinct keys so cooling
// one does not cool the others.
func TestSelectRouteKeysAreStableAndDistinct(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	nodes := []domain.Node{
		node("m4", ptr("https://m4.example"), "hub"),
		node("hub", ptr("https://hub.example")),
	}
	got, err := Select(nodes, "m4", nil, now)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	seen := map[string]bool{}
	for _, c := range got {
		if c.RouteKey == "" {
			t.Fatalf("candidate %+v has empty RouteKey", c)
		}
		if seen[c.RouteKey] {
			t.Fatalf("duplicate RouteKey %q across candidates", c.RouteKey)
		}
		seen[c.RouteKey] = true
	}
}
