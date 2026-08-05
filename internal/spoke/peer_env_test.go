package spoke

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestEnvPeerTokenNameMapsNodeIdsToVariables pins the mapping operators will
// write into their supervisor config. Getting it wrong means a correctly
// configured peer looks uncredentialed and is silently skipped.
func TestEnvPeerTokenNameMapsNodeIdsToVariables(t *testing.T) {
	cases := map[string]string{
		"hub":         "MERISTEM_PEER_TOKEN_HUB",
		"den":         "MERISTEM_PEER_TOKEN_DEN",
		"m4":          "MERISTEM_PEER_TOKEN_M4",
		"home-server": "MERISTEM_PEER_TOKEN_HOME_SERVER",
		"node-1":      "MERISTEM_PEER_TOKEN_NODE_1",
	}
	for nodeID, want := range cases {
		got, err := EnvPeerTokenName(nodeID)
		if err != nil {
			t.Fatalf("EnvPeerTokenName(%q): %v", nodeID, err)
		}
		if got != want {
			t.Errorf("EnvPeerTokenName(%q) = %q, want %q", nodeID, got, want)
		}
	}
}

// TestEnvPeerTokenNamesCannotCollide is why the hyphen mapping is safe. Node
// ids are lowercase letters, digits and hyphens — no underscores — so two
// distinct ids can never fold onto one variable. If they could, one peer would
// silently authenticate with another's bearer.
func TestEnvPeerTokenNamesCannotCollide(t *testing.T) {
	ids := []string{"home-server", "home", "server", "a-b", "ab", "node-1", "node1"}
	seen := map[string]string{}
	for _, id := range ids {
		name, err := EnvPeerTokenName(id)
		if err != nil {
			t.Fatalf("EnvPeerTokenName(%q): %v", id, err)
		}
		if prior, ok := seen[name]; ok {
			t.Fatalf("node ids %q and %q both map to %q", prior, id, name)
		}
		seen[name] = id
	}
}

// TestEnvPeerTokenNameRejectsMalformedIds keeps an unvalidated id from being
// concatenated into a variable name.
func TestEnvPeerTokenNameRejectsMalformedIds(t *testing.T) {
	for _, id := range []string{"", "HUB", "hub.example", "-hub", "hub-", "hub:8080", "a/b", "a_b"} {
		if name, err := EnvPeerTokenName(id); err == nil {
			t.Errorf("EnvPeerTokenName(%q) = %q, want refusal", id, name)
		}
	}
}

// TestEnvBearerResolverReadsPerPeerVariables is the happy path: each peer gets
// its own credential and nothing else.
func TestEnvBearerResolverReadsPerPeerVariables(t *testing.T) {
	t.Setenv("MERISTEM_PEER_TOKEN_HUB", "hub-secret")
	t.Setenv("MERISTEM_PEER_TOKEN_HOME_SERVER", "home-secret")
	resolve := EnvBearerResolver()

	for peer, want := range map[string]string{"hub": "hub-secret", "home-server": "home-secret"} {
		got, err := resolve(context.Background(), peer)
		if err != nil {
			t.Fatalf("resolve(%q): %v", peer, err)
		}
		if got != want {
			t.Errorf("resolve(%q) = %q, want %q", peer, got, want)
		}
	}
}

// TestEnvBearerResolverFailsClosedWithoutLeaking covers the refusal path and
// the shape of the refusal. The error must not carry the value, and must not
// distinguish "absent" from "empty" — an error that does is a probe for which
// peers this node holds credentials for.
func TestEnvBearerResolverFailsClosedWithoutLeaking(t *testing.T) {
	// One peer, three causes. Comparing across different peers would only prove
	// the message contains the peer name, which is not the property at issue.
	const peer = "den"
	refusal := func(t *testing.T, set bool, value string) string {
		t.Helper()
		if set {
			t.Setenv("MERISTEM_PEER_TOKEN_DEN", value)
		} else {
			t.Setenv("MERISTEM_PEER_TOKEN_DEN", "placeholder")
			if err := os.Unsetenv("MERISTEM_PEER_TOKEN_DEN"); err != nil {
				t.Fatalf("unset: %v", err)
			}
		}
		got, err := EnvBearerResolver()(context.Background(), peer)
		if err == nil {
			t.Fatalf("resolve(%q) = %q, want refusal", peer, got)
		}
		if got != "" {
			t.Errorf("resolve(%q) returned %q alongside an error", peer, got)
		}
		return err.Error()
	}

	var absent, empty, blank string
	t.Run("absent", func(t *testing.T) { absent = refusal(t, false, "") })
	t.Run("empty", func(t *testing.T) { empty = refusal(t, true, "") })
	t.Run("blank", func(t *testing.T) { blank = refusal(t, true, "   ") })

	if absent != empty || empty != blank {
		t.Errorf("refusals differ by cause, which probes configuration state:\nabsent=%q\nempty=%q\nblank=%q",
			absent, empty, blank)
	}
	for _, msg := range []string{absent, empty, blank} {
		if strings.Contains(msg, "placeholder") {
			t.Errorf("refusal echoed credential material: %q", msg)
		}
	}
}

// TestEnvBearerResolverNeverEchoesTheCredential pins the same rule on the
// success path's sibling: a resolver error for a peer that IS configured must
// still not contain the value.
func TestEnvBearerResolverNeverEchoesTheCredential(t *testing.T) {
	t.Setenv("MERISTEM_PEER_TOKEN_HUB", "super-secret-bearer")
	resolve := EnvBearerResolver()
	_, err := resolve(context.Background(), "NOT-A-NODE")
	if err == nil {
		t.Fatal("malformed peer id resolved, want refusal")
	}
	if strings.Contains(err.Error(), "super-secret-bearer") {
		t.Fatalf("error echoed credential material: %v", err)
	}
}

// TestProjectionPeerSourceValidatesConstruction keeps a misconfigured source
// from being built at all, rather than failing on the first tick.
func TestProjectionPeerSourceValidatesConstruction(t *testing.T) {
	if _, err := NewProjectionPeerSource(nil, "m4", 0); err == nil {
		t.Error("nil pool accepted, want refusal")
	}
}
