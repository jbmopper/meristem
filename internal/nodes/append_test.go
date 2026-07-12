package nodes

import (
	"encoding/json"
	"strings"
	"testing"
)

func strptr(s string) *string { return &s }

// marshal round-trips a built payload to its canonical JSON object so tests
// assert the wire shape the projector will decode, not the in-memory struct.
func marshal(t *testing.T, payload any) map[string]any {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return m
}

func TestBuildRegisteredPayloadFull(t *testing.T) {
	payload, err := BuildRegisteredPayload(RegisterParams{
		NodeID:    "m4",
		BaseURL:   strptr("https://ingress.example"),
		DirectURL: strptr("https://m4.peer.example"),
		RelayVia:  []string{"den"},
		Status:    "active",
	})
	if err != nil {
		t.Fatalf("BuildRegisteredPayload: %v", err)
	}
	m := marshal(t, payload)
	if m["node_id"] != "m4" {
		t.Errorf("node_id = %v, want m4", m["node_id"])
	}
	if m["base_url"] != "https://ingress.example" {
		t.Errorf("base_url = %v", m["base_url"])
	}
	if m["direct_url"] != "https://m4.peer.example" {
		t.Errorf("direct_url = %v", m["direct_url"])
	}
	if m["status"] != "active" {
		t.Errorf("status = %v, want active", m["status"])
	}
	relay, ok := m["relay_via"].([]any)
	if !ok || len(relay) != 1 || relay[0] != "den" {
		t.Errorf("relay_via = %v, want [den]", m["relay_via"])
	}
	if m["payload_version"] != float64(routePayloadVersion) {
		t.Errorf("payload_version = %v, want %d", m["payload_version"], routePayloadVersion)
	}
}

func TestBuildRegisteredPayloadOmitsAbsentURLs(t *testing.T) {
	// A pull-only spoke: no base_url, no direct_url. Both must be omitted so
	// the wire form (and the deterministic event id) is minimal and stable.
	payload, err := BuildRegisteredPayload(RegisterParams{
		NodeID: "lump",
		Status: "active",
	})
	if err != nil {
		t.Fatalf("BuildRegisteredPayload: %v", err)
	}
	m := marshal(t, payload)
	if m["payload_version"] != float64(routePayloadVersion) {
		t.Errorf("payload_version = %v, want %d", m["payload_version"], routePayloadVersion)
	}
	if _, present := m["base_url"]; present {
		t.Errorf("base_url should be omitted, got %v", m["base_url"])
	}
	if _, present := m["direct_url"]; present {
		t.Errorf("direct_url should be omitted, got %v", m["direct_url"])
	}
	if _, present := m["relay_via"]; present {
		t.Errorf("relay_via should be omitted when empty, got %v", m["relay_via"])
	}
}

func TestBuildRegisteredPayloadStable(t *testing.T) {
	// Identical params marshal identically: the writer's deterministic id
	// collapses a re-registration onto one event.
	params := RegisterParams{NodeID: "m4", BaseURL: strptr("https://x.example"), Status: "active"}
	a, err := BuildRegisteredPayload(params)
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	b, err := BuildRegisteredPayload(params)
	if err != nil {
		t.Fatalf("build b: %v", err)
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("identical params produced different wire forms:\n a=%s\n b=%s", ja, jb)
	}
}

func TestBuildRegisteredPayloadCanonicalizesOriginsBeforeEventIdentity(t *testing.T) {
	payload, err := BuildRegisteredPayload(RegisterParams{
		NodeID:    "m4",
		BaseURL:   strptr("HTTPS://Ingress.Example:443/"),
		DirectURL: strptr("https://M4.Peer.Example:8443/"),
		Status:    "active",
	})
	if err != nil {
		t.Fatalf("BuildRegisteredPayload: %v", err)
	}
	m := marshal(t, payload)
	if m["base_url"] != "https://ingress.example" {
		t.Errorf("base_url = %v", m["base_url"])
	}
	if m["direct_url"] != "https://m4.peer.example:8443" {
		t.Errorf("direct_url = %v", m["direct_url"])
	}
}

func TestBuildRegisteredPayloadRejectsUnsafeOrigins(t *testing.T) {
	for _, raw := range []string{
		"https://node.example/mcp",
		"https://user:pass@node.example",
		"http://10.0.0.63:8080",
	} {
		_, err := BuildRegisteredPayload(RegisterParams{
			NodeID:  "m4",
			BaseURL: strptr(raw),
			Status:  "active",
		})
		if err == nil || !strings.Contains(err.Error(), "invalid node origin") {
			t.Errorf("base_url %q: expected invalid node origin, got %v", raw, err)
		}
	}
}

func TestBuildRegisteredPayloadRejectsBadNodeID(t *testing.T) {
	for _, id := range []string{"", "M4", "-m4", "m4-", "m4:den", "node.one"} {
		if _, err := BuildRegisteredPayload(RegisterParams{NodeID: id, Status: "active"}); err == nil || !strings.Contains(err.Error(), "DNS-safe") {
			t.Errorf("node_id %q: expected DNS-safe error, got %v", id, err)
		}
	}
}

func TestBuildRegisteredPayloadRequiresStatus(t *testing.T) {
	if _, err := BuildRegisteredPayload(RegisterParams{NodeID: "m4"}); err == nil || !strings.Contains(err.Error(), "status is required") {
		t.Errorf("expected status required, got %v", err)
	}
}

func TestBuildRegisteredPayloadRejectsBadRelayHop(t *testing.T) {
	_, err := BuildRegisteredPayload(RegisterParams{NodeID: "m4", Status: "active", RelayVia: []string{"den", "BAD"}})
	if err == nil || !strings.Contains(err.Error(), "relay_via[1]") {
		t.Errorf("expected relay_via hop error, got %v", err)
	}
}

func TestBuildRouteUpdatedPayloadOmitsBaseURL(t *testing.T) {
	// route_updated never carries base_url — registration owns the ingress URL.
	payload, err := BuildRouteUpdatedPayload(RouteParams{
		NodeID:    "m4",
		DirectURL: strptr("https://m4.peer.example"),
		RelayVia:  []string{},
		Status:    "unreachable",
	})
	if err != nil {
		t.Fatalf("BuildRouteUpdatedPayload: %v", err)
	}
	m := marshal(t, payload)
	if m["payload_version"] != float64(routePayloadVersion) {
		t.Errorf("payload_version = %v, want %d", m["payload_version"], routePayloadVersion)
	}
	if _, present := m["base_url"]; present {
		t.Errorf("route_updated must not carry base_url, got %v", m["base_url"])
	}
	if m["direct_url"] != "https://m4.peer.example" {
		t.Errorf("direct_url = %v", m["direct_url"])
	}
	if m["status"] != "unreachable" {
		t.Errorf("status = %v, want unreachable", m["status"])
	}
}

func TestBuildRouteUpdatedPayloadRejectsBadNodeID(t *testing.T) {
	if _, err := BuildRouteUpdatedPayload(RouteParams{NodeID: "Bad", Status: "active"}); err == nil || !strings.Contains(err.Error(), "DNS-safe") {
		t.Errorf("expected DNS-safe error, got %v", err)
	}
}

func TestBuildRouteUpdatedPayloadRequiresStatus(t *testing.T) {
	if _, err := BuildRouteUpdatedPayload(RouteParams{NodeID: "m4"}); err == nil || !strings.Contains(err.Error(), "status is required") {
		t.Errorf("expected status required, got %v", err)
	}
}
