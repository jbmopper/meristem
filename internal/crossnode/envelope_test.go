package crossnode

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var envelopeID = uuid.MustParse("60959376-e0ff-5207-9270-dacfb403333e")

// TestRemoteReadEnvelopeCarriesProvenance pins the shape codex required for
// Q3: a remote read is evidence returned by the home at a moment in time, and
// the result must say so on its face.
func TestRemoteReadEnvelopeCarriesProvenance(t *testing.T) {
	observed := time.Date(2026, 8, 5, 6, 30, 0, 0, time.UTC)
	env, err := NewRemoteReadEnvelope("m4", envelopeID, observed, []byte(`{"work_item":{"id":"x"}}`))
	if err != nil {
		t.Fatalf("NewRemoteReadEnvelope: %v", err)
	}
	if env.Source != SourceRemoteHome {
		t.Errorf("source = %q, want %q", env.Source, SourceRemoteHome)
	}
	if env.HomeNodeID != "m4" {
		t.Errorf("home = %q, want m4", env.HomeNodeID)
	}
	if want := "mrs://m4/work-items/" + envelopeID.String(); env.CanonicalRef != want {
		t.Errorf("canonical_ref = %q, want %q", env.CanonicalRef, want)
	}
	if !env.ObservedAt.Equal(observed) {
		t.Errorf("observed_at = %s, want %s", env.ObservedAt, observed)
	}
}

// TestRemoteReadEnvelopeSurvivesSerialization is the reason the discriminant
// lives in the body rather than an HTTP header. An MCP tool result is
// serialized and handed to a caller that never sees the transport, so a
// header-borne marker would vanish exactly where the local/remote confusion
// would occur — and vanish silently.
func TestRemoteReadEnvelopeSurvivesSerialization(t *testing.T) {
	env, err := NewRemoteReadEnvelope("m4", envelopeID, time.Now(), []byte(`{"work_item":{"id":"x"}}`))
	if err != nil {
		t.Fatalf("NewRemoteReadEnvelope: %v", err)
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"source":"remote_home"`, `"home_node_id":"m4"`, `"canonical_ref":"mrs://m4/`, `"observed_at":`} {
		if !strings.Contains(string(encoded), key) {
			t.Errorf("serialized envelope is missing %s: %s", key, encoded)
		}
	}
	var round RemoteReadEnvelope
	if err := json.Unmarshal(encoded, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if round.Source != SourceRemoteHome || round.HomeNodeID != "m4" {
		t.Fatalf("provenance lost on round trip: %+v", round)
	}
}

// TestRemoteReadEnvelopeKeepsBodyUndigested pins that the home's response is
// carried verbatim. Decoding it into a local work-item type here is what would
// let a consumer treat it as a local projection, which is precisely the
// confusion the envelope exists to prevent.
func TestRemoteReadEnvelopeKeepsBodyUndigested(t *testing.T) {
	body := `{"work_item":{"id":"x","state":"running"},"unknown_future_field":42}`
	env, err := NewRemoteReadEnvelope("m4", envelopeID, time.Now(), []byte(body))
	if err != nil {
		t.Fatalf("NewRemoteReadEnvelope: %v", err)
	}
	if string(env.Body) != body {
		t.Fatalf("body = %s, want verbatim %s", env.Body, body)
	}
}

// TestRemoteReadEnvelopeFailsClosed covers the refusal cases. A half-identified
// envelope is worse than an error: it asserts provenance it cannot back, and
// the assertion is what downstream code will trust.
func TestRemoteReadEnvelopeFailsClosed(t *testing.T) {
	valid := []byte(`{"work_item":{}}`)
	cases := map[string]struct {
		home string
		body []byte
	}{
		"empty home":        {"", valid},
		"invalid home":      {"M4", valid},
		"dotted home":       {"m4.example", valid},
		"home with port":    {"m4:8080", valid},
		"empty body":        {"m4", nil},
		"zero-length body":  {"m4", []byte{}},
		"invalid home+body": {"", nil},
	}
	for name, tc := range cases {
		if env, err := NewRemoteReadEnvelope(tc.home, envelopeID, time.Now(), tc.body); err == nil {
			t.Errorf("%s: expected refusal, got envelope %+v", name, env)
		}
	}
}
