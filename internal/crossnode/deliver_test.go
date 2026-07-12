package crossnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const sampleWorkItemID = "11111111-1111-4111-8111-111111111111"

// capture records what one stub receiver saw, so tests assert the exact
// direct-REST versus durable-queue wire shape.
type capture struct {
	mu       sync.Mutex
	hits     int
	path     string
	auth     string
	queueFor string
	idemKey  string
	rawBody  []byte
	wireBody wireCommand
}

func (c *capture) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits++
	c.path = r.URL.Path
	c.auth = r.Header.Get("Authorization")
	c.queueFor = r.Header.Get(HeaderQueueFor)
	c.idemKey = r.Header.Get(HeaderIdempotencyKey)
	c.rawBody, _ = io.ReadAll(r.Body)
	_ = json.Unmarshal(c.rawBody, &c.wireBody)
}

func stub(t *testing.T, status int, cap *capture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cap != nil {
			cap.record(r)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func sampleCommand() Command {
	return Command{
		TargetNodeID:   "m4",
		IdempotencyKey: "idem-123",
		Path:           "/v1/work-items/" + sampleWorkItemID + "/transition",
		Body:           json.RawMessage(`{"to":"running"}`),
	}
}

func resolver(tokens map[string]string) BearerResolver {
	return func(_ context.Context, nodeID string) (string, error) {
		if token := tokens[nodeID]; token != "" {
			return token, nil
		}
		return "", fmt.Errorf("no credential for %s", nodeID)
	}
}

func TestDeliverDirectCallsCanonicalRESTWithTargetBearer(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusOK, cap)
	candidates := []Candidate{{Kind: KindDirect, URL: srv.URL, NodeID: "m4", RouteKey: "direct|m4"}}

	out, err := Deliver(context.Background(), srv.Client(), resolver(map[string]string{"m4": "target-token"}), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.StatusCode != http.StatusOK || out.Terminal.Kind != KindDirect {
		t.Fatalf("out = %+v, want direct 200", out)
	}
	if cap.path != sampleCommand().Path {
		t.Fatalf("path = %s, want canonical %s", cap.path, sampleCommand().Path)
	}
	if cap.queueFor != "" {
		t.Fatalf("direct request set queue-for header: %q", cap.queueFor)
	}
	if cap.auth != "Bearer target-token" {
		t.Fatalf("authorization = %q", cap.auth)
	}
	if cap.idemKey != "idem-123" || string(cap.rawBody) != `{"to":"running"}` {
		t.Fatalf("direct wire key/body = %q %s", cap.idemKey, cap.rawBody)
	}
	if len(out.Cooldowns) != 0 {
		t.Fatalf("success recorded cooldowns: %v", out.Cooldowns)
	}
}

func TestDeliverQueueUsesEnvelopeAndQueueHostBearer(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidates := []Candidate{{Kind: KindQueue, URL: srv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"}}

	out, err := Deliver(context.Background(), srv.Client(), resolver(map[string]string{"hub": "hub-token"}), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue {
		t.Fatalf("queue not delivered: %+v", out)
	}
	if cap.path != CommandPath || cap.queueFor != "m4" {
		t.Fatalf("queue path/target = %q %q", cap.path, cap.queueFor)
	}
	if cap.auth != "Bearer hub-token" {
		t.Fatalf("authorization = %q", cap.auth)
	}
	if cap.wireBody.CommandPath != sampleCommand().Path || string(cap.wireBody.CommandBody) != `{"to":"running"}` {
		t.Fatalf("queue envelope = %+v", cap.wireBody)
	}
}

func TestDeliverRejectsApplicationRelay(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidates := []Candidate{{Kind: KindRelay, URL: srv.URL, NodeID: "hub", Via: "hub", RouteKey: "relay|m4|hub"}}

	_, err := Deliver(context.Background(), srv.Client(), resolver(map[string]string{"hub": "hub-token"}), candidates, sampleCommand(), nil, time.Now())
	if !errors.Is(err, ErrUnsupportedRoute) {
		t.Fatalf("err = %v, want ErrUnsupportedRoute", err)
	}
	if cap.hits != 0 {
		t.Fatalf("unsupported relay reached network %d times", cap.hits)
	}
}

func TestDeliverAdvancesPast5xxToQueue(t *testing.T) {
	failing := stub(t, http.StatusBadGateway, nil)
	good := &capture{}
	goodSrv := stub(t, http.StatusAccepted, good)
	now := time.Unix(1_700_000_000, 0).UTC()
	candidates := []Candidate{
		{Kind: KindDirect, URL: failing.URL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: goodSrv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}

	out, err := Deliver(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), nil, now)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue {
		t.Fatalf("expected queue to win, got %+v", out)
	}
	if ts, ok := out.Cooldowns["direct|m4"]; !ok || !ts.Equal(now) {
		t.Fatalf("failed direct route not cooled at now: %v", out.Cooldowns)
	}
	if good.queueFor != "m4" {
		t.Fatalf("fallback was not queued for target: %q", good.queueFor)
	}
}

func TestDeliverAdvancesPastTransportFailure(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	good := &capture{}
	goodSrv := stub(t, http.StatusAccepted, good)
	now := time.Unix(42, 0).UTC()
	candidates := []Candidate{
		{Kind: KindDirect, URL: deadURL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: goodSrv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}

	out, err := Deliver(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), nil, now)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !out.Delivered || out.Terminal.Kind != KindQueue || out.Attempts[0].Err == nil {
		t.Fatalf("expected queue fallback after transport failure, got %+v", out)
	}
}

func TestDeliverStopsOnDefinitiveNon2xx(t *testing.T) {
	rejectSrv := stub(t, http.StatusConflict, nil)
	nextCap := &capture{}
	nextSrv := stub(t, http.StatusAccepted, nextCap)
	candidates := []Candidate{
		{Kind: KindDirect, URL: rejectSrv.URL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: nextSrv.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}

	out, err := Deliver(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), nil, time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if out.Delivered || out.StatusCode != http.StatusConflict || out.Terminal.Kind != KindDirect {
		t.Fatalf("expected terminal direct 409, got %+v", out)
	}
	if nextCap.hits != 0 || len(out.Cooldowns) != 0 {
		t.Fatalf("definitive rejection advanced/cooled: hits=%d cooldowns=%v", nextCap.hits, out.Cooldowns)
	}
}

func TestDeliverAllRoutesFail(t *testing.T) {
	a := stub(t, http.StatusInternalServerError, nil)
	b := stub(t, http.StatusServiceUnavailable, nil)
	now := time.Unix(7, 0).UTC()
	existing := map[string]time.Time{"stale|route": now.Add(-time.Hour)}
	candidates := []Candidate{
		{Kind: KindDirect, URL: a.URL, NodeID: "m4", RouteKey: "direct|m4"},
		{Kind: KindQueue, URL: b.URL, NodeID: "hub", Via: "hub", RouteKey: "queue|m4|hub"},
	}
	out, err := Deliver(context.Background(), http.DefaultClient, resolver(map[string]string{"m4": "m4-token", "hub": "hub-token"}), candidates, sampleCommand(), existing, now)
	if !errors.Is(err, ErrAllRoutesFailed) || out.Delivered {
		t.Fatalf("out=%+v err=%v, want exhausted", out, err)
	}
	for _, k := range []string{"direct|m4", "queue|m4|hub"} {
		if ts, ok := out.Cooldowns[k]; !ok || !ts.Equal(now) {
			t.Fatalf("route %q not cooled at now: %v", k, out.Cooldowns)
		}
	}
	if _, ok := existing["direct|m4"]; ok {
		t.Fatal("Deliver mutated the caller's cooldown map")
	}
}

func TestDeliverFailsClosedBeforeNetwork(t *testing.T) {
	cap := &capture{}
	srv := stub(t, http.StatusAccepted, cap)
	candidate := []Candidate{{Kind: KindDirect, URL: srv.URL, NodeID: "m4", RouteKey: "direct|m4"}}

	bad := sampleCommand()
	bad.Path = "/v1/approvals/11111111-1111-4111-8111-111111111111/decision"
	if _, err := Deliver(context.Background(), srv.Client(), resolver(map[string]string{"m4": "token"}), candidate, bad, nil, time.Now()); !errors.Is(err, ErrInvalidCommandPath) {
		t.Fatalf("invalid path err = %v", err)
	}
	if _, err := Deliver(context.Background(), srv.Client(), resolver(nil), candidate, sampleCommand(), nil, time.Now()); !errors.Is(err, ErrMissingCredential) {
		t.Fatalf("missing credential err = %v", err)
	}
	if cap.hits != 0 {
		t.Fatalf("fail-closed calls reached network %d times", cap.hits)
	}
}

func TestValidateOrigin(t *testing.T) {
	for _, good := range []string{"https://node.example", "https://node.example:8443/", "http://127.0.0.1:8080", "http://localhost:8080"} {
		if err := ValidateOrigin(good); err != nil {
			t.Errorf("ValidateOrigin(%q): %v", good, err)
		}
	}
	for _, bad := range []string{"http://10.0.0.63:8080", "https://user:pass@node.example", "https://node.example/mcp", "https://node.example?x=1", "ftp://node.example"} {
		if err := ValidateOrigin(bad); !errors.Is(err, ErrInvalidOrigin) {
			t.Errorf("ValidateOrigin(%q) err = %v", bad, err)
		}
	}
}

func TestValidateCommandPath(t *testing.T) {
	for _, good := range []string{
		"/v1/work-items",
		"/v1/work-items/" + sampleWorkItemID + "/children",
		"/v1/work-items/" + sampleWorkItemID + "/events",
		"/v1/work-items/" + sampleWorkItemID + "/metadata",
		"/v1/work-items/" + sampleWorkItemID + "/transition",
		"/v1/work-items/" + sampleWorkItemID + "/convergence-proposal",
	} {
		if err := ValidateCommandPath(good); err != nil {
			t.Errorf("ValidateCommandPath(%q): %v", good, err)
		}
	}
	for _, bad := range []string{
		"/v1/inbox/messages",
		"/v1/approvals/" + sampleWorkItemID + "/decision",
		"/v1/work-items/not-a-uuid/transition",
		"/v1/work-items/" + sampleWorkItemID + "/transition?force=true",
		"https://node.example/v1/work-items",
	} {
		if err := ValidateCommandPath(bad); !errors.Is(err, ErrInvalidCommandPath) {
			t.Errorf("ValidateCommandPath(%q) err = %v", bad, err)
		}
	}
}
