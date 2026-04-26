package meristem_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/pkg/meristem"
)

func TestNewValidatesConfig(t *testing.T) {
	if _, err := meristem.New(meristem.Config{Token: "t"}); !errors.Is(err, meristem.ErrBaseURLRequired) {
		t.Fatalf("missing BaseURL: got %v want ErrBaseURLRequired", err)
	}
	if _, err := meristem.New(meristem.Config{BaseURL: "https://x"}); !errors.Is(err, meristem.ErrTokenRequired) {
		t.Fatalf("missing Token: got %v want ErrTokenRequired", err)
	}
	if _, err := meristem.New(meristem.Config{BaseURL: "https://x", Token: "t"}); err != nil {
		t.Fatalf("valid config returned err: %v", err)
	}
}

// fakeServer captures inbound requests and responds with whatever the
// caller queues up. Keeping it small avoids the temptation to build a
// reimplementation of /v1/signals here; the SDK's job is HTTP shape,
// not server semantics.
type fakeServer struct {
	srv         *httptest.Server
	gotRequests []*recordedRequest
	respond     func(w http.ResponseWriter, r *http.Request)
}

type recordedRequest struct {
	method  string
	path    string
	headers http.Header
	body    []byte
}

func newFakeServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request)) *fakeServer {
	t.Helper()
	fs := &fakeServer{respond: respond}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fs.gotRequests = append(fs.gotRequests, &recordedRequest{
			method:  r.Method,
			path:    r.URL.Path,
			headers: r.Header.Clone(),
			body:    body,
		})
		fs.respond(w, r)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

func TestPostSignalHappyPath(t *testing.T) {
	signalID := uuid.New()
	workItemID := uuid.New()
	signalEventID := uuid.New()
	workItemEventID := uuid.New()

	fs := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"idempotency": map[string]any{"key": "from-server"},
			"dedupe":      map[string]any{"key": "k", "created_work_item": true},
			"resource":    map[string]any{"kind": "signal", "id": signalID},
			"work_item":   map[string]any{"id": workItemID},
			"events": map[string]any{
				"signal_received":   signalEventID,
				"work_item_created": workItemEventID,
			},
			"fingerprint": "sha256:abc",
		})
	})

	c, err := meristem.New(meristem.Config{
		BaseURL:    fs.srv.URL + "/", // trailing slash should be trimmed
		Token:      "mrs_test",
		HTTPClient: fs.srv.Client(),
		UserAgent:  "meristem-test/1.0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	resp, err := c.PostSignal(context.Background(), meristem.SignalRequest{
		Kind:      "human_request",
		DedupeKey: "k",
		Source: meristem.SignalSource{
			Kind:       "system_event",
			Identifier: "id",
		},
		WorkSpec: json.RawMessage(`{"hello":"world"}`),
	}, meristem.WithIdempotencyKey("client-pinned"))
	if err != nil {
		t.Fatalf("PostSignal: %v", err)
	}

	// Response decoding.
	if !resp.Dedupe.CreatedWorkItem {
		t.Errorf("CreatedWorkItem = false; want true")
	}
	if resp.Resource.ID != signalID {
		t.Errorf("Resource.ID = %s; want %s", resp.Resource.ID, signalID)
	}
	if resp.WorkItem.ID != workItemID {
		t.Errorf("WorkItem.ID = %s; want %s", resp.WorkItem.ID, workItemID)
	}
	if resp.Events.SignalReceived != signalEventID {
		t.Errorf("Events.SignalReceived = %s; want %s", resp.Events.SignalReceived, signalEventID)
	}
	if resp.Events.WorkItemCreated == nil || *resp.Events.WorkItemCreated != workItemEventID {
		t.Errorf("Events.WorkItemCreated = %v; want %s", resp.Events.WorkItemCreated, workItemEventID)
	}
	if resp.Fingerprint != "sha256:abc" {
		t.Errorf("Fingerprint = %q; want sha256:abc", resp.Fingerprint)
	}
	if resp.Replayed {
		t.Error("Replayed = true with no Idempotency-Replayed header")
	}

	// Request shape.
	if len(fs.gotRequests) != 1 {
		t.Fatalf("captured %d requests; want 1", len(fs.gotRequests))
	}
	got := fs.gotRequests[0]
	if got.method != http.MethodPost {
		t.Errorf("method = %s; want POST", got.method)
	}
	if got.path != "/v1/signals" {
		t.Errorf("path = %q; want /v1/signals", got.path)
	}
	if got.headers.Get("Authorization") != "Bearer mrs_test" {
		t.Errorf("Authorization = %q; want Bearer mrs_test", got.headers.Get("Authorization"))
	}
	if got.headers.Get("Idempotency-Key") != "client-pinned" {
		t.Errorf("Idempotency-Key = %q; want client-pinned", got.headers.Get("Idempotency-Key"))
	}
	if got.headers.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", got.headers.Get("Content-Type"))
	}
	if got.headers.Get("User-Agent") != "meristem-test/1.0" {
		t.Errorf("User-Agent = %q; want meristem-test/1.0", got.headers.Get("User-Agent"))
	}

	// Body round-trip: the WorkSpec rawmessage made it through verbatim.
	var sentBody struct {
		WorkSpec json.RawMessage `json:"work_spec"`
	}
	if err := json.Unmarshal(got.body, &sentBody); err != nil {
		t.Fatalf("decode sent body: %v", err)
	}
	if string(sentBody.WorkSpec) != `{"hello":"world"}` {
		t.Errorf("WorkSpec round-trip = %s; want {\"hello\":\"world\"}", sentBody.WorkSpec)
	}
}

func TestPostSignalAutoGeneratesIdempotencyKey(t *testing.T) {
	fs := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	c, err := meristem.New(meristem.Config{BaseURL: fs.srv.URL, Token: "t", HTTPClient: fs.srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := c.PostSignal(context.Background(), meristem.SignalRequest{}); err != nil {
		t.Fatalf("PostSignal: %v", err)
	}
	if _, err := c.PostSignal(context.Background(), meristem.SignalRequest{}); err != nil {
		t.Fatalf("PostSignal #2: %v", err)
	}

	if len(fs.gotRequests) != 2 {
		t.Fatalf("captured %d requests; want 2", len(fs.gotRequests))
	}
	k1 := fs.gotRequests[0].headers.Get("Idempotency-Key")
	k2 := fs.gotRequests[1].headers.Get("Idempotency-Key")
	if k1 == "" || k2 == "" {
		t.Fatalf("Idempotency-Key not auto-generated: k1=%q k2=%q", k1, k2)
	}
	if k1 == k2 {
		t.Errorf("auto-generated keys collided: %q", k1)
	}
	if _, err := uuid.Parse(k1); err != nil {
		t.Errorf("auto-generated key %q is not a uuid: %v", k1, err)
	}
}

func TestPostSignalSurfacesReplayHeader(t *testing.T) {
	fs := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Idempotency-Replayed", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	c, err := meristem.New(meristem.Config{BaseURL: fs.srv.URL, Token: "t", HTTPClient: fs.srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	resp, err := c.PostSignal(context.Background(), meristem.SignalRequest{})
	if err != nil {
		t.Fatalf("PostSignal: %v", err)
	}
	if !resp.Replayed {
		t.Error("Replayed = false; want true when Idempotency-Replayed header is set")
	}
}

func TestPostSignalDecodesAPIError(t *testing.T) {
	fs := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"work_spec_invalid","message":"missing acceptance_criteria"}}`))
	})

	c, err := meristem.New(meristem.Config{BaseURL: fs.srv.URL, Token: "t", HTTPClient: fs.srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.PostSignal(context.Background(), meristem.SignalRequest{})
	if err == nil {
		t.Fatal("PostSignal returned nil err on 400")
	}
	var apiErr *meristem.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err is not *APIError: %v (%T)", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d; want 400", apiErr.StatusCode)
	}
	if apiErr.Code != "work_spec_invalid" {
		t.Errorf("Code = %q; want work_spec_invalid", apiErr.Code)
	}
	if !strings.Contains(apiErr.Message, "acceptance_criteria") {
		t.Errorf("Message = %q; want it to mention acceptance_criteria", apiErr.Message)
	}

	// errors.Is matches on Code.
	if !errors.Is(err, &meristem.APIError{Code: "work_spec_invalid"}) {
		t.Error("errors.Is on Code did not match")
	}
	if errors.Is(err, &meristem.APIError{Code: "other"}) {
		t.Error("errors.Is matched the wrong code")
	}
}

func TestPostSignalAPIErrorWithoutEnvelope(t *testing.T) {
	fs := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream timeout\n"))
	})

	c, err := meristem.New(meristem.Config{BaseURL: fs.srv.URL, Token: "t", HTTPClient: fs.srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.PostSignal(context.Background(), meristem.SignalRequest{})
	if err == nil {
		t.Fatal("PostSignal returned nil err on 502")
	}
	var apiErr *meristem.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err is not *APIError: %v", err)
	}
	if apiErr.StatusCode != http.StatusBadGateway {
		t.Errorf("StatusCode = %d; want 502", apiErr.StatusCode)
	}
	if apiErr.Code != "unknown" {
		t.Errorf("Code = %q; want unknown", apiErr.Code)
	}
	if !strings.Contains(apiErr.Message, "upstream timeout") {
		t.Errorf("Message = %q; want it to mention the body", apiErr.Message)
	}
}

func TestPostSignalRespectsContextCancellation(t *testing.T) {
	// Server hangs forever; the canceled context should cut us loose.
	fs := newFakeServer(t, func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})

	c, err := meristem.New(meristem.Config{BaseURL: fs.srv.URL, Token: "t", HTTPClient: fs.srv.Client()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.PostSignal(ctx, meristem.SignalRequest{})
	if err == nil {
		t.Fatal("PostSignal succeeded against canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v; want context.Canceled in chain", err)
	}
}
