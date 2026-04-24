package wayline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// SignalRequest is the JSON body posted to POST /v1/signals. See
// docs/signals.md for the wire-level contract; types here are kept in
// step with the server's request struct in internal/api/signals.go.
type SignalRequest struct {
	// Kind classifies the signal at the transport layer. The server
	// currently treats this as opaque; convention is short snake_case
	// strings ("repairable_failure", "human_request", "scheduled_job").
	Kind string `json:"kind"`

	// DedupeKey is the optional semantic identity for the work this
	// signal asks for. When two signals carry the same DedupeKey and
	// the prior matching work_item is still live (not done/failed/
	// canceled), the new signal links to that existing work_item
	// instead of creating a new one. See docs/signals.md "Dedupe
	// semantics" for the full rules.
	DedupeKey string `json:"dedupe_key,omitempty"`

	// Source attributes the signal to a system, agent, or human.
	// Required by the server.
	Source SignalSource `json:"source"`

	// WorkSpec is the canonical work_spec payload. Must validate
	// against docs/schemas/wayline.work_spec.v1.json. The client
	// does not validate it locally; the server rejects malformed
	// bodies with a 400 and a structured error.
	WorkSpec json.RawMessage `json:"work_spec"`
}

// SignalSource attributes a signal to its origin.
type SignalSource struct {
	Kind        string `json:"kind"`
	Identifier  string `json:"identifier"`
	ExternalRef string `json:"external_ref,omitempty"`
}

// SignalResponse is the decoded response body of POST /v1/signals,
// plus the Replayed bit lifted from the Idempotency-Replayed HTTP
// header. The body itself does not carry a "replayed" field by
// design (see docs/signals.md "Endpoint → response notes" for the
// rationale).
type SignalResponse struct {
	// Replayed is true when the response was served from the
	// idempotency cache. Read from the Idempotency-Replayed HTTP
	// header; not present in the JSON body.
	Replayed bool `json:"-"`

	Idempotency SignalIdempotency `json:"idempotency"`
	Dedupe      SignalDedupe      `json:"dedupe"`
	Resource    SignalResource    `json:"resource"`
	WorkItem    SignalWorkItem    `json:"work_item"`
	Events      SignalEvents      `json:"events"`

	// Fingerprint is the content identity of the work_spec (sha256
	// over the canonical encoding). Two signals with byte-identical
	// work_specs share a fingerprint.
	Fingerprint string `json:"fingerprint"`
}

// SignalIdempotency echoes back the Idempotency-Key the client sent
// (or the one auto-generated if WithIdempotencyKey was not used).
type SignalIdempotency struct {
	Key string `json:"key"`
}

// SignalDedupe reports the deduplication outcome.
type SignalDedupe struct {
	// Key is the dedupe_key the server used (echoed from the request,
	// possibly normalized).
	Key string `json:"key,omitempty"`

	// CreatedWorkItem is true when this signal created a fresh
	// work_item, false when it linked to an existing live one.
	CreatedWorkItem bool `json:"created_work_item"`
}

// SignalResource identifies the signal row that was recorded.
type SignalResource struct {
	Kind string    `json:"kind"`
	ID   uuid.UUID `json:"id"`
}

// SignalWorkItem is the work_item this signal is attached to (either
// freshly created or pre-existing).
type SignalWorkItem struct {
	ID uuid.UUID `json:"id"`
}

// SignalEvents lists the event ids appended for this signal.
// WorkItemCreated is nil when the signal linked to an existing
// work_item.
type SignalEvents struct {
	SignalReceived  uuid.UUID  `json:"signal_received"`
	WorkItemCreated *uuid.UUID `json:"work_item_created,omitempty"`
}

// Option customizes a single PostSignal call. Use WithIdempotencyKey
// to make the same logical call retry-safe across process boundaries
// (the server caches the response for a generous window keyed by the
// Idempotency-Key header).
type Option func(*postSignalOptions)

type postSignalOptions struct {
	idempotencyKey string
}

// WithIdempotencyKey pins the Idempotency-Key header on a single
// PostSignal call. If not provided, a random uuid v4 is generated.
//
// When a caller wants two PostSignal invocations to be treated as
// "the same call" (e.g. on retry from disk-backed queue), they MUST
// supply the same key on both invocations.
func WithIdempotencyKey(key string) Option {
	return func(o *postSignalOptions) { o.idempotencyKey = key }
}

// PostSignal posts a work_spec signal and returns the decoded
// response. On a 4xx/5xx with a structured error envelope, the
// returned error is *APIError; on transport failures it is whatever
// the underlying *http.Client returned, wrapped with context.
//
// The returned *SignalResponse's Replayed field is sourced from the
// Idempotency-Replayed header.
func (c *Client) PostSignal(ctx context.Context, req SignalRequest, opts ...Option) (*SignalResponse, error) {
	o := postSignalOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.idempotencyKey == "" {
		o.idempotencyKey = uuid.NewString()
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("wayline: marshal signal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/signals", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("wayline: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Idempotency-Key", o.idempotencyKey)
	if c.userAgent != "" {
		httpReq.Header.Set("User-Agent", c.userAgent)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("wayline: post signal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, decodeAPIError(resp)
	}

	var sr SignalResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("wayline: decode signal response: %w", err)
	}
	sr.Replayed = strings.EqualFold(resp.Header.Get("Idempotency-Replayed"), "true")
	return &sr, nil
}

// decodeAPIError reads the error envelope `{"error": {"code","message"}}`
// from a 4xx/5xx response and returns it as *APIError. Falls back to
// a generic "unknown" code when the body is missing or malformed.
func decodeAPIError(resp *http.Response) error {
	apiErr := &APIError{StatusCode: resp.StatusCode}

	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil || len(raw) == 0 {
		apiErr.Code = "unknown"
		apiErr.Message = fmt.Sprintf("server returned status %d with no body", resp.StatusCode)
		return apiErr
	}

	var env struct {
		Error APIError `json:"error"`
	}
	if jsonErr := json.Unmarshal(raw, &env); jsonErr != nil || env.Error.Code == "" {
		apiErr.Code = "unknown"
		apiErr.Message = fmt.Sprintf("server returned status %d with non-envelope body: %s", resp.StatusCode, truncate(string(raw), 256))
		return apiErr
	}

	apiErr.Code = env.Error.Code
	apiErr.Message = env.Error.Message
	return apiErr
}

// truncate caps long strings so error messages stay loggable.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...[truncated]"
}
