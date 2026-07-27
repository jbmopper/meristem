package idempotency

import (
	"context"
	"encoding/base64"

	"github.com/google/uuid"
)

type contextKey struct{}
type recordedResponseKey struct{}
type refusalDispositionKey struct{}

type recordedResponseOverride struct {
	body []byte
}

// refusalDisposition is the explicit cache decision for a non-success
// response. The middleware cannot derive it from the HTTP status alone:
// some 4xx refusals are pure (validation, not-found — nothing committed, so
// the key must stay usable with a corrected body) while others are stateful
// conclusions (xylem/signal budget refusals commit authoritative events
// before returning 409) that MUST be pinned so a changed body conflicts
// instead of re-executing. The conservative default for an unmarked 4xx is
// therefore to record it; only a handler path that provably committed
// nothing may mark its refusal unconsumed.
type refusalDisposition struct {
	unconsumed bool
}

// Request is the canonical idempotency identity for one authenticated POST.
// Services use it to derive stable projection subject ids, so a retry that
// reaches the handler before the response cache is written still converges on
// the same durable events.
type Request struct {
	TokenID     uuid.UUID
	Scope       string
	Key         string
	RequestHash []byte
}

func withRequest(ctx context.Context, req Request) context.Context {
	return context.WithValue(ctx, contextKey{}, req)
}

func withRecordedResponseOverride(ctx context.Context, override *recordedResponseOverride) context.Context {
	return context.WithValue(ctx, recordedResponseKey{}, override)
}

// WithRequest is the public seam over the package-internal context injection
// used by transports that enforce an idempotency contract before calling
// write services. HTTP POSTs use the middleware; MCP mutation tools use their
// adapter boundary because they are not REST requests.
func WithRequest(ctx context.Context, req Request) context.Context {
	return withRequest(ctx, req)
}

// SubjectID derives a stable UUID for a domain object created by this POST.
// The label lets one request create multiple distinct objects, such as an
// inbox message and the work_item created from it.
func SubjectID(ctx context.Context, label string) (uuid.UUID, bool) {
	req, ok := ctx.Value(contextKey{}).(Request)
	if !ok || req.TokenID == uuid.Nil || req.Scope == "" || req.Key == "" || label == "" {
		return uuid.Nil, false
	}
	seed := req.TokenID.String() + "|" + req.Scope + "|" + req.Key + "|" + base64.StdEncoding.EncodeToString(req.RequestHash) + "|" + label
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)), true
}

// EventDiscriminator derives the event-identity discriminator for the
// mutation currently executing under the idempotency contract. It is stable
// across retries of one logical action (same token, scope, and key) and
// different across actions, which is exactly the property events.Spec needs
// to keep repeated-but-distinct actions from collapsing into one event row.
// Outside an idempotent mutation it returns ("", false) and callers append
// with payload-only identity, preserving pre-discriminator behavior.
func EventDiscriminator(ctx context.Context) (string, bool) {
	req, ok := ctx.Value(contextKey{}).(Request)
	if !ok || req.TokenID == uuid.Nil || req.Scope == "" || req.Key == "" {
		return "", false
	}
	return req.TokenID.String() + "|" + req.Scope + "|" + req.Key, true
}

// SetRecordedResponse replaces the body persisted in idempotency.recorded and
// served on replay. The current request still receives the handler's original
// response. Secret-returning handlers use this to avoid durable secret storage.
func SetRecordedResponse(ctx context.Context, body []byte) {
	override, ok := ctx.Value(recordedResponseKey{}).(*recordedResponseOverride)
	if !ok || override == nil {
		return
	}
	override.body = append(override.body[:0], body...)
}

func recordedResponse(ctx context.Context) ([]byte, bool) {
	override, ok := ctx.Value(recordedResponseKey{}).(*recordedResponseOverride)
	if !ok || override == nil || override.body == nil {
		return nil, false
	}
	return override.body, true
}

func withRefusalDisposition(ctx context.Context, disposition *refusalDisposition) context.Context {
	return context.WithValue(ctx, refusalDispositionKey{}, disposition)
}

// MarkRefusalUnconsumed declares that the 4xx response the current handler
// is about to return is a pure, side-effect-free refusal: nothing was
// committed, so the idempotency key must NOT be consumed and a retry with a
// corrected body may execute. Handlers must only call this on paths where no
// event was appended — a stateful refusal (budget exhaustion that records
// escalation events, for example) must stay unmarked so it is recorded and
// replayed like any committed conclusion. No-op outside an idempotent
// mutation.
func MarkRefusalUnconsumed(ctx context.Context) {
	disposition, ok := ctx.Value(refusalDispositionKey{}).(*refusalDisposition)
	if !ok || disposition == nil {
		return
	}
	disposition.unconsumed = true
}

func refusalUnconsumed(ctx context.Context) bool {
	disposition, ok := ctx.Value(refusalDispositionKey{}).(*refusalDisposition)
	return ok && disposition != nil && disposition.unconsumed
}
