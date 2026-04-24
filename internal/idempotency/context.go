package idempotency

import (
	"context"
	"encoding/base64"

	"github.com/google/uuid"
)

type contextKey struct{}

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

// WithRequest is the public seam over the package-internal context
// injection used by the middleware. It exists so other packages can
// exercise the SubjectID derivation in tests without going through HTTP
// or a real database. Production code should not call this; the
// middleware is the only caller in the request path.
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
