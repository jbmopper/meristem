package auth

import (
	"context"

	"github.com/jbmopper/meristem/internal/domain"
)

type contextKey struct{}

// WithToken annotates ctx with the authenticated token projection.
func WithToken(ctx context.Context, token domain.Token) context.Context {
	return context.WithValue(ctx, contextKey{}, token)
}

// TokenFromContext returns the authenticated token projection, if present.
func TokenFromContext(ctx context.Context) (domain.Token, bool) {
	tok, ok := ctx.Value(contextKey{}).(domain.Token)
	return tok, ok
}
