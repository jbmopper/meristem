package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

// Authenticator is the narrow seam Middleware needs from a token service.
// Service satisfies it; tests provide a stub that does not require a
// running database.
type Authenticator interface {
	Authenticate(ctx context.Context, secret string) (domain.Token, error)
}

// Middleware authenticates bearer tokens for protected routes and stores the
// token projection in the request context.
type Middleware struct {
	service Authenticator
}

func NewMiddleware(service Authenticator) *Middleware {
	return &Middleware{service: service}
}

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" || !strings.HasPrefix(header, "Bearer ") {
			writeAuthError(w, http.StatusUnauthorized, "missing_bearer_token", "missing bearer token")
			return
		}
		secret := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
		tok, err := m.service.Authenticate(r.Context(), secret)
		if err != nil {
			code := "invalid_bearer_token"
			message := "invalid bearer token"
			if errors.Is(err, ErrTokenRevoked) {
				code = "token_revoked"
				message = "token revoked"
			}
			writeAuthError(w, http.StatusUnauthorized, code, message)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithToken(r.Context(), tok)))
	})
}

func writeAuthError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%q,"message":%q}}`+"\n", code, message)
}
