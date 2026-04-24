package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/wayline/internal/domain"
)

// stubAuth lets the middleware tests run without a database. It satisfies
// the Authenticator interface declared in middleware.go.
type stubAuth struct {
	wantSecret string
	tok        domain.Token
	err        error
	calls      int
}

func (s *stubAuth) Authenticate(_ context.Context, secret string) (domain.Token, error) {
	s.calls++
	if s.err != nil {
		return domain.Token{}, s.err
	}
	if secret != s.wantSecret {
		return domain.Token{}, ErrInvalidToken
	}
	return s.tok, nil
}

func newDownstream(captured *domain.Token) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok, ok := TokenFromContext(r.Context()); ok {
			*captured = tok
		}
		w.WriteHeader(http.StatusOK)
	})
}

func decodeAuthError(t *testing.T, body []byte) (code, message string) {
	t.Helper()
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode error envelope: %v\nbody: %s", err, body)
	}
	return env.Error.Code, env.Error.Message
}

func TestMiddleware_MissingHeader_401(t *testing.T) {
	mw := NewMiddleware(&stubAuth{})
	var captured domain.Token
	rec := httptest.NewRecorder()
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/whatever", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	code, _ := decodeAuthError(t, rec.Body.Bytes())
	if code != "missing_bearer_token" {
		t.Errorf("expected code missing_bearer_token, got %q", code)
	}
	if captured.ID != (uuid.UUID{}) {
		t.Error("downstream must not run when bearer is missing")
	}
}

func TestMiddleware_NonBearerScheme_401(t *testing.T) {
	mw := NewMiddleware(&stubAuth{})
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Basic abcdef")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	code, _ := decodeAuthError(t, rec.Body.Bytes())
	if code != "missing_bearer_token" {
		t.Errorf("expected missing_bearer_token, got %q", code)
	}
}

func TestMiddleware_InvalidToken_401(t *testing.T) {
	mw := NewMiddleware(&stubAuth{err: ErrInvalidToken})
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Bearer wln_definitely-not-real")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	code, _ := decodeAuthError(t, rec.Body.Bytes())
	if code != "invalid_bearer_token" {
		t.Errorf("expected invalid_bearer_token, got %q", code)
	}
}

func TestMiddleware_RevokedToken_401_DistinctCode(t *testing.T) {
	mw := NewMiddleware(&stubAuth{err: ErrTokenRevoked})
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Bearer wln_revoked-token")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	code, _ := decodeAuthError(t, rec.Body.Bytes())
	// The middleware deliberately distinguishes "revoked" from "invalid"
	// so the operator-facing logs and clients can react differently
	// (re-auth vs. re-mint).
	if code != "token_revoked" {
		t.Errorf("expected token_revoked, got %q", code)
	}
}

func TestMiddleware_HappyPath_AttachesToken(t *testing.T) {
	want := domain.Token{ID: uuid.New(), Name: "iphone", Source: domain.SourceHuman}
	stub := &stubAuth{wantSecret: "wln_good", tok: want}
	mw := NewMiddleware(stub)
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Bearer wln_good")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if stub.calls != 1 {
		t.Errorf("expected exactly one Authenticate call, got %d", stub.calls)
	}
	if captured.ID != want.ID || captured.Name != want.Name || captured.Source != want.Source {
		t.Errorf("downstream saw wrong token: %+v vs %+v", captured, want)
	}
}

func TestMiddleware_TrimsBearerWhitespace(t *testing.T) {
	want := domain.Token{ID: uuid.New(), Name: "iphone", Source: domain.SourceHuman}
	stub := &stubAuth{wantSecret: "wln_good", tok: want}
	mw := NewMiddleware(stub)
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Bearer    wln_good   ")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected whitespace-tolerant happy path, got %d (%s)", rec.Code, rec.Body.String())
	}
	if captured.ID != want.ID {
		t.Errorf("downstream did not run with the right token")
	}
}

// Sanity check: the middleware uses our errors.Is plumbing; an error that
// is neither ErrInvalidToken nor ErrTokenRevoken still produces a 401
// rather than a 500. Today the generic case happens to share the
// invalid_bearer_token code; pin that so a future code path doesn't
// quietly start leaking 5xx for unfamiliar errors.
func TestMiddleware_GenericAuthError_401(t *testing.T) {
	mw := NewMiddleware(&stubAuth{err: errors.New("transport down")})
	var captured domain.Token
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.Header.Set("Authorization", "Bearer wln_anything")
	mw.Wrap(newDownstream(&captured)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
	code, _ := decodeAuthError(t, rec.Body.Bytes())
	if code != "invalid_bearer_token" {
		t.Errorf("expected invalid_bearer_token, got %q", code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected json content type, got %q", rec.Header().Get("Content-Type"))
	}
}
