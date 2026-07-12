package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/safety"
)

func newOAuthRouteTestServer() *Server {
	s := &Server{
		mux:    http.NewServeMux(),
		policy: safety.DefaultPolicy(),
		// A service with a nil pool is enough to exercise the request-shape
		// validation that runs before any DB access; the success path is
		// covered by internal/oauth's integration test.
		oauthClients: oauth.NewRegistrationService(nil, nil),
	}
	s.routes()
	return s
}

func TestAuthorizationServerMetadataAdvertisesRegistrationEndpoint(t *testing.T) {
	s := &Server{mux: http.NewServeMux(), policy: safety.DefaultPolicy()}
	s.routes()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	req.Host = "mcp.example.com"
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, _ := body["registration_endpoint"].(string)
	if !strings.HasSuffix(got, "/oauth/register") {
		t.Fatalf("registration_endpoint = %q, want .../oauth/register", got)
	}
}

func TestOAuthRegistrationRejectsBadRequests(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `{not json`},
		{"unsupported grant", `{"redirect_uris":["https://a.example/cb"],"grant_types":["client_credentials"]}`},
		{"unsupported response type", `{"redirect_uris":["https://a.example/cb"],"response_types":["token"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newOAuthRouteTestServer()
			req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			s.mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body["error"] == nil {
				t.Fatalf("expected an oauth error code, got %s", rec.Body.String())
			}
		})
	}
}

func TestGrantTypesSupportedAcceptsAuthorizationAndRefresh(t *testing.T) {
	if !grantTypesSupported([]string{oauth.GrantAuthorizationCode, oauth.GrantRefreshToken}) {
		t.Fatal("authorization_code + refresh_token should be accepted")
	}
	if grantTypesSupported([]string{oauth.GrantRefreshToken}) {
		t.Fatal("refresh_token without authorization_code should be rejected")
	}
}
