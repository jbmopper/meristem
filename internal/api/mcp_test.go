package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/safety"
)

type mcpRouteAuth struct {
	wantSecret string
	tok        domain.Token
	err        error
	calls      int
}

func (a *mcpRouteAuth) Authenticate(_ context.Context, secret string) (domain.Token, error) {
	a.calls++
	if a.err != nil {
		return domain.Token{}, a.err
	}
	if secret != a.wantSecret {
		return domain.Token{}, auth.ErrInvalidToken
	}
	return a.tok, nil
}

func newMCPRouteTestServer(authenticator auth.Authenticator) *Server {
	s := &Server{
		authenticator: authenticator,
		mux:           http.NewServeMux(),
		policy:        safety.DefaultPolicy(),
	}
	s.routes()
	return s
}

func TestHandleMCPPostDispatchesAuthenticatedActor(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := rec.Header().Get(mcp.HeaderProtocolVersion); got != "2025-06-18" {
		t.Fatalf("protocol header = %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"serverInfo"`) {
		t.Fatalf("initialize response missing serverInfo: %s", rec.Body.String())
	}
}

func TestHandleMCPPostRejectsMissingStreamableAccept(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Accept", "application/json")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want 406 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid_accept") {
		t.Fatalf("expected invalid_accept error, got %s", rec.Body.String())
	}
}

func TestHandleMCPPostExposesReadOnlyToolSurface(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceHuman}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "work_items.create") || strings.Contains(body, "work_items.transition") || strings.Contains(body, "convergence.propose_checks") {
		t.Fatalf("HTTP /mcp leaked write tools before idempotency contract: %s", body)
	}
	if !strings.Contains(body, "feed.read") || !strings.Contains(body, "work_items.get") {
		t.Fatalf("HTTP /mcp omitted expected read tools: %s", body)
	}
}

func TestHandleMCPGetReturns405UntilSSEExists(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "mcp_sse_unavailable") {
		t.Fatalf("expected mcp_sse_unavailable error, got %s", rec.Body.String())
	}
}

func TestOAuthProtectedResourceMetadataUsesConfiguredPublicBaseURL(t *testing.T) {
	t.Setenv(EnvPublicBaseURL, "https://mcp.example.test/")
	s := New(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-protected-resource/mcp", nil)
	rec := httptest.NewRecorder()

	s.handleOAuthProtectedResourceMetadata(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		ScopesSupported      []string `json:"scopes_supported"`
		BearerMethods        []string `json:"bearer_methods_supported"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode metadata: %v\nbody=%s", err, rec.Body.String())
	}
	if body.Resource != "https://mcp.example.test/mcp" {
		t.Fatalf("resource = %q", body.Resource)
	}
	if len(body.AuthorizationServers) != 1 || body.AuthorizationServers[0] != "https://mcp.example.test" {
		t.Fatalf("authorization_servers = %#v", body.AuthorizationServers)
	}
	if len(body.ScopesSupported) != 1 || body.ScopesSupported[0] != "mcp:read" {
		t.Fatalf("scopes_supported = %#v", body.ScopesSupported)
	}
	if len(body.BearerMethods) != 1 || body.BearerMethods[0] != "header" {
		t.Fatalf("bearer_methods_supported = %#v", body.BearerMethods)
	}
}

func TestOAuthAuthorizationServerMetadataUsesForwardedHost(t *testing.T) {
	s := New(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "http://internal/.well-known/oauth-authorization-server", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "mcp.example.test")
	rec := httptest.NewRecorder()

	s.handleOAuthAuthorizationServerMetadata(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Issuer              string   `json:"issuer"`
		Authorization       string   `json:"authorization_endpoint"`
		Token               string   `json:"token_endpoint"`
		GrantTypesSupported []string `json:"grant_types_supported"`
		PKCE                []string `json:"code_challenge_methods_supported"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode metadata: %v\nbody=%s", err, rec.Body.String())
	}
	if body.Issuer != "https://mcp.example.test" {
		t.Fatalf("issuer = %q", body.Issuer)
	}
	if body.Authorization != "https://mcp.example.test/oauth/authorize" {
		t.Fatalf("authorization_endpoint = %q", body.Authorization)
	}
	if body.Token != "https://mcp.example.test/oauth/token" {
		t.Fatalf("token_endpoint = %q", body.Token)
	}
	if len(body.GrantTypesSupported) != 2 || body.GrantTypesSupported[0] != "authorization_code" || body.GrantTypesSupported[1] != "refresh_token" {
		t.Fatalf("grant_types_supported = %#v", body.GrantTypesSupported)
	}
	if len(body.PKCE) != 1 || body.PKCE[0] != "S256" {
		t.Fatalf("code_challenge_methods_supported = %#v", body.PKCE)
	}
}

func TestMCPRouteMissingBearerAdvertisesProtectedResourceMetadata(t *testing.T) {
	authenticator := &mcpRouteAuth{}
	s := newMCPRouteTestServer(authenticator)
	s.publicBaseURL = normalizePublicBaseURL("https://mcp.example.test")

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if authenticator.calls != 0 {
		t.Fatalf("authenticator calls = %d, want 0", authenticator.calls)
	}
	challenge := rec.Header().Get("WWW-Authenticate")
	if !strings.Contains(challenge, `resource_metadata="https://mcp.example.test/.well-known/oauth-protected-resource/mcp"`) {
		t.Fatalf("WWW-Authenticate = %q", challenge)
	}
	if !strings.Contains(rec.Body.String(), "missing_bearer_token") {
		t.Fatalf("expected missing_bearer_token body, got %s", rec.Body.String())
	}
}

func TestMCPRouteInvalidBearerAdvertisesOAuthInvalidToken(t *testing.T) {
	authenticator := &mcpRouteAuth{err: auth.ErrInvalidToken}
	s := newMCPRouteTestServer(authenticator)
	s.publicBaseURL = normalizePublicBaseURL("https://mcp.example.test")

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer mrs_bad")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if authenticator.calls != 1 {
		t.Fatalf("authenticator calls = %d, want 1", authenticator.calls)
	}
	challenge := rec.Header().Get("WWW-Authenticate")
	if !strings.Contains(challenge, `error="invalid_token"`) {
		t.Fatalf("WWW-Authenticate missing invalid_token: %q", challenge)
	}
	if !strings.Contains(challenge, `resource_metadata="https://mcp.example.test/.well-known/oauth-protected-resource/mcp"`) {
		t.Fatalf("WWW-Authenticate = %q", challenge)
	}
}

func TestMCPRouteStaticBearerStillDispatches(t *testing.T) {
	authenticator := &mcpRouteAuth{
		wantSecret: "mrs_good",
		tok:        domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
	}
	s := newMCPRouteTestServer(authenticator)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer mrs_good")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if authenticator.calls != 1 {
		t.Fatalf("authenticator calls = %d, want 1", authenticator.calls)
	}
	if !strings.Contains(rec.Body.String(), `"serverInfo"`) {
		t.Fatalf("initialize response missing serverInfo: %s", rec.Body.String())
	}
}
