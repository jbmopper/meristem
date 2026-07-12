package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/mcp"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestResolveOAuthRuntimeConfigFailsClosed(t *testing.T) {
	actorID := uuid.New()
	cases := []struct {
		name  string
		base  string
		actor string
		mode  oauthRuntimeMode
	}{
		{name: "both absent", mode: oauthRuntimeDisabled},
		{name: "base only", base: "https://mcp.example.test", mode: oauthRuntimeInvalid},
		{name: "actor only", actor: actorID.String(), mode: oauthRuntimeInvalid},
		{name: "http base", base: "http://mcp.example.test", actor: actorID.String(), mode: oauthRuntimeInvalid},
		{name: "forwarded-looking base without scheme", base: "mcp.example.test", actor: actorID.String(), mode: oauthRuntimeInvalid},
		{name: "base with credentials", base: "https://owner:secret@mcp.example.test", actor: actorID.String(), mode: oauthRuntimeInvalid},
		{name: "base with query", base: "https://mcp.example.test?issuer=other", actor: actorID.String(), mode: oauthRuntimeInvalid},
		{name: "bad actor", base: "https://mcp.example.test", actor: "not-a-uuid", mode: oauthRuntimeInvalid},
		{name: "nil actor", base: "https://mcp.example.test", actor: uuid.Nil.String(), mode: oauthRuntimeInvalid},
		{name: "enabled", base: "https://mcp.example.test/", actor: actorID.String(), mode: oauthRuntimeEnabled},
		{name: "enabled uppercase uuid", base: "https://mcp.example.test", actor: strings.ToUpper(actorID.String()), mode: oauthRuntimeEnabled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveOAuthRuntimeConfig(tc.base, tc.actor)
			if got.mode != tc.mode {
				t.Fatalf("mode = %v, want %v", got.mode, tc.mode)
			}
			if tc.mode == oauthRuntimeEnabled && got.publicBaseURL != "https://mcp.example.test" {
				t.Fatalf("public base = %q", got.publicBaseURL)
			}
		})
	}
}

func TestOAuthRuntimeActorValidation(t *testing.T) {
	id := uuid.New()
	now := time.Now()
	tests := []struct {
		name string
		tok  domain.Token
		err  error
		want error
	}{
		{name: "active system", tok: domain.Token{ID: id, Source: domain.SourceSystem}},
		{name: "wrong id", tok: domain.Token{ID: uuid.New(), Source: domain.SourceSystem}, want: errOAuthSystemActor},
		{name: "root", tok: domain.Token{ID: id, Source: domain.SourceHuman, IsRoot: true}, want: errOAuthSystemActor},
		{name: "agent", tok: domain.Token{ID: id, Source: domain.SourceAgent}, want: errOAuthSystemActor},
		{name: "revoked", tok: domain.Token{ID: id, Source: domain.SourceSystem, RevokedAt: &now}, want: errOAuthSystemActor},
		{name: "lookup failure", err: errors.New("lookup failed"), want: errOAuthSystemActor},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{
				oauthRuntime: oauthRuntimeConfig{mode: oauthRuntimeEnabled, systemActorID: id},
				oauthActorLookup: func(context.Context, uuid.UUID) (domain.Token, error) {
					return tc.tok, tc.err
				},
			}
			err := s.checkOAuthRuntime(context.Background())
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDisabledOAuthRoutesCloseButStaticMCPStillWorks(t *testing.T) {
	authn := &mcpRouteAuth{
		wantSecret: "mrs_local",
		tok:        domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
	}
	s := &Server{
		authenticator: authn,
		mux:           http.NewServeMux(),
		policy:        safety.DefaultPolicy(),
		oauthRuntime:  oauthRuntimeConfig{mode: oauthRuntimeDisabled},
	}
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.routes()

	publicRoutes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/.well-known/oauth-protected-resource/mcp"},
		{http.MethodGet, "/.well-known/oauth-authorization-server"},
		{http.MethodPost, "/oauth/register"},
		{http.MethodGet, "/oauth/authorize"},
		{http.MethodPost, "/oauth/token"},
	}
	var firstBody string
	for _, route := range publicRoutes {
		req := httptest.NewRequest(route.method, route.path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s status = %d, want 503", route.method, route.path, rec.Code)
		}
		if firstBody == "" {
			firstBody = rec.Body.String()
		} else if rec.Body.String() != firstBody {
			t.Fatalf("%s %s leaked a distinct failure body: %s", route.method, route.path, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer mrs_local")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"serverInfo"`) {
		t.Fatalf("static MCP status = %d body=%s", rec.Code, rec.Body.String())
	}

	oauthReq := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"initialize","params":{}}`))
	oauthReq.Header.Set("Accept", "application/json, text/event-stream")
	oauthReq.Header.Set("Authorization", "Bearer mcpat_old_grant")
	oauthRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(oauthRec, oauthReq)
	if oauthRec.Code != http.StatusServiceUnavailable || !strings.Contains(oauthRec.Body.String(), `"code":"oauth_unavailable"`) {
		t.Fatalf("disabled OAuth MCP status = %d body=%s", oauthRec.Code, oauthRec.Body.String())
	}
	if authn.calls != 1 {
		t.Fatalf("static authenticator calls = %d, want only the static request", authn.calls)
	}
}

func TestEnabledOAuthMetadataUsesOnlyConfiguredBase(t *testing.T) {
	id := uuid.New()
	s := &Server{
		mux:           http.NewServeMux(),
		policy:        safety.DefaultPolicy(),
		publicBaseURL: "https://mcp.example.test",
		oauthRuntime: oauthRuntimeConfig{
			mode:          oauthRuntimeEnabled,
			publicBaseURL: "https://mcp.example.test",
			systemActorID: id,
		},
		oauthActorLookup: func(context.Context, uuid.UUID) (domain.Token, error) {
			return domain.Token{ID: id, Source: domain.SourceSystem}, nil
		},
	}
	s.routes()
	req := httptest.NewRequest(http.MethodGet, "http://attacker.invalid/.well-known/oauth-authorization-server", nil)
	req.Header.Set("X-Forwarded-Proto", "http")
	req.Header.Set("X-Forwarded-Host", "forwarded-attacker.invalid")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if got := body["issuer"]; got != "https://mcp.example.test" {
		t.Fatalf("issuer = %v", got)
	}
	if strings.Contains(rec.Body.String(), "attacker") {
		t.Fatalf("metadata trusted request routing headers: %s", rec.Body.String())
	}
}

func TestInvalidOAuthConfigurationFailsReadinessBeforeDatabase(t *testing.T) {
	t.Setenv(EnvPublicBaseURL, "https://mcp.example.test")
	t.Setenv(EnvOAuthSystemActorID, "")
	s := New(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"reason":"oauth_configuration"`) {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOAuthReadinessTracksSystemActorLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	authSvc := auth.NewService(pool, app.NewEventWriter())
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-runtime-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	systemActor, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-runtime-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system actor: %v", err)
	}
	t.Setenv(EnvPublicBaseURL, "https://mcp.example.test")
	t.Setenv(EnvOAuthSystemActorID, systemActor.Token.ID.String())
	s := New(pool, nil)

	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusOK || !strings.Contains(readyRec.Body.String(), `"oauth":"ok"`) {
		t.Fatalf("enabled readyz status = %d body=%s", readyRec.Code, readyRec.Body.String())
	}

	if err := authSvc.Revoke(ctx, systemActor.Token.ID, root.Token); err != nil {
		t.Fatalf("revoke system actor: %v", err)
	}
	readyRec = httptest.NewRecorder()
	s.Handler().ServeHTTP(readyRec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyRec.Code != http.StatusServiceUnavailable || !strings.Contains(readyRec.Body.String(), `"reason":"oauth_system_actor"`) {
		t.Fatalf("revoked readyz status = %d body=%s", readyRec.Code, readyRec.Body.String())
	}

	discoveryRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(discoveryRec, httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil))
	if discoveryRec.Code != http.StatusServiceUnavailable || !strings.Contains(discoveryRec.Body.String(), `"code":"oauth_unavailable"`) {
		t.Fatalf("revoked discovery status = %d body=%s", discoveryRec.Code, discoveryRec.Body.String())
	}
	oauthMCPReq := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":3,"method":"initialize","params":{}}`))
	oauthMCPReq.Header.Set("Authorization", "Bearer mcpat_existing")
	oauthMCPRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(oauthMCPRec, oauthMCPReq)
	if oauthMCPRec.Code != http.StatusServiceUnavailable || !strings.Contains(oauthMCPRec.Body.String(), `"code":"oauth_unavailable"`) {
		t.Fatalf("revoked actor OAuth MCP status = %d body=%s", oauthMCPRec.Code, oauthMCPRec.Body.String())
	}
}
