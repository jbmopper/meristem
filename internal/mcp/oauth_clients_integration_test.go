package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestOAuthClientAdminToolErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{"denied", oauth.ErrOAuthClientAdminDenied, "oauth_client_admin_denied"},
		{"not found", oauth.ErrClientNotFound, "oauth_client_not_found"},
		{"grant not found", oauth.ErrGrantNotFound, "oauth_grant_not_found"},
		{"invalid", oauth.ErrInvalidClientAdminInput, "invalid_oauth_client_admin_request"},
		{"conflict", oauth.ErrOAuthClientConflict, "oauth_client_conflict"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := oauthClientAdminToolErr(tc.err)
			if !isReplayableToolError(got) || !strings.Contains(got.Error(), tc.code) {
				t.Fatalf("mapped error = %v (replayable=%t), want code %s", got, isReplayableToolError(got), tc.code)
			}
		})
	}
	infra := errors.New("database unavailable")
	if got := oauthClientAdminToolErr(infra); got != infra || isReplayableToolError(got) {
		t.Fatalf("infrastructure error was transformed or made replayable: %v", got)
	}
}

func TestOAuthClientAdminMCPParityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-oauth-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	system, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-oauth-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "mcp-oauth-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	unscopedHuman, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-oauth-unscoped-human", Source: domain.SourceHuman, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "mcp-oauth-provider", Source: domain.SourceAgent, Scopes: authority.Scopes, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	clientID := "mcpc_mcp_parity_provider"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectOAuthClient, SubjectID: oauth.ClientSubjectID(clientID), Kind: domain.EventOAuthClientRegistered,
		Source: domain.SourceSystem, ActorTokenID: &system.Token.ID,
		Payload: map[string]any{
			"payload_version": 1, "client_id": clientID, "client_name": "MCP parity provider",
			"redirect_uris": []string{"https://provider.example/callback"}, "grant_types": []string{oauth.GrantAuthorizationCode},
			"response_types": []string{oauth.ResponseTypeCode}, "token_endpoint_auth_method": oauth.AuthMethodNone, "scope": oauth.ScopeMCPRead,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	s := New(Deps{
		Auth:             authSvc,
		Idempotency:      idempotency.NewMiddleware(pool, writer),
		OAuthClientAdmin: oauth.NewClientAdminService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	// Root and legacy-unscoped human credentials reach the ordinary domain
	// scope guard. The provider-marked agent is refused even earlier by its
	// transport-independent sealed profile. Neither path may reach idempotency
	// or the domain service and append an event.
	denialCases := []struct {
		name          string
		secret        string
		profileDenied bool
	}{
		{name: "root", secret: root.Secret},
		{name: "provider", secret: provider.Secret, profileDenied: true},
		{name: "unscoped-human", secret: unscopedHuman.Secret},
	}
	for _, denialCase := range denialCases {
		if err := s.Authenticate(ctx, denialCase.secret); err != nil {
			t.Fatal(err)
		}
		beforeBind := eventCount(t, pool, domain.EventOAuthClientActorBound)
		beforeRevoke := eventCount(t, pool, domain.EventOAuthClientRevoked)
		deniedCalls := []struct {
			tool string
			args map[string]any
		}{
			{"oauth_clients.bind_actor", map[string]any{"client_id": clientID, "actor_token_id": provider.Token.ID, "authority_profile": authority.Profile, "idempotency_key": "denied-bind-" + denialCase.name}},
			{"oauth_clients.revoke", map[string]any{"client_id": clientID, "reason": "must not run", "idempotency_key": "denied-revoke-" + denialCase.name}},
		}
		for _, call := range deniedCalls {
			if denialCase.profileDenied {
				request, err := json.Marshal(map[string]any{
					"jsonrpc": "2.0",
					"id":      12,
					"method":  "tools/call",
					"params": map[string]any{
						"name":      call.tool,
						"arguments": call.args,
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				response := roundtrip(t, s, string(request))
				if response.Error == nil ||
					response.Error.Code != errCodeMethodNotFound ||
					!strings.Contains(response.Error.Message, "not enabled on this HTTP MCP profile") {
					t.Fatalf("%s %s profile denial: %+v", denialCase.name, call.tool, response)
				}
				continue
			}
			isError, text := callToolForTest(t, s, call.tool, call.args)
			if !isError || !strings.Contains(text, "insufficient_scope") {
				t.Fatalf("%s %s denial: isError=%t text=%q", denialCase.name, call.tool, isError, text)
			}
		}
		if got := eventCount(t, pool, domain.EventOAuthClientActorBound); got != beforeBind {
			t.Fatalf("%s denial appended binding event: before=%d after=%d", denialCase.name, beforeBind, got)
		}
		if got := eventCount(t, pool, domain.EventOAuthClientRevoked); got != beforeRevoke {
			t.Fatalf("%s denial appended revocation event: before=%d after=%d", denialCase.name, beforeRevoke, got)
		}
	}

	if err := s.Authenticate(ctx, admin.Secret); err != nil {
		t.Fatal(err)
	}
	assertAdvertisedTools(t, s, "oauth_clients.bind_actor", "oauth_clients.revoke")

	bindArgs := map[string]any{
		"client_id": clientID, "actor_token_id": provider.Token.ID, "authority_profile": authority.Profile,
		"idempotency_key": "bind-provider",
	}
	for i := 0; i < 2; i++ {
		isError, text := callToolForTest(t, s, "oauth_clients.bind_actor", bindArgs)
		if isError {
			t.Fatalf("bind call %d: %s", i+1, text)
		}
		var response struct {
			ClientID         string    `json:"client_id"`
			ActorTokenID     uuid.UUID `json:"actor_token_id"`
			AuthorityProfile string    `json:"authority_profile"`
		}
		if err := json.Unmarshal([]byte(text), &response); err != nil {
			t.Fatalf("decode bind response: %v (%s)", err, text)
		}
		if response.ClientID != clientID || response.ActorTokenID != provider.Token.ID || response.AuthorityProfile != string(authority.Profile) {
			t.Fatalf("bind response differs from REST body: %+v", response)
		}
	}
	if got := eventCount(t, pool, domain.EventOAuthClientActorBound); got != 1 {
		t.Fatalf("binding event count after same-key replay = %d, want 1", got)
	}

	if isError, text := callToolForTest(t, s, "oauth_clients.bind_actor", map[string]any{
		"client_id": "missing", "actor_token_id": provider.Token.ID, "authority_profile": authority.Profile,
		"idempotency_key": "missing-client",
	}); !isError || !strings.Contains(text, "oauth_client_not_found") {
		t.Fatalf("missing client error mapping: isError=%t text=%q", isError, text)
	}

	revokeArgs := map[string]any{"client_id": clientID, "reason": "provider access retired", "idempotency_key": "revoke-provider"}
	for i := 0; i < 2; i++ {
		if isError, text := callToolForTest(t, s, "oauth_clients.revoke", revokeArgs); isError || text != "" {
			t.Fatalf("revoke call %d should mirror REST 204 body: isError=%t text=%q", i+1, isError, text)
		}
	}
	if got := eventCount(t, pool, domain.EventOAuthClientRevoked); got != 1 {
		t.Fatalf("revocation event count after same-key replay = %d, want 1", got)
	}
}

func TestProviderHTTPProfilesHideOAuthClientAdministrationIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatal(err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-oauth-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-oauth-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Idempotency: idempotency.NewMiddleware(pool, writer), OAuthClientAdmin: oauth.NewClientAdminService(pool, writer)}, ServerInfo{}, nil)
	before := durableEffectCounts(t, ctx, pool)
	for _, profile := range []*HTTPToolProfile{ProviderSafeReadHTTPProfile(), ProviderTrackerHTTPProfile()} {
		for _, name := range []string{"oauth_clients.bind_actor", "oauth_clients.revoke"} {
			result := callHTTPTool(t, s, admin.Token, profile, name, map[string]any{"client_id": "unreachable", "idempotency_key": profile.Name() + "-" + name})
			if !strings.Contains(result.TransportError, "not enabled on this HTTP MCP profile") {
				t.Fatalf("profile %s exposed %s: %+v", profile.Name(), name, result)
			}
		}
	}
	after := durableEffectCounts(t, ctx, pool)
	if after != before {
		t.Fatalf("hidden OAuth admin calls changed durable state: before=%+v after=%+v", before, after)
	}
}

func assertAdvertisedTools(t *testing.T, s *Server, names ...string) {
	t.Helper()
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":11,"method":"tools/list"}`)
	if resp.Error != nil {
		t.Fatalf("tools/list: %+v", resp.Error)
	}
	var result struct {
		Tools []toolDescriptor `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(result.Tools))
	for _, tool := range result.Tools {
		seen[tool.Name] = true
	}
	for _, name := range names {
		if !seen[name] {
			t.Errorf("tools/list did not advertise %s", name)
		}
	}
}
