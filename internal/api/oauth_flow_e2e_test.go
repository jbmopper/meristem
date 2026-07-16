package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/safety"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestOAuthAuthorizeTokenEndToEnd covers finding 3: an httptest walk of the
// real HTTP surface. It drives GET /oauth/authorize (begin + request_id
// continuation) through the code/state/iss redirect assembly and then a
// POST /oauth/token application/x-www-form-urlencoded exchange, asserting the
// glue the service-layer tests never touch.
func TestOAuthAuthorizeTokenEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	system, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "provider", Source: domain.SourceAgent, Scopes: authority.Scopes, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "decider", Source: domain.SourceHuman, Scopes: []string{access.ScopeApprovalsDecide}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	clientAdmin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-client-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}

	const baseURL = "https://mcp.example.test"
	t.Setenv(EnvPublicBaseURL, baseURL)
	t.Setenv(EnvOAuthSystemActorID, system.Token.ID.String())
	s := NewWithPolicy(pool, discardLogger(), safety.DefaultPolicy())
	if s.oauthAuthorization == nil || s.oauthTokens == nil || s.oauthClients == nil {
		t.Fatal("oauth runtime did not enable with a pinned public base URL and system actor")
	}

	const redirect = "https://client.example/callback"
	oauthClient, err := s.oauthClients.Register(ctx, oauth.RegisterInput{ClientName: "Claude", RedirectURIs: []string{redirect}, Scope: oauth.ScopeMCPRead})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.oauthClientAdmin.BindActor(ctx, oauthClient.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), clientAdmin.Token); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	httpClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	const state = "opaque-e2e-state"
	verifier := strings.Repeat("v", 60)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	authzQuery := url.Values{
		"client_id":             {oauthClient.ClientID},
		"redirect_uri":          {redirect},
		"response_type":         {"code"},
		"state":                 {state},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
		"scope":                 {oauth.ScopeMCPRead},
		"resource":              {baseURL + "/mcp"},
	}

	// 1) Begin: GET /oauth/authorize returns a 202 pending page while owner
	// consent is out of band.
	beginResp, err := httpClient.Get(srv.URL + "/oauth/authorize?" + authzQuery.Encode())
	if err != nil {
		t.Fatal(err)
	}
	_ = beginResp.Body.Close()
	if beginResp.StatusCode != http.StatusAccepted {
		t.Fatalf("begin status=%d want 202", beginResp.StatusCode)
	}
	if beginResp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("begin cache-control=%q", beginResp.Header.Get("Cache-Control"))
	}

	var reqID, approvalID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id,approval_id FROM oauth_authorization_requests WHERE state=$1`, state).Scan(&reqID, &approvalID); err != nil {
		t.Fatal(err)
	}

	// 2) Owner approves out of band.
	if _, err := s.approvals.Decide(ctx, approvals.DecisionInput{ApprovalID: approvalID, Decision: approvals.DecisionApproved, Reason: "owner approved", Actor: decider.Token}); err != nil {
		t.Fatal(err)
	}

	// 3) Continue: GET /oauth/authorize?request_id=... assembles the code/state/iss
	// redirect back to the client.
	contResp, err := httpClient.Get(srv.URL + "/oauth/authorize?request_id=" + url.QueryEscape(reqID.String()))
	if err != nil {
		t.Fatal(err)
	}
	_ = contResp.Body.Close()
	if contResp.StatusCode != http.StatusFound {
		t.Fatalf("continue status=%d want 302", contResp.StatusCode)
	}
	loc, err := url.Parse(contResp.Header.Get("Location"))
	if err != nil {
		t.Fatalf("continue location=%q: %v", contResp.Header.Get("Location"), err)
	}
	if got := loc.Scheme + "://" + loc.Host + loc.Path; got != redirect {
		t.Fatalf("redirect target=%q want %q", got, redirect)
	}
	locQ := loc.Query()
	code := locQ.Get("code")
	if code == "" {
		t.Fatalf("continue redirect missing code: %q", loc.RawQuery)
	}
	if locQ.Get("state") != state {
		t.Fatalf("redirect state=%q want %q", locQ.Get("state"), state)
	}
	if locQ.Get("iss") != baseURL {
		t.Fatalf("redirect iss=%q want %q", locQ.Get("iss"), baseURL)
	}
	if locQ.Get("error") != "" {
		t.Fatalf("redirect carried error=%q", locQ.Get("error"))
	}

	// 4) Token: POST /oauth/token as application/x-www-form-urlencoded exchanges
	// the code for an access token.
	tokenForm := url.Values{
		"grant_type":    {oauth.GrantAuthorizationCode},
		"code":          {code},
		"client_id":     {oauthClient.ClientID},
		"redirect_uri":  {redirect},
		"code_verifier": {verifier},
	}
	tokResp, err := httpClient.Post(srv.URL+"/oauth/token", "application/x-www-form-urlencoded", strings.NewReader(tokenForm.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tokResp.Body.Close() }()
	if tokResp.StatusCode != http.StatusOK {
		t.Fatalf("token status=%d want 200", tokResp.StatusCode)
	}
	var body struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if err := json.NewDecoder(tokResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(body.AccessToken, "mcpat_") || !strings.HasPrefix(body.RefreshToken, "mcprt_") {
		// Do not print the token secrets themselves — report shapes only.
		t.Fatalf("token prefixes wrong: access_len=%d refresh_len=%d (want mcpat_/mcprt_)", len(body.AccessToken), len(body.RefreshToken))
	}
	if body.TokenType != "Bearer" || body.Scope != oauth.ScopeMCPRead || body.ExpiresIn <= 0 {
		t.Fatalf("token metadata: type=%q scope=%q expires_in=%d", body.TokenType, body.Scope, body.ExpiresIn)
	}
}
