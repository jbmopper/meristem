package oauth_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

// boundOAuthFixture is a fully wired read-profile OAuth client bound to a
// provider actor, ready to run authorize -> approve -> continue -> token.
type boundOAuthFixture struct {
	pool      *pgxpool.Pool
	authorize *oauth.AuthorizationService
	tokens    *oauth.TokenService
	approvals *approvals.Service
	decider   domain.Token
	clientID  string
	verifier  string
	resource  string
	redirect  string
}

func newBoundOAuthFixture(t *testing.T, prefix string) boundOAuthFixture {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, prefix)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatal(err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	workitems.RegisterProjectors(reg)
	approvals.RegisterProjectors(reg)
	oauth.RegisterProjectors(reg)
	writer := events.NewWriter(reg)
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
	decider, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "decider", Source: domain.SourceHuman, Scopes: []string{"approvals.decide"}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	clientAdmin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-client-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	registration := oauth.NewRegistrationServiceWithSystemActor(pool, writer, system.Token.ID)
	client, err := registration.Register(ctx, oauth.RegisterInput{ClientName: "Claude", RedirectURIs: []string{"https://provider.example/callback"}, Scope: oauth.ScopeMCPRead})
	if err != nil {
		t.Fatal(err)
	}
	admin := oauth.NewClientAdminService(pool, writer)
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), clientAdmin.Token); err != nil {
		t.Fatal(err)
	}
	wi := workitems.NewService(pool, writer)
	ap := approvals.NewService(pool, writer)
	return boundOAuthFixture{
		pool:      pool,
		authorize: oauth.NewAuthorizationService(pool, writer, wi, ap, system.Token.ID),
		tokens:    oauth.NewTokenService(pool, writer, system.Token.ID),
		approvals: ap,
		decider:   decider.Token,
		clientID:  client.ClientID,
		verifier:  strings.Repeat("v", 60),
		resource:  "https://mcp.example/mcp",
		redirect:  "https://provider.example/callback",
	}
}

func (f boundOAuthFixture) input(state string) oauth.AuthorizationInput {
	sum := sha256.Sum256([]byte(f.verifier))
	return oauth.AuthorizationInput{ClientID: f.clientID, RedirectURI: f.redirect, ResponseType: "code", State: state, CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:]), CodeChallengeMethod: "S256", Scope: oauth.ScopeMCPRead, Resource: f.resource, ExpectedResource: f.resource}
}

// approvedCode runs authorize -> approve -> continue and returns the issued code.
func (f boundOAuthFixture) approvedCode(t *testing.T, state string) string {
	t.Helper()
	ctx := context.Background()
	req, err := f.authorize.Begin(ctx, f.input(state))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.approvals.Decide(ctx, approvals.DecisionInput{ApprovalID: req.ApprovalID, Decision: approvals.DecisionApproved, Reason: "owner approved", Actor: f.decider}); err != nil {
		t.Fatal(err)
	}
	continued, err := f.authorize.Continue(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Code == "" || continued.Pending {
		t.Fatalf("continue=%+v", continued)
	}
	return continued.Code
}

// TestProviderOAuthCodeReplayRevokesGrant proves RFC 6749 §4.1.2: replaying an
// already-redeemed authorization code revokes the grant minted from the first
// redemption, so its tokens stop authenticating, while the token endpoint keeps
// returning the identical ErrInvalidGrant it returns for any other rejection.
func TestProviderOAuthCodeReplayRevokesGrant(t *testing.T) {
	ctx := context.Background()
	fx := newBoundOAuthFixture(t, "oauth_code_replay")
	code := fx.approvedCode(t, "replay-state")

	pair, err := fx.tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: code, ClientID: fx.clientID, RedirectURI: fx.redirect, CodeVerifier: fx.verifier})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.tokens.AuthenticateAccess(ctx, pair.AccessToken, fx.resource); err != nil {
		t.Fatalf("first access token before replay: %v", err)
	}

	// Replaying the redeemed code fails with the same error shape as an expired
	// code or a binding mismatch: the replay must stay indistinguishable.
	if _, err := fx.tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: code, ClientID: fx.clientID, RedirectURI: fx.redirect, CodeVerifier: fx.verifier}); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("replay=%v", err)
	}

	// The replay revoked exactly the grant minted from the first redemption.
	var revokeEvents int
	if err := fx.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthGrantRevoked).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("revoke events=%d err=%v", revokeEvents, err)
	}
	var revokedAt *time.Time
	var reason string
	if err := fx.pool.QueryRow(ctx, `SELECT revoked_at,compromise_reason FROM oauth_grants`).Scan(&revokedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if revokedAt == nil || reason != "authorization_code_replay" {
		t.Fatalf("grant projection revoked_at=%v reason=%q", revokedAt, reason)
	}

	// The tokens minted from the first redemption no longer authenticate.
	if _, err := fx.tokens.AuthenticateAccess(ctx, pair.AccessToken, fx.resource); !errors.Is(err, oauth.ErrInvalidAccessToken) {
		t.Fatalf("access token survived code-replay revocation: %v", err)
	}
	if _, err := fx.tokens.Refresh(ctx, pair.RefreshToken, fx.clientID); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("refresh token survived code-replay revocation: %v", err)
	}

	// A second replay still reports invalid_grant but must not double-revoke the
	// already-revoked grant.
	if _, err := fx.tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: code, ClientID: fx.clientID, RedirectURI: fx.redirect, CodeVerifier: fx.verifier}); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("second replay=%v", err)
	}
	if err := fx.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthGrantRevoked).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("second replay double-revoked: events=%d err=%v", revokeEvents, err)
	}

	// The code -> grant link is durable in the projection.
	var linked int
	if err := fx.pool.QueryRow(ctx, `SELECT count(*) FROM oauth_authorization_codes WHERE grant_id IS NOT NULL`).Scan(&linked); err != nil || linked != 1 {
		t.Fatalf("code grant link=%d err=%v", linked, err)
	}
}

// TestProviderOAuthDenialCompletesWithoutCode exercises the owner-denial branch
// of Continue: a DecisionDenied approval yields access_denied with no code and a
// terminal work item.
func TestProviderOAuthDenialCompletesWithoutCode(t *testing.T) {
	ctx := context.Background()
	fx := newBoundOAuthFixture(t, "oauth_denial")
	req, err := fx.authorize.Begin(ctx, fx.input("denied-state"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.approvals.Decide(ctx, approvals.DecisionInput{ApprovalID: req.ApprovalID, Decision: approvals.DecisionDenied, Reason: "owner denied", Actor: fx.decider}); err != nil {
		t.Fatal(err)
	}
	result, err := fx.authorize.Continue(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.OAuthError != "access_denied" {
		t.Fatalf("denied continue OAuthError=%q, want access_denied", result.OAuthError)
	}
	if result.Code != "" {
		t.Fatalf("denied continue issued a code: %q", result.Code)
	}
	assertOAuthRequestTerminal(t, fx.pool, req, string(approvals.StatusDenied), string(domain.WorkItemFailed))

	// Denial mints neither an authorization code nor a grant.
	var codes, grants int
	if err := fx.pool.QueryRow(ctx, `SELECT count(*) FROM oauth_authorization_codes`).Scan(&codes); err != nil || codes != 0 {
		t.Fatalf("denied minted codes=%d err=%v", codes, err)
	}
	if err := fx.pool.QueryRow(ctx, `SELECT count(*) FROM oauth_grants`).Scan(&grants); err != nil || grants != 0 {
		t.Fatalf("denied minted grants=%d err=%v", grants, err)
	}
}
