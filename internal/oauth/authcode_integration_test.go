package oauth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// exchangeFixture wires up the minimum live objects the token endpoint touches
// when it exchanges an authorization code: a registered client bound to an
// active provider agent, an AuthCodeService that mints codes for that pairing,
// and a TokenService whose ExchangeCode is the strict, production redemption
// path (AuthCodeService.Redeem was removed as dead code). Codes are issued
// directly rather than through the approval flow so redemption edge cases stay
// isolated from authorization concerns.
type exchangeFixture struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	svc      *AuthCodeService
	tokens   *TokenService
	clientID string
	redirect string
	resource string
	verifier string
	actorID  uuid.UUID
	profile  string
}

func newExchangeFixture(t *testing.T, now func() time.Time) exchangeFixture {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_oauth_exchange_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	auth.RegisterProjectors(reg)
	RegisterProjectors(reg)
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
	clientAdmin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-client-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	redirect := "https://provider.example/callback"
	registration := NewRegistrationServiceWithSystemActor(pool, writer, system.Token.ID)
	client, err := registration.Register(ctx, RegisterInput{ClientName: "Claude", RedirectURIs: []string{redirect}, Scope: ScopeMCPRead})
	if err != nil {
		t.Fatal(err)
	}
	admin := NewClientAdminService(pool, writer)
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), clientAdmin.Token); err != nil {
		t.Fatal(err)
	}
	svc := NewAuthCodeService(pool, writer)
	svc.systemActorID = system.Token.ID
	tokens := NewTokenService(pool, writer, system.Token.ID)
	if now != nil {
		svc.now = now
		tokens.now = now
	}
	return exchangeFixture{
		ctx:      ctx,
		pool:     pool,
		svc:      svc,
		tokens:   tokens,
		clientID: client.ClientID,
		redirect: redirect,
		resource: "https://mcp.example.com/mcp",
		verifier: strings.Repeat("v", 60),
		actorID:  agent.Token.ID,
		profile:  string(access.ProviderOwnerTrackerReadV1),
	}
}

// issue mints a fresh authorization code bound to the fixture's client, actor,
// scope, and PKCE verifier.
func (f exchangeFixture) issue(t *testing.T) string {
	t.Helper()
	code, err := f.svc.Issue(f.ctx, IssueInput{
		ClientID:            f.clientID,
		RedirectURI:         f.redirect,
		CodeChallenge:       s256Challenge(f.verifier),
		CodeChallengeMethod: "S256",
		Scope:               ScopeMCPRead,
		Resource:            f.resource,
		ActorTokenID:        f.actorID,
		AuthorityProfile:    f.profile,
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !strings.HasPrefix(code, "mcpa_") {
		t.Fatalf("unexpected code shape %q", code)
	}
	return code
}

func TestExchangeCodeRoundTrip(t *testing.T) {
	f := newExchangeFixture(t, nil)
	code := f.issue(t)

	pair, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{
		Code:         code,
		ClientID:     f.clientID,
		RedirectURI:  f.redirect,
		CodeVerifier: f.verifier,
	})
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if !strings.HasPrefix(pair.AccessToken, "mcpat_") || !strings.HasPrefix(pair.RefreshToken, "mcprt_") {
		t.Fatalf("unexpected token shapes: %+v", pair)
	}
	if pair.Scope != ScopeMCPRead {
		t.Fatalf("scope = %q, want %q", pair.Scope, ScopeMCPRead)
	}

	// One-time: a second exchange of the same code must fail.
	if _, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{
		Code:         code,
		ClientID:     f.clientID,
		RedirectURI:  f.redirect,
		CodeVerifier: f.verifier,
	}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("second exchange err = %v, want ErrInvalidGrant (already redeemed)", err)
	}
}

// TestExchangeCodeReplayRevokesGrant covers finding 1: replaying an
// already-redeemed authorization code returns invalid_grant AND revokes the
// grant minted from the first redemption (RFC 6749 §4.1.2 / RFC 9700), so the
// tokens issued from that code stop authenticating.
func TestExchangeCodeReplayRevokesGrant(t *testing.T) {
	f := newExchangeFixture(t, nil)
	code := f.issue(t)

	pair, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: f.verifier})
	if err != nil {
		t.Fatalf("first exchange: %v", err)
	}
	// The freshly minted access and refresh tokens work before the replay.
	if _, err := f.tokens.AuthenticateAccess(f.ctx, pair.AccessToken, f.resource); err != nil {
		t.Fatalf("access token before replay: %v", err)
	}

	// Replaying the same code fails closed with the same generic invalid_grant
	// returned for any expired/mismatched/unknown code.
	if _, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: f.verifier}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("replay err = %v, want ErrInvalidGrant", err)
	}

	// A single grant.revoked event was appended for the code-replay reason and
	// folded into the grant projection.
	var revokeEvents int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthGrantRevoked).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("revoke events=%d err=%v", revokeEvents, err)
	}
	var revokedAt *time.Time
	var reason string
	if err := f.pool.QueryRow(f.ctx, `SELECT revoked_at,compromise_reason FROM oauth_grants`).Scan(&revokedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if revokedAt == nil || reason != "authorization_code_replay" {
		t.Fatalf("grant projection revoked_at=%v reason=%q", revokedAt, reason)
	}

	// The tokens minted from the replayed code no longer authenticate.
	if _, err := f.tokens.AuthenticateAccess(f.ctx, pair.AccessToken, f.resource); !errors.Is(err, ErrInvalidAccessToken) {
		t.Fatalf("access token survived replay revocation: %v", err)
	}
	if _, err := f.tokens.Refresh(f.ctx, pair.RefreshToken, f.clientID); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("refresh token survived replay revocation: %v", err)
	}

	// A second replay still fails closed but does not double-revoke.
	if _, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: f.verifier}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("second replay err = %v, want ErrInvalidGrant", err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthGrantRevoked).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("second replay double-revoked: events=%d err=%v", revokeEvents, err)
	}
}

func TestExchangeCodeRejectsBadInputs(t *testing.T) {
	t.Run("wrong pkce verifier", func(t *testing.T) {
		f := newExchangeFixture(t, nil)
		code := f.issue(t)
		_, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: strings.Repeat("w", 60)})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("redirect mismatch", func(t *testing.T) {
		f := newExchangeFixture(t, nil)
		code := f.issue(t)
		_, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: "https://evil.example/cb", CodeVerifier: f.verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("client mismatch", func(t *testing.T) {
		f := newExchangeFixture(t, nil)
		code := f.issue(t)
		_, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: "mcpc_other", RedirectURI: f.redirect, CodeVerifier: f.verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("unknown code", func(t *testing.T) {
		f := newExchangeFixture(t, nil)
		_, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: "mcpa_nope", ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: f.verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})
}

func TestExchangeCodeExpiry(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	clock := base
	f := newExchangeFixture(t, func() time.Time { return clock })
	code := f.issue(t)

	// The exact expiry instant is expired, not one final usable tick.
	clock = base.Add(CodeTTLSeconds * time.Second)
	if _, err := f.tokens.ExchangeCode(f.ctx, RedeemInput{Code: code, ClientID: f.clientID, RedirectURI: f.redirect, CodeVerifier: f.verifier}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("expired exchange err = %v, want ErrInvalidGrant", err)
	}
}
