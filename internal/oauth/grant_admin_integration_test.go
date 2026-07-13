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

// TestOAuthGrantRevoke proves a revoked grant's access token fails
// AuthenticateAccess and its refresh token fails Refresh, that revocation is
// scope-gated and idempotent, and that unknown grants are reported distinctly.
func TestOAuthGrantRevoke(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "oauth_grant_revoke")
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
	authorize := oauth.NewAuthorizationService(pool, writer, wi, ap, system.Token.ID)
	verifier := strings.Repeat("v", 60)
	sum := sha256.Sum256([]byte(verifier))
	input := oauth.AuthorizationInput{ClientID: client.ClientID, RedirectURI: "https://provider.example/callback", ResponseType: "code", State: "opaque-state", CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:]), CodeChallengeMethod: "S256", Scope: oauth.ScopeMCPRead, Resource: "https://mcp.example/mcp", ExpectedResource: "https://mcp.example/mcp"}
	req, err := authorize.Begin(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ap.Decide(ctx, approvals.DecisionInput{ApprovalID: req.ApprovalID, Decision: approvals.DecisionApproved, Reason: "owner approved", Actor: decider.Token}); err != nil {
		t.Fatal(err)
	}
	continued, err := authorize.Continue(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	tokens := oauth.NewTokenService(pool, writer, system.Token.ID)
	pair, err := tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: continued.Code, ClientID: client.ClientID, RedirectURI: input.RedirectURI, CodeVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}

	// Baseline: the freshly issued grant authenticates before revocation.
	if _, err := tokens.AuthenticateAccess(ctx, pair.AccessToken, input.Resource); err != nil {
		t.Fatalf("active grant failed AuthenticateAccess: %v", err)
	}
	var grantID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM oauth_grants WHERE client_id=$1`, client.ClientID).Scan(&grantID); err != nil {
		t.Fatal(err)
	}

	// Revocation is gated on the explicit non-root human oauth_clients.revoke
	// scope; root and the provider agent are rejected without emitting an event.
	if err := admin.RevokeGrant(ctx, grantID, "compromised", root.Token); !errors.Is(err, oauth.ErrOAuthClientAdminDenied) {
		t.Fatalf("root revoked grant: %v", err)
	}
	if err := admin.RevokeGrant(ctx, grantID, "compromised", agent.Token); !errors.Is(err, oauth.ErrOAuthClientAdminDenied) {
		t.Fatalf("agent revoked grant: %v", err)
	}
	if err := admin.RevokeGrant(ctx, uuid.New(), "compromised", clientAdmin.Token); !errors.Is(err, oauth.ErrGrantNotFound) {
		t.Fatalf("unknown grant err=%v, want ErrGrantNotFound", err)
	}

	if err := admin.RevokeGrant(ctx, grantID, "compromised", clientAdmin.Token); err != nil {
		t.Fatal(err)
	}

	// The revoked grant's access token no longer authenticates and its refresh
	// token can no longer rotate.
	if _, err := tokens.AuthenticateAccess(ctx, pair.AccessToken, input.Resource); !errors.Is(err, oauth.ErrInvalidAccessToken) {
		t.Fatalf("revoked access token authenticated: %v", err)
	}
	if _, err := tokens.Refresh(ctx, pair.RefreshToken, client.ClientID); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("revoked refresh token rotated: %v", err)
	}

	var revokedAt *time.Time
	var reason string
	if err := pool.QueryRow(ctx, `SELECT revoked_at,compromise_reason FROM oauth_grants WHERE id=$1`, grantID).Scan(&revokedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if revokedAt == nil || reason != "compromised" {
		t.Fatalf("projection revoked_at=%v reason=%q", revokedAt, reason)
	}

	// Re-revoking is a no-op, matching client revocation and the projector's
	// COALESCE(revoked_at) semantics: exactly one event, timestamp preserved.
	firstRevokedAt := *revokedAt
	if err := admin.RevokeGrant(ctx, grantID, "compromised again", clientAdmin.Token); err != nil {
		t.Fatalf("idempotent re-revoke: %v", err)
	}
	var revokeEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1 AND subject_id=$2`, domain.EventOAuthGrantRevoked, grantID).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("grant revoke events=%d err=%v, want 1", revokeEvents, err)
	}
	if err := pool.QueryRow(ctx, `SELECT revoked_at,compromise_reason FROM oauth_grants WHERE id=$1`, grantID).Scan(&revokedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if !revokedAt.Equal(firstRevokedAt) || reason != "compromised" {
		t.Fatalf("re-revoke mutated projection: revoked_at=%v reason=%q", revokedAt, reason)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1 AND subject_id=$2 AND actor_token_id=$3`, domain.EventOAuthGrantRevoked, grantID, clientAdmin.Token.ID).Scan(&revokeEvents); err != nil || revokeEvents != 1 {
		t.Fatalf("revoke attribution events=%d err=%v, want 1 attributed to admin", revokeEvents, err)
	}
}
