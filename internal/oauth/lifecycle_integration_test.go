package oauth_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

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

func TestProviderOAuthLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "oauth_lifecycle")
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
	registration := oauth.NewRegistrationServiceWithSystemActor(pool, writer, system.Token.ID)
	client, err := registration.Register(ctx, oauth.RegisterInput{ClientName: "Claude", RedirectURIs: []string{"https://provider.example/callback"}, Scope: oauth.ScopeMCPRead})
	if err != nil {
		t.Fatal(err)
	}
	wi := workitems.NewService(pool, writer)
	ap := approvals.NewService(pool, writer)
	authorize := oauth.NewAuthorizationService(pool, writer, wi, ap, system.Token.ID)
	verifier := strings.Repeat("v", 60)
	sum := sha256.Sum256([]byte(verifier))
	input := oauth.AuthorizationInput{ClientID: client.ClientID, RedirectURI: "https://provider.example/callback", ResponseType: "code", State: "opaque-state", CodeChallenge: base64.RawURLEncoding.EncodeToString(sum[:]), CodeChallengeMethod: "S256", Scope: oauth.ScopeMCPRead, Resource: "https://mcp.example/mcp", ExpectedResource: "https://mcp.example/mcp"}
	_, err = authorize.Begin(ctx, input)
	if !errors.Is(err, oauth.ErrProviderActorUnavailable) {
		t.Fatalf("unbound begin=%v", err)
	}
	var bindingID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT binding_work_item_id FROM oauth_clients WHERE client_id=$1`, client.ClientID).Scan(&bindingID); err != nil {
		t.Fatal(err)
	}
	_, err = authorize.Begin(ctx, input)
	if !errors.Is(err, oauth.ErrProviderActorUnavailable) || !strings.Contains(err.Error(), bindingID.String()) {
		t.Fatalf("unbound retry=%v", err)
	}
	var bindingCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_items WHERE title LIKE 'Bind OAuth provider actor:%'`).Scan(&bindingCount); err != nil || bindingCount != 1 {
		t.Fatalf("binding items=%d err=%v", bindingCount, err)
	}
	admin := oauth.NewClientAdminService(pool, writer)
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, "made_up_profile", root.Token); !errors.Is(err, access.ErrInvalidProviderAuthority) {
		t.Fatalf("arbitrary profile accepted: %v", err)
	}
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), root.Token); err != nil {
		t.Fatal(err)
	}
	req1, err := authorize.Begin(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	var createdBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind IN ($1,$2,$3)`, domain.EventWorkItemCreated, domain.EventApprovalCreated, domain.EventOAuthAuthorizationRequestCreated).Scan(&createdBefore); err != nil {
		t.Fatal(err)
	}
	req2, err := authorize.Begin(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if req1.ID != req2.ID || req1.WorkItemID != req2.WorkItemID || req1.ApprovalID != req2.ApprovalID {
		t.Fatalf("authorize retry diverged: %#v %#v", req1, req2)
	}
	var createdAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind IN ($1,$2,$3)`, domain.EventWorkItemCreated, domain.EventApprovalCreated, domain.EventOAuthAuthorizationRequestCreated).Scan(&createdAfter); err != nil || createdAfter != createdBefore {
		t.Fatalf("retry appended events: before=%d after=%d err=%v", createdBefore, createdAfter, err)
	}
	if _, err := ap.Decide(ctx, approvals.DecisionInput{ApprovalID: req1.ApprovalID, Decision: approvals.DecisionApproved, Reason: "owner approved", Actor: decider.Token}); err != nil {
		t.Fatal(err)
	}
	continued, err := authorize.Continue(ctx, req1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Code == "" || continued.Pending {
		t.Fatalf("continue=%+v", continued)
	}
	tokens := oauth.NewTokenService(pool, writer, system.Token.ID)
	pair, err := tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: continued.Code, ClientID: client.ClientID, RedirectURI: input.RedirectURI, CodeVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := tokens.AuthenticateAccess(ctx, pair.AccessToken, input.Resource)
	if err != nil {
		t.Fatal(err)
	}
	if actor.ID != agent.Token.ID {
		t.Fatalf("actor=%s", actor.ID)
	}
	if _, err := access.ProviderAuthorityProfileFromScopes(actor.Scopes); err != nil {
		t.Fatalf("unsealed scopes: %v", err)
	}
	rotated, err := tokens.Refresh(ctx, pair.RefreshToken, client.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.RefreshToken == pair.RefreshToken {
		t.Fatal("refresh token did not rotate")
	}
	if _, err := tokens.Refresh(ctx, pair.RefreshToken, client.ClientID); !errors.Is(err, oauth.ErrRefreshReuse) {
		t.Fatalf("reuse=%v", err)
	}
	if _, err := tokens.AuthenticateAccess(ctx, rotated.AccessToken, input.Resource); err != nil {
		t.Fatalf("healthy successor grant was revoked by lost-response retry: %v", err)
	}
	var reuseEvents, revokeEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthRefreshReuseDetected).Scan(&reuseEvents); err != nil || reuseEvents != 1 {
		t.Fatalf("reuse events=%d err=%v", reuseEvents, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind=$1`, domain.EventOAuthGrantRevoked).Scan(&revokeEvents); err != nil || revokeEvents != 0 {
		t.Fatalf("reuse unexpectedly revoked grant: events=%d err=%v", revokeEvents, err)
	}
	var unattributed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind LIKE 'oauth_%' AND actor_token_id IS NULL`).Scan(&unattributed); err != nil || unattributed != 0 {
		t.Fatalf("unattributed oauth events=%d err=%v", unattributed, err)
	}
}

func TestProviderOAuthRejectsMalformedInputs(t *testing.T) {
	if err := oauth.VerifyPKCE(strings.Repeat("!", 43), base64.RawURLEncoding.EncodeToString(make([]byte, 32))); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("bad verifier charset=%v", err)
	}
	ctx := context.Background()
	_ = ctx
}
