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
	"github.com/jackc/pgx/v5"

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
	clientAdmin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-client-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsBind, access.ScopeOAuthClientsRevoke}, Actor: &root.Token})
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
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), root.Token); !errors.Is(err, oauth.ErrOAuthClientAdminDenied) {
		t.Fatalf("root bound provider client: %v", err)
	}
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, "made_up_profile", clientAdmin.Token); !errors.Is(err, oauth.ErrInvalidClientAdminInput) {
		t.Fatalf("arbitrary profile accepted: %v", err)
	}
	if err := admin.BindActor(ctx, client.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), clientAdmin.Token); err != nil {
		t.Fatal(err)
	}
	var bindingActor uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT actor_token_id FROM events WHERE kind=$1 AND subject_id=$2`, domain.EventOAuthClientActorBound, oauth.ClientSubjectID(client.ClientID)).Scan(&bindingActor); err != nil || bindingActor != clientAdmin.Token.ID {
		t.Fatalf("binding attribution=%s err=%v want=%s", bindingActor, err, clientAdmin.Token.ID)
	}
	secondClient, err := registration.Register(ctx, oauth.RegisterInput{ClientName: "ChatGPT", RedirectURIs: []string{"https://second.example/callback"}, Scope: oauth.ScopeMCPRead})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.BindActor(ctx, secondClient.ClientID, agent.Token.ID, string(access.ProviderOwnerTrackerReadV1), clientAdmin.Token); !errors.Is(err, oauth.ErrOAuthClientConflict) {
		t.Fatalf("same actor bound to two active clients: %v", err)
	}
	mismatchedScope := input
	mismatchedScope.Scope = oauth.ScopeMCPRead + " " + oauth.ScopeMCPTrackerWrite
	if _, err := authorize.Begin(ctx, mismatchedScope); !errors.Is(err, oauth.ErrInvalidAuthorizationRequest) {
		t.Fatalf("read profile accepted write OAuth scope: %v", err)
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

	writeAuthority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerWriteV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	writeAgent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "provider-writer", Source: domain.SourceAgent, Scopes: writeAuthority.Scopes, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	writeClient, err := registration.Register(ctx, oauth.RegisterInput{ClientName: "Tracker writer", RedirectURIs: []string{"https://writer.example/callback"}, Scope: oauth.ScopeMCPRead + " " + oauth.ScopeMCPTrackerWrite})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.BindActor(ctx, writeClient.ClientID, writeAgent.Token.ID, string(access.ProviderOwnerTrackerWriteV1), clientAdmin.Token); err != nil {
		t.Fatal(err)
	}
	writeInput := input
	writeInput.ClientID = writeClient.ClientID
	writeInput.RedirectURI = "https://writer.example/callback"
	writeInput.Scope = "" // omitted scope defaults to the exact sealed profile contract
	writeInput.State = "write-profile"
	writeReq, err := authorize.Begin(ctx, writeInput)
	if err != nil {
		t.Fatal(err)
	}
	if writeReq.Scope != oauth.ScopeMCPRead+" "+oauth.ScopeMCPTrackerWrite {
		t.Fatalf("write authorize scope=%q", writeReq.Scope)
	}
	if _, err := ap.Decide(ctx, approvals.DecisionInput{ApprovalID: writeReq.ApprovalID, Decision: approvals.DecisionApproved, Reason: "owner approved writer", Actor: decider.Token}); err != nil {
		t.Fatal(err)
	}
	writeContinued, err := authorize.Continue(ctx, writeReq.ID)
	if err != nil {
		t.Fatal(err)
	}
	writePair, err := tokens.ExchangeCode(ctx, oauth.RedeemInput{Code: writeContinued.Code, ClientID: writeClient.ClientID, RedirectURI: writeInput.RedirectURI, CodeVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	if writePair.Scope != oauth.ScopeMCPRead+" "+oauth.ScopeMCPTrackerWrite {
		t.Fatalf("write token scope=%q", writePair.Scope)
	}

	// A decision at the request deadline is too late: no code is issued and
	// the request work item reaches a terminal state in the same transaction.
	clock := time.Now().UTC()
	clockedAuthorize := oauth.NewAuthorizationServiceWithClock(pool, writer, wi, ap, system.Token.ID, func() time.Time { return clock })
	lateInput := input
	lateInput.State = "late-approval"
	lateReq, err := clockedAuthorize.Begin(ctx, lateInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ap.Decide(ctx, approvals.DecisionInput{ApprovalID: lateReq.ApprovalID, Decision: approvals.DecisionApproved, Reason: "too late", Actor: decider.Token}); err != nil {
		t.Fatal(err)
	}
	clock = lateReq.ExpiresAt
	lateResult, err := clockedAuthorize.Continue(ctx, lateReq.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lateResult.Code != "" || lateResult.OAuthError == "" {
		t.Fatalf("late approval issued code: %+v", lateResult)
	}
	assertOAuthRequestTerminal(t, pool, lateReq, string(approvals.StatusApproved), string(domain.WorkItemFailed))

	// An undecided request that reaches its deadline expires its approval and
	// fails its work item atomically; it cannot remain pending forever.
	clock = time.Now().UTC()
	pendingInput := input
	pendingInput.State = "pending-expiry"
	pendingReq, err := clockedAuthorize.Begin(ctx, pendingInput)
	if err != nil {
		t.Fatal(err)
	}
	clock = pendingReq.ExpiresAt
	pendingResult, err := clockedAuthorize.Continue(ctx, pendingReq.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pendingResult.Code != "" || pendingResult.OAuthError == "" {
		t.Fatalf("expired pending request issued code: %+v", pendingResult)
	}
	assertOAuthRequestTerminal(t, pool, pendingReq, string(approvals.StatusExpired), string(domain.WorkItemFailed))
	var unattributed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind LIKE 'oauth_%' AND actor_token_id IS NULL`).Scan(&unattributed); err != nil || unattributed != 0 {
		t.Fatalf("unattributed oauth events=%d err=%v", unattributed, err)
	}
}

func assertOAuthRequestTerminal(t *testing.T, pool pgxQuerier, req oauth.AuthorizationRequest, wantApproval, wantWorkItem string) {
	t.Helper()
	var approvalStatus, itemState string
	var completedAt *time.Time
	if err := pool.QueryRow(context.Background(), `SELECT a.status,w.state,r.completed_at FROM approvals a JOIN work_items w ON w.id=a.work_item_id JOIN oauth_authorization_requests r ON r.approval_id=a.id WHERE a.id=$1`, req.ApprovalID).Scan(&approvalStatus, &itemState, &completedAt); err != nil {
		t.Fatal(err)
	}
	if approvalStatus != wantApproval || itemState != wantWorkItem || completedAt == nil {
		t.Fatalf("approval=%s work_item=%s completed=%v; want %s %s completed", approvalStatus, itemState, completedAt, wantApproval, wantWorkItem)
	}
}

type pgxQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func TestProviderOAuthRejectsMalformedInputs(t *testing.T) {
	if err := oauth.VerifyPKCE(strings.Repeat("!", 43), base64.RawURLEncoding.EncodeToString(make([]byte, 32))); !errors.Is(err, oauth.ErrInvalidGrant) {
		t.Fatalf("bad verifier charset=%v", err)
	}
	ctx := context.Background()
	_ = ctx
}
