package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestOAuthProjectionsRebuildFromApprovedGrantLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)
	root, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-rebuild-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	systemActor, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "oauth-rebuild-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system actor: %v", err)
	}
	admin, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "oauth-rebuild-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeOAuthClientsBind}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create client admin: %v", err)
	}
	decider, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "oauth-rebuild-decider", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeApprovalsDecide}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create approval decider: %v", err)
	}
	authority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerWriteV1, uuid.Nil)
	if err != nil {
		t.Fatalf("reduce provider authority: %v", err)
	}
	providerActor, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "oauth-rebuild-provider", Source: domain.SourceAgent,
		Scopes: authority.Scopes, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create provider actor: %v", err)
	}

	registration := oauth.NewRegistrationServiceWithSystemActor(pool, writer, systemActor.Token.ID)
	client, err := registration.Register(ctx, oauth.RegisterInput{
		ClientName: "UNTRUSTED rebuild client", RedirectURIs: []string{"https://provider.example/callback"},
		Scope: oauth.ScopeMCPRead + " " + oauth.ScopeMCPTrackerWrite,
	})
	if err != nil {
		t.Fatalf("register client: %v", err)
	}
	clientAdmin := oauth.NewClientAdminService(pool, writer)
	if err := clientAdmin.BindActor(ctx, client.ClientID, providerActor.Token.ID, string(access.ProviderOwnerTrackerWriteV1), admin.Token); err != nil {
		t.Fatalf("bind provider actor: %v", err)
	}

	workItemService := workitems.NewService(pool, writer)
	approvalService := approvals.NewService(pool, writer)
	authorization := oauth.NewAuthorizationService(pool, writer, workItemService, approvalService, systemActor.Token.ID)
	verifier := strings.Repeat("v", 60)
	challenge := sha256.Sum256([]byte(verifier))
	authorizationRequest, err := authorization.Begin(ctx, oauth.AuthorizationInput{
		ClientID: client.ClientID, RedirectURI: "https://provider.example/callback", ResponseType: oauth.ResponseTypeCode,
		State: "oauth-rebuild-state", CodeChallenge: base64.RawURLEncoding.EncodeToString(challenge[:]), CodeChallengeMethod: oauth.PKCEMethodS256,
		Scope: oauth.ScopeMCPRead + " " + oauth.ScopeMCPTrackerWrite, Resource: "https://mcp.example/mcp", ExpectedResource: "https://mcp.example/mcp",
	})
	if err != nil {
		t.Fatalf("begin authorization: %v", err)
	}
	if _, err := approvalService.Decide(ctx, approvals.DecisionInput{
		ApprovalID: authorizationRequest.ApprovalID, Decision: approvals.DecisionApproved,
		Reason: "approved for rebuild proof", Actor: decider.Token,
	}); err != nil {
		t.Fatalf("approve authorization: %v", err)
	}
	continued, err := authorization.Continue(ctx, authorizationRequest.ID)
	if err != nil || continued.Code == "" {
		t.Fatalf("continue authorization: result=%+v err=%v", continued, err)
	}
	tokenService := oauth.NewTokenService(pool, writer, systemActor.Token.ID)
	pair, err := tokenService.ExchangeCode(ctx, oauth.RedeemInput{
		Code: continued.Code, ClientID: client.ClientID,
		RedirectURI: "https://provider.example/callback", CodeVerifier: verifier,
	})
	if err != nil {
		t.Fatalf("exchange code: %v", err)
	}
	if _, err := tokenService.Refresh(ctx, pair.RefreshToken, client.ClientID); err != nil {
		t.Fatalf("rotate refresh token: %v", err)
	}

	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "oauth_rebuild", slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err != nil {
		t.Fatalf("rebuild OAuth projections: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("OAuth rebuild had mismatches: %+v", report.mismatches)
	}
}

// TestOAuthRebuildToleratesLegacyGrantIssuedWithoutCodeID proves the
// deterministic rebuild re-folds a pre-code-linking oauth_grant.issued event
// — one carrying no code_id, exactly as ExchangeCode wrote it before migration
// 0034 — without aborting. Before the fix, grantIssuedProjector hard-required
// code_id, so the whole-log re-fold (and the synchronous write-apply) rejected
// any such legacy event.
func TestOAuthRebuildToleratesLegacyGrantIssuedWithoutCodeID(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)
	root, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "legacy-grant-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	actor, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "legacy-grant-actor", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	grantID := uuid.New()
	accessHash := sha256.Sum256([]byte("legacy-access-token"))
	refreshHash := sha256.Sum256([]byte("legacy-refresh-token"))
	now := time.Now().UTC()
	// Legacy shape: every field the projector needs EXCEPT code_id.
	legacy := events.Spec{
		SubjectKind:  domain.SubjectOAuthGrant,
		SubjectID:    grantID,
		Kind:         domain.EventOAuthGrantIssued,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.Token.ID,
		Payload: map[string]any{
			"payload_version":         1,
			"grant_id":                grantID,
			"client_id":               "legacy-client",
			"actor_token_id":          actor.Token.ID,
			"authority_profile":       string(access.ProviderOwnerTrackerReadV1),
			"scope":                   oauth.ScopeMCPRead,
			"resource":                "https://mcp.example/mcp",
			"access_token_id":         "mcpat_legacy",
			"access_token_hash_b64":   base64.StdEncoding.EncodeToString(accessHash[:]),
			"access_expires_at_unix":  now.Add(time.Hour).Unix(),
			"refresh_token_id":        "mcprt_legacy",
			"refresh_token_hash_b64":  base64.StdEncoding.EncodeToString(refreshHash[:]),
			"refresh_expires_at_unix": now.Add(24 * time.Hour).Unix(),
			"generation":              1,
			// no code_id — pre-migration-0034 event payload
		},
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, _, err := writer.Append(ctx, tx, legacy); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append legacy code_id-less grant.issued: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy event: %v", err)
	}

	// The re-fold must succeed and match: before the fix it aborted here.
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "oauth_rebuild_legacy", slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err != nil {
		t.Fatalf("rebuild aborted on legacy code_id-less grant.issued: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("legacy grant rebuild had mismatches: %+v", report.mismatches)
	}
}
