package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// The stdio MCP session authenticates once and lives arbitrarily long;
// cached authority must not outlive the token (ee916614 slice 3a round-1
// finding). Every protected dispatch revalidates revocation and
// database-clock expiry by token id.
func TestStdioSessionRevalidatesTokenAuthority(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "revalidation-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	root := rootResult.Token
	newServer := func() *Server {
		return New(Deps{
			Auth:        authSvc,
			Access:      access.NewService(pool),
			Idempotency: idempotency.NewMiddleware(pool, writer),
			WorkItems:   workitems.NewService(pool, writer),
			Feed:        feed.NewService(pool),
		}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	}
	listTools := `{"jsonrpc":"2.0","id":7,"method":"tools/list"}`

	// Revocation bites mid-session: the already-initialized server refuses
	// the very next protected call.
	revocable, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "revalidation-agent", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsReadAll}, Actor: &root,
	})
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	s := newServer()
	if err := s.Authenticate(ctx, revocable.Secret); err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if resp := roundtrip(t, s, listTools); resp.Error != nil {
		t.Fatalf("pre-revocation tools/list = %+v, want success", resp.Error)
	}
	if err := authSvc.Revoke(ctx, revocable.Token.ID, root); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if resp := roundtrip(t, s, listTools); resp.Error == nil || !strings.Contains(resp.Error.Message, "revoked") {
		t.Fatalf("post-revocation tools/list = %+v, want revoked refusal", resp.Error)
	}

	// Expiry bites mid-session on the database clock, no re-handshake
	// required: mint a real short-lived reviewer-style credential and watch
	// the initialized session lose authority when it lapses.
	issuerResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "revalidation-issuer", Source: domain.SourceSystem,
		Scopes: []string{auth.ScopeReviewerCredentialsIssue}, Actor: &root,
	})
	if err != nil {
		t.Fatalf("create issuer: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "expiring session target", Actor: root,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	minted, err := authSvc.MintReviewerCredential(ctx, tx, auth.MintReviewerCredentialInput{
		Name: "expiring-session-credential", ChildID: item.ID,
		TemplateScopes: []string{access.ScopeWorkItemsRead, "work_items.tree:{root}"},
		ExpiresAt:      time.Now().Add(1500 * time.Millisecond),
		Actor:          issuerResult.Token,
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("mint expiring credential: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	expiring := newServer()
	if err := expiring.Authenticate(ctx, minted.Secret); err != nil {
		t.Fatalf("authenticate expiring credential: %v", err)
	}
	if resp := roundtrip(t, expiring, listTools); resp.Error != nil {
		t.Fatalf("pre-expiry tools/list = %+v, want success", resp.Error)
	}
	time.Sleep(1700 * time.Millisecond)
	if resp := roundtrip(t, expiring, listTools); resp.Error == nil || !strings.Contains(resp.Error.Message, "expired") {
		t.Fatalf("post-expiry tools/list = %+v, want expired refusal", resp.Error)
	}
}
