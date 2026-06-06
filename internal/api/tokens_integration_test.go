package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestPanicRevokeTokensIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	authSvc := auth.NewService(pool, app.NewEventWriter())
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "panic-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token
	agentA, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "panic-agent-a",
		Source: domain.SourceAgent,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create agent A token: %v", err)
	}
	agentB, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "panic-agent-b",
		Source: domain.SourceAgent,
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create agent B token: %v", err)
	}

	server := New(pool, nil)
	beforeDenied := totalEventCount(t, pool)
	denied := doREST(t, server.Handler(), http.MethodPost, "/v1/tokens/revoke-all", agentA.Secret, "panic-denied", nil)
	assertRESTStatus(t, denied, http.StatusForbidden)
	assertErrorCode(t, denied, "root_token_required")
	if after := totalEventCount(t, pool); after != beforeDenied {
		t.Fatalf("denied panic revoke appended events: before=%d after=%d body=%s", beforeDenied, after, denied.Body.String())
	}

	first := doREST(t, server.Handler(), http.MethodPost, "/v1/tokens/revoke-all", rootResult.Secret, "panic-root", nil)
	assertRESTStatus(t, first, http.StatusOK)
	firstBody := append([]byte(nil), first.Body.Bytes()...)
	var firstResp struct {
		RevokedCount int      `json:"revoked_count"`
		Revoked      []string `json:"revoked"`
	}
	decodeResponse(t, first, &firstResp)
	if firstResp.RevokedCount != 2 {
		t.Fatalf("revoked_count = %d, want 2; body=%s", firstResp.RevokedCount, first.Body.String())
	}
	if !containsString(firstResp.Revoked, agentA.Token.ID.String()) || !containsString(firstResp.Revoked, agentB.Token.ID.String()) {
		t.Fatalf("panic revoke response missing token ids: %+v", firstResp.Revoked)
	}
	if _, err := authSvc.Authenticate(ctx, rootResult.Secret); err != nil {
		t.Fatalf("root token should remain active after panic revoke: %v", err)
	}
	if _, err := authSvc.Authenticate(ctx, agentA.Secret); !errors.Is(err, auth.ErrTokenRevoked) {
		t.Fatalf("agent A authenticate error = %v, want %v", err, auth.ErrTokenRevoked)
	}
	if _, err := authSvc.Authenticate(ctx, agentB.Secret); !errors.Is(err, auth.ErrTokenRevoked) {
		t.Fatalf("agent B authenticate error = %v, want %v", err, auth.ErrTokenRevoked)
	}
	assertEventCount(t, pool, domain.EventTokenRevoked, 2)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)

	replay := doREST(t, server.Handler(), http.MethodPost, "/v1/tokens/revoke-all", rootResult.Secret, "panic-root", nil)
	assertRESTStatus(t, replay, http.StatusOK)
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("expected replay header, got headers=%v", replay.Header())
	}
	if !bytes.Equal(firstBody, replay.Body.Bytes()) {
		t.Fatalf("replay body diverged:\nfirst=%s\nreplay=%s", string(firstBody), replay.Body.String())
	}
	assertEventCount(t, pool, domain.EventTokenRevoked, 2)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
