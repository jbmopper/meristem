package oauth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func newAuthCodeService(t *testing.T, now func() time.Time) (*AuthCodeService, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_oauth_authcode_itest")
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
	svc := NewAuthCodeService(pool, writer)
	svc.systemActorID = system.Token.ID
	if now != nil {
		svc.now = now
	}
	return svc, ctx
}

func TestAuthCodeIssueRedeemRoundTrip(t *testing.T) {
	svc, ctx := newAuthCodeService(t, nil)
	verifier := strings.Repeat("v", 60)
	actor := uuid.New()

	code, err := svc.Issue(ctx, IssueInput{
		ClientID:            "mcpc_abc",
		RedirectURI:         "https://claude.ai/cb",
		CodeChallenge:       s256Challenge(verifier),
		CodeChallengeMethod: "S256",
		Scope:               "mcp:read",
		Resource:            "https://mcp.example.com/mcp",
		ActorTokenID:        actor,
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !strings.HasPrefix(code, "mcpa_") {
		t.Fatalf("unexpected code shape %q", code)
	}

	res, err := svc.Redeem(ctx, RedeemInput{
		Code:         code,
		ClientID:     "mcpc_abc",
		RedirectURI:  "https://claude.ai/cb",
		CodeVerifier: verifier,
	})
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}
	if res.ActorTokenID != actor {
		t.Fatalf("actor = %s, want %s", res.ActorTokenID, actor)
	}
	if res.Scope != "mcp:read" || res.Resource != "https://mcp.example.com/mcp" {
		t.Fatalf("unexpected grant: %+v", res)
	}

	// One-time: a second redemption of the same code must fail.
	if _, err := svc.Redeem(ctx, RedeemInput{
		Code:         code,
		ClientID:     "mcpc_abc",
		RedirectURI:  "https://claude.ai/cb",
		CodeVerifier: verifier,
	}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("second redeem err = %v, want ErrInvalidGrant (already redeemed)", err)
	}
}

func TestAuthCodeRedeemRejectsBadInputs(t *testing.T) {
	verifier := strings.Repeat("v", 60)
	issue := func(svc *AuthCodeService, ctx context.Context, actor uuid.UUID) string {
		code, err := svc.Issue(ctx, IssueInput{
			ClientID:            "mcpc_abc",
			RedirectURI:         "https://claude.ai/cb",
			CodeChallenge:       s256Challenge(verifier),
			CodeChallengeMethod: "S256",
			Resource:            "https://mcp.example.com/mcp",
			ActorTokenID:        actor,
		})
		if err != nil {
			t.Fatalf("issue: %v", err)
		}
		return code
	}

	t.Run("wrong pkce verifier", func(t *testing.T) {
		svc, ctx := newAuthCodeService(t, nil)
		code := issue(svc, ctx, uuid.New())
		_, err := svc.Redeem(ctx, RedeemInput{Code: code, ClientID: "mcpc_abc", RedirectURI: "https://claude.ai/cb", CodeVerifier: strings.Repeat("w", 60)})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("redirect mismatch", func(t *testing.T) {
		svc, ctx := newAuthCodeService(t, nil)
		code := issue(svc, ctx, uuid.New())
		_, err := svc.Redeem(ctx, RedeemInput{Code: code, ClientID: "mcpc_abc", RedirectURI: "https://evil.example/cb", CodeVerifier: verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("client mismatch", func(t *testing.T) {
		svc, ctx := newAuthCodeService(t, nil)
		code := issue(svc, ctx, uuid.New())
		_, err := svc.Redeem(ctx, RedeemInput{Code: code, ClientID: "mcpc_other", RedirectURI: "https://claude.ai/cb", CodeVerifier: verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})

	t.Run("unknown code", func(t *testing.T) {
		svc, ctx := newAuthCodeService(t, nil)
		_, err := svc.Redeem(ctx, RedeemInput{Code: "mcpa_nope", ClientID: "mcpc_abc", RedirectURI: "https://claude.ai/cb", CodeVerifier: verifier})
		if !errors.Is(err, ErrInvalidGrant) {
			t.Fatalf("err = %v, want ErrInvalidGrant", err)
		}
	})
}

func TestAuthCodeExpiry(t *testing.T) {
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	clock := base
	svc, ctx := newAuthCodeService(t, func() time.Time { return clock })
	verifier := strings.Repeat("v", 60)

	code, err := svc.Issue(ctx, IssueInput{
		ClientID:            "mcpc_abc",
		RedirectURI:         "https://claude.ai/cb",
		CodeChallenge:       s256Challenge(verifier),
		CodeChallengeMethod: "S256",
		Resource:            "https://mcp.example.com/mcp",
		ActorTokenID:        uuid.New(),
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	// Advance past the code TTL.
	clock = base.Add((CodeTTLSeconds + 5) * time.Second)
	if _, err := svc.Redeem(ctx, RedeemInput{Code: code, ClientID: "mcpc_abc", RedirectURI: "https://claude.ai/cb", CodeVerifier: verifier}); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("expired redeem err = %v, want ErrInvalidGrant", err)
	}
}
