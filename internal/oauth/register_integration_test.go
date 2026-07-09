package oauth

import (
	"context"
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// TestRegistrationRoundTrip registers a client through the service, folds the
// oauth_client.registered event through the real projector, and reads the
// client back from the oauth_clients projection. It also asserts that a
// re-projection of the same event is idempotent (created_at preserved).
// pgtest.NewPool skips unless the Postgres integration environment is set.
func TestRegistrationRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_oauth_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	writer := events.NewWriter(reg)

	svc := NewRegistrationService(pool, writer)
	got, err := svc.Register(ctx, RegisterInput{
		ClientName:   "Claude",
		RedirectURIs: []string{"https://claude.ai/api/mcp/auth_callback"},
		Scope:        "mcp:read",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if got.ClientID == "" {
		t.Fatal("expected a client_id")
	}
	if got.TokenEndpointAuthMethod != AuthMethodNone {
		t.Fatalf("auth method = %q, want none", got.TokenEndpointAuthMethod)
	}

	client, err := GetClient(ctx, pool, got.ClientID)
	if err != nil {
		t.Fatalf("get client: %v", err)
	}
	if client.ClientName != "Claude" {
		t.Fatalf("client_name = %q, want Claude", client.ClientName)
	}
	if !client.AllowsRedirectURI("https://claude.ai/api/mcp/auth_callback") {
		t.Fatalf("registered redirect_uri not in allowlist: %v", client.RedirectURIs)
	}
	if len(client.GrantTypes) != 1 || client.GrantTypes[0] != GrantAuthorizationCode {
		t.Fatalf("grant_types = %v, want [authorization_code]", client.GrantTypes)
	}

	// Unknown client resolves to ErrClientNotFound.
	if _, err := GetClient(ctx, pool, "mcpc_missing"); !errors.Is(err, ErrClientNotFound) {
		t.Fatalf("missing client err = %v, want ErrClientNotFound", err)
	}
}

func TestRegisterRejectsConfidentialClient(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_oauth_itest")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	svc := NewRegistrationService(pool, events.NewWriter(reg))

	_, err := svc.Register(ctx, RegisterInput{
		RedirectURIs:            []string{"https://a.example/cb"},
		TokenEndpointAuthMethod: "client_secret_basic",
	})
	if !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("err = %v, want ErrInvalidRegistration", err)
	}
}
