package access

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestOAuthClientAdminRequiresExplicitScopedHuman(t *testing.T) {
	human := domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeOAuthClientsBind, ScopeOAuthClientsRevoke}}
	if !CanBindOAuthClient(human) || !CanRevokeOAuthClient(human) {
		t.Fatal("scoped human was denied OAuth client administration")
	}
	for name, actor := range map[string]domain.Token{
		"root":     {ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: human.Scopes},
		"agent":    {ID: uuid.New(), Source: domain.SourceAgent, Scopes: human.Scopes},
		"unscoped": {ID: uuid.New(), Source: domain.SourceHuman},
		"revoked":  {ID: uuid.New(), Source: domain.SourceHuman, Scopes: human.Scopes, RevokedAt: func() *time.Time { now := time.Now(); return &now }()},
	} {
		if CanBindOAuthClient(actor) || CanRevokeOAuthClient(actor) {
			t.Errorf("%s actor gained OAuth client administration", name)
		}
	}
}

func TestOAuthClientAdminToolsUseTheSameStrictPolicy(t *testing.T) {
	bindOnly := domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeOAuthClientsBind}}
	if !ToolVisible(bindOnly, "oauth_clients.bind_actor") {
		t.Fatal("bind-scoped human cannot see bind tool")
	}
	if ToolVisible(bindOnly, "oauth_clients.revoke") || ToolVisible(bindOnly, "oauth_grants.revoke") {
		t.Fatal("bind-only human can see a revoke tool")
	}
	// Grant revocation reuses the client revoke scope, so a revoke-scoped human
	// sees it and a bind-only human does not.
	revokeOnly := domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeOAuthClientsRevoke}}
	if !ToolVisible(revokeOnly, "oauth_clients.revoke") || !ToolVisible(revokeOnly, "oauth_grants.revoke") {
		t.Fatal("revoke-scoped human cannot see grant revoke tool")
	}
	for name, actor := range map[string]domain.Token{
		"root":     {ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: []string{ScopeOAuthClientsBind, ScopeOAuthClientsRevoke}},
		"agent":    {ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{ScopeOAuthClientsBind, ScopeOAuthClientsRevoke}},
		"unscoped": {ID: uuid.New(), Source: domain.SourceHuman},
	} {
		if ToolVisible(actor, "oauth_clients.bind_actor") || ToolVisible(actor, "oauth_clients.revoke") || ToolVisible(actor, "oauth_grants.revoke") {
			t.Errorf("%s actor can see OAuth client administration tools", name)
		}
	}
}
