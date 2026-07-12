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
