package access

// CanAdminListeners is the single listener-administration authority (LCP2-B3).
// Like OAuth-client administration it is strict: explicitly scoped, non-root
// human — the legacy unscoped compatibility path deliberately does not apply,
// and advertisement (ToolVisible) uses the same reducer, evaluated before the
// root/legacy shortcut.

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestCanAdminListeners(t *testing.T) {
	revoked := time.Now()
	cases := []struct {
		name  string
		actor domain.Token
		want  bool
	}{
		{"scoped non-root human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeListenersAdmin}}, true},
		{"legacy unscoped human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman}, false},
		{"root with the scope", domain.Token{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: []string{ScopeListenersAdmin}}, false},
		{"agent with the scope", domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{ScopeListenersAdmin}}, false},
		{"system with the scope", domain.Token{ID: uuid.New(), Source: domain.SourceSystem, Scopes: []string{ScopeListenersAdmin}}, false},
		{"revoked scoped human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeListenersAdmin}, RevokedAt: &revoked}, false},
		{"human with other scopes only", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeWorkItemsWriteAll}}, false},
		{"zero token", domain.Token{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanAdminListeners(c.actor); got != c.want {
				t.Errorf("CanAdminListeners = %v, want %v", got, c.want)
			}
		})
	}
}

func TestListenerAdminToolAdvertisementUsesReducer(t *testing.T) {
	scopedHuman := domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeListenersAdmin}}
	legacyHuman := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}
	rootToken := domain.Token{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman}
	for _, tool := range []string{"listeners.create", "listeners.bind_credential", "listeners.retire"} {
		if !ToolVisible(scopedHuman, tool) {
			t.Errorf("scoped human missing %s", tool)
		}
		// The root/legacy shortcut does not apply: advertisement is the
		// reducer, so tokens the service will refuse never see the tool.
		if ToolVisible(legacyHuman, tool) {
			t.Errorf("legacy unscoped human sees %s the service would refuse", tool)
		}
		if ToolVisible(rootToken, tool) {
			t.Errorf("root sees %s the service would refuse", tool)
		}
	}
}
