package access

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestCanDecideHumanReviewRequiresExplicitScopedHuman(t *testing.T) {
	revoked := time.Now()
	scope := []string{ScopeWorkItemsReviewDecide}
	cases := []struct {
		name  string
		actor domain.Token
		want  bool
	}{
		{"scoped non-root human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: scope}, true},
		{"legacy unscoped human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman}, false},
		{"root with scope", domain.Token{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: scope}, false},
		{"agent with scope", domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: scope}, false},
		{"system with scope", domain.Token{ID: uuid.New(), Source: domain.SourceSystem, Scopes: scope}, false},
		{"revoked human with scope", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: scope, RevokedAt: &revoked}, false},
		{"tracker-only human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeWorkItemsTrackerWriteAll}}, false},
		{"ordinary writer human", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{ScopeWorkItemsWriteAll}}, false},
		{"zero token", domain.Token{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanDecideHumanReview(tc.actor); got != tc.want {
				t.Fatalf("CanDecideHumanReview = %t, want %t", got, tc.want)
			}
		})
	}
}
