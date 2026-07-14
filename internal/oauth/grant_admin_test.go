package oauth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

// RevokeGrant rejects unauthorized actors and malformed input before touching
// the database, so these cases run without a pool.
func TestRevokeGrantValidation(t *testing.T) {
	scoped := domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsRevoke}}
	revoked := time.Now()
	cases := []struct {
		name    string
		grantID uuid.UUID
		reason  string
		actor   domain.Token
		want    error
	}{
		{"root denied", uuid.New(), "compromised", domain.Token{ID: uuid.New(), IsRoot: true, Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsRevoke}}, ErrOAuthClientAdminDenied},
		{"agent denied", uuid.New(), "compromised", domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeOAuthClientsRevoke}}, ErrOAuthClientAdminDenied},
		{"unscoped human denied", uuid.New(), "compromised", domain.Token{ID: uuid.New(), Source: domain.SourceHuman}, ErrOAuthClientAdminDenied},
		{"revoked human denied", uuid.New(), "compromised", domain.Token{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{access.ScopeOAuthClientsRevoke}, RevokedAt: &revoked}, ErrOAuthClientAdminDenied},
		{"nil grant id", uuid.Nil, "compromised", scoped, ErrInvalidClientAdminInput},
		{"empty reason", uuid.New(), "   ", scoped, ErrInvalidClientAdminInput},
	}
	svc := NewClientAdminService(nil, nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := svc.RevokeGrant(context.Background(), tc.grantID, tc.reason, tc.actor); !errors.Is(err, tc.want) {
				t.Fatalf("RevokeGrant err=%v, want %v", err, tc.want)
			}
		})
	}
}
