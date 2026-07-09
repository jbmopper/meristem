package oauth

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the oauth-client projection writer to registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(registeredProjector{})
}

type registeredProjector struct{}

func (registeredProjector) Kind() string { return domain.EventOAuthClientRegistered }

// Apply folds an oauth_client.registered event into the `oauth_clients` table.
// A replay upserts the same row deterministically: created_at is preserved
// from the first registration (events fold in seq order), updated_at advances
// to this event.
func (registeredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectOAuthClient {
		return fmt.Errorf("oauth_client.registered: expected subject_kind %q, got %q", domain.SubjectOAuthClient, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyRegisteredV1(ctx, tx, event)
	default:
		return fmt.Errorf("oauth_client.registered: unknown payload_version %d", v)
	}
}

func applyRegisteredV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p registeredPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("oauth_client.registered: decode payload: %w", err)
	}
	if p.ClientID == "" {
		return fmt.Errorf("oauth_client.registered: client_id is required")
	}
	if p.TokenEndpointAuthMethod == "" {
		return fmt.Errorf("oauth_client.registered: token_endpoint_auth_method is required")
	}
	redirects, err := json.Marshal(nonNil(p.RedirectURIs))
	if err != nil {
		return fmt.Errorf("oauth_client.registered: encode redirect_uris: %w", err)
	}
	grants, err := json.Marshal(nonNil(p.GrantTypes))
	if err != nil {
		return fmt.Errorf("oauth_client.registered: encode grant_types: %w", err)
	}
	responses, err := json.Marshal(nonNil(p.ResponseTypes))
	if err != nil {
		return fmt.Errorf("oauth_client.registered: encode response_types: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO oauth_clients (
			client_id, client_name, redirect_uris, grant_types, response_types,
			token_endpoint_auth_method, scope, created_at, updated_at
		)
		VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6, $7, $8, $8)
		ON CONFLICT (client_id) DO UPDATE SET
			client_name = EXCLUDED.client_name,
			redirect_uris = EXCLUDED.redirect_uris,
			grant_types = EXCLUDED.grant_types,
			response_types = EXCLUDED.response_types,
			token_endpoint_auth_method = EXCLUDED.token_endpoint_auth_method,
			scope = EXCLUDED.scope,
			updated_at = EXCLUDED.updated_at
	`, p.ClientID, p.ClientName, redirects, grants, responses, p.TokenEndpointAuthMethod, p.Scope, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("oauth_client.registered: upsert projection: %w", err)
	}
	return nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
