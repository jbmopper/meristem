package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds the oauth projection writers to registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(registeredProjector{})
	registry.Register(codeIssuedProjector{})
	registry.Register(codeRedeemedProjector{})
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

type codeIssuedProjector struct{}

func (codeIssuedProjector) Kind() string { return domain.EventOAuthAuthorizationCodeIssued }

// Apply folds an oauth_authorization_code.issued event into the
// oauth_authorization_codes table as an insert. A replay upserts the same row
// (created_at preserved from the first issue).
func (codeIssuedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectOAuthAuthorizationCode {
		return fmt.Errorf("oauth_authorization_code.issued: expected subject_kind %q, got %q", domain.SubjectOAuthAuthorizationCode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyCodeIssuedV1(ctx, tx, event)
	default:
		return fmt.Errorf("oauth_authorization_code.issued: unknown payload_version %d", v)
	}
}

func applyCodeIssuedV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p issuedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("oauth_authorization_code.issued: decode payload: %w", err)
	}
	if p.CodeID == "" || p.CodeHashB64 == "" {
		return fmt.Errorf("oauth_authorization_code.issued: code_id and code_hash are required")
	}
	hash, err := base64.StdEncoding.DecodeString(p.CodeHashB64)
	if err != nil {
		return fmt.Errorf("oauth_authorization_code.issued: decode code_hash: %w", err)
	}
	if p.ActorTokenID == uuid.Nil {
		return fmt.Errorf("oauth_authorization_code.issued: actor_token_id is required")
	}
	if p.Resource == "" {
		return fmt.Errorf("oauth_authorization_code.issued: resource is required")
	}
	expiresAt := time.Unix(p.ExpiresAtUnix, 0).UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO oauth_authorization_codes (
			code_id, code_hash, client_id, redirect_uri, code_challenge,
			code_challenge_method, scope, resource, actor_token_id, expires_at,
			redeemed_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $11)
		ON CONFLICT (code_id) DO NOTHING
	`, p.CodeID, hash, p.ClientID, p.RedirectURI, p.CodeChallenge,
		p.CodeChallengeMethod, p.Scope, p.Resource, p.ActorTokenID, expiresAt, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("oauth_authorization_code.issued: insert projection: %w", err)
	}
	return nil
}

type codeRedeemedProjector struct{}

func (codeRedeemedProjector) Kind() string { return domain.EventOAuthAuthorizationCodeRedeemed }

// Apply folds an oauth_authorization_code.redeemed event by stamping
// redeemed_at on the code row. The redeem service already guards single-use;
// this makes the one-time state durable and rebuild-deterministic.
func (codeRedeemedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectOAuthAuthorizationCode {
		return fmt.Errorf("oauth_authorization_code.redeemed: expected subject_kind %q, got %q", domain.SubjectOAuthAuthorizationCode, event.SubjectKind)
	}
	switch v := payloadVersion(event.Payload); v {
	case 1:
		return applyCodeRedeemedV1(ctx, tx, event)
	default:
		return fmt.Errorf("oauth_authorization_code.redeemed: unknown payload_version %d", v)
	}
}

func applyCodeRedeemedV1(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p redeemedPayload
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("oauth_authorization_code.redeemed: decode payload: %w", err)
	}
	if p.CodeID == "" {
		return fmt.Errorf("oauth_authorization_code.redeemed: code_id is required")
	}
	redeemedAt := time.Unix(p.RedeemedAtUnix, 0).UTC()
	// COALESCE keeps the first redemption's timestamp on replay.
	_, err := tx.Exec(ctx, `
		UPDATE oauth_authorization_codes
		SET redeemed_at = COALESCE(redeemed_at, $2), updated_at = $2
		WHERE code_id = $1
	`, p.CodeID, redeemedAt)
	if err != nil {
		return fmt.Errorf("oauth_authorization_code.redeemed: update projection: %w", err)
	}
	return nil
}
