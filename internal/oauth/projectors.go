package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	registry.Register(clientActorBoundProjector{})
	registry.Register(clientActorBindingRequestedProjector{})
	registry.Register(clientRevokedProjector{})
	registry.Register(codeIssuedProjector{})
	registry.Register(codeRedeemedProjector{})
	registry.Register(authorizationRequestCreatedProjector{})
	registry.Register(authorizationRequestCompletedProjector{})
	registry.Register(grantIssuedProjector{})
	registry.Register(grantRefreshedProjector{})
	registry.Register(grantRevokedProjector{})
	registry.Register(refreshReuseProjector{})
}

type clientActorBindingRequestedProjector struct{}

func (clientActorBindingRequestedProjector) Kind() string {
	return domain.EventOAuthClientActorBindingRequested
}
func (clientActorBindingRequestedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		ClientID   string    `json:"client_id"`
		WorkItemID uuid.UUID `json:"work_item_id"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE oauth_clients SET binding_work_item_id=COALESCE(binding_work_item_id,$2), updated_at=$3 WHERE client_id=$1`, p.ClientID, p.WorkItemID, event.OccurredAt)
	return err
}

type grantIssuedProjector struct{}

func (grantIssuedProjector) Kind() string { return domain.EventOAuthGrantIssued }
func (grantIssuedProjector) Apply(ctx context.Context, tx pgx.Tx, e domain.Event) error {
	var p struct {
		GrantID              uuid.UUID `json:"grant_id"`
		ActorTokenID         uuid.UUID `json:"actor_token_id"`
		ClientID             string    `json:"client_id"`
		AuthorityProfile     string    `json:"authority_profile"`
		Scope                string    `json:"scope"`
		Resource             string    `json:"resource"`
		AccessTokenID        string    `json:"access_token_id"`
		AccessTokenHashB64   string    `json:"access_token_hash_b64"`
		RefreshTokenID       string    `json:"refresh_token_id"`
		RefreshTokenHashB64  string    `json:"refresh_token_hash_b64"`
		AccessExpiresAtUnix  int64     `json:"access_expires_at_unix"`
		RefreshExpiresAtUnix int64     `json:"refresh_expires_at_unix"`
		Generation           int       `json:"generation"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return err
	}
	if p.GrantID == uuid.Nil || p.ActorTokenID == uuid.Nil || p.ClientID == "" || p.AuthorityProfile == "" || p.Resource == "" || p.AccessTokenID == "" || p.RefreshTokenID == "" || p.AccessExpiresAtUnix <= 0 || p.RefreshExpiresAtUnix <= 0 || p.Generation != 1 {
		return errors.New("oauth_grant.issued: required field missing or invalid")
	}
	ah, err := base64.StdEncoding.DecodeString(p.AccessTokenHashB64)
	if err != nil {
		return err
	}
	rh, err := base64.StdEncoding.DecodeString(p.RefreshTokenHashB64)
	if err != nil {
		return err
	}
	re := time.Unix(p.RefreshExpiresAtUnix, 0).UTC()
	ae := time.Unix(p.AccessExpiresAtUnix, 0).UTC()
	if _, err = tx.Exec(ctx, `INSERT INTO oauth_grants(id,client_id,actor_token_id,authority_profile,scope,resource,refresh_expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT(id) DO NOTHING`, p.GrantID, p.ClientID, p.ActorTokenID, p.AuthorityProfile, p.Scope, p.Resource, re, e.OccurredAt); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO oauth_access_tokens(token_id,token_hash,grant_id,expires_at,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(token_id) DO NOTHING`, p.AccessTokenID, ah, p.GrantID, ae, e.OccurredAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO oauth_refresh_tokens(token_id,token_hash,grant_id,generation,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(token_id) DO NOTHING`, p.RefreshTokenID, rh, p.GrantID, p.Generation, re, e.OccurredAt)
	return err
}

type grantRefreshedProjector struct{}

func (grantRefreshedProjector) Kind() string { return domain.EventOAuthGrantRefreshed }
func (grantRefreshedProjector) Apply(ctx context.Context, tx pgx.Tx, e domain.Event) error {
	var p struct {
		OldRefreshTokenID      string `json:"old_refresh_token_id"`
		NewRefreshTokenID      string `json:"new_refresh_token_id"`
		NewRefreshTokenHashB64 string `json:"new_refresh_token_hash_b64"`
		AccessTokenID          string `json:"access_token_id"`
		AccessTokenHashB64     string `json:"access_token_hash_b64"`
		Generation             int    `json:"generation"`
		RefreshExpiresAtUnix   int64  `json:"refresh_expires_at_unix"`
		AccessExpiresAtUnix    int64  `json:"access_expires_at_unix"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return err
	}
	if p.OldRefreshTokenID == "" || p.NewRefreshTokenID == "" || p.AccessTokenID == "" || p.Generation < 2 {
		return errors.New("oauth_grant.refreshed: required field missing")
	}
	rh, err := base64.StdEncoding.DecodeString(p.NewRefreshTokenHashB64)
	if err != nil {
		return err
	}
	ah, err := base64.StdEncoding.DecodeString(p.AccessTokenHashB64)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE oauth_refresh_tokens SET used_at=$2 WHERE token_id=$1`, p.OldRefreshTokenID, e.OccurredAt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO oauth_refresh_tokens(token_id,token_hash,grant_id,generation,expires_at,created_at) SELECT $1,$2,grant_id,$3,$4,$5 FROM oauth_refresh_tokens WHERE token_id=$6 ON CONFLICT(token_id) DO NOTHING`, p.NewRefreshTokenID, rh, p.Generation, time.Unix(p.RefreshExpiresAtUnix, 0).UTC(), e.OccurredAt, p.OldRefreshTokenID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO oauth_access_tokens(token_id,token_hash,grant_id,expires_at,created_at) SELECT $1,$2,grant_id,$3,$4 FROM oauth_refresh_tokens WHERE token_id=$5 ON CONFLICT(token_id) DO NOTHING`, p.AccessTokenID, ah, time.Unix(p.AccessExpiresAtUnix, 0).UTC(), e.OccurredAt, p.OldRefreshTokenID)
	return err
}

type grantRevokedProjector struct{}

func (grantRevokedProjector) Kind() string { return domain.EventOAuthGrantRevoked }
func (grantRevokedProjector) Apply(ctx context.Context, tx pgx.Tx, e domain.Event) error {
	var p struct {
		RevokedAtUnix int64  `json:"revoked_at_unix"`
		Reason        string `json:"reason"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return err
	}
	at := time.Unix(p.RevokedAtUnix, 0).UTC()
	_, err := tx.Exec(ctx, `UPDATE oauth_grants SET revoked_at=COALESCE(revoked_at,$2),compromise_reason=$3,updated_at=$2 WHERE id=$1`, e.SubjectID, at, p.Reason)
	return err
}

type refreshReuseProjector struct{}

func (refreshReuseProjector) Kind() string                                      { return domain.EventOAuthRefreshReuseDetected }
func (refreshReuseProjector) Apply(context.Context, pgx.Tx, domain.Event) error { return nil }

type authorizationRequestCreatedProjector struct{}

func (authorizationRequestCreatedProjector) Kind() string {
	return domain.EventOAuthAuthorizationRequestCreated
}
func (authorizationRequestCreatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		WorkItemID          uuid.UUID `json:"work_item_id"`
		ApprovalID          uuid.UUID `json:"approval_id"`
		ClientID            string    `json:"client_id"`
		RedirectURI         string    `json:"redirect_uri"`
		ResponseType        string    `json:"response_type"`
		State               string    `json:"state"`
		CodeChallenge       string    `json:"code_challenge"`
		CodeChallengeMethod string    `json:"code_challenge_method"`
		Scope               string    `json:"scope"`
		Resource            string    `json:"resource"`
		ActorTokenID        uuid.UUID `json:"actor_token_id"`
		AuthorityProfile    string    `json:"authority_profile"`
		ExpiresAtUnix       int64     `json:"expires_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	if p.WorkItemID == uuid.Nil || p.ApprovalID == uuid.Nil || p.ClientID == "" || p.RedirectURI == "" || p.ActorTokenID == uuid.Nil || p.AuthorityProfile == "" || p.ExpiresAtUnix <= 0 {
		return errors.New("oauth_authorization_request.created: required field missing")
	}
	_, err := tx.Exec(ctx, `INSERT INTO oauth_authorization_requests(id,work_item_id,approval_id,client_id,redirect_uri,response_type,state,code_challenge,code_challenge_method,scope,resource,actor_token_id,authority_profile,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$15) ON CONFLICT(id) DO NOTHING`, event.SubjectID, p.WorkItemID, p.ApprovalID, p.ClientID, p.RedirectURI, p.ResponseType, p.State, p.CodeChallenge, p.CodeChallengeMethod, p.Scope, p.Resource, p.ActorTokenID, p.AuthorityProfile, time.Unix(p.ExpiresAtUnix, 0).UTC(), event.OccurredAt)
	return err
}

type authorizationRequestCompletedProjector struct{}

func (authorizationRequestCompletedProjector) Kind() string {
	return domain.EventOAuthAuthorizationRequestCompleted
}
func (authorizationRequestCompletedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		Outcome         string `json:"outcome"`
		CompletedAtUnix int64  `json:"completed_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	at := time.Unix(p.CompletedAtUnix, 0).UTC()
	_, err := tx.Exec(ctx, `UPDATE oauth_authorization_requests SET completed_at=$2,outcome=$3,updated_at=$2 WHERE id=$1`, event.SubjectID, at, p.Outcome)
	return err
}

type clientActorBoundProjector struct{}

func (clientActorBoundProjector) Kind() string { return domain.EventOAuthClientActorBound }
func (clientActorBoundProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		ClientID         string    `json:"client_id"`
		ActorTokenID     uuid.UUID `json:"actor_token_id"`
		AuthorityProfile string    `json:"authority_profile"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	if p.ClientID == "" || p.ActorTokenID == uuid.Nil || p.AuthorityProfile == "" {
		return fmt.Errorf("oauth_client.actor_bound: client_id, actor_token_id, and authority_profile required")
	}
	_, err := tx.Exec(ctx, `UPDATE oauth_clients SET actor_token_id=$2, authority_profile=$3, updated_at=$4 WHERE client_id=$1`, p.ClientID, p.ActorTokenID, p.AuthorityProfile, event.OccurredAt)
	return err
}

type clientRevokedProjector struct{}

func (clientRevokedProjector) Kind() string { return domain.EventOAuthClientRevoked }
func (clientRevokedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var p struct {
		ClientID      string `json:"client_id"`
		RevokedAtUnix int64  `json:"revoked_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return err
	}
	if p.ClientID == "" || p.RevokedAtUnix == 0 {
		return fmt.Errorf("oauth_client.revoked: client_id and revoked_at required")
	}
	at := time.Unix(p.RevokedAtUnix, 0).UTC()
	_, err := tx.Exec(ctx, `UPDATE oauth_clients SET revoked_at=COALESCE(revoked_at,$2), updated_at=$2 WHERE client_id=$1`, p.ClientID, at)
	return err
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
			code_challenge_method, scope, resource, actor_token_id, authority_profile, expires_at,
			redeemed_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, $12, $12)
		ON CONFLICT (code_id) DO NOTHING
	`, p.CodeID, hash, p.ClientID, p.RedirectURI, p.CodeChallenge,
		p.CodeChallengeMethod, p.Scope, p.Resource, p.ActorTokenID, p.AuthorityProfile, expiresAt, event.OccurredAt)
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
