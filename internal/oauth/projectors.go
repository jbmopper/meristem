package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jbmopper/meristem/internal/access"
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
	const kind = domain.EventOAuthClientActorBindingRequested
	if err := requireOAuthSubject(event, domain.SubjectOAuthClient, kind); err != nil {
		return err
	}
	if v := payloadVersion(event.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		ClientID   string    `json:"client_id"`
		WorkItemID uuid.UUID `json:"work_item_id"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.ClientID == "" || p.WorkItemID == uuid.Nil || event.SubjectID != ClientSubjectID(p.ClientID) {
		return fmt.Errorf("%s: client_id/work_item_id required and subject_id must match client_id", kind)
	}
	tag, err := tx.Exec(ctx, `UPDATE oauth_clients SET binding_work_item_id=COALESCE(binding_work_item_id,$2), updated_at=$3 WHERE client_id=$1`, p.ClientID, p.WorkItemID, event.OccurredAt)
	return requireOneRow(kind, "oauth_clients dependency", tag, err)
}

type grantIssuedProjector struct{}

func (grantIssuedProjector) Kind() string { return domain.EventOAuthGrantIssued }
func (grantIssuedProjector) Apply(ctx context.Context, tx pgx.Tx, e domain.Event) error {
	const kind = domain.EventOAuthGrantIssued
	if err := requireOAuthSubject(e, domain.SubjectOAuthGrant, kind); err != nil {
		return err
	}
	if v := payloadVersion(e.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
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
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.GrantID == uuid.Nil || e.SubjectID != p.GrantID || p.ActorTokenID == uuid.Nil || p.ClientID == "" || !validProfileOAuthScope(p.AuthorityProfile, p.Scope) || p.Resource == "" || p.AccessTokenID == "" || p.RefreshTokenID == "" || p.AccessExpiresAtUnix <= 0 || p.RefreshExpiresAtUnix <= 0 || p.AccessExpiresAtUnix > p.RefreshExpiresAtUnix || p.Generation != 1 {
		return fmt.Errorf("%s: required field missing, invalid, or subject_id does not match grant_id", kind)
	}
	ah, err := decodeSHA256(kind, "access_token_hash_b64", p.AccessTokenHashB64)
	if err != nil {
		return err
	}
	rh, err := decodeSHA256(kind, "refresh_token_hash_b64", p.RefreshTokenHashB64)
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
	const kind = domain.EventOAuthGrantRefreshed
	if err := requireOAuthSubject(e, domain.SubjectOAuthGrant, kind); err != nil {
		return err
	}
	if v := payloadVersion(e.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		GrantID                uuid.UUID `json:"grant_id"`
		OldRefreshTokenID      string    `json:"old_refresh_token_id"`
		NewRefreshTokenID      string    `json:"new_refresh_token_id"`
		NewRefreshTokenHashB64 string    `json:"new_refresh_token_hash_b64"`
		AccessTokenID          string    `json:"access_token_id"`
		AccessTokenHashB64     string    `json:"access_token_hash_b64"`
		Generation             int       `json:"generation"`
		RefreshExpiresAtUnix   int64     `json:"refresh_expires_at_unix"`
		AccessExpiresAtUnix    int64     `json:"access_expires_at_unix"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.GrantID == uuid.Nil || e.SubjectID != p.GrantID || p.OldRefreshTokenID == "" || p.NewRefreshTokenID == "" || p.OldRefreshTokenID == p.NewRefreshTokenID || p.AccessTokenID == "" || p.Generation < 2 || p.RefreshExpiresAtUnix <= 0 || p.AccessExpiresAtUnix <= 0 || p.AccessExpiresAtUnix > p.RefreshExpiresAtUnix {
		return fmt.Errorf("%s: required field missing, invalid, or subject_id does not match grant_id", kind)
	}
	rh, err := decodeSHA256(kind, "new_refresh_token_hash_b64", p.NewRefreshTokenHashB64)
	if err != nil {
		return err
	}
	ah, err := decodeSHA256(kind, "access_token_hash_b64", p.AccessTokenHashB64)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE oauth_refresh_tokens SET used_at=COALESCE(used_at,$3) WHERE token_id=$1 AND grant_id=$2`, p.OldRefreshTokenID, p.GrantID, e.OccurredAt)
	if err := requireOneRow(kind, "old refresh token dependency", tag, err); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO oauth_refresh_tokens(token_id,token_hash,grant_id,generation,expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(token_id) DO NOTHING`, p.NewRefreshTokenID, rh, p.GrantID, p.Generation, time.Unix(p.RefreshExpiresAtUnix, 0).UTC(), e.OccurredAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO oauth_access_tokens(token_id,token_hash,grant_id,expires_at,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(token_id) DO NOTHING`, p.AccessTokenID, ah, p.GrantID, time.Unix(p.AccessExpiresAtUnix, 0).UTC(), e.OccurredAt)
	return err
}

type grantRevokedProjector struct{}

func (grantRevokedProjector) Kind() string { return domain.EventOAuthGrantRevoked }
func (grantRevokedProjector) Apply(ctx context.Context, tx pgx.Tx, e domain.Event) error {
	const kind = domain.EventOAuthGrantRevoked
	if err := requireOAuthSubject(e, domain.SubjectOAuthGrant, kind); err != nil {
		return err
	}
	if v := payloadVersion(e.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		GrantID       uuid.UUID `json:"grant_id"`
		RevokedAtUnix int64     `json:"revoked_at_unix"`
		Reason        string    `json:"reason"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.GrantID == uuid.Nil || p.GrantID != e.SubjectID || p.RevokedAtUnix <= 0 || p.Reason == "" {
		return fmt.Errorf("%s: grant_id, revoked_at_unix, and reason required and subject_id must match grant_id", kind)
	}
	at := time.Unix(p.RevokedAtUnix, 0).UTC()
	tag, err := tx.Exec(ctx, `UPDATE oauth_grants SET revoked_at=COALESCE(revoked_at,$2),compromise_reason=$3,updated_at=$2 WHERE id=$1`, e.SubjectID, at, p.Reason)
	return requireOneRow(kind, "oauth_grants dependency", tag, err)
}

type refreshReuseProjector struct{}

func (refreshReuseProjector) Kind() string { return domain.EventOAuthRefreshReuseDetected }
func (refreshReuseProjector) Apply(_ context.Context, _ pgx.Tx, e domain.Event) error {
	const kind = domain.EventOAuthRefreshReuseDetected
	if err := requireOAuthSubject(e, domain.SubjectOAuthGrant, kind); err != nil {
		return err
	}
	if v := payloadVersion(e.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		GrantID        uuid.UUID `json:"grant_id"`
		TokenID        string    `json:"token_id"`
		DetectedAtUnix int64     `json:"detected_at_unix"`
		Reason         string    `json:"reason"`
	}
	if err := decode(e.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.GrantID == uuid.Nil || p.GrantID != e.SubjectID || p.TokenID == "" || p.DetectedAtUnix <= 0 || p.Reason == "" {
		return fmt.Errorf("%s: grant_id, token_id, detected_at_unix, and reason required and subject_id must match grant_id", kind)
	}
	return nil
}

type authorizationRequestCreatedProjector struct{}

func (authorizationRequestCreatedProjector) Kind() string {
	return domain.EventOAuthAuthorizationRequestCreated
}
func (authorizationRequestCreatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	const kind = domain.EventOAuthAuthorizationRequestCreated
	if err := requireOAuthSubject(event, domain.SubjectOAuthAuthorizationRequest, kind); err != nil {
		return err
	}
	if v := payloadVersion(event.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		AuthorizationRequestID uuid.UUID `json:"authorization_request_id"`
		WorkItemID             uuid.UUID `json:"work_item_id"`
		ApprovalID             uuid.UUID `json:"approval_id"`
		ClientID               string    `json:"client_id"`
		RedirectURI            string    `json:"redirect_uri"`
		ResponseType           string    `json:"response_type"`
		State                  string    `json:"state"`
		CodeChallenge          string    `json:"code_challenge"`
		CodeChallengeMethod    string    `json:"code_challenge_method"`
		Scope                  string    `json:"scope"`
		Resource               string    `json:"resource"`
		ActorTokenID           uuid.UUID `json:"actor_token_id"`
		AuthorityProfile       string    `json:"authority_profile"`
		ExpiresAtUnix          int64     `json:"expires_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.AuthorizationRequestID == uuid.Nil || p.AuthorizationRequestID != event.SubjectID || p.WorkItemID == uuid.Nil || p.ApprovalID == uuid.Nil || p.ClientID == "" || p.RedirectURI == "" || p.ResponseType != ResponseTypeCode || p.ActorTokenID == uuid.Nil || !validProfileOAuthScope(p.AuthorityProfile, p.Scope) || p.Resource == "" || p.ExpiresAtUnix <= 0 {
		return fmt.Errorf("%s: required field missing, enum invalid, or subject_id does not match authorization_request_id", kind)
	}
	if err := ValidateCodeChallenge(p.CodeChallenge, p.CodeChallengeMethod); err != nil {
		return fmt.Errorf("%s: invalid PKCE binding: %w", kind, err)
	}
	_, err := tx.Exec(ctx, `INSERT INTO oauth_authorization_requests(id,work_item_id,approval_id,client_id,redirect_uri,response_type,state,code_challenge,code_challenge_method,scope,resource,actor_token_id,authority_profile,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$15) ON CONFLICT(id) DO NOTHING`, event.SubjectID, p.WorkItemID, p.ApprovalID, p.ClientID, p.RedirectURI, p.ResponseType, p.State, p.CodeChallenge, p.CodeChallengeMethod, p.Scope, p.Resource, p.ActorTokenID, p.AuthorityProfile, time.Unix(p.ExpiresAtUnix, 0).UTC(), event.OccurredAt)
	return err
}

type authorizationRequestCompletedProjector struct{}

func (authorizationRequestCompletedProjector) Kind() string {
	return domain.EventOAuthAuthorizationRequestCompleted
}
func (authorizationRequestCompletedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	const kind = domain.EventOAuthAuthorizationRequestCompleted
	if err := requireOAuthSubject(event, domain.SubjectOAuthAuthorizationRequest, kind); err != nil {
		return err
	}
	if v := payloadVersion(event.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		AuthorizationRequestID uuid.UUID `json:"authorization_request_id"`
		Outcome                string    `json:"outcome"`
		CompletedAtUnix        int64     `json:"completed_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.AuthorizationRequestID == uuid.Nil || p.AuthorizationRequestID != event.SubjectID || p.CompletedAtUnix <= 0 || (p.Outcome != "approved" && p.Outcome != "denied" && p.Outcome != "expired") {
		return fmt.Errorf("%s: authorization_request_id/completed_at_unix required, outcome invalid, or subject_id mismatch", kind)
	}
	at := time.Unix(p.CompletedAtUnix, 0).UTC()
	tag, err := tx.Exec(ctx, `UPDATE oauth_authorization_requests SET completed_at=COALESCE(completed_at,$2),outcome=COALESCE(outcome,$3),updated_at=$2 WHERE id=$1`, event.SubjectID, at, p.Outcome)
	return requireOneRow(kind, "oauth_authorization_requests dependency", tag, err)
}

type clientActorBoundProjector struct{}

func (clientActorBoundProjector) Kind() string { return domain.EventOAuthClientActorBound }
func (clientActorBoundProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	const kind = domain.EventOAuthClientActorBound
	if err := requireOAuthSubject(event, domain.SubjectOAuthClient, kind); err != nil {
		return err
	}
	if v := payloadVersion(event.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		ClientID         string    `json:"client_id"`
		ActorTokenID     uuid.UUID `json:"actor_token_id"`
		AuthorityProfile string    `json:"authority_profile"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.ClientID == "" || event.SubjectID != ClientSubjectID(p.ClientID) || p.ActorTokenID == uuid.Nil || !validAuthorityProfile(p.AuthorityProfile) {
		return fmt.Errorf("oauth_client.actor_bound: client_id, actor_token_id, and authority_profile required")
	}
	tag, err := tx.Exec(ctx, `UPDATE oauth_clients SET actor_token_id=$2, authority_profile=$3, updated_at=$4 WHERE client_id=$1`, p.ClientID, p.ActorTokenID, p.AuthorityProfile, event.OccurredAt)
	return requireOneRow(kind, "oauth_clients dependency", tag, err)
}

type clientRevokedProjector struct{}

func (clientRevokedProjector) Kind() string { return domain.EventOAuthClientRevoked }
func (clientRevokedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	const kind = domain.EventOAuthClientRevoked
	if err := requireOAuthSubject(event, domain.SubjectOAuthClient, kind); err != nil {
		return err
	}
	if v := payloadVersion(event.Payload); v != 1 {
		return fmt.Errorf("%s: unknown payload_version %d", kind, v)
	}
	var p struct {
		ClientID      string `json:"client_id"`
		RevokedAtUnix int64  `json:"revoked_at_unix"`
	}
	if err := decode(event.Payload, &p); err != nil {
		return fmt.Errorf("%s: decode payload: %w", kind, err)
	}
	if p.ClientID == "" || event.SubjectID != ClientSubjectID(p.ClientID) || p.RevokedAtUnix <= 0 {
		return fmt.Errorf("oauth_client.revoked: client_id and revoked_at required")
	}
	at := time.Unix(p.RevokedAtUnix, 0).UTC()
	tag, err := tx.Exec(ctx, `UPDATE oauth_clients SET revoked_at=COALESCE(revoked_at,$2), updated_at=$2 WHERE client_id=$1`, p.ClientID, at)
	return requireOneRow(kind, "oauth_clients dependency", tag, err)
}

type registeredProjector struct{}

func (registeredProjector) Kind() string { return domain.EventOAuthClientRegistered }

// Apply folds an oauth_client.registered event into the `oauth_clients` table.
// A replay upserts the same row deterministically: created_at is preserved
// from the first registration (events fold in seq order), updated_at advances
// to this event.
func (registeredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if err := requireOAuthSubject(event, domain.SubjectOAuthClient, domain.EventOAuthClientRegistered); err != nil {
		return err
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
	if event.SubjectID != ClientSubjectID(p.ClientID) {
		return fmt.Errorf("oauth_client.registered: subject_id does not match client_id")
	}
	if p.TokenEndpointAuthMethod != AuthMethodNone {
		return fmt.Errorf("oauth_client.registered: token_endpoint_auth_method must be %q", AuthMethodNone)
	}
	normalizedScope, err := normalizeRegistrationScope(p.Scope)
	if err != nil || (p.Scope != "" && normalizedScope != p.Scope) {
		return fmt.Errorf("oauth_client.registered: invalid or non-canonical scope %q", p.Scope)
	}
	legacyGrantSet := len(p.GrantTypes) == 1 && p.GrantTypes[0] == GrantAuthorizationCode
	currentGrantSet := len(p.GrantTypes) == 2 && p.GrantTypes[0] == GrantAuthorizationCode && p.GrantTypes[1] == GrantRefreshToken
	if len(p.RedirectURIs) == 0 || len(p.RedirectURIs) > MaxRedirectURIs || (!legacyGrantSet && !currentGrantSet) || len(p.ResponseTypes) != 1 || p.ResponseTypes[0] != ResponseTypeCode {
		return fmt.Errorf("oauth_client.registered: invalid redirect, grant, or response metadata")
	}
	if _, err := validateRedirectURIs(p.RedirectURIs); err != nil {
		return fmt.Errorf("oauth_client.registered: %w", err)
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
	if err := requireOAuthSubject(event, domain.SubjectOAuthAuthorizationCode, domain.EventOAuthAuthorizationCodeIssued); err != nil {
		return err
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
	if p.CodeID == "" || event.SubjectID != CodeSubjectID(p.CodeID) || p.ClientID == "" || p.RedirectURI == "" || !validProfileOAuthScope(p.AuthorityProfile, p.Scope) || p.ExpiresAtUnix <= 0 {
		return fmt.Errorf("oauth_authorization_code.issued: required field missing or subject_id does not match code_id")
	}
	hash, err := decodeSHA256(domain.EventOAuthAuthorizationCodeIssued, "code_hash_b64", p.CodeHashB64)
	if err != nil {
		return err
	}
	if p.ActorTokenID == uuid.Nil {
		return fmt.Errorf("oauth_authorization_code.issued: actor_token_id is required")
	}
	if p.Resource == "" {
		return fmt.Errorf("oauth_authorization_code.issued: resource is required")
	}
	if err := ValidateCodeChallenge(p.CodeChallenge, p.CodeChallengeMethod); err != nil {
		return fmt.Errorf("oauth_authorization_code.issued: invalid PKCE binding: %w", err)
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
	if err := requireOAuthSubject(event, domain.SubjectOAuthAuthorizationCode, domain.EventOAuthAuthorizationCodeRedeemed); err != nil {
		return err
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
	if p.CodeID == "" || event.SubjectID != CodeSubjectID(p.CodeID) || p.RedeemedAtUnix <= 0 {
		return fmt.Errorf("oauth_authorization_code.redeemed: code_id/redeemed_at_unix required and subject_id must match code_id")
	}
	redeemedAt := time.Unix(p.RedeemedAtUnix, 0).UTC()
	// grantID is the grant minted from this redemption; NULL for codes redeemed
	// before the link existed. COALESCE keeps the first redemption's timestamp
	// and grant link on replay.
	var grantID any
	if p.GrantID != uuid.Nil {
		grantID = p.GrantID
	}
	tag, err := tx.Exec(ctx, `
		UPDATE oauth_authorization_codes
		SET redeemed_at = COALESCE(redeemed_at, $2), grant_id = COALESCE(grant_id, $3), updated_at = $2
		WHERE code_id = $1
	`, p.CodeID, redeemedAt, grantID)
	return requireOneRow(domain.EventOAuthAuthorizationCodeRedeemed, "oauth_authorization_codes dependency", tag, err)
}

func requireOAuthSubject(event domain.Event, wantSubject, kind string) error {
	if event.SubjectKind != wantSubject {
		return fmt.Errorf("%s: expected subject_kind %q, got %q", kind, wantSubject, event.SubjectKind)
	}
	if event.SubjectID == uuid.Nil {
		return fmt.Errorf("%s: subject_id is required", kind)
	}
	return nil
}

func decodeSHA256(kind, field, encoded string) ([]byte, error) {
	hash, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%s: decode %s: %w", kind, field, err)
	}
	if len(hash) != 32 {
		return nil, fmt.Errorf("%s: %s must encode a 32-byte SHA-256 digest", kind, field)
	}
	return hash, nil
}

func validAuthorityProfile(profile string) bool {
	switch access.ProviderAuthorityProfile(profile) {
	case access.ProviderOwnerTrackerReadV1, access.ProviderOwnerTrackerWriteV1, access.ProviderDelegatedTreeReadV1, access.ProviderDelegatedTreeWriteV1:
		return true
	default:
		return false
	}
}

func validProfileOAuthScope(profile, scope string) bool {
	expected, err := OAuthScopeForAuthorityProfile(access.ProviderAuthorityProfile(profile))
	return err == nil && scope == expected
}

func requireOneRow(kind, dependency string, tag pgconn.CommandTag, err error) error {
	if err != nil {
		return fmt.Errorf("%s: update %s: %w", kind, dependency, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: expected one %s row, affected %d", kind, dependency, tag.RowsAffected())
	}
	return nil
}
