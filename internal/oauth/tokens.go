package oauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

const AccessTokenTTL = time.Hour
const RefreshTokenTTL = 30 * 24 * time.Hour

var ErrInvalidAccessToken = errors.New("oauth: invalid access token")
var ErrRefreshReuse = errors.New("oauth: refresh token reuse detected")

type TokenService struct {
	pool          *pgxpool.Pool
	writer        *events.Writer
	systemActorID uuid.UUID
	now           func() time.Time
}

func NewTokenService(pool *pgxpool.Pool, writer *events.Writer, systemActorID uuid.UUID) *TokenService {
	return &TokenService{pool: pool, writer: writer, systemActorID: systemActorID, now: time.Now}
}

type TokenPair struct {
	AccessToken  string
	RefreshToken string
	TokenType    string
	ExpiresIn    int
	Scope        string
}

func tokenSecret(prefix string) (string, string, []byte, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", "", nil, err
	}
	s := prefix + base64.RawURLEncoding.EncodeToString(b[:])
	h := sha256.Sum256([]byte(s))
	return s, hex.EncodeToString(h[:]), h[:], nil
}

func (s *TokenService) ExchangeCode(ctx context.Context, in RedeemInput) (TokenPair, error) {
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return TokenPair{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TokenPair{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	hash := HashCode(in.Code)
	var codeID, clientID, redirectURI, challenge, challengeMethod, scope, resource, authorityProfile string
	var actorID uuid.UUID
	var expires time.Time
	var redeemed *time.Time
	err = tx.QueryRow(ctx, `SELECT code_id,client_id,redirect_uri,code_challenge,code_challenge_method,scope,resource,actor_token_id,authority_profile,expires_at,redeemed_at FROM oauth_authorization_codes WHERE code_hash=$1 FOR UPDATE`, hash).Scan(&codeID, &clientID, &redirectURI, &challenge, &challengeMethod, &scope, &resource, &actorID, &authorityProfile, &expires, &redeemed)
	if err != nil {
		return TokenPair{}, fmt.Errorf("%w: unknown code", ErrInvalidGrant)
	}
	if redeemed != nil || !s.now().UTC().Before(expires) || in.ClientID != clientID || in.RedirectURI != redirectURI {
		return TokenPair{}, fmt.Errorf("%w: code expired, used, or binding mismatch", ErrInvalidGrant)
	}
	if err := verifyStoredS256(in.CodeVerifier, challenge, challengeMethod); err != nil {
		return TokenPair{}, err
	}
	client, err := GetClient(ctx, s.pool, clientID)
	if err != nil || client.RevokedAt != nil || client.ActorTokenID == nil || *client.ActorTokenID != actorID {
		return TokenPair{}, fmt.Errorf("%w: client or actor binding inactive", ErrInvalidGrant)
	}
	providerActor, err := validateProviderActor(ctx, s.pool, actorID)
	if err != nil {
		return TokenPair{}, err
	}
	sealedProfile, err := access.ProviderAuthorityProfileFromScopes(providerActor.Scopes)
	if err != nil || string(sealedProfile) != authorityProfile {
		return TokenPair{}, fmt.Errorf("%w: authority profile no longer matches actor scopes", ErrInvalidGrant)
	}
	access, accessID, accessHash, err := tokenSecret("mcpat_")
	if err != nil {
		return TokenPair{}, err
	}
	refresh, refreshID, refreshHash, err := tokenSecret("mcprt_")
	if err != nil {
		return TokenPair{}, err
	}
	now := s.now().UTC()
	accessExpires := now.Add(AccessTokenTTL)
	refreshExpires := now.Add(RefreshTokenTTL)
	grantID := uuid.New()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthAuthorizationCode, SubjectID: CodeSubjectID(codeID), Kind: domain.EventOAuthAuthorizationCodeRedeemed, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: redeemedPayload{PayloadVersion: 1, CodeID: codeID, RedeemedAtUnix: now.Unix()}})
	if err != nil {
		return TokenPair{}, err
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, Kind: domain.EventOAuthGrantIssued, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "grant_id": grantID, "client_id": clientID, "actor_token_id": actorID, "authority_profile": authorityProfile, "scope": scope, "resource": resource, "access_token_id": accessID, "access_token_hash_b64": base64.StdEncoding.EncodeToString(accessHash), "access_expires_at_unix": accessExpires.Unix(), "refresh_token_id": refreshID, "refresh_token_hash_b64": base64.StdEncoding.EncodeToString(refreshHash), "refresh_expires_at_unix": refreshExpires.Unix(), "generation": 1}})
	if err != nil {
		return TokenPair{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: refresh, TokenType: "Bearer", ExpiresIn: int(accessExpires.Sub(now).Seconds()), Scope: scope}, nil
}

func (s *TokenService) Refresh(ctx context.Context, secret, clientID string) (TokenPair, error) {
	systemActor, err := loadActor(ctx, s.pool, s.systemActorID, domain.SourceSystem)
	if err != nil {
		return TokenPair{}, err
	}
	h := sha256.Sum256([]byte(secret))
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TokenPair{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tokenID string
	var grantID uuid.UUID
	var generation int
	var tokenExpiry, grantExpiry time.Time
	var used, revoked *time.Time
	var storedClient, scope, resource, grantProfile, currentProfile string
	var grantActor, currentActor *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT rt.token_id,rt.grant_id,rt.generation,rt.expires_at,rt.used_at,g.client_id,g.scope,g.resource,g.refresh_expires_at,g.revoked_at,g.actor_token_id,g.authority_profile,c.actor_token_id,c.authority_profile FROM oauth_refresh_tokens rt JOIN oauth_grants g ON g.id=rt.grant_id JOIN oauth_clients c ON c.client_id=g.client_id WHERE rt.token_hash=$1 FOR UPDATE OF rt,g`, h[:]).Scan(&tokenID, &grantID, &generation, &tokenExpiry, &used, &storedClient, &scope, &resource, &grantExpiry, &revoked, &grantActor, &grantProfile, &currentActor, &currentProfile)
	if err != nil {
		return TokenPair{}, fmt.Errorf("%w: unknown refresh token", ErrInvalidGrant)
	}
	now := s.now().UTC()
	if used != nil {
		if _, _, err := s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, Kind: domain.EventOAuthRefreshReuseDetected, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Payload: map[string]any{"payload_version": 1, "grant_id": grantID, "token_id": tokenID, "detected_at_unix": now.Unix(), "reason": "rotated refresh token replayed"}}); err != nil {
			return TokenPair{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return TokenPair{}, err
		}
		return TokenPair{}, ErrRefreshReuse
	}
	if revoked != nil || !now.Before(tokenExpiry) || !now.Before(grantExpiry) || clientID != storedClient || grantActor == nil || currentActor == nil || *grantActor != *currentActor || grantProfile != currentProfile {
		return TokenPair{}, fmt.Errorf("%w: refresh token expired, revoked, or client mismatch", ErrInvalidGrant)
	}
	client, err := GetClient(ctx, s.pool, storedClient)
	if err != nil || client.RevokedAt != nil {
		return TokenPair{}, fmt.Errorf("%w: client inactive", ErrInvalidGrant)
	}
	access, accessID, accessHash, err := tokenSecret("mcpat_")
	if err != nil {
		return TokenPair{}, err
	}
	refresh, refreshID, refreshHash, err := tokenSecret("mcprt_")
	if err != nil {
		return TokenPair{}, err
	}
	accessExpires := now.Add(AccessTokenTTL)
	if accessExpires.After(grantExpiry) {
		accessExpires = grantExpiry
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthGrant, SubjectID: grantID, Kind: domain.EventOAuthGrantRefreshed, Source: domain.SourceSystem, ActorTokenID: &systemActor.ID, Discriminator: tokenID, Payload: map[string]any{"payload_version": 1, "grant_id": grantID, "old_refresh_token_id": tokenID, "new_refresh_token_id": refreshID, "new_refresh_token_hash_b64": base64.StdEncoding.EncodeToString(refreshHash), "generation": generation + 1, "refresh_expires_at_unix": grantExpiry.Unix(), "access_token_id": accessID, "access_token_hash_b64": base64.StdEncoding.EncodeToString(accessHash), "access_expires_at_unix": accessExpires.Unix()}})
	if err != nil {
		return TokenPair{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, err
	}
	return TokenPair{AccessToken: access, RefreshToken: refresh, TokenType: "Bearer", ExpiresIn: int(accessExpires.Sub(now).Seconds()), Scope: scope}, nil
}

func (s *TokenService) AuthenticateAccess(ctx context.Context, secret, expectedResource string) (domain.Token, error) {
	if !validTokenSecret(secret, "mcpat_") {
		return domain.Token{}, ErrInvalidAccessToken
	}
	h := sha256.Sum256([]byte(secret))
	var tok domain.Token
	var scopesJSON, storedHash []byte
	var source string
	var tokenExpiry, grantExpiry time.Time
	var grantRevoked, clientRevoked *time.Time
	var scope, resource, authorityProfile, currentProfile string
	var currentActor *uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT t.id,t.name,t.is_root,t.scopes,t.source,t.created_at,t.revoked_at,a.token_hash,a.expires_at,g.refresh_expires_at,g.revoked_at,c.revoked_at,g.scope,g.resource,g.authority_profile,c.actor_token_id,c.authority_profile FROM oauth_access_tokens a JOIN oauth_grants g ON g.id=a.grant_id JOIN oauth_clients c ON c.client_id=g.client_id JOIN tokens t ON t.id=g.actor_token_id WHERE a.token_hash=$1`, h[:]).Scan(&tok.ID, &tok.Name, &tok.IsRoot, &scopesJSON, &source, &tok.CreatedAt, &tok.RevokedAt, &storedHash, &tokenExpiry, &grantExpiry, &grantRevoked, &clientRevoked, &scope, &resource, &authorityProfile, &currentActor, &currentProfile)
	if err != nil {
		return domain.Token{}, ErrInvalidAccessToken
	}
	tok.Source = domain.Source(source)
	if !auth.EqualHash(storedHash, h[:]) {
		return domain.Token{}, ErrInvalidAccessToken
	}
	if err := json.Unmarshal(scopesJSON, &tok.Scopes); err != nil {
		return domain.Token{}, ErrInvalidAccessToken
	}
	profile, err := access.ProviderAuthorityProfileFromScopes(tok.Scopes)
	if err != nil || string(profile) != authorityProfile || currentActor == nil || *currentActor != tok.ID || currentProfile != authorityProfile {
		return domain.Token{}, ErrInvalidAccessToken
	}
	now := s.now().UTC()
	if tok.IsRoot || tok.Source != domain.SourceAgent || tok.RevokedAt != nil || grantRevoked != nil || clientRevoked != nil || !now.Before(tokenExpiry) || !now.Before(grantExpiry) || resource != expectedResource || normalizeScopeContains(scope, ScopeMCPRead) == false {
		return domain.Token{}, ErrInvalidAccessToken
	}
	return tok, nil
}

func validTokenSecret(secret, prefix string) bool {
	if !strings.HasPrefix(secret, prefix) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(secret, prefix))
	return err == nil && len(raw) == 32
}

func normalizeScopeContains(raw, want string) bool {
	for _, s := range strings.Fields(raw) {
		if s == want {
			return true
		}
	}
	return false
}
