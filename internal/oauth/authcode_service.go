package oauth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

// ErrInvalidGrant maps to the OAuth invalid_grant / invalid_request family: a
// bad PKCE challenge, an expired or already-redeemed code, or a client/redirect
// mismatch at redemption. Callers return an OAuth 400 error object.
var ErrInvalidGrant = errors.New("oauth: invalid grant")

// AuthCodeService issues and redeems authorization codes, each backed by
// oauth_authorization_code.issued / .redeemed events folding into the
// oauth_authorization_codes projection.
type AuthCodeService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
	now    func() time.Time
}

func NewAuthCodeService(pool *pgxpool.Pool, writer *events.Writer) *AuthCodeService {
	return &AuthCodeService{pool: pool, writer: writer, now: time.Now}
}

// IssueInput is the validated authorize-request state captured at owner consent.
// actorTokenID is the meristem actor the eventual access token attributes to,
// so provider traffic never collapses to one shared actor.
type IssueInput struct {
	ClientID            string
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string
	Scope               string
	Resource            string
	ActorTokenID        uuid.UUID
}

// Issue mints a one-time authorization code and records its issue event. The
// raw code is returned once for the redirect; only its hash is persisted.
func (s *AuthCodeService) Issue(ctx context.Context, in IssueInput) (string, error) {
	if s.pool == nil || s.writer == nil {
		return "", errors.New("oauth: authcode service is not configured")
	}
	if in.ActorTokenID == uuid.Nil {
		return "", fmt.Errorf("%w: actor_token_id is required for attribution", ErrInvalidGrant)
	}
	if err := ValidateCodeChallenge(in.CodeChallenge, in.CodeChallengeMethod); err != nil {
		return "", err
	}
	if in.Resource == "" {
		return "", fmt.Errorf("%w: resource (audience) is required", ErrInvalidGrant)
	}

	secret, codeID, hash, err := generateCode()
	if err != nil {
		return "", err
	}
	issuedAt := s.now().UTC()
	expiresAt := issuedAt.Add(CodeTTLSeconds * time.Second)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectOAuthAuthorizationCode,
		SubjectID:   CodeSubjectID(codeID),
		Kind:        domain.EventOAuthAuthorizationCodeIssued,
		Source:      domain.SourceSystem,
		Payload: issuedPayload{
			PayloadVersion:      1,
			CodeID:              codeID,
			CodeHashB64:         b64(hash),
			ClientID:            in.ClientID,
			RedirectURI:         in.RedirectURI,
			CodeChallenge:       in.CodeChallenge,
			CodeChallengeMethod: in.CodeChallengeMethod,
			Scope:               in.Scope,
			Resource:            in.Resource,
			ActorTokenID:        in.ActorTokenID,
			ExpiresAtUnix:       expiresAt.Unix(),
		},
	}); err != nil {
		return "", fmt.Errorf("oauth: append authorization_code.issued: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return secret, nil
}

// RedeemInput is a token-endpoint code-exchange request.
type RedeemInput struct {
	Code         string
	ClientID     string
	RedirectURI  string
	CodeVerifier string
}

// RedeemResult is what the token endpoint needs to mint an access token: the
// attributed meristem actor, the granted scope, and the bound audience.
type RedeemResult struct {
	ActorTokenID uuid.UUID
	Scope        string
	Resource     string
}

// Redeem consumes an authorization code exactly once. It verifies the code
// exists, is unexpired and unredeemed, matches the presenting client and
// redirect_uri, and satisfies PKCE, then records the redeem event. One-time
// use is enforced by the redeemed_at projection guard plus the deterministic
// redeem event id.
func (s *AuthCodeService) Redeem(ctx context.Context, in RedeemInput) (RedeemResult, error) {
	if s.pool == nil || s.writer == nil {
		return RedeemResult{}, errors.New("oauth: authcode service is not configured")
	}
	if in.Code == "" {
		return RedeemResult{}, fmt.Errorf("%w: code is required", ErrInvalidGrant)
	}
	hash := HashCode(in.Code)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RedeemResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// SELECT ... FOR UPDATE serializes concurrent redemptions of the same code
	// so the redeemed_at guard cannot race.
	var (
		codeID       string
		clientID     string
		redirectURI  string
		challenge    string
		method       string
		scope        string
		resource     string
		actorTokenID uuid.UUID
		expiresAt    time.Time
		redeemedAt   *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT code_id, client_id, redirect_uri, code_challenge, code_challenge_method,
		       scope, resource, actor_token_id, expires_at, redeemed_at
		FROM oauth_authorization_codes
		WHERE code_hash = $1
		FOR UPDATE
	`, hash).Scan(&codeID, &clientID, &redirectURI, &challenge, &method,
		&scope, &resource, &actorTokenID, &expiresAt, &redeemedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RedeemResult{}, fmt.Errorf("%w: unknown authorization code", ErrInvalidGrant)
		}
		return RedeemResult{}, fmt.Errorf("oauth: load authorization code: %w", err)
	}

	if redeemedAt != nil {
		return RedeemResult{}, fmt.Errorf("%w: authorization code already redeemed", ErrInvalidGrant)
	}
	if s.now().UTC().After(expiresAt) {
		return RedeemResult{}, fmt.Errorf("%w: authorization code expired", ErrInvalidGrant)
	}
	if in.ClientID != "" && in.ClientID != clientID {
		return RedeemResult{}, fmt.Errorf("%w: client_id does not match the code", ErrInvalidGrant)
	}
	if in.RedirectURI != redirectURI {
		return RedeemResult{}, fmt.Errorf("%w: redirect_uri does not match the code", ErrInvalidGrant)
	}
	if err := VerifyPKCE(in.CodeVerifier, challenge); err != nil {
		return RedeemResult{}, err
	}

	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectOAuthAuthorizationCode,
		SubjectID:   CodeSubjectID(codeID),
		Kind:        domain.EventOAuthAuthorizationCodeRedeemed,
		Source:      domain.SourceSystem,
		Payload: redeemedPayload{
			PayloadVersion: 1,
			CodeID:         codeID,
			RedeemedAtUnix: s.now().UTC().Unix(),
		},
	}); err != nil {
		return RedeemResult{}, fmt.Errorf("oauth: append authorization_code.redeemed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RedeemResult{}, err
	}

	return RedeemResult{ActorTokenID: actorTokenID, Scope: scope, Resource: resource}, nil
}
