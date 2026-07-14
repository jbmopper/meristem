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
	pool          *pgxpool.Pool
	writer        *events.Writer
	now           func() time.Time
	systemActorID uuid.UUID
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
	SystemActorTokenID  uuid.UUID
	AuthorityProfile    string
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
	if in.SystemActorTokenID == uuid.Nil {
		in.SystemActorTokenID = s.systemActorID
	}
	if in.SystemActorTokenID == uuid.Nil {
		return "", fmt.Errorf("%w: system_actor_token_id is required for attribution", ErrInvalidGrant)
	}
	if err := ValidateCodeChallenge(in.CodeChallenge, in.CodeChallengeMethod); err != nil {
		return "", err
	}
	if in.Resource == "" {
		return "", fmt.Errorf("%w: resource (audience) is required", ErrInvalidGrant)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	secret, err := s.issueInTx(ctx, tx, in)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return secret, nil
}

func (s *AuthCodeService) issueInTx(ctx context.Context, tx pgx.Tx, in IssueInput) (string, error) {
	secret, codeID, hash, err := generateCode()
	if err != nil {
		return "", err
	}
	expiresAt := s.now().UTC().Add(CodeTTLSeconds * time.Second)
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectOAuthAuthorizationCode,
		SubjectID:    CodeSubjectID(codeID),
		Kind:         domain.EventOAuthAuthorizationCodeIssued,
		Source:       domain.SourceSystem,
		ActorTokenID: &in.SystemActorTokenID,
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
			AuthorityProfile:    in.AuthorityProfile,
			ExpiresAtUnix:       expiresAt.Unix(),
		},
	}); err != nil {
		return "", fmt.Errorf("oauth: append authorization_code.issued: %w", err)
	}
	return secret, nil
}

// RedeemInput is a token-endpoint code-exchange request.
type RedeemInput struct {
	Code               string
	ClientID           string
	RedirectURI        string
	CodeVerifier       string
	SystemActorTokenID uuid.UUID
}
