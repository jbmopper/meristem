package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var (
	ErrOAuthClientAdminDenied  = errors.New("oauth: client administration denied")
	ErrInvalidClientAdminInput = errors.New("oauth: invalid client administration input")
	ErrOAuthClientConflict     = errors.New("oauth: client administration conflict")
)

type ClientAdminService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewClientAdminService(pool *pgxpool.Pool, writer *events.Writer) *ClientAdminService {
	return &ClientAdminService{pool: pool, writer: writer}
}

func (s *ClientAdminService) BindActor(ctx context.Context, clientID string, actorID uuid.UUID, authorityProfile string, actor domain.Token) error {
	if !access.CanBindOAuthClient(actor) {
		return ErrOAuthClientAdminDenied
	}
	clientID = strings.TrimSpace(clientID)
	authorityProfile = strings.TrimSpace(authorityProfile)
	if clientID == "" || actorID == uuid.Nil || authorityProfile == "" {
		return fmt.Errorf("%w: client_id, actor_token_id, and authority_profile are required", ErrInvalidClientAdminInput)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the target token first. Concurrent attempts to bind the same actor
	// to different clients therefore serialize even without a projection-only
	// uniqueness constraint.
	providerActor, err := loadProviderActorForUpdate(ctx, tx, actorID)
	if err != nil {
		return err
	}
	sealed, err := access.ProviderAuthorityProfileFromScopes(providerActor.Scopes)
	if err != nil || string(sealed) != authorityProfile {
		return fmt.Errorf("%w: actor scopes do not exactly match sealed authority profile %q", ErrInvalidClientAdminInput, authorityProfile)
	}
	expectedOAuthScope, err := OAuthScopeForAuthorityProfile(sealed)
	if err != nil {
		return fmt.Errorf("%w: invalid authority profile", ErrInvalidClientAdminInput)
	}

	var clientScope, currentProfile string
	var currentActor *uuid.UUID
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT scope,actor_token_id,authority_profile,revoked_at FROM oauth_clients WHERE client_id=$1 FOR UPDATE`, clientID).Scan(&clientScope, &currentActor, &currentProfile, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrClientNotFound
	}
	if err != nil {
		return err
	}
	if revokedAt != nil {
		return fmt.Errorf("%w: client is revoked", ErrOAuthClientConflict)
	}
	if !registrationScopeAllows(clientScope, expectedOAuthScope) {
		return fmt.Errorf("%w: registered OAuth scope %q does not allow profile scope %q", ErrInvalidClientAdminInput, clientScope, expectedOAuthScope)
	}
	if currentActor != nil {
		if *currentActor == actorID && currentProfile == authorityProfile {
			return nil
		}
		return fmt.Errorf("%w: client is already bound", ErrOAuthClientConflict)
	}

	var otherClient string
	err = tx.QueryRow(ctx, `SELECT client_id FROM oauth_clients WHERE actor_token_id=$1 AND revoked_at IS NULL AND client_id<>$2 LIMIT 1`, actorID, clientID).Scan(&otherClient)
	if err == nil {
		return fmt.Errorf("%w: actor is already bound to active client %s", ErrOAuthClientConflict, otherClient)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID(clientID),
		Kind: domain.EventOAuthClientActorBound, Source: actor.Source, ActorTokenID: &actor.ID,
		Payload: map[string]any{"payload_version": 1, "client_id": clientID, "actor_token_id": actorID, "authority_profile": authorityProfile},
	})
	if err != nil {
		return fmt.Errorf("oauth: bind actor event: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *ClientAdminService) Revoke(ctx context.Context, clientID, reason string, actor domain.Token) error {
	if !access.CanRevokeOAuthClient(actor) {
		return ErrOAuthClientAdminDenied
	}
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return fmt.Errorf("%w: client_id is required", ErrInvalidClientAdminInput)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var revokedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT revoked_at FROM oauth_clients WHERE client_id=$1 FOR UPDATE`, clientID).Scan(&revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrClientNotFound
	}
	if err != nil {
		return err
	}
	if revokedAt != nil {
		return nil
	}
	now := time.Now().UTC()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID(clientID),
		Kind: domain.EventOAuthClientRevoked, Source: actor.Source, ActorTokenID: &actor.ID,
		Payload: map[string]any{"payload_version": 1, "client_id": clientID, "reason": strings.TrimSpace(reason), "revoked_at_unix": now.Unix()},
	})
	if err != nil {
		return fmt.Errorf("oauth: revoke client event: %w", err)
	}
	return tx.Commit(ctx)
}

func loadProviderActorForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Token, error) {
	var tok domain.Token
	var scopesJSON []byte
	var source string
	err := tx.QueryRow(ctx, `SELECT id,name,is_root,scopes,source,created_at,revoked_at FROM tokens WHERE id=$1 FOR UPDATE`, id).Scan(&tok.ID, &tok.Name, &tok.IsRoot, &scopesJSON, &source, &tok.CreatedAt, &tok.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Token{}, fmt.Errorf("%w: provider actor is unavailable", ErrInvalidClientAdminInput)
	}
	if err != nil {
		return domain.Token{}, err
	}
	tok.Source = domain.Source(source)
	if err := json.Unmarshal(scopesJSON, &tok.Scopes); err != nil {
		return domain.Token{}, err
	}
	if tok.IsRoot || tok.RevokedAt != nil || tok.Source != domain.SourceAgent {
		return domain.Token{}, fmt.Errorf("%w: provider actor must be an active non-root agent token", ErrInvalidClientAdminInput)
	}
	return tok, nil
}
