package oauth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

type ClientAdminService struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewClientAdminService(pool *pgxpool.Pool, writer *events.Writer) *ClientAdminService {
	return &ClientAdminService{pool: pool, writer: writer}
}

func (s *ClientAdminService) BindActor(ctx context.Context, clientID string, actorID uuid.UUID, authorityProfile string, actor domain.Token) error {
	if !actor.IsRoot {
		return errors.New("oauth: root token required to bind provider actor")
	}
	if _, err := validateProviderActor(ctx, s.pool, actorID); err != nil {
		return err
	}
	authorityProfile = strings.TrimSpace(authorityProfile)
	if authorityProfile == "" {
		return errors.New("oauth: authority_profile is required")
	}
	if _, err := GetClient(ctx, s.pool, clientID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID(clientID), Kind: domain.EventOAuthClientActorBound, Source: actor.Source, ActorTokenID: &actor.ID, Payload: map[string]any{"payload_version": 1, "client_id": clientID, "actor_token_id": actorID, "authority_profile": authorityProfile}})
	if err != nil {
		return fmt.Errorf("oauth: bind actor event: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *ClientAdminService) Revoke(ctx context.Context, clientID, reason string, actor domain.Token) error {
	if !actor.IsRoot {
		return errors.New("oauth: root token required to revoke provider client")
	}
	if _, err := GetClient(ctx, s.pool, clientID); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := time.Now().UTC()
	_, _, err = s.writer.Append(ctx, tx, events.Spec{SubjectKind: domain.SubjectOAuthClient, SubjectID: ClientSubjectID(clientID), Kind: domain.EventOAuthClientRevoked, Source: actor.Source, ActorTokenID: &actor.ID, Payload: map[string]any{"payload_version": 1, "client_id": clientID, "reason": reason, "revoked_at_unix": now.Unix()}})
	if err != nil {
		return fmt.Errorf("oauth: revoke client event: %w", err)
	}
	return tx.Commit(ctx)
}
