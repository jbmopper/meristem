package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
)

var ErrSystemActorUnavailable = errors.New("oauth: dedicated system actor is not configured")
var ErrProviderActorUnavailable = errors.New("oauth: provider client has no active agent actor binding")

func loadActor(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, source domain.Source) (domain.Token, error) {
	if id == uuid.Nil {
		return domain.Token{}, ErrSystemActorUnavailable
	}
	var tok domain.Token
	var gotSource string
	var scopes []byte
	err := pool.QueryRow(ctx, `SELECT id, name, is_root, scopes, source, created_at, revoked_at FROM tokens WHERE id=$1`, id).
		Scan(&tok.ID, &tok.Name, &tok.IsRoot, &scopes, &gotSource, &tok.CreatedAt, &tok.RevokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Token{}, fmt.Errorf("%w: token %s does not exist", ErrSystemActorUnavailable, id)
		}
		return domain.Token{}, err
	}
	tok.Source = domain.Source(gotSource)
	if tok.IsRoot || tok.RevokedAt != nil || tok.Source != source {
		return domain.Token{}, fmt.Errorf("%w: token %s must be active, non-root, source=%s", ErrSystemActorUnavailable, id, source)
	}
	if err := json.Unmarshal(scopes, &tok.Scopes); err != nil {
		return domain.Token{}, fmt.Errorf("oauth: decode actor scopes: %w", err)
	}
	return tok, nil
}

func validateProviderActor(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (domain.Token, error) {
	if id == uuid.Nil {
		return domain.Token{}, ErrProviderActorUnavailable
	}
	tok, err := loadActor(ctx, pool, id, domain.SourceAgent)
	if err != nil {
		return domain.Token{}, fmt.Errorf("%w: %v", ErrProviderActorUnavailable, err)
	}
	return tok, nil
}
