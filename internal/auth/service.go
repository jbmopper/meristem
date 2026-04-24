package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var (
	ErrRootExists    = errors.New("auth: root token already exists")
	ErrRootRequired  = errors.New("auth: root token required")
	ErrTokenNotFound = errors.New("auth: token not found")
	ErrTokenShape    = errors.New("auth: token has invalid shape")
	ErrTokenRevoked  = errors.New("auth: token revoked")
)

// Service owns token creation, revocation, listing, and bearer lookup. All
// state-changing methods append token events; token rows are projections.
type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type CreateTokenInput struct {
	Name    string
	IsRoot  bool
	Scopes  []string
	Source  domain.Source
	Replace bool
	Actor   *domain.Token
}

type CreateTokenResult struct {
	Token  domain.Token
	Secret string
}

// CreateToken appends token.created, producing the token projection in the
// same transaction. Root bootstrap may run without an actor; non-root tokens
// require an existing root actor.
func (s *Service) CreateToken(ctx context.Context, in CreateTokenInput) (CreateTokenResult, error) {
	if in.Name == "" {
		return CreateTokenResult{}, fmt.Errorf("auth: token name is required")
	}
	if !in.IsRoot {
		if in.Actor == nil || !in.Actor.IsRoot || in.Actor.RevokedAt != nil {
			return CreateTokenResult{}, ErrRootRequired
		}
	}
	tokenSource := in.Source
	if tokenSource == "" {
		tokenSource = domain.SourceHuman
	}
	if !tokenSource.Valid() {
		return CreateTokenResult{}, fmt.Errorf("auth: invalid token source %q", in.Source)
	}
	eventSource := sourceForToken(in.Actor)
	secret, hash, err := NewSecret()
	if err != nil {
		return CreateTokenResult{}, err
	}
	tokenID := uuid.New()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CreateTokenResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if in.IsRoot {
		var existing uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM tokens WHERE is_root AND revoked_at IS NULL LIMIT 1`).Scan(&existing)
		if err == nil && !in.Replace {
			return CreateTokenResult{}, ErrRootExists
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return CreateTokenResult{}, err
		}
		if err == nil && in.Replace {
			actorID := (*uuid.UUID)(nil)
			if in.Actor != nil {
				actorID = &in.Actor.ID
			}
			if _, _, err := s.writer.Append(ctx, tx, events.Spec{
				SubjectKind:  domain.SubjectToken,
				SubjectID:    existing,
				Kind:         domain.EventTokenRevoked,
				Source:       eventSource,
				ActorTokenID: actorID,
				Payload:      map[string]any{"reason": "replace_root"},
			}); err != nil {
				return CreateTokenResult{}, err
			}
		}
	}

	var actorID *uuid.UUID
	if in.Actor != nil {
		actorID = &in.Actor.ID
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectToken,
		SubjectID:    tokenID,
		Kind:         domain.EventTokenCreated,
		Source:       eventSource,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"name":    in.Name,
			"hash":    base64.StdEncoding.EncodeToString(hash),
			"is_root": in.IsRoot,
			"scopes":  in.Scopes,
			"source":  tokenSource,
		},
	})
	if err != nil {
		return CreateTokenResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateTokenResult{}, err
	}

	tok, err := s.Get(ctx, tokenID)
	if err != nil {
		return CreateTokenResult{}, err
	}
	return CreateTokenResult{Token: tok, Secret: secret}, nil
}

func (s *Service) Revoke(ctx context.Context, id uuid.UUID, actor domain.Token) error {
	if !actor.IsRoot && actor.ID != id {
		return ErrRootRequired
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanToken(ctx, tx, id); err != nil {
		return err
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectToken,
		SubjectID:    id,
		Kind:         domain.EventTokenRevoked,
		Source:       sourceForToken(&actor),
		ActorTokenID: &actor.ID,
		Payload:      map[string]any{"reason": "operator_revoke"},
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) Authenticate(ctx context.Context, secret string) (domain.Token, error) {
	if !ValidSecretShape(secret) {
		return domain.Token{}, ErrTokenShape
	}
	hash := HashSecret(secret)
	tok, err := scanTokenRow(s.pool.QueryRow(ctx, `
		SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at
		FROM tokens
		WHERE hash = $1
	`, hash))
	if err != nil {
		if errors.Is(err, ErrTokenNotFound) {
			return domain.Token{}, ErrInvalidToken
		}
		return domain.Token{}, err
	}
	if !EqualHash(tok.Hash, hash) {
		return domain.Token{}, ErrInvalidToken
	}
	if tok.RevokedAt != nil {
		return domain.Token{}, ErrTokenRevoked
	}
	return tok, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Token, error) {
	return scanToken(ctx, s.pool, id)
}

func (s *Service) List(ctx context.Context) ([]domain.Token, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at FROM tokens ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Token
	for rows.Next() {
		tok, err := scanTokenRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tok)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanToken(ctx context.Context, q queryer, id uuid.UUID) (domain.Token, error) {
	row := q.QueryRow(ctx, `SELECT id, name, hash, is_root, scopes, source, created_at, revoked_at FROM tokens WHERE id = $1`, id)
	return scanTokenRow(row)
}

func scanTokenRow(row rowScanner) (domain.Token, error) {
	var tok domain.Token
	var scopesRaw []byte
	var source string
	var revokedAt *time.Time
	if err := row.Scan(&tok.ID, &tok.Name, &tok.Hash, &tok.IsRoot, &scopesRaw, &source, &tok.CreatedAt, &revokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Token{}, ErrTokenNotFound
		}
		return domain.Token{}, err
	}
	if len(scopesRaw) > 0 {
		if err := json.Unmarshal(scopesRaw, &tok.Scopes); err != nil {
			return domain.Token{}, err
		}
	}
	tok.Source = domain.Source(source)
	if !tok.Source.Valid() {
		tok.Source = domain.SourceHuman
	}
	tok.RevokedAt = revokedAt
	return tok, nil
}

func sourceForToken(tok *domain.Token) domain.Source {
	if tok != nil && tok.Source.Valid() {
		return tok.Source
	}
	return domain.SourceHuman
}
