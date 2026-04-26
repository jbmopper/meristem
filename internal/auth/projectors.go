package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds auth projection writers to registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(tokenCreatedProjector{})
	registry.Register(tokenRevokedProjector{})
}

type tokenCreatedProjector struct{}

func (tokenCreatedProjector) Kind() string { return domain.EventTokenCreated }

func (tokenCreatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		Name   string   `json:"name"`
		Hash   string   `json:"hash"`
		IsRoot bool     `json:"is_root"`
		Scopes []string `json:"scopes"`
		Source string   `json:"source"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.Name == "" {
		return fmt.Errorf("token.created: name is required")
	}
	source := domain.Source(payload.Source)
	if source == "" {
		source = domain.SourceHuman
	}
	if !source.Valid() {
		return fmt.Errorf("token.created: invalid source %q", payload.Source)
	}
	hash, err := base64.StdEncoding.DecodeString(payload.Hash)
	if err != nil {
		return fmt.Errorf("token.created: decode hash: %w", err)
	}
	scopes, err := json.Marshal(payload.Scopes)
	if err != nil {
		return fmt.Errorf("token.created: marshal scopes: %w", err)
	}
	// DO NOTHING on conflict: the events writer only fires projectors on
	// a fresh event-row insert, so a duplicate token id reaching this
	// statement means a real bug (two distinct token.created events
	// claiming the same subject_id). DO UPDATE would silently mask that;
	// DO NOTHING leaves the original projection row in place and lets
	// downstream queries notice the discrepancy.
	_, err = tx.Exec(ctx, `
		INSERT INTO tokens (id, name, hash, is_root, scopes, source, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, payload.Name, hash, payload.IsRoot, scopes, string(source), event.OccurredAt)
	return err
}

type tokenRevokedProjector struct{}

func (tokenRevokedProjector) Kind() string { return domain.EventTokenRevoked }

func (tokenRevokedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	_, err := tx.Exec(ctx, `UPDATE tokens SET revoked_at = $2 WHERE id = $1`, event.SubjectID, event.OccurredAt)
	return err
}

func decodePayload(payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
