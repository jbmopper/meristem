package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestRootReplacementRequiresAndRecordsRootActor(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "auth_root_replace")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	reg := projections.NewRegistry()
	RegisterProjectors(reg)
	svc := NewService(pool, events.NewWriter(reg))

	root, err := svc.CreateToken(ctx, CreateTokenInput{
		Name:   "root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}

	_, err = svc.CreateToken(ctx, CreateTokenInput{
		Name:    "replacement",
		IsRoot:  true,
		Source:  domain.SourceHuman,
		Replace: true,
	})
	if !errors.Is(err, ErrRootRequired) {
		t.Fatalf("unauthenticated root replacement error = %v, want %v", err, ErrRootRequired)
	}

	replacement, err := svc.CreateToken(ctx, CreateTokenInput{
		Name:    "replacement",
		IsRoot:  true,
		Source:  domain.SourceHuman,
		Replace: true,
		Actor:   &root.Token,
	})
	if err != nil {
		t.Fatalf("replace root with actor: %v", err)
	}

	var actorTokenID string
	err = pool.QueryRow(ctx, `
		SELECT actor_token_id::text
		FROM events
		WHERE kind = $1 AND subject_id = $2
	`, domain.EventTokenRevoked, root.Token.ID).Scan(&actorTokenID)
	if err != nil {
		t.Fatalf("read revocation actor: %v", err)
	}
	if actorTokenID != root.Token.ID.String() {
		t.Fatalf("revocation actor = %s, want %s", actorTokenID, root.Token.ID)
	}

	var activeRootID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM tokens WHERE is_root AND revoked_at IS NULL`).Scan(&activeRootID); err != nil {
		t.Fatalf("read active root: %v", err)
	}
	if activeRootID != replacement.Token.ID.String() {
		t.Fatalf("active root = %s, want replacement %s", activeRootID, replacement.Token.ID)
	}
}
