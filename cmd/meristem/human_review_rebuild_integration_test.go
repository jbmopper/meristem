package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestHumanReviewGateRebuildToleratesPreBoundaryTerminalHistory reproduces the
// two event shapes that existed before migration 0040 made the terminal
// human-review gate a projector invariant. In particular, the blocked->done
// transition is the shape emitted by successful OAuth authorization before
// that flow moved its separate approval gate to human_review_status=waved_through.
func TestHumanReviewGateRebuildToleratesPreBoundaryTerminalHistory(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}
	if err := storage.MigrateDown(ctx, pool, nil); err != nil {
		t.Fatalf("roll back boundary migration: %v", err)
	}

	writer := app.NewEventWriter()
	authService := auth.NewService(pool, writer)
	rootResult, err := authService.CreateToken(ctx, auth.CreateTokenInput{
		Name: "pre-boundary-human-review-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create fixture actor: %v", err)
	}
	actor := rootResult.Token

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin legacy fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	transitionedID := uuid.New()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: transitionedID,
		Kind: domain.EventWorkItemCreated, Source: actor.Source, ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title": "pre-boundary OAuth authorization", "state": domain.WorkItemRunning,
			"human_review_status": domain.HumanReviewBlocked,
		},
	}); err != nil {
		t.Fatalf("append legacy blocked work item: %v", err)
	}
	terminalAtCreateID := uuid.New()
	terminalCreateEventID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: terminalAtCreateID,
		Kind: domain.EventWorkItemCreated, Source: actor.Source, ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"title": "pre-boundary terminal-at-create", "state": domain.WorkItemDone,
			"human_review_status": domain.HumanReviewBlocked,
		},
	})
	if err != nil {
		t.Fatalf("append legacy done-and-blocked creation: %v", err)
	}
	var terminalCreateSeq int64
	if err := tx.QueryRow(ctx, `SELECT seq FROM events WHERE id=$1`, terminalCreateEventID).Scan(&terminalCreateSeq); err != nil {
		t.Fatalf("read legacy create seq: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy fixture: %v", err)
	}

	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("apply boundary migration: %v", err)
	}
	var boundary int64
	if err := pool.QueryRow(ctx, `
		SELECT event_seq_boundary FROM schema_migrations WHERE version=40
	`).Scan(&boundary); err != nil {
		t.Fatalf("read recorded boundary: %v", err)
	}
	if boundary < terminalCreateSeq {
		t.Fatalf("boundary=%d does not cover legacy create seq=%d", boundary, terminalCreateSeq)
	}

	// Complete the pre-boundary item after the boundary is active. This is the
	// deploy-window shape: an authorization begun by the old binary receives its
	// approval after the new binary and migration are live.
	tx, err = pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin post-boundary OAuth completion: %v", err)
	}
	transitionEventID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: transitionedID,
		Kind: domain.EventWorkItemTransitioned, Source: domain.SourceSystem, ActorTokenID: &actor.ID,
		Discriminator: "pre-boundary-oauth-completion",
		Payload: map[string]any{
			"from": domain.WorkItemRunning, "to": domain.WorkItemDone,
			"reason": "oauth_authorization_approved",
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append post-boundary completion for legacy authorization: %v", err)
	}
	var transitionSeq int64
	if err := tx.QueryRow(ctx, `SELECT seq FROM events WHERE id=$1`, transitionEventID).Scan(&transitionSeq); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("read post-boundary transition seq: %v", err)
	}
	if transitionSeq <= boundary {
		_ = tx.Rollback(ctx)
		t.Fatalf("transition seq=%d is not above boundary=%d", transitionSeq, boundary)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit post-boundary OAuth completion: %v", err)
	}

	report, err := rebuildAndDiff(
		ctx,
		pool,
		app.NewProjectionRegistry(),
		"human_review_boundary_rebuild",
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	if err != nil {
		t.Fatalf("rebuild rejected pre-boundary terminal history: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("pre-boundary human-review rebuild had mismatches: %+v", report.mismatches)
	}
}
