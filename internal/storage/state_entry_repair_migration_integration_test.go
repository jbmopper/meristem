package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestStateEntryRepairMigrationFoldsLegacyMissingFromNoop(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "state_entry_repair_0041")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}
	// Re-open the exact pre-0041 schema so the fixture represents an upgrading
	// node. The down migration is deliberately data-preserving.
	if err := storage.MigrateDown(ctx, pool, nil); err != nil {
		t.Fatalf("migrate down 0041: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "state-entry-repair-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "legacy missing-from state epoch", State: domain.WorkItemTriaged, Actor: root.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	var createdAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT occurred_at FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3
	`, domain.SubjectWorkItem, item.ID, domain.EventWorkItemCreated).Scan(&createdAt); err != nil {
		t.Fatalf("read created timestamp: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin legacy transition: %v", err)
	}
	noopID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     item.ID,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        root.Token.Source,
		ActorTokenID:  &root.Token.ID,
		Discriminator: "legacy-missing-from-noop",
		Payload: map[string]any{
			"to": domain.WorkItemTriaged, "reason": "legacy same-state no-op",
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append legacy transition: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy transition: %v", err)
	}
	var noopAt time.Time
	if err := pool.QueryRow(ctx, `SELECT occurred_at FROM events WHERE id=$1`, noopID).Scan(&noopAt); err != nil {
		t.Fatalf("read no-op timestamp: %v", err)
	}
	if !noopAt.After(createdAt) {
		t.Fatalf("no-op timestamp = %s, want after created %s", noopAt, createdAt)
	}

	// Simulate the pre-fix projector result without altering event truth.
	if _, err := pool.Exec(ctx, `UPDATE work_items SET state_entered_at=$2 WHERE id=$1`, item.ID, noopAt); err != nil {
		t.Fatalf("install stale projection timestamp: %v", err)
	}
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("apply 0041 repair: %v", err)
	}
	var repairedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT state_entered_at FROM work_items WHERE id=$1`, item.ID).Scan(&repairedAt); err != nil {
		t.Fatalf("read repaired projection: %v", err)
	}
	if !repairedAt.Equal(createdAt) {
		t.Fatalf("repaired state_entered_at = %s, want canonical entry %s (not no-op %s)", repairedAt, createdAt, noopAt)
	}
}
