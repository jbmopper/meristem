package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/nodes"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestRegistrySnapshotProjectionRebuildsFromObservedEvent(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actor := createCmdSystemToken(t, ctx, pool, writer, "registry-snapshot-rebuild")
	if err := registerNode(ctx, pool, writer, actor, []string{"--node-id", "hub", "--base-url", "https://hub.example", "--status", "active"}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	var sourceRevision, entryRevision int64
	if err := pool.QueryRow(ctx, `SELECT MAX(seq) FROM events`).Scan(&sourceRevision); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT registry_revision FROM nodes WHERE node_id = 'hub'`).Scan(&entryRevision); err != nil {
		t.Fatal(err)
	}
	snapshot := nodes.RegistrySnapshot{
		PayloadVersion: 1,
		SourceNodeID:   "hub",
		SourceRevision: sourceRevision,
		Nodes: []nodes.SnapshotEntry{{
			NodeID:           "hub",
			QueueVia:         []string{},
			Status:           domain.NodeStatusActive,
			RegistryRevision: entryRevision,
		}},
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectRegistrySnapshot,
		SubjectID:    nodes.SnapshotSubjectID("hub"),
		Kind:         domain.EventRegistrySnapshotObserved,
		Source:       actor.Source,
		ActorTokenID: &actor.ID,
		Payload:      snapshot,
	}); err != nil {
		t.Fatalf("append snapshot: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "registry_snapshot_rebuild", slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("registry snapshot rebuild mismatches: %+v", report.mismatches)
	}
}
