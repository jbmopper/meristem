package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
)

// TestNetworkProjectionsRebuildFromEvents pins nodes and command_queue as
// honest event-log projections, including the first-terminal-ack reduction.
func TestNetworkProjectionsRebuildFromEvents(t *testing.T) {
	ctx := context.Background()
	pool := newCmdIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	actor := createCmdSystemToken(t, ctx, pool, writer, "network-rebuild")
	if err := registerNode(ctx, pool, writer, actor, []string{
		"--node-id", "m4", "--base-url", "https://m4.example", "--status", "active",
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if err := updateNodeRoute(ctx, pool, writer, actor, []string{
		"--node-id", "m4", "--direct-url", "https://m4.internal", "--status", "active",
	}); err != nil {
		t.Fatalf("update route: %v", err)
	}

	queue := crossnode.NewQueueService(pool, writer)
	queued, err := queue.Enqueue(ctx, crossnode.EnqueueInput{
		TargetNodeID:         "m4",
		CommandPath:          "/v1/work-items/11111111-1111-4111-8111-111111111111/transition",
		CommandBody:          json.RawMessage(`{"to":"running"}`),
		OriginIdempotencyKey: "network-rebuild-command",
		Source:               domain.SourceAgent,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := queue.Ack(ctx, crossnode.AckInput{
		CommandQueueID: queued.EventID,
		StatusCode:     201,
		OK:             true,
		Source:         domain.SourceAgent,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := queue.Ack(ctx, crossnode.AckInput{
		CommandQueueID: queued.EventID,
		StatusCode:     500,
		OK:             false,
		Source:         domain.SourceAgent,
	}); err != nil {
		t.Fatalf("contradictory ack: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report, err := rebuildAndDiff(ctx, pool, app.NewProjectionRegistry(), "network_rebuild", logger, false)
	if err != nil {
		t.Fatalf("rebuild networking projections: %v", err)
	}
	if len(report.mismatches) != 0 {
		t.Fatalf("network rebuild had mismatches: %+v", report.mismatches)
	}
}
