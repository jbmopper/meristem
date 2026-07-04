package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/storage"
)

func TestMCPInboxCaptureReturnsCapturedAt(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "mcp-inbox-root",
		IsRoot: true,
		Source: "human",
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		Inbox:       inbox.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = root.Token

	isError, text := callToolForTest(t, s, "inbox.capture", map[string]any{
		"text":            "capture through MCP",
		"idempotency_key": "mcp-inbox-captured-at",
	})
	if isError {
		t.Fatalf("inbox.capture failed: %s", text)
	}
	var result struct {
		MessageID  uuid.UUID `json:"message_id"`
		WorkItemID uuid.UUID `json:"work_item_id"`
		CapturedAt string    `json:"captured_at"`
	}
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("decode tool text payload: %v\n%s", err, text)
	}
	if result.MessageID == uuid.Nil || result.WorkItemID == uuid.Nil {
		t.Fatalf("missing ids in result: %+v", result)
	}
	capturedAt, err := time.Parse(time.RFC3339Nano, result.CapturedAt)
	if err != nil {
		t.Fatalf("captured_at is not RFC3339Nano: %q", result.CapturedAt)
	}

	var projectedCreatedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT created_at FROM work_items WHERE id = $1`, result.WorkItemID).Scan(&projectedCreatedAt); err != nil {
		t.Fatalf("read projected work item timestamp: %v", err)
	}
	if !capturedAt.Equal(projectedCreatedAt) {
		t.Fatalf("captured_at = %s, projected created_at = %s", capturedAt, projectedCreatedAt)
	}
}
