package escalations

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	envIntegrationEnabled = "MERISTEM_INTEGRATION"
	envTestDatabaseURL    = "MERISTEM_TEST_DATABASE_URL"
)

func TestRequestCreatesHumanVisibleEscalation(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	writer := app.NewEventWriter()
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "escalation-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "escalation-agent",
		Source: domain.SourceAgent,
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create agent token: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:             "Needs judgment",
		Body:              "A reducer cannot dispose this.",
		State:             domain.WorkItemRunning,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
		Actor:             agent.Token,
	})
	if err != nil {
		t.Fatalf("create parent work item: %v", err)
	}

	result, err := NewService(pool, writer).Request(ctx, RequestInput{
		WorkItemID: parent.ID,
		Reason:     "ambiguous reducer verdict",
		Summary:    "Pick a path before continuing.",
		Actor:      agent.Token,
	})
	if err != nil {
		t.Fatalf("request escalation: %v", err)
	}
	if result.EscalationID == uuid.Nil || result.HumanWorkItemID == uuid.Nil {
		t.Fatalf("expected escalation and human work item ids, got %+v", result)
	}
	replayed, err := NewService(pool, writer).Request(ctx, RequestInput{
		WorkItemID: parent.ID,
		Reason:     "ambiguous reducer verdict",
		Summary:    "Pick a path before continuing.",
		Actor:      agent.Token,
	})
	if err != nil {
		t.Fatalf("replay escalation: %v", err)
	}
	if replayed.Fresh {
		t.Fatalf("replayed escalation Fresh = true, want false")
	}
	if replayed.EscalationID != result.EscalationID || replayed.HumanWorkItemID != result.HumanWorkItemID {
		t.Fatalf("replayed ids = %+v, want %+v", replayed, result)
	}

	blockedParent, err := workSvc.Get(ctx, parent.ID)
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if blockedParent.State != domain.WorkItemBlocked {
		t.Fatalf("parent state = %s, want blocked", blockedParent.State)
	}
	if blockedParent.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("parent human_review_status = %s, want blocked", blockedParent.HumanReviewStatus)
	}
	humanItem, err := workSvc.Get(ctx, result.HumanWorkItemID)
	if err != nil {
		t.Fatalf("get human work item: %v", err)
	}
	if !strings.HasPrefix(humanItem.Title, "Human attention: Needs judgment") {
		t.Fatalf("human item title = %q", humanItem.Title)
	}
	if humanItem.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("human item review status = %s, want blocked", humanItem.HumanReviewStatus)
	}

	assertRelation(t, ctx, pool, parent.ID, result.HumanWorkItemID)
	assertEscalationEvent(t, ctx, pool, result.EscalationID, parent.ID, result.HumanWorkItemID)
	assertSingleEscalationProjection(t, ctx, pool, result.EscalationID, result.HumanWorkItemID)
}

func assertRelation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, parentID, childID uuid.UUID) {
	t.Helper()
	var ok bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM work_item_relations WHERE parent_id = $1 AND child_id = $2
		)
	`, parentID, childID).Scan(&ok); err != nil {
		t.Fatalf("query relation: %v", err)
	}
	if !ok {
		t.Fatalf("expected relation %s -> %s", parentID, childID)
	}
}

func assertEscalationEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, escalationID, workItemID, humanWorkItemID uuid.UUID) {
	t.Helper()
	var payload struct {
		WorkItemID       uuid.UUID `json:"work_item_id"`
		HumanWorkItemID  uuid.UUID `json:"human_work_item_id"`
		Reason           string    `json:"reason"`
		OriginState      string    `json:"origin_state"`
		OriginStateKnown bool      `json:"-"`
	}
	var payloadJSON []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectEscalation, escalationID, domain.EventEscalationRequested).Scan(&payloadJSON); err != nil {
		t.Fatalf("query escalation event: %v", err)
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatalf("decode escalation payload: %v", err)
	}
	if payload.WorkItemID != workItemID {
		t.Fatalf("payload work_item_id = %s, want %s", payload.WorkItemID, workItemID)
	}
	if payload.HumanWorkItemID != humanWorkItemID {
		t.Fatalf("payload human_work_item_id = %s, want %s", payload.HumanWorkItemID, humanWorkItemID)
	}
	if payload.Reason != "ambiguous reducer verdict" {
		t.Fatalf("payload reason = %q", payload.Reason)
	}
}

func assertSingleEscalationProjection(t *testing.T, ctx context.Context, pool *pgxpool.Pool, escalationID, humanWorkItemID uuid.UUID) {
	t.Helper()
	var eventCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectEscalation, escalationID, domain.EventEscalationRequested).Scan(&eventCount); err != nil {
		t.Fatalf("count escalation events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("escalation event count = %d, want 1", eventCount)
	}
	var workItemCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM work_items
		WHERE id = $1
	`, humanWorkItemID).Scan(&workItemCount); err != nil {
		t.Fatalf("count human work item projection: %v", err)
	}
	if workItemCount != 1 {
		t.Fatalf("human work item count = %d, want 1", workItemCount)
	}
}

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	baseURL := os.Getenv(envTestDatabaseURL)
	if baseURL == "" {
		if os.Getenv(envIntegrationEnabled) != "1" {
			t.Skipf("set %s=1 and %s (or %s) to run Postgres integration tests", envIntegrationEnabled, storage.EnvDatabaseURL, envTestDatabaseURL)
		}
		baseURL = os.Getenv(storage.EnvDatabaseURL)
	}
	if baseURL == "" {
		t.Skipf("%s is required for integration tests", storage.EnvDatabaseURL)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Fatalf("integration tests require postgres URL DSN, got scheme %q", parsed.Scheme)
	}
	dbName := "meristem_itest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminURL := *parsed
	adminURL.Path = "/postgres"
	testURL := *parsed
	testURL.Path = "/" + dbName

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminURL.String())
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdentifier(dbName)); err != nil {
		admin.Close()
		t.Fatalf("create temp database %s: %v", dbName, err)
	}
	admin.Close()

	pool, err := storage.Open(ctx, storage.Config{
		DatabaseURL:    testURL.String(),
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		dropIntegrationDatabase(t, adminURL.String(), dbName)
		t.Fatalf("open temp database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropIntegrationDatabase(t, adminURL.String(), dbName)
	})
	return pool
}

func dropIntegrationDatabase(t *testing.T, adminDSN string, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Logf("open admin database for cleanup: %v", err)
		return
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdentifier(dbName)+` WITH (FORCE)`); err != nil {
		t.Logf("drop temp database %s: %v", dbName, err)
	}
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
