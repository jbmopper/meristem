package escalations

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
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
	if blockedParent.HumanReviewStatus != domain.HumanReviewWavedThrough {
		t.Fatalf("parent human_review_status = %s, want waved_through", blockedParent.HumanReviewStatus)
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

	approvedParent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:             "Approved direction needs attention",
		State:             domain.WorkItemRunning,
		HumanReviewStatus: domain.HumanReviewApproved,
		Actor:             agent.Token,
	})
	if err != nil {
		t.Fatalf("create approved parent: %v", err)
	}
	if _, err := NewService(pool, writer).Request(ctx, RequestInput{
		WorkItemID: approvedParent.ID,
		Reason:     "approved work stalled",
		Summary:    "Surface the stall without revoking approval.",
		Actor:      agent.Token,
	}); err != nil {
		t.Fatalf("request escalation for approved parent: %v", err)
	}
	preservedApproved, err := workSvc.Get(ctx, approvedParent.ID)
	if err != nil {
		t.Fatalf("get approved parent: %v", err)
	}
	if preservedApproved.State != domain.WorkItemBlocked {
		t.Fatalf("approved parent state = %s, want blocked", preservedApproved.State)
	}
	if preservedApproved.HumanReviewStatus != domain.HumanReviewApproved {
		t.Fatalf("approved parent human_review_status = %s, want approved", preservedApproved.HumanReviewStatus)
	}
}

func TestConcurrentDistinctRequestsSerializeParentTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := newIntegrationPool(t)
	writer := app.NewEventWriter()
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "concurrent-escalation-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	agent, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "concurrent-escalation-agent", Source: domain.SourceAgent, Actor: &root.Token,
	})
	if err != nil {
		t.Fatalf("create agent token: %v", err)
	}
	parent, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "Concurrent escalation parent", State: domain.WorkItemRunning,
		HumanReviewStatus: domain.HumanReviewWavedThrough, Actor: agent.Token,
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	var lockedID uuid.UUID
	if err := blocker.QueryRow(ctx, `SELECT id FROM work_items WHERE id=$1 FOR UPDATE`, parent.ID).Scan(&lockedID); err != nil {
		t.Fatalf("lock parent: %v", err)
	}

	type outcome struct {
		result RequestResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	requestSvc := NewService(pool, writer)
	for _, reason := range []string{"concurrent escalation alpha", "concurrent escalation beta"} {
		go func(reason string) {
			<-start
			result, err := requestSvc.Request(ctx, RequestInput{
				WorkItemID: parent.ID, Reason: reason, Summary: reason, Actor: agent.Token,
			})
			results <- outcome{result: result, err: err}
		}(reason)
	}
	close(start)
	waitForEscalationLockWaiters(t, ctx, pool, 2)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release parent blocker: %v", err)
	}

	seenEscalations := make(map[uuid.UUID]bool, 2)
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatalf("concurrent escalation failed: %v", got.err)
		}
		if got.result.EscalationID == uuid.Nil || seenEscalations[got.result.EscalationID] {
			t.Fatalf("concurrent escalation result = %+v, seen=%v", got.result, seenEscalations)
		}
		seenEscalations[got.result.EscalationID] = true
	}

	var transitionCount int
	var fromState string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), min(payload->>'from')
		FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3
		  AND payload->>'to'=$4
	`, domain.SubjectWorkItem, parent.ID, domain.EventWorkItemTransitioned, domain.WorkItemBlocked).Scan(&transitionCount, &fromState); err != nil {
		t.Fatalf("read parent transition history: %v", err)
	}
	if transitionCount != 1 || fromState != string(domain.WorkItemRunning) {
		t.Fatalf("parent blocked transitions = %d from %q, want 1 from running", transitionCount, fromState)
	}
	var escalationCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE subject_kind=$1 AND kind=$2
		  AND payload->>'work_item_id'=$3
	`, domain.SubjectEscalation, domain.EventEscalationRequested, parent.ID.String()).Scan(&escalationCount); err != nil {
		t.Fatalf("count escalation requests: %v", err)
	}
	if escalationCount != 2 {
		t.Fatalf("escalation requests = %d, want 2", escalationCount)
	}
}

func waitForEscalationLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
		`).Scan(&waiting); err != nil {
			t.Fatalf("observe escalation lock waiters: %v", err)
		}
		if waiting >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("observed %d escalation lock waiters, want %d", waiting, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	return pgtest.NewPool(t, "meristem_escalations_itest")
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
