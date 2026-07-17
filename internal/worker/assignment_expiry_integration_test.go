package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestAssignmentExpiryUsesDatabaseClockAndReleasesOnceAcrossWorkers(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "meristem_assignment_expiry")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-expiry-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	assignee, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-expiry-assignee", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatalf("assignee: %v", err)
	}
	system, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-expiry-system", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("system: %v", err)
	}
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title: "one-second assignment", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"expiry release recorded"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: assignee.Token,
	})
	if err != nil {
		t.Fatalf("item: %v", err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var claimedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&claimedAt); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("read assignment clock: %v", err)
	}
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemAssigned, Source: domain.SourceAgent, ActorTokenID: &assignee.Token.ID,
		Discriminator: "assignment-expiry-test",
		Payload: map[string]any{
			"payload_version": 1, "assignee_token_id": assignee.Token.ID,
			"mode": domain.WorkItemAssignmentClaim, "lease_seconds": 1,
			"lease_source": "test:one-second",
			"claimed_at":   claimedAt, "expires_at": claimedAt.Add(time.Second),
		},
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append assignment: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assignment, err := workitems.NewService(pool, writer).GetAssignment(ctx, item.ID)
	if err != nil {
		t.Fatalf("assignment: %v", err)
	}

	// An absurdly future application clock cannot expire a lease while the DB
	// clock still says it is live.
	early, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &system.Token.ID, func() time.Time {
		return time.Now().Add(100 * 365 * 24 * time.Hour)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := early.expireAssignments(ctx); err != nil || got != 0 {
		t.Fatalf("early expiry with skewed app clock = %d, %v", got, err)
	}

	if wait := time.Until(assignment.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	// Projection reads remain event-defined: merely passing the wall-clock
	// deadline does not erase the holder before an explicit release event.
	unswept, err := workitems.NewService(pool, writer).GetAssignment(ctx, item.ID)
	if err != nil || unswept.AssignmentEventID != assignment.AssignmentEventID {
		t.Fatalf("expired-but-unswept assignment = %+v, %v", unswept, err)
	}
	w1, _ := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &system.Token.ID, nil)
	w2, _ := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &system.Token.ID, nil)
	results := make(chan int, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, w := range []*Worker{w1, w2} {
		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			got, err := worker.expireAssignments(ctx)
			results <- got
			errs <- err
		}(w)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent expiry: %v", err)
		}
	}
	total := 0
	for got := range results {
		total += got
	}
	if total != 1 {
		t.Fatalf("concurrent expired count total = %d, want 1", total)
	}
	if got, err := w1.expireAssignments(ctx); err != nil || got != 0 {
		t.Fatalf("restart expiry = %d, %v", got, err)
	}
	var releases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2 AND payload->>'reason'=$3`, item.ID, domain.EventWorkItemAssignmentReleased, domain.AssignmentReleaseExpired).Scan(&releases); err != nil {
		t.Fatal(err)
	}
	if releases != 1 {
		t.Fatalf("expired release events = %d, want 1", releases)
	}
}
