package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestJobQueueEnqueuesDispatchAndClaimsWithSkipLocked(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "job-queue-claim")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}

	const jobs = 12
	dispatchIDs := make(map[uuid.UUID]bool, jobs)
	for i := 0; i < jobs; i++ {
		item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "queued dispatch")
		dispatchID := appendDispatchRequestedForTest(t, ctx, pool, writer, systemTok.Token, item.ID)
		dispatchIDs[dispatchID] = true
		// Re-appending the same dispatch fact must not enqueue another row.
		appendDispatchRequestedForTest(t, ctx, pool, writer, systemTok.Token, item.ID)
	}
	if got := countJobQueueRows(t, ctx, pool); got != jobs {
		t.Fatalf("job_queue rows = %d, want %d", got, jobs)
	}

	queue := jobqueue.NewService(pool)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[uuid.UUID]bool{}
	errs := make(chan error, jobs+4)

	for i := 0; i < jobs+4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			job, found, err := queue.ClaimNext(ctx, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			if !found {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if claimed[job.ID] {
				t.Errorf("job %s claimed more than once", job.ID)
			}
			claimed[job.ID] = true
			if job.State != jobqueue.JobLeased {
				t.Errorf("job %s state = %s, want leased", job.ID, job.State)
			}
			if job.Attempts != 1 {
				t.Errorf("job %s attempts = %d, want 1", job.ID, job.Attempts)
			}
			if job.LeaseUntil == nil || !job.LeaseUntil.After(time.Now().UTC()) {
				t.Errorf("job %s lease_until = %v, want future timestamp", job.ID, job.LeaseUntil)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("claim error: %v", err)
	}
	if len(claimed) != jobs {
		t.Fatalf("claimed jobs = %d, want %d", len(claimed), jobs)
	}
	for id := range claimed {
		if !dispatchIDs[id] {
			t.Fatalf("claimed unexpected job id %s", id)
		}
	}
	if got := countJobsInState(t, ctx, pool, "leased"); got != jobs {
		t.Fatalf("leased rows = %d, want %d", got, jobs)
	}
}

func TestJobQueueReclaimsExpiredLeaseAndSkipsTerminalJobs(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "job-queue-reclaim")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "reclaim dispatch")
	dispatchID := appendDispatchRequestedForTest(t, ctx, pool, writer, systemTok.Token, item.ID)

	queue := jobqueue.NewService(pool)
	first, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !found || first.ID != dispatchID {
		t.Fatalf("first claim found=%t id=%s, want %s", found, first.ID, dispatchID)
	}
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("second claim before expiry: %v", err)
	} else if found {
		t.Fatalf("second claim before expiry found a job, want none")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE job_queue
		SET lease_until = now() - interval '1 second'
		WHERE id = $1
	`, dispatchID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaimed, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if !found || reclaimed.ID != dispatchID {
		t.Fatalf("reclaim found=%t id=%s, want %s", found, reclaimed.ID, dispatchID)
	}
	if reclaimed.Attempts != 2 {
		t.Fatalf("reclaimed attempts = %d, want 2", reclaimed.Attempts)
	}
	if err := queue.MarkDone(ctx, dispatchID); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim after done: %v", err)
	} else if found {
		t.Fatalf("claim after done found a job, want none")
	}
}

func createDispatchableItem(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, title string) domain.WorkItem {
	t.Helper()
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title:                      title,
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		Actor:                      actor,
	})
	if err != nil {
		t.Fatalf("create dispatchable item: %v", err)
	}
	return item
}

func appendDispatchRequestedForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, workItemID uuid.UUID) uuid.UUID {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin dispatch append: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    workItemID,
		Kind:         domain.EventDispatchRequested,
		Source:       domain.SourceSystem,
		ActorTokenID: &actor.ID,
		Payload: map[string]any{
			"work_item_id":           workItemID,
			"state":                  string(domain.WorkItemTriaged),
			"state_entered_at_unix":  int64(12345),
			"cultivar":               "checklist-worker@1",
			"reason":                 "agent_attention_requested",
			"source_reconciler_pass": "dispatch",
		},
	})
	if err != nil {
		t.Fatalf("append dispatch.requested: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit dispatch append: %v", err)
	}
	return id
}

func countJobQueueRows(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_queue`).Scan(&count); err != nil {
		t.Fatalf("count job_queue rows: %v", err)
	}
	return count
}

func countJobsInState(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, state string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_queue WHERE state = $1`, state).Scan(&count); err != nil {
		t.Fatalf("count job_queue state %s: %v", state, err)
	}
	return count
}
