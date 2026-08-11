package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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
	if err := queue.MarkDone(ctx, dispatchID, first.Attempts); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("late completion of expired attempt = %v, want pgx.ErrNoRows", err)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobLeased, 1, true)
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
	if err := queue.MarkDone(ctx, dispatchID, first.Attempts); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale completion of renewed attempt = %v, want pgx.ErrNoRows", err)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobLeased, 2, true)
	if err := queue.MarkDone(ctx, dispatchID, reclaimed.Attempts); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim after done: %v", err)
	} else if found {
		t.Fatalf("claim after done found a job, want none")
	}
}

func TestTerminalAcknowledgementRechecksLeaseAfterRowLockWait(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "terminal-lock-wait-fence")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "terminal lock wait fence")
	dispatchID := appendDispatchRequestedForTest(t, ctx, pool, writer, systemTok.Token, item.ID)
	queue := jobqueue.NewService(pool)
	job, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil || !found || job.ID != dispatchID {
		t.Fatalf("claim terminal fence job = found %t id %s err %v, want %s", found, job.ID, err, dispatchID)
	}

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin row lock: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	var expiresAt time.Time
	if err := lockTx.QueryRow(ctx, `
		UPDATE job_queue
		SET lease_until=clock_timestamp()+interval '400 milliseconds'
		WHERE id=$1
		RETURNING lease_until
	`, dispatchID).Scan(&expiresAt); err != nil {
		t.Fatalf("shorten lease while holding row lock: %v", err)
	}
	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(started)
		errCh <- queue.MarkDone(ctx, dispatchID, job.Attempts)
	}()
	<-started
	select {
	case err := <-errCh:
		t.Fatalf("terminal acknowledgement returned before row lock released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if wait := time.Until(expiresAt) + 50*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("release row lock after lease expiry: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("terminal acknowledgement after lock-wait expiry = %v, want pgx.ErrNoRows", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal acknowledgement remained blocked after row lock release")
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobLeased, job.Attempts, true)
}

func TestReviewDispatchLeaseRestartAndAtomicCompletion(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-restart")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review restart", domain.HumanReviewWavedThrough)
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")

	queue := jobqueue.NewService(pool)
	first, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("first review claim: %v", err)
	}
	if !found || first.ID != dispatchID || first.Attempts != 1 || first.LeaseUntil == nil {
		t.Fatalf("first review claim = found %t job %+v, want id %s attempts 1 with lease", found, first, dispatchID)
	}
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, dispatchID); err != nil {
		t.Fatalf("expire review lease: %v", err)
	}
	restarted, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("restart review claim: %v", err)
	}
	if !found || restarted.ID != dispatchID || restarted.Attempts != 2 || restarted.LeaseUntil == nil {
		t.Fatalf("restart review claim = found %t job %+v, want id %s attempts 2 with lease", found, restarted, dispatchID)
	}

	service := workitems.NewService(pool, writer)
	started, err := service.StartReviewDispatch(ctx, dispatchID, restarted.Attempts, systemTok.Token)
	if err != nil {
		t.Fatalf("start review dispatch: %v", err)
	}
	if started.Outcome != workitems.ReviewDispatchStarted || !started.Transitioned {
		t.Fatalf("start outcome = %+v, want started with transition", started)
	}
	current, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get started review item: %v", err)
	}
	if current.State != domain.WorkItemRunning {
		t.Fatalf("review item state = %s, want running", current.State)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobDone, 2, false)
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 1 {
		t.Fatalf("review transition events = %d, want 1", got)
	}

	replayed, err := service.StartReviewDispatch(ctx, dispatchID, restarted.Attempts, systemTok.Token)
	if err != nil {
		t.Fatalf("replay completed review dispatch: %v", err)
	}
	if replayed.Outcome != workitems.ReviewDispatchAlreadyDone || replayed.Transitioned {
		t.Fatalf("replay outcome = %+v, want already_done without transition", replayed)
	}
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 1 {
		t.Fatalf("review transition events after replay = %d, want 1", got)
	}
	if _, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim after review completion: %v", err)
	} else if found {
		t.Fatal("claim after review completion found a job, want none")
	}
}

func TestReviewDispatchExpiredAndStaleAttemptsCannotTransition(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-attempt-fence")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review attempt fence", domain.HumanReviewWavedThrough)
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")
	queue := jobqueue.NewService(pool)
	first, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim first review attempt: %v", err)
	}
	if !found || first.ID != dispatchID || first.Attempts != 1 {
		t.Fatalf("first review claim = found %t job %+v, want id %s attempt 1", found, first, dispatchID)
	}
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, dispatchID); err != nil {
		t.Fatalf("expire first review attempt: %v", err)
	}

	service := workitems.NewService(pool, writer)
	expired, err := service.StartReviewDispatch(ctx, dispatchID, first.Attempts, systemTok.Token)
	if err != nil {
		t.Fatalf("resolve expired review attempt: %v", err)
	}
	if expired.Outcome != workitems.ReviewDispatchDormant || expired.Transitioned {
		t.Fatalf("expired review attempt = %+v, want dormant without transition", expired)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobPending, 1, false)
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 0 {
		t.Fatalf("transition events after expired review attempt = %d, want 0", got)
	}

	reclaimed, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("reclaim review attempt: %v", err)
	}
	if !found || reclaimed.ID != dispatchID || reclaimed.Attempts != 2 {
		t.Fatalf("reclaimed review = found %t job %+v, want id %s attempt 2", found, reclaimed, dispatchID)
	}
	if _, err := service.StartReviewDispatch(ctx, dispatchID, first.Attempts, systemTok.Token); err == nil {
		t.Fatal("stale review attempt unexpectedly admitted a renewed lease")
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobLeased, 2, true)
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 0 {
		t.Fatalf("transition events after stale review attempt = %d, want 0", got)
	}

	started, err := service.StartReviewDispatch(ctx, dispatchID, reclaimed.Attempts, systemTok.Token)
	if err != nil {
		t.Fatalf("start current review attempt: %v", err)
	}
	if started.Outcome != workitems.ReviewDispatchStarted || !started.Transitioned {
		t.Fatalf("current review attempt = %+v, want started", started)
	}
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 1 {
		t.Fatalf("transition events after current review attempt = %d, want 1", got)
	}
}

func TestReviewDispatchBlockedRaceReturnsJobDormant(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-blocked-race")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review blocked race", domain.HumanReviewWavedThrough)
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")

	queue := jobqueue.NewService(pool)
	if job, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim review before block: %v", err)
	} else if !found || job.ID != dispatchID {
		t.Fatalf("claim review before block = found %t id %s, want %s", found, job.ID, dispatchID)
	}
	service := workitems.NewService(pool, writer)
	if _, err := service.Transition(ctx, item.ID, domain.WorkItemBlocked, "review gate appeared after claim", systemTok.Token); err != nil {
		t.Fatalf("block claimed review item: %v", err)
	}

	result, err := service.StartReviewDispatch(ctx, dispatchID, 1, systemTok.Token)
	if err != nil {
		t.Fatalf("resolve blocked review claim: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchDormant || result.Transitioned {
		t.Fatalf("blocked review outcome = %+v, want dormant without transition", result)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobPending, 1, false)
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile blocked review job: %v", err)
	} else if canceled != 0 {
		t.Fatalf("reconcile blocked review canceled %d jobs, want 0", canceled)
	}
	if _, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim blocked review: %v", err)
	} else if found {
		t.Fatal("blocked review job was claimable, want dormant")
	}
}

func TestReviewDispatchChecklistRaceCancelsWithoutTransition(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-checklist-race")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review checklist race", domain.HumanReviewWavedThrough)
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")

	queue := jobqueue.NewService(pool)
	if job, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim review before checklist mutation: %v", err)
	} else if !found || job.ID != dispatchID {
		t.Fatalf("claim review before checklist mutation = found %t id %s, want %s", found, job.ID, dispatchID)
	}

	service := workitems.NewService(pool, writer)
	if _, err := service.UpdateMetadata(ctx, item.ID, workitems.UpdateMetadataInput{
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	}); err != nil {
		t.Fatalf("replace review checklist after lease: %v", err)
	}

	result, err := service.StartReviewDispatch(ctx, dispatchID, 1, systemTok.Token)
	if err != nil {
		t.Fatalf("resolve checklist-changed review claim: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchCanceled || result.Transitioned {
		t.Fatalf("checklist-changed review outcome = %+v, want canceled without transition", result)
	}
	current, err := service.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("get checklist-changed review item: %v", err)
	}
	if current.State != domain.WorkItemTriaged {
		t.Fatalf("checklist-changed review state = %s, want triaged", current.State)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobCanceled, 1, false)
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 0 {
		t.Fatalf("checklist-changed review transition events = %d, want 0", got)
	}
}

func TestDispatchReconciliationCancelsOnlyTerminalStaleAndMalformed(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-reconcile")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	service := workitems.NewService(pool, writer)

	terminal := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "terminal review", domain.HumanReviewWavedThrough)
	terminalJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, terminal.ID, terminal.State, terminal.StateEnteredAt.Unix(), "reviewer@1")
	if _, err := service.Transition(ctx, terminal.ID, domain.WorkItemDone, "already completed", systemTok.Token); err != nil {
		t.Fatalf("terminalize queued review: %v", err)
	}

	stale := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "stale review", domain.HumanReviewWavedThrough)
	staleJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, stale.ID, stale.State, stale.StateEnteredAt.Unix(), "reviewer@1")
	if _, err := service.Transition(ctx, stale.ID, domain.WorkItemPlanned, "new state epoch", systemTok.Token); err != nil {
		t.Fatalf("advance queued review epoch: %v", err)
	}

	malformed := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "malformed review", domain.HumanReviewWavedThrough)
	malformedJob := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, malformed.ID, map[string]any{
		"work_item_id":          malformed.ID,
		"state":                 malformed.State,
		"state_entered_at_unix": "not-an-epoch",
		"cultivar":              "reviewer@1",
	})
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET state = 'leased', lease_until = now() - interval '1 second' WHERE id = $1`, malformedJob); err != nil {
		t.Fatalf("make malformed job an expired lease: %v", err)
	}

	humanBlocked := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "human blocked review", domain.HumanReviewBlocked)
	humanBlockedJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, humanBlocked.ID, humanBlocked.State, humanBlocked.StateEnteredAt.Unix(), "reviewer@1")

	lifecycleBlocked := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "lifecycle blocked review", domain.HumanReviewWavedThrough)
	lifecycleBlockedJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, lifecycleBlocked.ID, lifecycleBlocked.State, lifecycleBlocked.StateEnteredAt.Unix(), "reviewer@1")
	if _, err := service.Transition(ctx, lifecycleBlocked.ID, domain.WorkItemBlocked, "waiting on external signal", systemTok.Token); err != nil {
		t.Fatalf("block queued review lifecycle: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET state = 'leased', attempts = 1, lease_until = now() - interval '1 second' WHERE id = $1`, lifecycleBlockedJob); err != nil {
		t.Fatalf("make lifecycle-blocked job an expired lease: %v", err)
	}

	wrongChecks, err := service.Create(ctx, workitems.CreateInput{
		Title:                      "reviewer with wrong checks",
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{"cmd:go test ./..."},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   "reviewer@1",
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create reviewer with wrong checks: %v", err)
	}
	wrongChecksJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, wrongChecks.ID, wrongChecks.State, wrongChecks.StateEnteredAt.Unix(), "reviewer@1")

	nonReview := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "valid non-review dispatch")
	nonReviewJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, nonReview.ID, nonReview.State, nonReview.StateEnteredAt.Unix(), "checklist-worker@1")

	queue := jobqueue.NewService(pool)
	canceled, err := queue.ReconcileDispatchJobs(ctx)
	if err != nil {
		t.Fatalf("reconcile dispatch jobs: %v", err)
	}
	if canceled != 4 {
		t.Fatalf("reconciled canceled jobs = %d, want 4", canceled)
	}
	for _, id := range []uuid.UUID{terminalJob, staleJob, malformedJob, wrongChecksJob} {
		assertJobState(t, ctx, pool, id, jobqueue.JobCanceled, 0, false)
	}
	for _, id := range []uuid.UUID{humanBlockedJob, nonReviewJob} {
		assertJobState(t, ctx, pool, id, jobqueue.JobPending, 0, false)
	}
	assertJobState(t, ctx, pool, lifecycleBlockedJob, jobqueue.JobPending, 1, false)
	if _, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim after reconciliation: %v", err)
	} else if found {
		t.Fatal("claim after reconciliation found blocked/non-review job, want none")
	}
}

func TestDispatchSupersessionKeepsNewestGenerationAcrossReplay(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-supersession-generations")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "dispatch payload generations")

	legacyID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "convergence-scribe@1",
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	routedID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "convergence-scribe@1",
		"capability":             "convergence-scribe@1",
		"origin_token_id":        systemTok.Token.ID,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	semanticID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "convergence-scribe@1",
		"capability":             "convergence.propose_checks",
		"origin_token_id":        systemTok.Token.ID,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	if legacyID == routedID || legacyID == semanticID || routedID == semanticID {
		t.Fatalf("payload generations must have distinct event ids: legacy=%s routed=%s semantic=%s", legacyID, routedID, semanticID)
	}

	var legacySeq, routedSeq, semanticSeq int64
	if err := pool.QueryRow(ctx, `SELECT seq FROM events WHERE id = $1`, legacyID).Scan(&legacySeq); err != nil {
		t.Fatalf("read legacy seq: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT seq FROM events WHERE id = $1`, routedID).Scan(&routedSeq); err != nil {
		t.Fatalf("read routed seq: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT seq FROM events WHERE id = $1`, semanticID).Scan(&semanticSeq); err != nil {
		t.Fatalf("read semantic seq: %v", err)
	}
	if !(legacySeq < routedSeq && routedSeq < semanticSeq) {
		t.Fatalf("dispatch seq order = %d, %d, %d, want strictly increasing", legacySeq, routedSeq, semanticSeq)
	}

	// Projection is append-only with respect to sibling queue rows so it never
	// acquires old-job locks while the producer holds the new row. Claim-time
	// reduction still exposes only the latest generation before cleanup runs.
	assertJobState(t, ctx, pool, legacyID, jobqueue.JobPending, 0, false)
	assertJobState(t, ctx, pool, routedID, jobqueue.JobPending, 0, false)
	assertJobState(t, ctx, pool, semanticID, jobqueue.JobPending, 0, false)
	queue := jobqueue.NewService(pool)
	latest, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim latest projected generation: %v", err)
	}
	if !found || latest.ID != semanticID {
		t.Fatalf("latest projected claim = found %t id %s, want %s", found, latest.ID, semanticID)
	}
	if err := queue.MarkCanceled(ctx, semanticID, latest.Attempts); err != nil {
		t.Fatalf("close latest projected claim: %v", err)
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile projected generations: %v", err)
	} else if canceled != 2 {
		t.Fatalf("reconcile projected generations canceled %d jobs, want 2", canceled)
	}

	// Simulate the migration/replay shape: reconstruct one pending queue row per
	// immutable dispatch event, then reduce operational state from event seq.
	if _, err := pool.Exec(ctx, `DELETE FROM job_queue WHERE id IN ($1, $2, $3)`, legacyID, routedID, semanticID); err != nil {
		t.Fatalf("clear reconstructed dispatch jobs: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO job_queue (id, kind, work_item_id, state, payload, created_at, updated_at)
		SELECT id, 'dispatch', subject_id, 'pending', payload, occurred_at, occurred_at
		FROM events
		WHERE id IN ($1, $2, $3)
	`, legacyID, routedID, semanticID); err != nil {
		t.Fatalf("rebuild dispatch jobs from events: %v", err)
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile reconstructed generations: %v", err)
	} else if canceled != 2 {
		t.Fatalf("reconcile reconstructed generations canceled %d jobs, want 2", canceled)
	}
	assertJobState(t, ctx, pool, legacyID, jobqueue.JobCanceled, 0, false)
	assertJobState(t, ctx, pool, routedID, jobqueue.JobCanceled, 0, false)
	assertJobState(t, ctx, pool, semanticID, jobqueue.JobPending, 0, false)
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("second reconstructed reconcile: %v", err)
	} else if canceled != 0 {
		t.Fatalf("second reconstructed reconcile canceled %d jobs, want 0", canceled)
	}
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventDispatchRequested); got != 3 {
		t.Fatalf("dispatch events after operational replay = %d, want 3 immutable facts", got)
	}
}

func TestDispatchRepairsMalformedLatestGeneration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-malformed-repair")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedChecklistWorkerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "repair malformed latest dispatch")
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	// The malformed event is the first demand, covering restart idempotency
	// when no older plain valid payload exists to absorb a later retry.
	malformedID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":          item.ID,
		"state":                 item.State,
		"state_event_id":        uuid.New(),
		"state_entered_at_unix": item.StateEnteredAt.Unix(),
		"cultivar":              "checklist-worker@1",
		"capability":            "work_items.execute_checks",
		"reason":                dispatchReasonAgentAttentionRequested,
	})
	if _, err := jobqueue.LatestValidDispatch(ctx, pool, item.ID); !errors.Is(err, jobqueue.ErrInvalidDispatchDemand) {
		t.Fatalf("malformed newest identity error = %v, want ErrInvalidDispatchDemand", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE job_queue
		SET state='leased', attempts=1, lease_until=clock_timestamp()+interval '1 minute'
		WHERE id=$1
	`, malformedID); err != nil {
		t.Fatalf("simulate legacy lease of malformed demand: %v", err)
	}

	repaired, err := w.scanDispatch(ctx)
	if err != nil || repaired.DispatchesRequested != 1 {
		t.Fatalf("repair dispatch = %+v (%v), want one fresh repair", repaired, err)
	}
	var repairID uuid.UUID
	var repairPayload struct {
		StateEventID              uuid.UUID `json:"state_event_id"`
		SupersedesDispatchEventID uuid.UUID `json:"supersedes_dispatch_event_id"`
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT id, payload FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3
		ORDER BY seq DESC LIMIT 1
	`, domain.SubjectWorkItem, item.ID, domain.EventDispatchRequested).Scan(&repairID, &raw); err != nil {
		t.Fatalf("load repair: %v", err)
	}
	if err := json.Unmarshal(raw, &repairPayload); err != nil {
		t.Fatalf("decode repair payload: %v", err)
	}
	stateEventID, err := dispatchStateEntryID(ctx, pool, item.ID)
	if err != nil {
		t.Fatalf("resolve current state event: %v", err)
	}
	if repairID == malformedID || repairPayload.StateEventID != stateEventID || repairPayload.SupersedesDispatchEventID != malformedID {
		t.Fatalf("repair id/payload = %s %+v, want distinct repair of %s at state event %s", repairID, repairPayload, malformedID, stateEventID)
	}
	repeat, err := w.scanDispatch(ctx)
	if err != nil || repeat.DispatchesRequested != 0 || repeat.DispatchesAlreadyRequested != 1 {
		t.Fatalf("repeat repair scan = %+v (%v), want idempotent existing demand", repeat, err)
	}
	queue := jobqueue.NewService(pool)
	job, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim while malformed predecessor live: %v", err)
	}
	if found {
		t.Fatalf("claim while malformed predecessor live = %s, want replacement parked", job.ID)
	}
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, malformedID); err != nil {
		t.Fatalf("expire malformed predecessor lease: %v", err)
	}
	job, found, err = queue.ClaimNext(ctx, time.Minute)
	if err != nil || !found || job.ID != repairID {
		t.Fatalf("claim repaired demand = found %t id %s err %v, want %s", found, job.ID, err, repairID)
	}
}

func TestDispatchQuotedEpochFailsClosedInQueueSQL(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-quoted-epoch")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "quoted dispatch epoch")
	stateEventID, err := dispatchStateEntryID(ctx, pool, item.ID)
	if err != nil {
		t.Fatalf("resolve state entry: %v", err)
	}
	demandID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id": item.ID, "state": item.State, "state_event_id": stateEventID,
		"state_entered_at_unix": strconv.FormatInt(item.StateEnteredAt.Unix(), 10),
		"cultivar":              "checklist-worker@1", "capability": "work_items.execute_checks",
		"reason": dispatchReasonAgentAttentionRequested,
	})
	if _, err := jobqueue.ResolveDispatchIdentity(ctx, pool, demandID); !errors.Is(err, jobqueue.ErrInvalidDispatchDemand) {
		t.Fatalf("quoted epoch identity = %v, want ErrInvalidDispatchDemand", err)
	}
	queue := jobqueue.NewService(pool)
	if job, found, err := queue.ClaimNext(ctx, time.Minute); err != nil || found {
		t.Fatalf("quoted epoch queue claim = found %t id %s err %v, want refusal", found, job.ID, err)
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil || canceled != 1 {
		t.Fatalf("quoted epoch reconcile canceled %d err %v, want 1", canceled, err)
	}
	assertJobState(t, ctx, pool, demandID, jobqueue.JobCanceled, 0, false)
}

func TestDispatchGenerationCausalChainAllowsRouteAToBToA(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-route-cycle")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "dispatch route A to B to A")
	stateEventID, err := dispatchStateEntryID(ctx, pool, item.ID)
	if err != nil {
		t.Fatalf("resolve dispatch state entry: %v", err)
	}
	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	candidate := dispatchCandidate{
		ID: item.ID, State: item.State, StateEnteredAt: item.StateEnteredAt,
		StateEventID: stateEventID,
	}
	routeA := dispatchRoute{Cultivar: "route-a@1", Capability: "route.a"}
	routeB := dispatchRoute{Cultivar: "route-b@1", Capability: "route.b"}
	for i, route := range []dispatchRoute{routeA, routeB, routeA} {
		fresh, err := w.appendDispatch(ctx, candidate, route, dispatchReasonAgentAttentionRequested)
		if err != nil {
			t.Fatalf("append route generation %d: %v", i+1, err)
		}
		if !fresh {
			t.Fatalf("append route generation %d reported existing, want fresh", i+1)
		}
	}
	if fresh, err := w.appendDispatch(ctx, candidate, routeA, dispatchReasonAgentAttentionRequested); err != nil {
		t.Fatalf("repeat final route: %v", err)
	} else if fresh {
		t.Fatal("repeat final route appended another generation")
	}

	rows, err := pool.Query(ctx, `
		SELECT id, payload->>'supersedes_dispatch_event_id'
		FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3
		ORDER BY seq ASC
	`, domain.SubjectWorkItem, item.ID, domain.EventDispatchRequested)
	if err != nil {
		t.Fatalf("query route generations: %v", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	var predecessors []*uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		var predecessor *uuid.UUID
		if err := rows.Scan(&id, &predecessor); err != nil {
			t.Fatalf("scan route generation: %v", err)
		}
		ids = append(ids, id)
		predecessors = append(predecessors, predecessor)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate route generations: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("route generation count = %d, want 3", len(ids))
	}
	if ids[0] == ids[2] {
		t.Fatalf("final A generation reused original A event id %s", ids[0])
	}
	if predecessors[0] != nil || predecessors[1] == nil || *predecessors[1] != ids[0] || predecessors[2] == nil || *predecessors[2] != ids[1] {
		t.Fatalf("causal predecessors = %v, want nil, %s, %s", predecessors, ids[0], ids[1])
	}
	job, found, err := jobqueue.NewService(pool).ClaimNext(ctx, time.Minute)
	if err != nil || !found || job.ID != ids[2] {
		t.Fatalf("claim final route generation = found %t id %s err %v, want %s", found, job.ID, err, ids[2])
	}
}

func TestDispatchStateEventIdentitySeparatesSameSecondReentryAndSkipsNoop(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-same-second-state-entry")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedChecklistWorkerCultivar(t, ctx, pool, writer, systemTok.Token)

	itemID := uuid.New()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin same-second fixture: %v", err)
	}
	createdID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventWorkItemCreated, Source: domain.SourceSystem, ActorTokenID: &systemTok.Token.ID,
		Payload: map[string]any{
			"title": "same-second triaged re-entry", "state": domain.WorkItemTriaged,
			"suggested_convergence_checks": []string{"cmd:go test ./..."},
			"human_review_status":          domain.HumanReviewWavedThrough,
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append created: %v", err)
	}
	var enteredAt time.Time
	if err := tx.QueryRow(ctx, `SELECT occurred_at FROM events WHERE id=$1`, createdID).Scan(&enteredAt); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("read created time: %v", err)
	}
	oldDemandID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventDispatchRequested, Source: domain.SourceSystem, ActorTokenID: &systemTok.Token.ID,
		Payload: map[string]any{
			"work_item_id": itemID, "state": domain.WorkItemTriaged, "state_event_id": createdID,
			"state_entered_at_unix": enteredAt.Unix(), "cultivar": "checklist-worker@1",
			"capability": "work_items.execute_checks", "reason": dispatchReasonAgentAttentionRequested,
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append old demand: %v", err)
	}
	appendTransition := func(from, to domain.WorkItemState, discriminator string) uuid.UUID {
		t.Helper()
		id, _, appendErr := writer.Append(ctx, tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
			Kind: domain.EventWorkItemTransitioned, Source: domain.SourceSystem, ActorTokenID: &systemTok.Token.ID,
			Discriminator: discriminator,
			Payload:       map[string]any{"from": from, "to": to, "reason": discriminator},
		})
		if appendErr != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("append %s: %v", discriminator, appendErr)
		}
		return id
	}
	appendTransition(domain.WorkItemTriaged, domain.WorkItemPlanned, "same-second-leave")
	reentryID := appendTransition(domain.WorkItemPlanned, domain.WorkItemTriaged, "same-second-reenter")
	noopID := appendTransition(domain.WorkItemTriaged, domain.WorkItemTriaged, "same-second-noop")
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit same-second fixture: %v", err)
	}
	var reentryAt time.Time
	if err := pool.QueryRow(ctx, `SELECT occurred_at FROM events WHERE id=$1`, reentryID).Scan(&reentryAt); err != nil {
		t.Fatalf("read reentry time: %v", err)
	}
	if enteredAt.Unix() != reentryAt.Unix() {
		t.Fatalf("fixture crossed Unix second: created=%s reentry=%s", enteredAt, reentryAt)
	}
	legacyAmbiguousID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, itemID, map[string]any{
		"work_item_id": itemID, "state": domain.WorkItemTriaged,
		"state_entered_at_unix": reentryAt.Unix(), "cultivar": "checklist-worker@1",
		"capability": "work_items.execute_checks", "reason": dispatchReasonAgentAttentionRequested,
	})
	if _, err := jobqueue.ResolveDispatchIdentity(ctx, pool, legacyAmbiguousID); !errors.Is(err, jobqueue.ErrInvalidDispatchDemand) {
		t.Fatalf("same-second legacy identity = %v, want ambiguous ErrInvalidDispatchDemand", err)
	}
	if job, found, err := jobqueue.NewService(pool).ClaimNext(ctx, time.Minute); err != nil || found {
		t.Fatalf("ambiguous legacy claim = found %t id %s err %v, want refusal", found, job.ID, err)
	}

	w, err := New(pool, writer, Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &systemTok.Token.ID, nil)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	result, err := w.scanDispatch(ctx)
	if err != nil || result.DispatchesRequested != 1 {
		t.Fatalf("dispatch reentered epoch = %+v (%v), want one fresh", result, err)
	}
	var newDemandID uuid.UUID
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT id, payload FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3
		ORDER BY seq DESC LIMIT 1
	`, domain.SubjectWorkItem, itemID, domain.EventDispatchRequested).Scan(&newDemandID, &raw); err != nil {
		t.Fatalf("load new demand: %v", err)
	}
	var payload struct {
		StateEventID              uuid.UUID `json:"state_event_id"`
		SupersedesDispatchEventID uuid.UUID `json:"supersedes_dispatch_event_id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode new demand: %v", err)
	}
	if newDemandID == oldDemandID || payload.StateEventID != reentryID || payload.StateEventID == noopID || payload.SupersedesDispatchEventID != legacyAmbiguousID {
		t.Fatalf("new demand %s payload=%+v, want distinct repair of %s at reentry %s (not noop %s)", newDemandID, payload, legacyAmbiguousID, reentryID, noopID)
	}
	job, found, err := jobqueue.NewService(pool).ClaimNext(ctx, time.Minute)
	if err != nil || !found || job.ID != newDemandID {
		t.Fatalf("same-second claim = found %t id %s err %v, want %s", found, job.ID, err, newDemandID)
	}
}

func TestDispatchSupersessionWaitsForOlderBoundedLease(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-supersession-lease")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "bounded superseded lease")
	olderID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "checklist-worker@1")
	queue := jobqueue.NewService(pool)
	claimed, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim older generation: %v", err)
	}
	if !found || claimed.ID != olderID {
		t.Fatalf("older claim = found %t id %s, want %s", found, claimed.ID, olderID)
	}

	newerID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "checklist-worker@1",
		"capability":             "work_items.execute_checks",
		"origin_token_id":        systemTok.Token.ID,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	assertJobState(t, ctx, pool, olderID, jobqueue.JobLeased, 1, true)
	assertJobState(t, ctx, pool, newerID, jobqueue.JobPending, 0, false)
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim replacement behind live predecessor: %v", err)
	} else if found {
		t.Fatal("replacement was claimable while an older generation held a live lease")
	}

	if _, err := pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, olderID); err != nil {
		t.Fatalf("expire older generation lease: %v", err)
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile expired predecessor: %v", err)
	} else if canceled != 1 {
		t.Fatalf("reconcile expired predecessor canceled %d jobs, want 1", canceled)
	}
	assertJobState(t, ctx, pool, olderID, jobqueue.JobCanceled, 1, false)
	latest, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim replacement after predecessor expiry: %v", err)
	}
	if !found || latest.ID != newerID {
		t.Fatalf("replacement claim = found %t id %s, want %s", found, latest.ID, newerID)
	}
}

func TestExpiredSupersededAttemptCannotCompleteReplacement(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-expired-superseded-attempt")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	queue := jobqueue.NewService(pool)

	for _, lateCompletionFirst := range []bool{true, false} {
		name := "replacement_claim_then_late_completion"
		if lateCompletionFirst {
			name = "late_completion_then_replacement_claim"
		}
		t.Run(name, func(t *testing.T) {
			item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, name)
			olderID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "checklist-worker@1")
			older, found, err := queue.ClaimNext(ctx, time.Minute)
			if err != nil {
				t.Fatalf("claim older generation: %v", err)
			}
			if !found || older.ID != olderID {
				t.Fatalf("older claim = found %t id %s, want %s", found, older.ID, olderID)
			}
			newerID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
				"work_item_id":           item.ID,
				"state":                  item.State,
				"state_entered_at_unix":  item.StateEnteredAt.Unix(),
				"cultivar":               "checklist-worker@1",
				"capability":             "work_items.execute_checks",
				"origin_token_id":        systemTok.Token.ID,
				"reason":                 "agent_attention_requested",
				"source_reconciler_pass": "dispatch",
			})
			if _, err := pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, olderID); err != nil {
				t.Fatalf("expire older generation: %v", err)
			}

			assertLateCompletionRejected := func() {
				t.Helper()
				if err := queue.MarkDone(ctx, olderID, older.Attempts); !errors.Is(err, pgx.ErrNoRows) {
					t.Fatalf("late older completion = %v, want pgx.ErrNoRows", err)
				}
			}
			if lateCompletionFirst {
				assertLateCompletionRejected()
			}
			latest, found, err := queue.ClaimNext(ctx, time.Minute)
			if err != nil {
				t.Fatalf("claim replacement: %v", err)
			}
			if !found || latest.ID != newerID {
				t.Fatalf("replacement claim = found %t id %s, want %s", found, latest.ID, newerID)
			}
			if !lateCompletionFirst {
				assertLateCompletionRejected()
			}
			assertJobState(t, ctx, pool, olderID, jobqueue.JobLeased, older.Attempts, true)
			assertJobState(t, ctx, pool, newerID, jobqueue.JobLeased, latest.Attempts, true)

			if err := queue.MarkDone(ctx, newerID, latest.Attempts); err != nil {
				t.Fatalf("complete replacement: %v", err)
			}
			if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
				t.Fatalf("reconcile expired predecessor: %v", err)
			} else if canceled != 1 {
				t.Fatalf("reconcile expired predecessor canceled %d jobs, want 1", canceled)
			}
			assertJobState(t, ctx, pool, olderID, jobqueue.JobCanceled, older.Attempts, false)
			assertJobState(t, ctx, pool, newerID, jobqueue.JobDone, latest.Attempts, false)
		})
	}
}

func TestDispatchCompletionSatisfiesLogicalDemandAcrossGenerations(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-generation-completion")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "completed dispatch generation")
	olderID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "checklist-worker@1")
	queue := jobqueue.NewService(pool)
	if job, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim older generation: %v", err)
	} else if !found || job.ID != olderID {
		t.Fatalf("older generation claim = found %t id %s, want %s", found, job.ID, olderID)
	}
	newerID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "checklist-worker@1",
		"capability":             "work_items.execute_checks",
		"origin_token_id":        systemTok.Token.ID,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	if err := queue.MarkDone(ctx, olderID, 1); err != nil {
		t.Fatalf("complete older generation: %v", err)
	}
	assertJobState(t, ctx, pool, olderID, jobqueue.JobDone, 1, false)
	assertJobState(t, ctx, pool, newerID, jobqueue.JobPending, 0, false)
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim completed logical demand: %v", err)
	} else if found {
		t.Fatal("replacement was claimable after an older generation completed the logical demand")
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile completed logical demand: %v", err)
	} else if canceled != 1 {
		t.Fatalf("reconcile completed logical demand canceled %d jobs, want 1", canceled)
	}
	assertJobState(t, ctx, pool, newerID, jobqueue.JobCanceled, 0, false)

	// Repair a pre-fix/replayed operational row without changing the immutable
	// event facts. Reconciliation must reach the same completed reduction.
	if _, err := pool.Exec(ctx, `UPDATE job_queue SET state = 'pending', lease_until = NULL WHERE id = $1`, newerID); err != nil {
		t.Fatalf("restore replay-shaped replacement: %v", err)
	}
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile replacement after completed sibling: %v", err)
	} else if canceled != 1 {
		t.Fatalf("reconcile replacement after completed sibling canceled %d jobs, want 1", canceled)
	}
	assertJobState(t, ctx, pool, newerID, jobqueue.JobCanceled, 0, false)
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("idempotent completed-demand reconcile: %v", err)
	} else if canceled != 0 {
		t.Fatalf("idempotent completed-demand reconcile canceled %d jobs, want 0", canceled)
	}
}

func TestDispatchFailedAndCanceledGenerationsDoNotSatisfyDemand(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-generation-unsatisfied")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	queue := jobqueue.NewService(pool)

	for _, terminal := range []jobqueue.JobState{jobqueue.JobFailed, jobqueue.JobCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "unsatisfied "+string(terminal)+" generation")
			olderID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "checklist-worker@1")
			if job, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
				t.Fatalf("claim older generation: %v", err)
			} else if !found || job.ID != olderID {
				t.Fatalf("older generation claim = found %t id %s, want %s", found, job.ID, olderID)
			}
			newerID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
				"work_item_id":           item.ID,
				"state":                  item.State,
				"state_entered_at_unix":  item.StateEnteredAt.Unix(),
				"cultivar":               "checklist-worker@1",
				"capability":             "work_items.execute_checks",
				"origin_token_id":        systemTok.Token.ID,
				"reason":                 "agent_attention_requested",
				"source_reconciler_pass": "dispatch",
			})
			if terminal == jobqueue.JobFailed {
				err = queue.MarkFailed(ctx, olderID, 1)
			} else {
				err = queue.MarkCanceled(ctx, olderID, 1)
			}
			if err != nil {
				t.Fatalf("mark older generation %s: %v", terminal, err)
			}
			latest, found, err := queue.ClaimNext(ctx, time.Minute)
			if err != nil {
				t.Fatalf("claim replacement after %s: %v", terminal, err)
			}
			if !found || latest.ID != newerID {
				t.Fatalf("replacement after %s = found %t id %s, want %s", terminal, found, latest.ID, newerID)
			}
			if err := queue.MarkCanceled(ctx, newerID, latest.Attempts); err != nil {
				t.Fatalf("clean up replacement lease: %v", err)
			}
		})
	}
}

func TestClaimNextRejectsMalformedAndStaleDispatchBeforeReconcile(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-claim-revalidation")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)

	stale := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "stale generic dispatch")
	staleID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, stale.ID, stale.State, stale.StateEnteredAt.Unix(), "checklist-worker@1")
	if _, err := service.Transition(ctx, stale.ID, domain.WorkItemPlanned, "advance before claim", systemTok.Token); err != nil {
		t.Fatalf("advance stale dispatch item: %v", err)
	}

	malformed := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "malformed generic dispatch")
	malformedID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, malformed.ID, map[string]any{
		"work_item_id":           malformed.ID,
		"state":                  malformed.State,
		"state_entered_at_unix":  "not-an-epoch",
		"cultivar":               "checklist-worker@1",
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})

	valid := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "valid generic dispatch")
	validID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, valid.ID, valid.State, valid.StateEnteredAt.Unix(), "checklist-worker@1")
	queue := jobqueue.NewService(pool)
	job, found, err := queue.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim with malformed and stale predecessors: %v", err)
	}
	if !found || job.ID != validID {
		t.Fatalf("claim with malformed and stale predecessors = found %t id %s, want valid %s", found, job.ID, validID)
	}
	assertJobState(t, ctx, pool, staleID, jobqueue.JobPending, 0, false)
	assertJobState(t, ctx, pool, malformedID, jobqueue.JobPending, 0, false)
	if err := queue.MarkCanceled(ctx, validID, job.Attempts); err != nil {
		t.Fatalf("clean up valid claim: %v", err)
	}
	if _, found, err := queue.ClaimNext(ctx, time.Minute); err != nil {
		t.Fatalf("claim after valid dispatch: %v", err)
	} else if found {
		t.Fatal("generic claim admitted a malformed or prior-epoch dispatch")
	}
}

func TestDispatchSupersessionDoesNotCollapseNewStateEpoch(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "dispatch-supersession-epoch")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	item := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "distinct dispatch epochs")
	oldEpochID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "checklist-worker@1")
	advanced, err := workitems.NewService(pool, writer).Transition(ctx, item.ID, domain.WorkItemPlanned, "advance dispatch epoch", systemTok.Token)
	if err != nil {
		t.Fatalf("advance work item: %v", err)
	}
	newEpochID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, advanced.ID, advanced.State, advanced.StateEnteredAt.Unix(), "checklist-worker@1")

	// Projection-time supersession must not conflate two lifecycle epochs.
	assertJobState(t, ctx, pool, oldEpochID, jobqueue.JobPending, 0, false)
	assertJobState(t, ctx, pool, newEpochID, jobqueue.JobPending, 0, false)
	queue := jobqueue.NewService(pool)
	if canceled, err := queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("reconcile distinct epochs: %v", err)
	} else if canceled != 1 {
		t.Fatalf("reconcile distinct epochs canceled %d jobs, want stale prior epoch only", canceled)
	}
	assertJobState(t, ctx, pool, oldEpochID, jobqueue.JobCanceled, 0, false)
	assertJobState(t, ctx, pool, newEpochID, jobqueue.JobPending, 0, false)
}

func TestReviewDispatchFinalAdmissionRejectsNewerGeneration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-superseded-after-claim")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)
	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review dispatch generation race", domain.HumanReviewWavedThrough)
	olderID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")
	queue := jobqueue.NewService(pool)
	if job, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim older review generation: %v", err)
	} else if !found || job.ID != olderID {
		t.Fatalf("older review claim = found %t id %s, want %s", found, job.ID, olderID)
	}

	newerID := appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, systemTok.Token, item.ID, map[string]any{
		"work_item_id":           item.ID,
		"state":                  item.State,
		"state_entered_at_unix":  item.StateEnteredAt.Unix(),
		"cultivar":               "reviewer@1",
		"capability":             "review.exact_artifact",
		"origin_token_id":        systemTok.Token.ID,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
	if _, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim latest review behind older lease: %v", err)
	} else if found {
		t.Fatal("latest review generation was claimable while its predecessor held a live lease")
	}
	result, err := workitems.NewService(pool, writer).StartReviewDispatch(ctx, olderID, 1, systemTok.Token)
	if err != nil {
		t.Fatalf("admit superseded review generation: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchCanceled || result.Transitioned {
		t.Fatalf("superseded review admission = %+v, want canceled without transition", result)
	}
	assertJobState(t, ctx, pool, olderID, jobqueue.JobCanceled, 1, false)
	if got := countEventsForSubject(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 0 {
		t.Fatalf("transition events from superseded review generation = %d, want 0", got)
	}

	latest, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim latest review generation: %v", err)
	}
	if !found || latest.ID != newerID {
		t.Fatalf("latest review claim = found %t id %s, want %s", found, latest.ID, newerID)
	}
}

func TestReviewDispatchCrossChecksCreatedCultivarAndStateEntry(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-fractional-epoch")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	seedReviewerCultivar(t, ctx, pool, writer, systemTok.Token)

	// A reviewer-looking payload on an item whose creation event has no
	// reviewer cultivar must not be claimed.
	impostor := createDispatchableItem(t, ctx, pool, writer, systemTok.Token, "payload-only reviewer impostor")
	impostorJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, impostor.ID, impostor.State, impostor.StateEnteredAt.Unix(), "reviewer@1")

	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "review state entry", domain.HumanReviewWavedThrough)
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, item.StateEnteredAt.Unix(), "reviewer@1")

	queue := jobqueue.NewService(pool)
	job, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim review state entry: %v", err)
	}
	if !found || job.ID != dispatchID {
		t.Fatalf("claim review state entry = found %t id %s, want %s", found, job.ID, dispatchID)
	}
	assertJobState(t, ctx, pool, impostorJob, jobqueue.JobPending, 0, false)

	result, err := workitems.NewService(pool, writer).StartReviewDispatch(ctx, dispatchID, job.Attempts, systemTok.Token)
	if err != nil {
		t.Fatalf("start review state entry: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchStarted || !result.Transitioned {
		t.Fatalf("review start = %+v, want started", result)
	}
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobDone, 1, false)
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

func createReviewerDispatchableItem(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, title string, humanReview domain.HumanReviewStatus) domain.WorkItem {
	t.Helper()
	item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
		Title:                      title,
		State:                      domain.WorkItemTriaged,
		SuggestedConvergenceChecks: []string{workitems.ReviewVerdictCheck},
		HumanReviewStatus:          humanReview,
		Cultivar:                   "reviewer@1",
		Actor:                      actor,
	})
	if err != nil {
		t.Fatalf("create reviewer dispatchable item: %v", err)
	}
	return item
}

func appendDispatchRequestedForEpoch(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, workItemID uuid.UUID, state domain.WorkItemState, epoch int64, cultivar string) uuid.UUID {
	t.Helper()
	return appendDispatchRequestedPayloadForTest(t, ctx, pool, writer, actor, workItemID, map[string]any{
		"work_item_id":           workItemID,
		"state":                  state,
		"state_entered_at_unix":  epoch,
		"cultivar":               cultivar,
		"reason":                 "agent_attention_requested",
		"source_reconciler_pass": "dispatch",
	})
}

func appendDispatchRequestedPayloadForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, workItemID uuid.UUID, payload map[string]any) uuid.UUID {
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
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("append dispatch.requested: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit dispatch append: %v", err)
	}
	return id
}

func appendDispatchRequestedForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, workItemID uuid.UUID) uuid.UUID {
	t.Helper()
	var (
		state          string
		stateEnteredAt time.Time
	)
	if err := pool.QueryRow(ctx, `SELECT state, state_entered_at FROM work_items WHERE id = $1`, workItemID).Scan(&state, &stateEnteredAt); err != nil {
		t.Fatalf("read dispatch work item epoch: %v", err)
	}
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
			"state":                  state,
			"state_entered_at_unix":  stateEnteredAt.Unix(),
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

func assertJobState(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id uuid.UUID, want jobqueue.JobState, attempts int, wantLease bool) {
	t.Helper()
	var (
		state       string
		gotAttempts int
		leaseUntil  *time.Time
	)
	if err := pool.QueryRow(ctx, `SELECT state, attempts, lease_until FROM job_queue WHERE id = $1`, id).Scan(&state, &gotAttempts, &leaseUntil); err != nil {
		t.Fatalf("read job %s: %v", id, err)
	}
	if state != string(want) || gotAttempts != attempts || (leaseUntil != nil) != wantLease {
		t.Fatalf("job %s = state %s attempts %d lease %v, want state %s attempts %d lease=%t", id, state, gotAttempts, leaseUntil, want, attempts, wantLease)
	}
}
