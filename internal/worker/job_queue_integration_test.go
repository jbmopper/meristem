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
	"github.com/jbmopper/meristem/internal/registry"
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
	started, err := service.StartReviewDispatch(ctx, dispatchID, systemTok.Token)
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

	replayed, err := service.StartReviewDispatch(ctx, dispatchID, systemTok.Token)
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

	result, err := service.StartReviewDispatch(ctx, dispatchID, systemTok.Token)
	if err != nil {
		t.Fatalf("resolve blocked review claim: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchDormant || result.Transitioned {
		t.Fatalf("blocked review outcome = %+v, want dormant without transition", result)
	}
	// The dormant pass did no review work, so its claim attempt is refunded.
	assertJobState(t, ctx, pool, dispatchID, jobqueue.JobPending, 0, false)
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

// TestReviewDispatchBudgetDormantRefundsAttemptAndEscalates pins the 55d7995
// accepted-review nit (fixed under ee916614): a job made dormant by the
// concurrent-running budget stays claimable, so before the dormant refund
// every worker pass consumed one attempt against a job that was never
// startable — unbounded inflation for as long as the budget stayed full.
// Each full claim→dormant cycle must now be attempts-neutral.
func TestReviewDispatchBudgetDormantRefundsAttemptAndEscalates(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	systemTok, err := createSystemToken(t, ctx, pool, writer, "review-dispatch-budget-dormant")
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	// This test's database defines reviewer@1 itself (rootstocks are
	// immutable, so it cannot layer a variant on seedReviewerCultivar): the
	// xylem caps the dispatch actor at ONE concurrently running item, so a
	// single started review child exhausts the budget for every later one.
	svc := registry.NewService(pool, writer)
	if _, _, err := svc.DefineTropism(ctx, systemTok.Token, registry.DefineTropismInput{
		Name:        "checklist-all",
		Version:     1,
		Reducer:     registry.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "all checklist items pass",
	}); err != nil {
		t.Fatalf("define reviewer tropism: %v", err)
	}
	if _, _, err := svc.DefineCultivar(ctx, systemTok.Token, registry.DefineCultivarInput{
		Name:      "reviewer",
		Version:   1,
		Rootstock: true,
		Tropism:   registry.TropismRef{Name: "checklist-all", Version: 1},
		Profile: registry.Profile{
			Briefing:       "briefings/reviewer.md",
			ScopesTemplate: []string{"work_items.tree:{root}", "work_items.read", "work_items.write"},
		},
		Xylem:       registry.Xylem{MaxAttempts: 2, MaxWallSeconds: 3600, MaxDepth: 1, MaxConcurrentRunningPerToken: 1},
		Phloem:      "projection:work-item-brief",
		Description: "reviewer rootstock with a one-item concurrency budget",
	}); err != nil {
		t.Fatalf("define capped reviewer cultivar: %v", err)
	}

	createCapped := func(title string) domain.WorkItem {
		t.Helper()
		item, err := workitems.NewService(pool, writer).Create(ctx, workitems.CreateInput{
			Title:                      title,
			State:                      domain.WorkItemTriaged,
			SuggestedConvergenceChecks: []string{workitems.ReviewVerdictCheck},
			HumanReviewStatus:          domain.HumanReviewWavedThrough,
			Cultivar:                   "reviewer@1",
			Actor:                      systemTok.Token,
		})
		if err != nil {
			t.Fatalf("create capped reviewer item: %v", err)
		}
		return item
	}

	queue := jobqueue.NewService(pool)
	service := workitems.NewService(pool, writer)

	// First review child consumes the whole one-item budget.
	first := createCapped("budget dormant first")
	firstJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, first.ID, first.State, first.StateEnteredAt.Unix(), "reviewer@1")
	if job, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil || !found || job.ID != firstJob {
		t.Fatalf("claim first review = (%+v, %t, %v), want job %s", job, found, err, firstJob)
	}
	if started, err := service.StartReviewDispatch(ctx, firstJob, systemTok.Token); err != nil || started.Outcome != workitems.ReviewDispatchStarted {
		t.Fatalf("start first review = (%+v, %v), want started", started, err)
	}

	// Second child can be claimed but not started while the budget is full.
	// The dormant pass must refund its attempt — attempts count startable
	// work, never gate collisions — and the exhaustion escalates the item to
	// a human, which parks the job unclaimable instead of letting it spin.
	second := createCapped("budget dormant second")
	secondJob := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, second.ID, second.State, second.StateEnteredAt.Unix(), "reviewer@1")
	job, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil || !found || job.ID != secondJob {
		t.Fatalf("claim second review = (%+v, %t, %v), want job %s", job, found, err, secondJob)
	}
	if job.Attempts != 1 {
		t.Fatalf("claimed attempts = %d, want 1", job.Attempts)
	}
	result, err := service.StartReviewDispatch(ctx, secondJob, systemTok.Token)
	if err != nil {
		t.Fatalf("start second review: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchDormant || result.Transitioned {
		t.Fatalf("budget outcome = %+v, want dormant without transition", result)
	}
	assertJobState(t, ctx, pool, secondJob, jobqueue.JobPending, 0, false)

	// The exhaustion escalated the child to human attention; while the human
	// gate holds, the pending job is not claimable, so attempts cannot spin.
	got, err := service.Get(ctx, second.ID)
	if err != nil {
		t.Fatalf("get dormant child: %v", err)
	}
	if got.HumanReviewStatus != domain.HumanReviewBlocked {
		t.Fatalf("dormant child human_review_status = %s, want blocked via xylem escalation", got.HumanReviewStatus)
	}
	if _, found, err := queue.ClaimNextReview(ctx, time.Minute); err != nil {
		t.Fatalf("claim while escalated: %v", err)
	} else if found {
		t.Fatal("escalated dormant job was claimable, want parked")
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

	result, err := service.StartReviewDispatch(ctx, dispatchID, systemTok.Token)
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

func TestReviewDispatchEpochUsesFloorAndCrossChecksCreatedCultivar(t *testing.T) {
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

	item := createReviewerDispatchableItem(t, ctx, pool, writer, systemTok.Token, "fractional review epoch", domain.HumanReviewWavedThrough)
	var entered time.Time
	if err := pool.QueryRow(ctx, `
		UPDATE work_items
		SET state_entered_at = TIMESTAMPTZ '2030-01-01 00:00:00.9+00'
		WHERE id = $1
		RETURNING state_entered_at
	`, item.ID).Scan(&entered); err != nil {
		t.Fatalf("set fractional state epoch: %v", err)
	}
	if entered.Nanosecond() != 900_000_000 {
		t.Fatalf("fractional epoch nanoseconds = %d, want 900000000", entered.Nanosecond())
	}
	dispatchID := appendDispatchRequestedForEpoch(t, ctx, pool, writer, systemTok.Token, item.ID, item.State, entered.Unix(), "reviewer@1")

	queue := jobqueue.NewService(pool)
	job, found, err := queue.ClaimNextReview(ctx, time.Minute)
	if err != nil {
		t.Fatalf("claim .9 review epoch: %v", err)
	}
	if !found || job.ID != dispatchID {
		t.Fatalf("claim .9 review epoch = found %t id %s, want %s", found, job.ID, dispatchID)
	}
	assertJobState(t, ctx, pool, impostorJob, jobqueue.JobPending, 0, false)

	result, err := workitems.NewService(pool, writer).StartReviewDispatch(ctx, dispatchID, systemTok.Token)
	if err != nil {
		t.Fatalf("start .9 review epoch: %v", err)
	}
	if result.Outcome != workitems.ReviewDispatchStarted || !result.Transitioned {
		t.Fatalf(".9 review start = %+v, want started", result)
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
