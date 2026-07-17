package workitems

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/projections"
	registrydomain "github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// Slice 3a (ee916614, accepted lifecycle design revision 4): one transaction
// verifies capability and exact lease, mints the single-use exact-child
// credential, binds it, and reserves durable launch capacity — with
// event-caused review_launch state, hard ordinary-token expiry, and a
// durable-state recovery reconciler.

type provisionStack struct {
	pool   *pgxpool.Pool
	writer *events.Writer
	svc    *Service
	auth   *auth.Service
	queue  *jobqueue.Service
	root   domain.Token
	actorA domain.Token
	issuer domain.Token
}

func newProvisionStack(t *testing.T, ctx context.Context, reviewerWallSeconds int) provisionStack {
	t.Helper()
	pool := pgtest.NewPool(t, "meristem_provision")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	projectorRegistry := projections.NewRegistry()
	auth.RegisterProjectors(projectorRegistry)
	RegisterProjectors(projectorRegistry)
	registrydomain.RegisterProjectors(projectorRegistry)
	jobqueue.RegisterProjectors(projectorRegistry)
	writer := events.NewWriter(projectorRegistry)
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "provision-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	root := rootResult.Token
	actorA := createAssignmentToken(t, ctx, pool, writer, "provision-a", domain.SourceAgent, false, root)
	issuerResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "launch-issuer",
		Source: domain.SourceSystem,
		Scopes: []string{auth.ScopeReviewerCredentialsIssue},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create issuer: %v", err)
	}

	// The reviewer cultivar: claimNextReview matches cultivar root
	// "reviewer", and its ScopesTemplate carries the {root} placeholder that
	// provisioning must resolve to the exact child.
	registrySvc := registrydomain.NewService(pool, writer)
	if _, _, err := registrySvc.DefineTropism(ctx, root, registrydomain.DefineTropismInput{
		Name: "reviewer-checklist", Version: 1,
		Reducer:     registrydomain.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      []byte(`{"budget":{"max_attempts":3,"escalation":"hand_to_human"}}`),
		Description: "provision test reducer",
	}); err != nil {
		t.Fatalf("define reviewer tropism: %v", err)
	}
	if _, _, err := registrySvc.DefineCultivar(ctx, root, registrydomain.DefineCultivarInput{
		Name: "reviewer", Version: 1,
		Tropism: registrydomain.TropismRef{Name: "reviewer-checklist", Version: 1},
		Profile: registrydomain.Profile{
			Briefing:       "briefings/reviewer.md",
			ScopesTemplate: []string{"work_items.read", "work_items.tree:{root}"},
		},
		Xylem:       registrydomain.Xylem{MaxAttempts: 3, MaxWallSeconds: reviewerWallSeconds, MaxDepth: 1},
		Phloem:      "projection:work-item-brief",
		Description: "provision test reviewer",
	}); err != nil {
		t.Fatalf("define reviewer cultivar: %v", err)
	}

	return provisionStack{
		pool: pool, writer: writer,
		svc: NewService(pool, writer), auth: authSvc,
		queue: jobqueue.NewService(pool),
		root:  root, actorA: actorA, issuer: issuerResult.Token,
	}
}

// admitReviewChild walks the honest path: create the child on the reviewer
// cultivar, declare its round, request dispatch, claim the job with a
// concrete owner, admit via the launch protocol (job stays leased).
func (s provisionStack) admitReviewChild(t *testing.T, ctx context.Context, title, commit string) (domain.WorkItem, jobqueue.Job) {
	t.Helper()
	item, err := s.svc.Create(ctx, CreateInput{
		Title: title, State: domain.WorkItemCaptured,
		SuggestedConvergenceChecks: []string{ReviewVerdictCheck},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   "reviewer@1", Actor: s.actorA,
	})
	if err != nil {
		t.Fatalf("create review child: %v", err)
	}
	if commit != "" {
		if err := s.svc.AppendEvent(ctx, item.ID, "implementation.ready_for_review", map[string]any{"commit": commit}, s.actorA); err != nil {
			t.Fatalf("declare round: %v", err)
		}
	}
	stateEntered := item.StateEnteredAt.Unix()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin dispatch tx: %v", err)
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    item.ID,
		Kind:         domain.EventDispatchRequested,
		Source:       domain.SourceSystem,
		ActorTokenID: &s.issuer.ID,
		Payload: map[string]any{
			"work_item_id":          item.ID.String(),
			"state":                 string(item.State),
			"state_entered_at_unix": stateEntered,
			"cultivar":              "reviewer@1",
		},
	}); err != nil {
		t.Fatalf("append dispatch.requested: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit dispatch tx: %v", err)
	}
	job, found, err := s.queue.ClaimNextReviewAs(ctx, s.issuer.ID, time.Minute)
	if err != nil || !found {
		t.Fatalf("claim review job: found=%t err=%v", found, err)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != s.issuer.ID || job.LeaseGeneration == 0 {
		t.Fatalf("claimed lease not owner-fenced: %+v", job)
	}
	result, err := s.svc.StartReviewDispatchForLaunch(ctx, job.ID, s.issuer)
	if err != nil {
		t.Fatalf("launch admission: %v", err)
	}
	if result.Outcome != ReviewDispatchStarted || !result.Transitioned {
		t.Fatalf("launch admission outcome = %+v, want started+transitioned", result)
	}
	var jobState string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM job_queue WHERE id = $1`, job.ID).Scan(&jobState); err != nil {
		t.Fatalf("read job state: %v", err)
	}
	if jobState != "leased" {
		t.Fatalf("launch admission left job %s, want leased (dispatch is not complete until a launch outcome)", jobState)
	}
	return item, job
}

func (s provisionStack) provision(t *testing.T, ctx context.Context, item domain.WorkItem, job jobqueue.Job, maxConcurrent int) ProvisionSpawnedReviewResult {
	t.Helper()
	result, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, ProvisionSpawnedReviewInput{
		JobID: job.ID, WorkItemID: item.ID,
		Attempt: job.Attempts, LeaseGeneration: job.LeaseGeneration,
		MaxConcurrent: maxConcurrent,
	}, s.issuer)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	return result
}

func TestProvisionSpawnedReviewAtomicMintBindReserve(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "atomic provisioning child", "abc1234")
	result := s.provision(t, ctx, item, job, 4)

	if !result.SecretAvailable || result.Secret == "" {
		t.Fatal("first provision must return the secret exactly once")
	}
	if result.RoundCommit != "abc1234" {
		t.Fatalf("round commit = %q, want abc1234", result.RoundCommit)
	}

	// One database clock: credential expiry, binding ExpiresAt, and the
	// reservation deadline are the same instant, equal by construction.
	minted, err := s.auth.Authenticate(ctx, result.Secret)
	if err != nil {
		t.Fatalf("authenticate minted credential: %v", err)
	}
	if minted.ID != result.ReviewerTokenID {
		t.Fatalf("authenticated token %s, want %s", minted.ID, result.ReviewerTokenID)
	}
	if minted.ExpiresAt == nil || !minted.ExpiresAt.Equal(result.Assignment.ExpiresAt) {
		t.Fatalf("token expiry %v != binding ExpiresAt %v", minted.ExpiresAt, result.Assignment.ExpiresAt)
	}
	if !result.Deadline.Equal(result.Assignment.ExpiresAt) {
		t.Fatalf("reservation deadline %v != binding ExpiresAt %v", result.Deadline, result.Assignment.ExpiresAt)
	}

	// Scopes resolve {root} to the exact child; nothing portfolio-wide.
	wantScopes := []string{"work_items.read", "work_items.tree:" + item.ID.String()}
	if len(minted.Scopes) != len(wantScopes) || minted.Scopes[0] != wantScopes[0] || minted.Scopes[1] != wantScopes[1] {
		t.Fatalf("minted scopes = %v, want %v", minted.Scopes, wantScopes)
	}
	if minted.IsRoot || minted.Source != domain.SourceAgent {
		t.Fatalf("minted credential shape: root=%t source=%s", minted.IsRoot, minted.Source)
	}
	if result.Assignment.HolderTokenID != minted.ID || result.Assignment.Mode != domain.WorkItemAssignmentSpawn {
		t.Fatalf("binding = %+v, want spawn-mode holder %s", result.Assignment, minted.ID)
	}

	var launchState string
	var deadline time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT state, deadline FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, item.ID, result.RoundSeq, job.Attempts).Scan(&launchState, &deadline); err != nil {
		t.Fatalf("read reservation: %v", err)
	}
	if launchState != "reserved" || !deadline.Equal(result.Deadline) {
		t.Fatalf("reservation = %s@%v, want reserved@%v", launchState, deadline, result.Deadline)
	}

	// Replay of the committed attempt: same identifiers, no secret, no second
	// credential or generation.
	replay := s.provision(t, ctx, item, job, 4)
	if replay.SecretAvailable || replay.Secret != "" {
		t.Fatal("replay must never return or re-derive the secret")
	}
	if replay.ReviewerTokenID != result.ReviewerTokenID {
		t.Fatalf("replay minted a different credential: %s vs %s", replay.ReviewerTokenID, result.ReviewerTokenID)
	}
	if replay.Assignment.AssignmentEventID != result.Assignment.AssignmentEventID {
		t.Fatalf("replay produced a second generation: %s vs %s", replay.Assignment.AssignmentEventID, result.Assignment.AssignmentEventID)
	}
	if got := countAssignmentEvents(t, ctx, s.pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("assigned events after replay = %d, want 1", got)
	}
}

func TestProvisionSpawnedReviewFencing(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "fenced child", "fence111")

	base := ProvisionSpawnedReviewInput{
		JobID: job.ID, WorkItemID: item.ID,
		Attempt: job.Attempts, LeaseGeneration: job.LeaseGeneration,
		MaxConcurrent: 4,
	}

	// Capability and actor-class fences.
	plainSystem := createAssignmentToken(t, ctx, s.pool, s.writer, "no-capability-system", domain.SourceSystem, false, s.root)
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, base, plainSystem); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("no-capability actor = %v, want ErrInvalidRequest", err)
	}
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, base, s.root); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("root actor = %v, want ErrInvalidRequest", err)
	}
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, base, s.actorA); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("agent actor = %v, want ErrInvalidRequest", err)
	}

	// Concrete lease fencing: generation and attempt must match exactly.
	stale := base
	stale.LeaseGeneration = base.LeaseGeneration + 1
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, stale, s.issuer); !errors.Is(err, ErrProvisionLeaseFenced) {
		t.Fatalf("stale generation = %v, want ErrProvisionLeaseFenced", err)
	}
	wrongAttempt := base
	wrongAttempt.Attempt = base.Attempt + 1
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, wrongAttempt, s.issuer); !errors.Is(err, ErrProvisionLeaseFenced) {
		t.Fatalf("wrong attempt = %v, want ErrProvisionLeaseFenced", err)
	}

	// An owner-less legacy lease can never pass the fence: renewing under a
	// different owner is refused, and a job whose lease_owner is NULL is
	// fenced out even with a matching generation.
	if _, err := s.queue.RenewReviewLease(ctx, job.ID, uuid.New(), job.LeaseGeneration, time.Minute); err == nil {
		t.Fatal("foreign-owner renewal must be refused")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE job_queue SET lease_owner = NULL WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("null lease owner: %v", err)
	}
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, base, s.issuer); !errors.Is(err, ErrProvisionLeaseFenced) {
		t.Fatalf("owner-less lease = %v, want ErrProvisionLeaseFenced", err)
	}

	// A child without a valid current round has nothing to review.
	roundless, roundlessJob := s.admitReviewChild(t, ctx, "roundless child", "")
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, ProvisionSpawnedReviewInput{
		JobID: roundlessJob.ID, WorkItemID: roundless.ID,
		Attempt: roundlessJob.Attempts, LeaseGeneration: roundlessJob.LeaseGeneration,
		MaxConcurrent: 4,
	}, s.issuer); !errors.Is(err, ErrProvisionRoundInvalid) {
		t.Fatalf("roundless child = %v, want ErrProvisionRoundInvalid", err)
	}
}

func TestProvisionSpawnedReviewCapacityAndDormantRefund(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	first, firstJob := s.admitReviewChild(t, ctx, "capacity holder", "cap1111")
	s.provision(t, ctx, first, firstJob, 1)

	second, secondJob := s.admitReviewChild(t, ctx, "capacity blocked", "cap2222")
	_, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, ProvisionSpawnedReviewInput{
		JobID: secondJob.ID, WorkItemID: second.ID,
		Attempt: secondJob.Attempts, LeaseGeneration: secondJob.LeaseGeneration,
		MaxConcurrent: 1,
	}, s.issuer)
	if !errors.Is(err, ErrReviewLaunchCapacity) {
		t.Fatalf("capacity-blocked provision = %v, want ErrReviewLaunchCapacity", err)
	}
	// No credential and no binding leaked from the aborted transaction.
	if got := countAssignmentEvents(t, ctx, s.pool, second.ID, domain.EventWorkItemAssigned); got != 0 {
		t.Fatalf("blocked provision left %d assignments", got)
	}

	// Pre-binding capacity shortage does no review work: the claim's attempt
	// is refunded and the job parks dormant, fenced on owner+generation.
	if err := s.queue.ReturnReviewDormant(ctx, secondJob.ID, s.issuer.ID, secondJob.LeaseGeneration); err != nil {
		t.Fatalf("dormant return: %v", err)
	}
	var jobState string
	var attempts int
	if err := s.pool.QueryRow(ctx, `SELECT state, attempts FROM job_queue WHERE id = $1`, secondJob.ID).Scan(&jobState, &attempts); err != nil {
		t.Fatalf("read dormant job: %v", err)
	}
	if jobState != "pending" || attempts != 0 {
		t.Fatalf("dormant job = %s attempts=%d, want pending attempts=0", jobState, attempts)
	}
}

func TestSpawnAssignmentStructuralTeeth(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	spawner := createAssignmentToken(t, ctx, pool, writer, "teeth-spawner", domain.SourceSystem, false, root)

	// Self-review: the author of the current round marker is structurally
	// ineligible, whatever attachment strategy proposes it.
	implementer := createAssignmentToken(t, ctx, pool, writer, "teeth-implementer", domain.SourceAgent, false, root)
	item := createClaimableItem(t, ctx, svc, actorA, "self-review fenced item")
	if err := svc.AppendEvent(ctx, item.ID, "implementation.ready_for_review", map[string]any{"commit": "self1111"}, implementer); err != nil {
		t.Fatalf("declare round as implementer: %v", err)
	}
	if _, err := svc.AssignSpawned(ctx, item.ID, implementer.ID, spawner); !errors.Is(err, ErrSpawnAssigneeIsImplementer) {
		t.Fatalf("implementer binding = %v, want ErrSpawnAssigneeIsImplementer", err)
	}

	// Single-use: an identity that was ever bound — even released — never
	// binds again; the live same-assignee retry stays idempotent.
	once := createAssignmentToken(t, ctx, pool, writer, "teeth-once", domain.SourceAgent, false, root)
	firstItem := createClaimableItem(t, ctx, svc, actorA, "single-use first binding")
	bound, err := svc.AssignSpawned(ctx, firstItem.ID, once.ID, spawner)
	if err != nil {
		t.Fatalf("first binding: %v", err)
	}
	retry, err := svc.AssignSpawned(ctx, firstItem.ID, once.ID, spawner)
	if err != nil || retry.AssignmentEventID != bound.AssignmentEventID {
		t.Fatalf("idempotent retry = %+v err=%v, want same generation", retry, err)
	}
	if _, err := svc.Yield(ctx, firstItem.ID, once); err != nil {
		t.Fatalf("yield: %v", err)
	}
	secondItem := createClaimableItem(t, ctx, svc, actorA, "single-use second binding")
	if _, err := svc.AssignSpawned(ctx, secondItem.ID, once.ID, spawner); !errors.Is(err, ErrSpawnAssigneeAlreadyUsed) {
		t.Fatalf("second binding of used identity = %v, want ErrSpawnAssigneeAlreadyUsed", err)
	}
	if _, err := svc.AssignSpawned(ctx, firstItem.ID, once.ID, spawner); !errors.Is(err, ErrSpawnAssigneeAlreadyUsed) {
		t.Fatalf("rebinding released identity = %v, want ErrSpawnAssigneeAlreadyUsed", err)
	}
}

func TestResolveReviewLaunchReleasesExactGenerationAndRevokes(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "resolve child", "res1111")
	result := s.provision(t, ctx, item, job, 4)

	// succeeded requires the containment handshake.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: result.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchSucceeded,
	}, s.issuer); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("handle-less success = %v, want ErrReviewLaunchState", err)
	}

	// Post-binding failure: durable outcome, exact-generation release with
	// reason launch_failed, credential revoked — one transaction.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: result.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchFailed, Stage: "exec",
	}, s.issuer); err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	var launchState, stage string
	if err := s.pool.QueryRow(ctx, `
		SELECT state, COALESCE(stage, '') FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, item.ID, result.RoundSeq, job.Attempts).Scan(&launchState, &stage); err != nil {
		t.Fatalf("read resolved launch: %v", err)
	}
	if launchState != "failed" || stage != "exec" {
		t.Fatalf("launch = %s/%s, want failed/exec", launchState, stage)
	}
	var releaseReason *string
	var holder *uuid.UUID
	if err := s.pool.QueryRow(ctx, `
		SELECT last_release_reason, holder_token_id FROM work_item_assignment_state WHERE work_item_id = $1
	`, item.ID).Scan(&releaseReason, &holder); err != nil {
		t.Fatalf("read assignment state: %v", err)
	}
	if holder != nil || releaseReason == nil || *releaseReason != string(domain.AssignmentReleaseLaunchFailed) {
		t.Fatalf("release = holder %v reason %v, want released launch_failed", holder, releaseReason)
	}
	revoked, err := s.auth.Get(ctx, result.ReviewerTokenID)
	if err != nil {
		t.Fatalf("read revoked credential: %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatal("failed launch must revoke the reviewer credential")
	}

	// Retry rides a renewed lease under the same owner (the pending-claim
	// predicate cannot see a running child): attempts and generation advance,
	// and the fresh attempt provisions a fresh single-use identity.
	renewed, err := s.queue.RenewReviewLease(ctx, job.ID, s.issuer.ID, job.LeaseGeneration, time.Minute)
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if renewed.Attempts != job.Attempts+1 || renewed.LeaseGeneration != job.LeaseGeneration+1 {
		t.Fatalf("renewed lease = attempts %d gen %d, want %d/%d", renewed.Attempts, renewed.LeaseGeneration, job.Attempts+1, job.LeaseGeneration+1)
	}
	retry := s.provision(t, ctx, item, renewed, 4)
	if !retry.SecretAvailable || retry.ReviewerTokenID == result.ReviewerTokenID {
		t.Fatalf("retry = secret %t token %s, want fresh single-use credential", retry.SecretAvailable, retry.ReviewerTokenID)
	}

	// Handle, then success: the handshake makes succeeded legal, and the
	// reconciler completes a job the crashed worker left leased.
	if err := s.svc.RecordReviewLaunchHandle(ctx, ReviewLaunchHandleInput{
		WorkItemID: item.ID, RoundSeq: retry.RoundSeq, Attempt: renewed.Attempts,
		AssignmentEventID: retry.Assignment.AssignmentEventID,
		Pid:               4242, Pgid: 4242, StartToken: "starttime:12345",
	}, s.issuer); err != nil {
		t.Fatalf("record handle: %v", err)
	}
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: retry.RoundSeq, Attempt: renewed.Attempts,
		Outcome: ReviewLaunchSucceeded,
	}, s.issuer); err != nil {
		t.Fatalf("resolve succeeded: %v", err)
	}
	if _, err := s.svc.ReconcileReviewLaunches(ctx, s.auth, s.issuer); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var jobState string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM job_queue WHERE id = $1`, job.ID).Scan(&jobState); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if jobState != "done" {
		t.Fatalf("job after succeeded launch = %s, want done", jobState)
	}
}

func TestReviewLaunchDeadlineReconciliationAndTokenExpiry(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 1)
	item, job := s.admitReviewChild(t, ctx, "expiring child", "exp1111")
	result := s.provision(t, ctx, item, job, 4)

	if wait := time.Until(result.Deadline) + 150*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}

	// Hard ordinary-token expiry on the database clock: the credential's
	// authority ends at the binding deadline no matter what died.
	if _, err := s.auth.Authenticate(ctx, result.Secret); !errors.Is(err, auth.ErrTokenExpired) {
		t.Fatalf("expired credential authenticate = %v, want ErrTokenExpired", err)
	}

	// The durable-state reconciler resolves the expired launch failed,
	// releases what is still the exact bound generation, and revokes.
	resolved, err := s.svc.ReconcileReviewLaunches(ctx, s.auth, s.issuer)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if resolved != 1 {
		t.Fatalf("reconciled = %d, want 1", resolved)
	}
	var launchState, stage string
	if err := s.pool.QueryRow(ctx, `
		SELECT state, COALESCE(stage, '') FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, item.ID, result.RoundSeq, job.Attempts).Scan(&launchState, &stage); err != nil {
		t.Fatalf("read launch: %v", err)
	}
	if launchState != "failed" || stage != "deadline_expired" {
		t.Fatalf("launch = %s/%s, want failed/deadline_expired", launchState, stage)
	}
	revoked, err := s.auth.Get(ctx, result.ReviewerTokenID)
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatal("deadline reconciliation must revoke the credential")
	}

	// Abandoned launches hold capacity until deadline but resolve cleanly.
	second, secondJob := s.admitReviewChild(t, ctx, "abandoned child", "abn1111")
	res2 := s.provision(t, ctx, second, secondJob, 4)
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: second.ID, RoundSeq: res2.RoundSeq, Attempt: secondJob.Attempts,
		Outcome: ReviewLaunchAbandoned, Stage: "supervisor_lost_no_handle",
	}, s.issuer); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	var abandonedState string
	if err := s.pool.QueryRow(ctx, `
		SELECT state FROM review_launch WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, second.ID, res2.RoundSeq, secondJob.Attempts).Scan(&abandonedState); err != nil {
		t.Fatalf("read abandoned launch: %v", err)
	}
	if abandonedState != "abandoned" {
		t.Fatalf("abandoned launch = %s, want abandoned", abandonedState)
	}
	tok, err := s.auth.Get(ctx, res2.ReviewerTokenID)
	if err != nil || tok.RevokedAt == nil {
		t.Fatalf("abandon must revoke immediately: tok=%+v err=%v", tok, err)
	}
}
