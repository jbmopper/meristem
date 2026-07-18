package workitems

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
)

// Round-1 repair regressions (verdict 278dac74): launch admission stays
// idempotent and reclaimable, running reviewers keep their capacity until
// confirmed death, handle/success are fenced to the exact issuer and lease
// incarnation, and self-review is structurally closed on the claim path too.

func TestLaunchAdmissionIdempotentAndReclaimable(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "idempotent admission child", "idem111")

	// Lost-response retry: the admission transition already exists; a launch
	// retry must NOT complete the job (the legacy repair path would).
	retry, err := s.svc.StartReviewDispatchForLaunch(ctx, job.ID, s.issuer)
	if err != nil {
		t.Fatalf("admission retry: %v", err)
	}
	if retry.Outcome != ReviewDispatchStarted {
		t.Fatalf("admission retry outcome = %+v, want started", retry)
	}
	var jobState string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM job_queue WHERE id = $1`, job.ID).Scan(&jobState); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if jobState != "leased" {
		t.Fatalf("job after admission retry = %s, want leased", jobState)
	}

	// Post-admission crash: the lease lapses with the child running. The
	// ordinary claim predicate can never see it; the admitted-reclaim path
	// must, and the generic dispatch reconciler must not cancel it.
	if _, err := s.pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("lapse lease: %v", err)
	}
	if _, err := s.queue.ReconcileDispatchJobs(ctx); err != nil {
		t.Fatalf("dispatch reconcile: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT state FROM job_queue WHERE id = $1`, job.ID).Scan(&jobState); err != nil {
		t.Fatalf("read job after reconcile: %v", err)
	}
	if jobState == "canceled" {
		t.Fatal("generic dispatch reconciler canceled a running launch-protocol job")
	}
	reclaimed, found, err := s.queue.ClaimAdmittedReviewAs(ctx, s.issuer.ID, time.Minute)
	if err != nil || !found {
		t.Fatalf("reclaim admitted job: found=%t err=%v", found, err)
	}
	if reclaimed.ID != job.ID || reclaimed.LeaseGeneration != job.LeaseGeneration+1 {
		t.Fatalf("reclaimed = %+v, want job %s generation %d", reclaimed, job.ID, job.LeaseGeneration+1)
	}

	// Capacity-return-then-reclaim: a dormant return (attempt refunded) must
	// also be reclaimable through the admitted path.
	if err := s.queue.ReturnReviewDormant(ctx, reclaimed.ID, s.issuer.ID, reclaimed.LeaseGeneration); err != nil {
		t.Fatalf("dormant return: %v", err)
	}
	again, found, err := s.queue.ClaimAdmittedReviewAs(ctx, s.issuer.ID, time.Minute)
	if err != nil || !found || again.ID != job.ID {
		t.Fatalf("reclaim after dormant return: job=%+v found=%t err=%v", again, found, err)
	}
	result := s.provision(t, ctx, item, again, 4)
	if !result.SecretAvailable {
		t.Fatal("provision after reclaim must mint a fresh credential")
	}
}

func TestRunningReviewerHoldsCapacityUntilConfirmedExit(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	first, firstJob := s.admitReviewChild(t, ctx, "running capacity holder", "run1111")
	res := s.provision(t, ctx, first, firstJob, 1)

	if err := s.svc.RecordReviewLaunchHandle(ctx, ReviewLaunchHandleInput{
		WorkItemID: first.ID, RoundSeq: res.RoundSeq, Attempt: firstJob.Attempts,
		AssignmentEventID: res.Assignment.AssignmentEventID,
		Pid:               5151, Pgid: 5151, StartToken: "starttime:5151",
	}, s.issuer); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: first.ID, RoundSeq: res.RoundSeq, Attempt: firstJob.Attempts,
		Outcome: ReviewLaunchSucceeded,
	}, s.issuer); err != nil {
		t.Fatalf("succeed: %v", err)
	}

	// Launch success is process creation, not exit: the running reviewer
	// still holds the only slot.
	second, secondJob := s.admitReviewChild(t, ctx, "blocked by running reviewer", "run2222")
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, ProvisionSpawnedReviewInput{
		JobID: secondJob.ID, WorkItemID: second.ID,
		Attempt: secondJob.Attempts, LeaseGeneration: secondJob.LeaseGeneration,
		MaxConcurrent: 1,
	}, s.issuer); !errors.Is(err, ErrReviewLaunchCapacity) {
		t.Fatalf("provision while reviewer runs = %v, want ErrReviewLaunchCapacity", err)
	}

	// Only confirmed exit frees it.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: first.ID, RoundSeq: res.RoundSeq, Attempt: firstJob.Attempts,
		Outcome: ReviewLaunchExited, Stage: "wait",
	}, s.issuer); err != nil {
		t.Fatalf("exit: %v", err)
	}
	revoked, err := s.auth.Get(ctx, res.ReviewerTokenID)
	if err != nil || revoked.RevokedAt == nil {
		t.Fatalf("exited launch must revoke its credential: tok=%+v err=%v", revoked, err)
	}
	if _, err := s.svc.ProvisionSpawnedReview(ctx, s.auth, ProvisionSpawnedReviewInput{
		JobID: secondJob.ID, WorkItemID: second.ID,
		Attempt: secondJob.Attempts, LeaseGeneration: secondJob.LeaseGeneration,
		MaxConcurrent: 1,
	}, s.issuer); err != nil {
		t.Fatalf("provision after exit: %v", err)
	}
}

func TestHandledLaunchAtDeadlineIsMarkedNotFreed(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 1)
	item, job := s.admitReviewChild(t, ctx, "handled deadline child", "hdl1111")
	res := s.provision(t, ctx, item, job, 4)
	if err := s.svc.RecordReviewLaunchHandle(ctx, ReviewLaunchHandleInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		AssignmentEventID: res.Assignment.AssignmentEventID,
		Pid:               6161, Pgid: 6161, StartToken: "starttime:6161",
	}, s.issuer); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Termination marking is an authorized post-deadline act (round-2
	// finding): before the deadline it is refused even for the issuer, and
	// an unrelated system actor without the recovery capability can never
	// demand a kill.
	if err := s.svc.MarkReviewLaunchTerminationDue(ctx, item.ID, res.RoundSeq, job.Attempts, s.issuer); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("pre-deadline termination mark = %v, want ErrReviewLaunchState", err)
	}
	stranger := createAssignmentToken(t, ctx, s.pool, s.writer, "termination-stranger", domain.SourceSystem, false, s.root)
	if wait := time.Until(res.Deadline) + 150*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := s.svc.MarkReviewLaunchTerminationDue(ctx, item.ID, res.RoundSeq, job.Attempts, stranger); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("unauthorized termination mark = %v, want ErrReviewLaunchState", err)
	}
	if _, err := s.svc.ReconcileReviewLaunches(ctx, s.auth, s.issuer); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The deadline pass demands termination but frees nothing: a handled
	// tree may still be alive and only confirmed death resolves it.
	var state string
	var terminationDue bool
	if err := s.pool.QueryRow(ctx, `
		SELECT state, termination_due FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, item.ID, res.RoundSeq, job.Attempts).Scan(&state, &terminationDue); err != nil {
		t.Fatalf("read launch: %v", err)
	}
	if state != "handled" || !terminationDue {
		t.Fatalf("handled launch at deadline = %s/termination_due=%t, want handled/true", state, terminationDue)
	}
	tok, err := s.auth.Get(ctx, res.ReviewerTokenID)
	if err != nil || tok.RevokedAt != nil {
		t.Fatalf("deadline pass must not revoke an unconfirmed handled launch: tok=%+v err=%v", tok, err)
	}
	// A recovery actor that killed and confirmed the tree records failed;
	// that terminal act releases and revokes.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchFailed, Stage: "adoption_confirmed_dead",
	}, s.issuer); err != nil {
		t.Fatalf("confirmed failure: %v", err)
	}
	tok, err = s.auth.Get(ctx, res.ReviewerTokenID)
	if err != nil || tok.RevokedAt == nil {
		t.Fatalf("confirmed failure must revoke: tok=%+v err=%v", tok, err)
	}
}

func TestLaunchHandleAndSuccessFencing(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "fenced handle child", "fen1111")
	res := s.provision(t, ctx, item, job, 4)

	// Only the exact reservation issuer records handles — another
	// capability-holding system identity is not the supervisor.
	otherIssuerResult, err := s.auth.CreateToken(ctx, auth.CreateTokenInput{
		Name: "other-issuer", Source: domain.SourceSystem,
		Scopes: []string{auth.ScopeReviewerCredentialsIssue}, Actor: &s.root,
	})
	if err != nil {
		t.Fatalf("mint other issuer: %v", err)
	}
	handle := ReviewLaunchHandleInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		AssignmentEventID: res.Assignment.AssignmentEventID,
		Pid:               7171, Pgid: 7171, StartToken: "starttime:7171",
	}
	if err := s.svc.RecordReviewLaunchHandle(ctx, handle, otherIssuerResult.Token); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("foreign-issuer handle = %v, want ErrReviewLaunchState", err)
	}
	if err := s.svc.RecordReviewLaunchHandle(ctx, handle, s.issuer); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Exact retry passes; conflicting replay fails closed.
	if err := s.svc.RecordReviewLaunchHandle(ctx, handle, s.issuer); err != nil {
		t.Fatalf("exact handle retry: %v", err)
	}
	conflicting := handle
	conflicting.Pid = 9999
	if err := s.svc.RecordReviewLaunchHandle(ctx, conflicting, s.issuer); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("conflicting handle replay = %v, want ErrReviewLaunchState", err)
	}

	// While a launch is reserved/handled/running, the lease generation
	// cannot legally move underneath it (round-2 race finding): renewal is
	// refused because the attempt's launch is not failed/abandoned, and the
	// admitted-reclaim path excludes the job entirely.
	if _, err := s.queue.RenewReviewLease(ctx, job.ID, s.issuer.ID, job.LeaseGeneration, time.Minute); err == nil {
		t.Fatal("renewal must be refused while the attempt's launch is handled")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE job_queue SET lease_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("lapse lease: %v", err)
	}
	if reclaimed, found, err := s.queue.ClaimAdmittedReviewAs(ctx, s.issuer.ID, time.Minute); err != nil {
		t.Fatalf("reclaim probe: %v", err)
	} else if found && reclaimed.ID == job.ID {
		t.Fatal("reclaim must exclude a job whose launch is still handled")
	}
	// With the lease lapsed, the handled reservation no longer owns a live
	// incarnation: success fails closed rather than completing anything.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchSucceeded,
	}, s.issuer); !errors.Is(err, ErrReviewLaunchState) {
		t.Fatalf("lapsed-lease success = %v, want ErrReviewLaunchState", err)
	}
}

func TestClaimRefusesSelfReviewOnReviewChildren(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)

	// On a review child, the current round's author cannot volunteer via
	// Claim any more than it can be spawn-bound.
	implementer := createAssignmentToken(t, ctx, pool, writer, "claim-implementer", domain.SourceAgent, false, root)
	reviewChild, err := svc.Create(ctx, CreateInput{
		Title: "claim self-review fenced", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{ReviewVerdictCheck},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: actorA,
	})
	if err != nil {
		t.Fatalf("create review child: %v", err)
	}
	if err := svc.AppendEvent(ctx, reviewChild.ID, "implementation.ready_for_review", map[string]any{"commit": "own1111"}, implementer); err != nil {
		t.Fatalf("declare round: %v", err)
	}
	if _, err := svc.Claim(ctx, reviewChild.ID, implementer); !errors.Is(err, ErrSpawnAssigneeIsImplementer) {
		t.Fatalf("implementer claim on review child = %v, want ErrSpawnAssigneeIsImplementer", err)
	}
	// A different reviewer may claim it.
	other := createAssignmentToken(t, ctx, pool, writer, "claim-other-reviewer", domain.SourceAgent, false, root)
	if _, err := svc.Claim(ctx, reviewChild.ID, other); err != nil {
		t.Fatalf("non-author claim: %v", err)
	}

	// Ordinary items keep legal self-claims even with a round marker.
	ordinary := createClaimableItem(t, ctx, svc, actorA, "ordinary self-claim item")
	if err := svc.AppendEvent(ctx, ordinary.ID, "implementation.ready_for_review", map[string]any{"commit": "own2222"}, implementer); err != nil {
		t.Fatalf("declare ordinary round: %v", err)
	}
	if _, err := svc.Claim(ctx, ordinary.ID, implementer); err != nil {
		t.Fatalf("ordinary self-claim = %v, want success", err)
	}
}

func TestReviewerCredentialAuthorityNarrowing(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "narrow authority child", "nar1111")
	res := s.provision(t, ctx, item, job, 4)

	// The issuer's revocation authority reaches only provisioned reviewer
	// credentials: an unrelated agent token is out of bounds even though the
	// issuer holds reviewer_credentials.issue.
	bystander := createAssignmentToken(t, ctx, s.pool, s.writer, "bystander-agent", domain.SourceAgent, false, s.root)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.auth.RevokeInTx(ctx, tx, bystander.ID, s.issuer, "test"); err == nil {
		t.Fatal("issuer revoked an unrelated agent token")
	}
	_ = tx.Rollback(ctx)

	// Scope canonicalization judges AFTER trimming and admits only the
	// reviewer-safe vocabulary plus the exact child tree (round-2 finding:
	// blank scopes minted legacy-unscoped broad authority and whitespace
	// smuggled foreign trees past the prefix check).
	for name, template := range map[string][]string{
		"foreign tree":            {"work_items.tree:" + uuid.NewString(), "work_items.tree:{root}"},
		"blank scope":             {"", "work_items.tree:{root}"},
		"whitespace foreign tree": {" work_items.tree:" + uuid.NewString(), "work_items.tree:{root}"},
		"portfolio-wide scope":    {"work_items.write_all", "work_items.tree:{root}"},
		"duplicate scope":         {"work_items.read", "work_items.read", "work_items.tree:{root}"},
		"missing child tree":      {"work_items.read"},
	} {
		tx, err = s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.auth.MintReviewerCredential(ctx, tx, auth.MintReviewerCredentialInput{
			Name: "widened", ChildID: item.ID,
			TemplateScopes: template,
			ExpiresAt:      time.Now().Add(time.Hour), Actor: s.issuer,
		}); err == nil {
			t.Fatalf("minted a credential from a %s template", name)
		}
		_ = tx.Rollback(ctx)
	}

	// ValidateLive is the session-revalidation seam: live now, refused the
	// moment the credential is revoked mid-session.
	if _, err := s.auth.ValidateLive(ctx, res.ReviewerTokenID); err != nil {
		t.Fatalf("live credential ValidateLive = %v", err)
	}
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchFailed, Stage: "exec",
	}, s.issuer); err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if _, err := s.auth.ValidateLive(ctx, res.ReviewerTokenID); !errors.Is(err, auth.ErrTokenRevoked) {
		t.Fatalf("revoked credential ValidateLive = %v, want ErrTokenRevoked", err)
	}
}
