package listeners_test

// LCP3-R1-B1/B2 regressions: the listener-bound claim revalidates
// EVERYTHING inside its transaction — policy revision, retirement, credential
// binding, demand eligibility, actor authority, and listener capacity — and
// the resulting assignment is generation-bound to the listener. The candidate
// listing applies the caller's own object authority, so a broad policy on a
// tree-scoped principal cannot enumerate portfolio-wide demand.

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

type claimDemandFixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	writer *events.Writer
	auth   *auth.Service
	work   *workitems.Service
	svc    *listeners.Service
	root   auth.CreateTokenResult
	admin  auth.CreateTokenResult
	tree   domain.WorkItem
}

func newClaimDemandFixture(t *testing.T, name string) *claimDemandFixture {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, name)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-admin", Source: domain.SourceHuman, Scopes: []string{access.ScopeListenersAdmin}, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	work := workitems.NewService(pool, writer)
	tree, err := work.Create(ctx, workitems.CreateInput{Title: name + "-tree", Actor: root.Token})
	if err != nil {
		t.Fatal(err)
	}
	return &claimDemandFixture{
		ctx: ctx, pool: pool, writer: writer, auth: authSvc, work: work,
		svc: listeners.NewService(pool, writer), root: root, admin: admin, tree: tree,
	}
}

func (f *claimDemandFixture) principal(t *testing.T, name string, treeIDs ...uuid.UUID) auth.CreateTokenResult {
	t.Helper()
	scopes := []string{access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite}
	for _, id := range treeIDs {
		scopes = append(scopes, "work_items.tree:"+id.String())
	}
	result, err := f.auth.CreateToken(f.ctx, auth.CreateTokenInput{
		Name: name, Source: domain.SourceAgent, Scopes: scopes, Actor: &f.root.Token,
	})
	if err != nil {
		t.Fatalf("mint %s: %v", name, err)
	}
	return result
}

func (f *claimDemandFixture) listener(t *testing.T, name string, principal uuid.UUID) listeners.Registration {
	t.Helper()
	reg, err := f.svc.Register(f.ctx, listeners.RegisterInput{
		Name: name, PrincipalTokenID: principal,
		Capabilities: []string{"review.complementary"}, Actor: f.admin.Token,
	})
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	if _, err := f.svc.SetPolicy(f.ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1},
		Actor:  f.admin.Token,
	}); err != nil {
		t.Fatalf("set policy for %s: %v", name, err)
	}
	reg, err = f.svc.Get(f.ctx, reg.ID)
	if err != nil {
		t.Fatalf("re-read %s: %v", name, err)
	}
	return reg
}

func (f *claimDemandFixture) demand(t *testing.T, parent uuid.UUID, title string) (domain.WorkItem, uuid.UUID) {
	t.Helper()
	item, err := f.work.SpawnChild(f.ctx, parent, workitems.CreateInput{
		Title: title, State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"event:claim-demand-fixture"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      f.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn %s: %v", title, err)
	}
	return item, f.appendDemand(t, item, title, nil)
}

func (f *claimDemandFixture) appendDemand(t *testing.T, item domain.WorkItem, reason string, extra map[string]any) uuid.UUID {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	payload := map[string]any{
		"work_item_id":          item.ID,
		"state":                 item.State,
		"state_entered_at_unix": item.StateEnteredAt.Unix(),
		"capability":            "review.complementary",
		"cultivar":              "review.complementary",
		"origin_token_id":       f.root.Token.ID,
		"reason":                reason,
	}
	for key, value := range extra {
		payload[key] = value
	}
	demandID, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   item.ID,
		Kind:        domain.EventDispatchRequested,
		Source:      domain.SourceSystem,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("append demand for %s: %v", reason, err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	return demandID
}

func (f *claimDemandFixture) assignedEventCount(t *testing.T, workItemID uuid.UUID) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM events
		WHERE subject_kind=$1 AND subject_id=$2 AND kind=$3`,
		domain.SubjectWorkItem, workItemID, domain.EventWorkItemAssigned,
	).Scan(&count); err != nil {
		t.Fatalf("count assignment events for %s: %v", workItemID, err)
	}
	return count
}

func (f *claimDemandFixture) seedReviewerCultivar(t *testing.T) {
	t.Helper()
	svc := registry.NewService(f.pool, f.writer)
	if _, _, err := svc.DefineTropism(f.ctx, f.root.Token, registry.DefineTropismInput{
		Name: "claim-review-checklist", Version: 1,
		Reducer: registry.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:  []byte(`{"budget":{"max_attempts":2,"escalation":"hand_to_human"}}`),
	}); err != nil {
		t.Fatalf("define reviewer tropism: %v", err)
	}
	if _, _, err := svc.DefineCultivar(f.ctx, f.root.Token, registry.DefineCultivarInput{
		Name: "reviewer", Version: 1, Rootstock: true,
		Tropism: registry.TropismRef{Name: "claim-review-checklist", Version: 1},
		Profile: registry.Profile{
			Briefing: "briefings/reviewer.md", DispatchCapability: "review.complementary",
			ScopesTemplate: []string{"work_items.tree:{root}", "work_items.read", "work_items.write", "feed.read_assigned"},
		},
		Xylem:  registry.Xylem{MaxAttempts: 2, MaxWallSeconds: 3600, MaxDepth: 1},
		Phloem: "projection:work-item-brief",
	}); err != nil {
		t.Fatalf("define reviewer cultivar: %v", err)
	}
}

// A candidate snapshot is optimistic. If a rolling payload change appends a
// newer immutable dispatch fact before claim, the older same-epoch generation
// must refuse without an assignment event; the newest generation remains
// claimable under the same listener policy.
func TestClaimDemandRejectsSupersededGenerationAtClaimBoundary(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_superseded")
	principal := f.principal(t, "superseded-principal", f.tree.ID)
	reg := f.listener(t, "superseded-listener", principal.Token.ID)
	item, older := f.demand(t, f.tree.ID, "superseded-demand")

	candidates, err := f.svc.ListDemandCandidates(f.ctx, reg.ID, principal.Token)
	if err != nil {
		t.Fatalf("snapshot candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].DemandEventID != older {
		t.Fatalf("initial candidates = %+v, want older demand %s", candidates, older)
	}

	// Same logical state epoch, newer payload generation. This models the
	// candidate-to-claim race during a rolling routing-schema deployment.
	newer := f.appendDemand(t, item, "superseded-demand", map[string]any{
		"payload_version":        1,
		"source_reconciler_pass": "dispatch",
	})
	if newer == older {
		t.Fatal("new payload generation collapsed onto older demand id")
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: older, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrDemandNotEligible) {
		t.Fatalf("superseded claim: err = %v, want ErrDemandNotEligible", err)
	}
	if got := f.assignedEventCount(t, item.ID); got != 0 {
		t.Fatalf("superseded refusal appended %d assignment events, want 0", got)
	}

	assignment, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: newer, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("claim latest generation: %v", err)
	}
	if assignment.DemandEventID == nil || *assignment.DemandEventID != newer {
		t.Fatalf("assignment demand = %v, want latest %s", assignment.DemandEventID, newer)
	}
}

func TestClaimDemandRejectsPriorStateEpochWithoutEvents(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_prior_epoch")
	principal := f.principal(t, "prior-epoch-principal", f.tree.ID)
	reg := f.listener(t, "prior-epoch-listener", principal.Token.ID)
	item, priorEpoch := f.demand(t, f.tree.ID, "prior-epoch-demand")

	if _, err := f.work.Transition(f.ctx, item.ID, domain.WorkItemPlanned, "enter a new claimable epoch", f.root.Token); err != nil {
		t.Fatalf("transition to new epoch: %v", err)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: priorEpoch, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrDemandNotEligible) {
		t.Fatalf("prior-epoch claim: err = %v, want ErrDemandNotEligible", err)
	}
	if got := f.assignedEventCount(t, item.ID); got != 0 {
		t.Fatalf("prior-epoch refusal appended %d assignment events, want 0", got)
	}
}

func TestClaimDemandRejectsMalformedCurrentMetadataWithoutEvents(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_malformed_epoch")
	principal := f.principal(t, "malformed-principal", f.tree.ID)
	reg := f.listener(t, "malformed-listener", principal.Token.ID)
	item, _ := f.demand(t, f.tree.ID, "malformed-demand")
	malformed := f.appendDemand(t, item, "malformed-demand", map[string]any{
		"state": nil,
	})

	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: malformed, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrInvalidRequest) {
		t.Fatalf("malformed claim: err = %v, want ErrInvalidRequest", err)
	}
	if got := f.assignedEventCount(t, item.ID); got != 0 {
		t.Fatalf("malformed refusal appended %d assignment events, want 0", got)
	}
}

func TestMalformedNewestDemandShadowsOlderValidListenerCandidate(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_malformed_newest")
	principal := f.principal(t, "malformed-newest-principal", f.tree.ID)
	reg := f.listener(t, "malformed-newest-listener", principal.Token.ID)
	item, older := f.demand(t, f.tree.ID, "malformed-newest-demand")
	malformed := f.appendDemand(t, item, "malformed-newest-demand", map[string]any{
		"state_entered_at_unix": strconv.FormatInt(item.StateEnteredAt.Unix(), 10),
	})
	if malformed == older {
		t.Fatal("malformed newest demand collapsed onto older valid demand")
	}
	candidates, err := f.svc.ListDemandCandidates(f.ctx, reg.ID, principal.Token)
	if err != nil {
		t.Fatalf("list with malformed newest demand: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates with malformed newest demand = %+v, want none", candidates)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: older, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrDemandNotEligible) {
		t.Fatalf("claim older behind malformed newest = %v, want ErrDemandNotEligible", err)
	}
	if got := f.assignedEventCount(t, item.ID); got != 0 {
		t.Fatalf("malformed-newest refusal appended %d assignment events, want 0", got)
	}
}

func TestLegacyMissingFromSameStateNoopHasOneDispatchStateEntry(t *testing.T) {
	f := newClaimDemandFixture(t, "legacy_missing_from_noop")
	principal := f.principal(t, "legacy-noop-principal", f.tree.ID)
	reg := f.listener(t, "legacy-noop-listener", principal.Token.ID)
	itemID := uuid.New()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin legacy no-op fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	createdID, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventWorkItemCreated, Source: domain.SourceHuman, ActorTokenID: &f.root.Token.ID,
		Payload: map[string]any{
			"title": "legacy missing-from no-op", "state": domain.WorkItemTriaged,
			"suggested_convergence_checks": []string{"event:legacy-noop"},
			"human_review_status":          domain.HumanReviewWavedThrough,
		},
	})
	if err != nil {
		t.Fatalf("append created: %v", err)
	}
	if _, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: f.tree.ID,
		Kind: domain.EventWorkItemRelationAdded, Source: domain.SourceHuman, ActorTokenID: &f.root.Token.ID,
		Payload: map[string]any{"parent_id": f.tree.ID, "child_id": itemID},
	}); err != nil {
		t.Fatalf("append parent relation: %v", err)
	}
	var createdAt time.Time
	if err := tx.QueryRow(f.ctx, `SELECT occurred_at FROM events WHERE id=$1`, createdID).Scan(&createdAt); err != nil {
		t.Fatalf("read created timestamp: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatalf("commit legacy created fixture: %v", err)
	}

	// Use a later transaction so this regression proves a missing-from
	// same-result legacy transition cannot advance the projected state epoch.
	// The old projector only appeared correct when both facts shared one
	// transaction timestamp.
	time.Sleep(2 * time.Millisecond)
	noopTx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin legacy no-op transition: %v", err)
	}
	defer func() { _ = noopTx.Rollback(f.ctx) }()
	noopID, _, err := f.writer.Append(f.ctx, noopTx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventWorkItemTransitioned, Source: domain.SourceHuman, ActorTokenID: &f.root.Token.ID,
		Discriminator: "legacy-missing-from-noop",
		Payload:       map[string]any{"to": domain.WorkItemTriaged, "reason": "legacy missing-from no-op"},
	})
	if err != nil {
		t.Fatalf("append legacy no-op transition: %v", err)
	}
	var noopAt time.Time
	if err := noopTx.QueryRow(f.ctx, `SELECT occurred_at FROM events WHERE id=$1`, noopID).Scan(&noopAt); err != nil {
		t.Fatalf("read no-op timestamp: %v", err)
	}
	if err := noopTx.Commit(f.ctx); err != nil {
		t.Fatalf("commit legacy no-op transition: %v", err)
	}
	if !noopAt.After(createdAt) {
		t.Fatalf("fixture no-op timestamp = %s, want after created %s", noopAt, createdAt)
	}
	var projectedEnteredAt time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT state_entered_at FROM work_items WHERE id=$1`, itemID).Scan(&projectedEnteredAt); err != nil {
		t.Fatalf("read projected state epoch: %v", err)
	}
	if !projectedEnteredAt.Equal(createdAt) {
		t.Fatalf("missing-from same-state no-op advanced projection to %s, want %s", projectedEnteredAt, createdAt)
	}

	demandTx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin legacy demand: %v", err)
	}
	defer func() { _ = demandTx.Rollback(f.ctx) }()
	demandID, _, err := f.writer.Append(f.ctx, demandTx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventDispatchRequested, Source: domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id": itemID, "state": domain.WorkItemTriaged,
			"state_entered_at_unix": createdAt.Unix(), "capability": "review.complementary",
			"cultivar": "review.complementary", "origin_token_id": f.root.Token.ID,
			"reason": "legacy missing-from no-op",
		},
	})
	if err != nil {
		t.Fatalf("append legacy demand: %v", err)
	}
	if err := demandTx.Commit(f.ctx); err != nil {
		t.Fatalf("commit legacy no-op fixture: %v", err)
	}

	identity, err := jobqueue.ResolveDispatchIdentity(f.ctx, f.pool, demandID)
	if err != nil {
		t.Fatalf("resolve legacy no-op demand: %v", err)
	}
	if identity.StateEntryID != createdID || identity.StateEntryID == noopID {
		t.Fatalf("legacy identity state entry = %s, want created %s (not no-op %s)", identity.StateEntryID, createdID, noopID)
	}
	candidates, err := f.svc.ListDemandCandidates(f.ctx, reg.ID, principal.Token)
	if err != nil || len(candidates) != 1 || candidates[0].DemandEventID != demandID {
		t.Fatalf("legacy no-op candidates = %+v err %v, want %s", candidates, err, demandID)
	}
	queue := jobqueue.NewService(f.pool)
	if canceled, err := queue.ReconcileDispatchJobs(f.ctx); err != nil || canceled != 0 {
		t.Fatalf("legacy no-op reconcile canceled %d err %v, want 0", canceled, err)
	}
	job, found, err := queue.ClaimNext(f.ctx, time.Minute)
	if err != nil || !found || job.ID != demandID {
		t.Fatalf("legacy no-op queue claim = found %t id %s err %v, want %s", found, job.ID, err, demandID)
	}
	assignment, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("legacy no-op listener claim: %v", err)
	}
	if assignment.DemandEventID == nil || *assignment.DemandEventID != demandID {
		t.Fatalf("legacy no-op assignment demand = %v, want %s", assignment.DemandEventID, demandID)
	}
}

func TestReviewerAdmissionAndListenerClaimCommute(t *testing.T) {
	for _, tc := range []struct {
		name            string
		assignmentFirst bool
	}{
		{name: "assignment_before_admission", assignmentFirst: true},
		{name: "admission_before_assignment", assignmentFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClaimDemandFixture(t, "review_commute_"+tc.name)
			f.seedReviewerCultivar(t)
			principal := f.principal(t, tc.name+"-principal", f.tree.ID)
			reg := f.listener(t, tc.name+"-listener", principal.Token.ID)
			system, err := f.auth.CreateToken(f.ctx, auth.CreateTokenInput{
				Name: tc.name + "-system", Source: domain.SourceSystem, Actor: &f.root.Token,
			})
			if err != nil {
				t.Fatalf("create system token: %v", err)
			}
			item, err := f.work.SpawnChild(f.ctx, f.tree.ID, workitems.CreateInput{
				Title: tc.name, State: domain.WorkItemTriaged, Cultivar: "reviewer@1",
				SuggestedConvergenceChecks: []string{workitems.ReviewVerdictCheck},
				HumanReviewStatus:          domain.HumanReviewWavedThrough,
				Actor:                      f.root.Token,
			})
			if err != nil {
				t.Fatalf("spawn reviewer item: %v", err)
			}
			stateEntry, err := jobqueue.ResolveCurrentStateEntry(f.ctx, f.pool, item.ID)
			if err != nil {
				t.Fatalf("resolve reviewer state entry: %v", err)
			}
			demandID := f.appendDemand(t, item, tc.name, map[string]any{
				"state_event_id": stateEntry.ID,
				"cultivar":       "reviewer@1",
			})
			claim := func() domain.WorkItemAssignment {
				t.Helper()
				assignment, claimErr := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
					DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
				})
				if claimErr != nil {
					t.Fatalf("listener claim: %v", claimErr)
				}
				return assignment
			}
			admit := func() {
				t.Helper()
				job, found, claimErr := jobqueue.NewService(f.pool).ClaimNextReview(f.ctx, time.Minute)
				if claimErr != nil || !found || job.ID != demandID {
					t.Fatalf("review queue claim = found %t id %s err %v, want %s", found, job.ID, claimErr, demandID)
				}
				result, startErr := f.work.StartReviewDispatch(f.ctx, job.ID, job.Attempts, system.Token)
				if startErr != nil || result.Outcome != workitems.ReviewDispatchStarted || !result.Transitioned {
					t.Fatalf("review admission = %+v err %v, want started", result, startErr)
				}
			}

			if tc.assignmentFirst {
				first := claim()
				admit()
				retry := claim()
				if retry.AssignmentEventID != first.AssignmentEventID || f.assignedEventCount(t, item.ID) != 1 {
					t.Fatalf("same-bound retry after running = %+v, first %+v events %d", retry, first, f.assignedEventCount(t, item.ID))
				}
			} else {
				admit()
				candidates, listErr := f.svc.ListDemandCandidates(f.ctx, reg.ID, principal.Token)
				if listErr != nil || len(candidates) != 1 || candidates[0].DemandEventID != demandID {
					t.Fatalf("causal running candidates = %+v err %v, want %s", candidates, listErr, demandID)
				}
				claim()
			}
		})
	}
}

func TestClaimDemandRejectsAfterLaterStateEntryBeyondReviewerAdmission(t *testing.T) {
	f := newClaimDemandFixture(t, "review_causal_later_state")
	f.seedReviewerCultivar(t)
	principal := f.principal(t, "causal-later-principal", f.tree.ID)
	reg := f.listener(t, "causal-later-listener", principal.Token.ID)
	system, err := f.auth.CreateToken(f.ctx, auth.CreateTokenInput{Name: "causal-later-system", Source: domain.SourceSystem, Actor: &f.root.Token})
	if err != nil {
		t.Fatal(err)
	}
	item, err := f.work.SpawnChild(f.ctx, f.tree.ID, workitems.CreateInput{
		Title: "causal reviewer then later state", State: domain.WorkItemTriaged, Cultivar: "reviewer@1",
		SuggestedConvergenceChecks: []string{workitems.ReviewVerdictCheck}, HumanReviewStatus: domain.HumanReviewWavedThrough, Actor: f.root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := jobqueue.ResolveCurrentStateEntry(f.ctx, f.pool, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	demandID := f.appendDemand(t, item, "causal-later", map[string]any{"state_event_id": entry.ID, "cultivar": "reviewer@1"})
	job, found, err := jobqueue.NewService(f.pool).ClaimNextReview(f.ctx, time.Minute)
	if err != nil || !found {
		t.Fatalf("claim review: found %t err %v", found, err)
	}
	if _, err := f.work.StartReviewDispatch(f.ctx, job.ID, job.Attempts, system.Token); err != nil {
		t.Fatalf("start review: %v", err)
	}
	if _, err := f.work.Transition(f.ctx, item.ID, domain.WorkItemPlanned, "later state entry", f.root.Token); err != nil {
		t.Fatalf("later transition: %v", err)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrDemandNotEligible) {
		t.Fatalf("claim after later state = %v, want ErrDemandNotEligible", err)
	}
	if got := f.assignedEventCount(t, item.ID); got != 0 {
		t.Fatalf("later-state refusal appended %d assignments, want 0", got)
	}
}

func TestClaimDemandRevalidatesAtomically(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_demand")
	principal := f.principal(t, "cd-principal", f.tree.ID)
	reg := f.listener(t, "cd-listener", principal.Token.ID)
	item, demandID := f.demand(t, f.tree.ID, "cd-demand-a")

	// Stale policy revision is a PURE conflict: the demand snapshot was
	// taken under a revision administration has since replaced.
	staleRevision := uuid.New()
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: &staleRevision, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrStalePolicy) {
		t.Fatalf("stale-revision claim: err = %v, want ErrStalePolicy", err)
	}
	var assignedEvents int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`,
		item.ID, domain.EventWorkItemAssigned).Scan(&assignedEvents); err != nil || assignedEvents != 0 {
		t.Fatalf("stale-revision refusal appended events: %d (%v)", assignedEvents, err)
	}

	// The bound claim succeeds and the assignment is generation-bound to the
	// LISTENER: event payload and projection both carry the attribution.
	assignment, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("bound claim: %v", err)
	}
	if assignment.ListenerID == nil || *assignment.ListenerID != reg.ID ||
		assignment.DemandEventID == nil || *assignment.DemandEventID != demandID ||
		assignment.PolicyEventID == nil || *assignment.PolicyEventID != *reg.PolicyEventID {
		t.Fatalf("assignment not generation-bound to the listener: %+v", assignment)
	}
	held, err := f.work.ListAssignmentsForHolder(f.ctx, principal.Token.ID)
	if err != nil || len(held) != 1 || held[0].ListenerID == nil || *held[0].ListenerID != reg.ID {
		t.Fatalf("holder listing lost listener attribution: %+v (%v)", held, err)
	}

	// Capacity under the registration lock: a second demand for the same
	// listener refuses while one assignment is active.
	_, secondDemand := f.demand(t, f.tree.ID, "cd-demand-b")
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: secondDemand, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrListenerAtCapacity) {
		t.Fatalf("over-capacity claim: err = %v, want ErrListenerAtCapacity", err)
	}

	// Retire/rebind race: after yield, a retired registration refuses; after
	// un-retirement is impossible, use a second listener to prove a REBOUND
	// registration refuses its former principal.
	if _, err := f.work.Yield(f.ctx, item.ID, assignment.AssignmentEventID, principal.Token); err != nil {
		t.Fatalf("yield: %v", err)
	}
	other := f.principal(t, "cd-principal-other", f.tree.ID)
	if _, err := f.svc.BindCredential(f.ctx, reg.ID, other.Token.ID, f.admin.Token); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: secondDemand, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Fatalf("former-principal claim after rebind: err = %v, want ErrNotAuthorized", err)
	}
	if _, err := f.svc.Retire(f.ctx, reg.ID, "claim fixture", f.admin.Token); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: secondDemand, ObservedPolicyEventID: reg.PolicyEventID, Actor: other.Token,
	}); !errors.Is(err, listeners.ErrRetired) {
		t.Fatalf("retired-listener claim: err = %v, want ErrRetired", err)
	}
}

// TestClaimDemandSingleConnectionPool (LCP3-R2-B1): the actor-authority
// reducer is FENCED INSIDE the claim transaction — it must run through the
// open tx, never a nested pool acquire. With a one-connection pool a nested
// acquire would deadlock against the connection the transaction itself
// holds, so an authorized bound claim completing here proves the fence.
func TestClaimDemandSingleConnectionPool(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_one_conn")
	principal := f.principal(t, "oneconn-principal", f.tree.ID)
	reg := f.listener(t, "oneconn-listener", principal.Token.ID)
	item, demandID := f.demand(t, f.tree.ID, "oneconn-demand")

	config, err := pgxpool.ParseConfig(f.pool.Config().ConnString())
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	config.MaxConns = 1
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	narrow, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open one-connection pool: %v", err)
	}
	defer narrow.Close()

	svc := listeners.NewService(narrow, f.writer)
	assignment, err := svc.ClaimDemand(ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("bound claim on a one-connection pool: %v (a hang or timeout here means the authority reducer escaped the transaction)", err)
	}
	if assignment.WorkItemID != item.ID || assignment.ListenerID == nil || *assignment.ListenerID != reg.ID {
		t.Fatalf("one-connection claim mis-attributed: %+v", assignment)
	}
}

// TestClaimDemandCapacityRace: two supervisor processes for ONE listener
// racing two different open demands serialize on the registration lock, so
// max_concurrent_assignments=1 holds even though the work-item locks are
// per item: exactly one claim wins, the other is a pure capacity conflict.
func TestClaimDemandCapacityRace(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_race")
	principal := f.principal(t, "race-principal", f.tree.ID)
	reg := f.listener(t, "race-listener", principal.Token.ID)
	_, demandA := f.demand(t, f.tree.ID, "race-demand-a")
	_, demandB := f.demand(t, f.tree.ID, "race-demand-b")

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, demandID := range []uuid.UUID{demandA, demandB} {
		wg.Add(1)
		go func(slot int, id uuid.UUID) {
			defer wg.Done()
			_, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
				DemandEventID: id, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
			})
			results[slot] = err
		}(i, demandID)
	}
	wg.Wait()
	successes, capacityRefusals := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, listeners.ErrListenerAtCapacity):
			capacityRefusals++
		default:
			t.Fatalf("unexpected race outcome: %v", err)
		}
	}
	if successes != 1 || capacityRefusals != 1 {
		t.Fatalf("race outcome: %d successes, %d capacity refusals; want exactly 1 and 1", successes, capacityRefusals)
	}
	var active int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM work_item_assignment_state WHERE listener_id=$1 AND holder_token_id IS NOT NULL`,
		reg.ID).Scan(&active); err != nil || active != 1 {
		t.Fatalf("active listener assignments = %d (%v), want 1", active, err)
	}
}

// TestClaimDemandSameTokenTwoListeners: one principal backing two listener
// registrations holds two leases distinguished by listener attribution —
// each listener's capacity is its own, and restart derivation can prove
// which listener owns which lease.
func TestClaimDemandSameTokenTwoListeners(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_two_listeners")
	principal := f.principal(t, "two-principal", f.tree.ID)
	regA := f.listener(t, "two-listener-a", principal.Token.ID)
	regB := f.listener(t, "two-listener-b", principal.Token.ID)
	itemA, demandA := f.demand(t, f.tree.ID, "two-demand-a")
	itemB, demandB := f.demand(t, f.tree.ID, "two-demand-b")

	first, err := f.svc.ClaimDemand(f.ctx, regA.ID, listeners.ClaimDemandInput{
		DemandEventID: demandA, ObservedPolicyEventID: regA.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("listener A claim: %v", err)
	}
	second, err := f.svc.ClaimDemand(f.ctx, regB.ID, listeners.ClaimDemandInput{
		DemandEventID: demandB, ObservedPolicyEventID: regB.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatalf("listener B claim: %v", err)
	}
	if first.WorkItemID != itemA.ID || second.WorkItemID != itemB.ID {
		t.Fatalf("claims landed on wrong items: %s %s", first.WorkItemID, second.WorkItemID)
	}
	held, err := f.work.ListAssignmentsForHolder(f.ctx, principal.Token.ID)
	if err != nil || len(held) != 2 {
		t.Fatalf("holder listing = %+v (%v), want 2 attributed leases", held, err)
	}
	byListener := map[uuid.UUID]uuid.UUID{}
	for _, h := range held {
		if h.ListenerID == nil {
			t.Fatalf("lease without listener attribution: %+v", h)
		}
		byListener[*h.ListenerID] = h.WorkItemID
	}
	if byListener[regA.ID] != itemA.ID || byListener[regB.ID] != itemB.ID {
		t.Fatalf("attribution mismatch: %v", byListener)
	}
}

// TestDemandCandidatesApplyCallerAuthority (LCP3-R1-B2): a broad base policy
// on a tree-scoped principal yields ONLY in-tree candidates — out-of-tree
// demand is absent from the listing, not merely unclaimable, and the bound
// claim refuses it with the same reducer.
func TestDemandCandidatesApplyCallerAuthority(t *testing.T) {
	f := newClaimDemandFixture(t, "claim_authority")
	otherTree, err := f.work.Create(f.ctx, workitems.CreateInput{Title: "authority-other-tree", Actor: f.root.Token})
	if err != nil {
		t.Fatal(err)
	}
	principal := f.principal(t, "authority-principal", f.tree.ID) // scoped to ONE tree
	reg := f.listener(t, "authority-listener", principal.Token.ID)
	inTree, inDemand := f.demand(t, f.tree.ID, "authority-demand-in")
	_, outDemand := f.demand(t, otherTree.ID, "authority-demand-out")

	candidates, err := f.svc.ListDemandCandidates(f.ctx, reg.ID, principal.Token)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Envelope.WorkItemID != inTree.ID {
		t.Fatalf("candidates = %+v, want exactly the in-tree demand %s", candidates, inTree.ID)
	}
	for _, c := range candidates {
		if c.DemandEventID == outDemand {
			t.Fatal("out-of-tree demand enumerated")
		}
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: outDemand, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); !errors.Is(err, listeners.ErrNotAuthorized) {
		t.Fatalf("out-of-tree bound claim: err = %v, want ErrNotAuthorized", err)
	}
	if _, err := f.svc.ClaimDemand(f.ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: inDemand, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	}); err != nil {
		t.Fatalf("in-tree bound claim: %v", err)
	}
}
