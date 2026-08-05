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
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/listeners"
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
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(f.ctx) }()
	demandID, _, err := f.writer.Append(f.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   item.ID,
		Kind:        domain.EventDispatchRequested,
		Source:      domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id":    item.ID,
			"capability":      "review.complementary",
			"cultivar":        "review.complementary",
			"origin_token_id": f.root.Token.ID,
			"reason":          title,
		},
	})
	if err != nil {
		t.Fatalf("append demand for %s: %v", title, err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	return item, demandID
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
