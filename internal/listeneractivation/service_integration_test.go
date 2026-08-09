package listeneractivation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/listeners"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
	"github.com/jbmopper/meristem/internal/workitems"
)

type fixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	writer     *events.Writer
	principal  auth.CreateTokenResult
	other      auth.CreateTokenResult
	listener   listeners.Registration
	assignment domain.WorkItemAssignment
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.NewPool(t, name)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := projections.NewRegistry()
	auth.RegisterProjectors(registry)
	workitems.RegisterProjectors(registry)
	listeners.RegisterProjectors(registry)
	RegisterProjectors(registry)
	writer := events.NewWriter(registry)
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: name + "-admin", Source: domain.SourceHuman,
		Scopes: []string{access.ScopeListenersAdmin}, Actor: &root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	work := workitems.NewService(pool, writer)
	tree, err := work.Create(ctx, workitems.CreateInput{Title: name + "-tree", Actor: root.Token})
	if err != nil {
		t.Fatal(err)
	}
	scopes := []string{access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite, "work_items.tree:" + tree.ID.String()}
	principal, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-principal", Source: domain.SourceAgent, Scopes: scopes, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	other, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: name + "-other", Source: domain.SourceAgent, Scopes: scopes, Actor: &root.Token})
	if err != nil {
		t.Fatal(err)
	}
	listenerSvc := listeners.NewService(pool, writer)
	reg, err := listenerSvc.Register(ctx, listeners.RegisterInput{
		Name: name + "-listener", PrincipalTokenID: principal.Token.ID,
		Provider: "codex", Capabilities: []string{"review.complementary"}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	reg, err = listenerSvc.SetPolicy(ctx, reg.ID, listeners.SetPolicyInput{
		Policy: listeners.Policy{Capabilities: []string{"review.complementary"}, MaxConcurrentAssignments: 1}, Actor: admin.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	item, err := work.SpawnChild(ctx, tree.ID, workitems.CreateInput{
		Title: name + "-demand", State: domain.WorkItemPlanned,
		SuggestedConvergenceChecks: []string{"event:activation.completed"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough, Actor: root.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	demandID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventDispatchRequested, Source: domain.SourceSystem,
		Payload: map[string]any{
			"work_item_id": item.ID, "capability": "review.complementary",
			"cultivar": "review.complementary", "origin_token_id": root.Token.ID,
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assignment, err := listenerSvc.ClaimDemand(ctx, reg.ID, listeners.ClaimDemandInput{
		DemandEventID: demandID, ObservedPolicyEventID: reg.PolicyEventID, Actor: principal.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{ctx: ctx, pool: pool, writer: writer, principal: principal, other: other, listener: reg, assignment: assignment}
}

func TestListenerActivationMigrationDownUpRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "listener_activation_migration")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	if err := storage.MigrateDown(ctx, pool, nil); err != nil {
		t.Fatalf("migrate 0039 down: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.listener_activations') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("listener_activations still exists after migration 0039 down")
	}
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate 0039 back up: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.listener_activations') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("listener_activations missing after migration 0039 reapplied")
	}
}

func (f *fixture) ensure(t *testing.T, svc *Service) Activation {
	t.Helper()
	a, err := svc.Ensure(f.ctx, EnsureInput{
		ListenerID: f.listener.ID, AssignmentEventID: f.assignment.AssignmentEventID,
		BindingGeneration: "codex-task-binding-v1", Actor: f.principal.Token,
	})
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	return a
}

func TestEnsureIsDeterministicAndPrincipalBound(t *testing.T) {
	f := newFixture(t, "activation_ensure")
	svc := NewService(f.pool, f.writer)
	first := f.ensure(t, svc)
	second := f.ensure(t, svc)
	if first.ID != second.ID || first.State != StateRequested {
		t.Fatalf("ensure did not converge: first=%+v second=%+v", first, second)
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, first.ID, domain.EventListenerActivationRequested).Scan(&count); err != nil || count != 1 {
		t.Fatalf("requested event count=%d err=%v", count, err)
	}
	_, err := svc.Ensure(f.ctx, EnsureInput{
		ListenerID: f.listener.ID, AssignmentEventID: f.assignment.AssignmentEventID,
		BindingGeneration: "codex-task-binding-v1", Actor: f.other.Token,
	})
	if !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("other principal ensure err=%v, want not authorized", err)
	}
}

func TestDispatchAcceptedCompletedLifecycle(t *testing.T) {
	f := newFixture(t, "activation_complete")
	svc := NewService(f.pool, f.writer)
	a := f.ensure(t, svc)
	begin, err := svc.Begin(f.ctx, BeginInput{ActivationID: a.ID, ConsumerGeneration: "consumer-v1", Actor: f.principal.Token})
	if err != nil || begin.Action != ActionDispatch || begin.Activation.State != StateDispatching {
		t.Fatalf("begin=%+v err=%v", begin, err)
	}
	wait, err := svc.Begin(f.ctx, BeginInput{ActivationID: a.ID, ConsumerGeneration: "competing-consumer", Actor: f.principal.Token})
	if err != nil || wait.Action != ActionWait {
		t.Fatalf("competing begin=%+v err=%v", wait, err)
	}
	accepted, err := svc.RecordReceipt(f.ctx, ReceiptInput{
		ActivationID: a.ID, ObservedStateEventID: begin.Activation.StateEventID,
		ConsumerGeneration: "consumer-v1", Outcome: StateAccepted, Reason: "turn_admitted", Actor: f.principal.Token,
	})
	if err != nil || accepted.State != StateAccepted || accepted.LeaseExpiresAt == nil {
		t.Fatalf("accepted=%+v err=%v", accepted, err)
	}
	completed, err := svc.RecordReceipt(f.ctx, ReceiptInput{
		ActivationID: a.ID, ObservedStateEventID: accepted.StateEventID,
		ConsumerGeneration: "consumer-v1", Outcome: StateCompleted, Reason: "turn_completed", Actor: f.principal.Token,
	})
	if err != nil || completed.State != StateCompleted || completed.ConsumerGeneration != "" {
		t.Fatalf("completed=%+v err=%v", completed, err)
	}
	terminal, err := svc.Begin(f.ctx, BeginInput{ActivationID: a.ID, ConsumerGeneration: "consumer-v2", Actor: f.principal.Token})
	if err != nil || terminal.Action != ActionTerminal {
		t.Fatalf("terminal begin=%+v err=%v", terminal, err)
	}
}

func TestExpiredDispatchCanOnlyReconcile(t *testing.T) {
	f := newFixture(t, "activation_ambiguous")
	svc := NewService(f.pool, f.writer)
	svc.dispatchLease = -time.Second
	a := f.ensure(t, svc)
	first, err := svc.Begin(f.ctx, BeginInput{ActivationID: a.ID, ConsumerGeneration: "consumer-v1", Actor: f.principal.Token})
	if err != nil || first.Action != ActionDispatch {
		t.Fatalf("first begin=%+v err=%v", first, err)
	}
	second, err := svc.Begin(f.ctx, BeginInput{ActivationID: a.ID, ConsumerGeneration: "consumer-v2", Actor: f.principal.Token})
	if err != nil || second.Action != ActionReconcile || second.Activation.DispatchMode != ModeReconcile || second.Activation.ReconcileCount != 1 {
		t.Fatalf("second begin=%+v err=%v", second, err)
	}
	var ambiguous int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, a.ID, domain.EventListenerActivationAmbiguous).Scan(&ambiguous); err != nil || ambiguous != 1 {
		t.Fatalf("ambiguous event count=%d err=%v", ambiguous, err)
	}
}

func TestBusyTargetDoesNotExhaustDispatchBudgetAndRemainsAssignmentBound(t *testing.T) {
	f := newFixture(t, "activation_busy")
	svc := NewService(f.pool, f.writer)
	svc.retryDelay = -time.Second
	a := f.ensure(t, svc)

	for attempt := 1; attempt <= MaxDispatches+2; attempt++ {
		begin, err := svc.Begin(f.ctx, BeginInput{
			ActivationID: a.ID, ConsumerGeneration: "busy-consumer", Actor: f.principal.Token,
		})
		if err != nil || begin.Action != ActionDispatch {
			t.Fatalf("busy begin %d = %+v err=%v, want dispatch beyond ordinary budget", attempt, begin, err)
		}
		busy, err := svc.RecordReceipt(f.ctx, ReceiptInput{
			ActivationID: a.ID, ObservedStateEventID: begin.Activation.StateEventID,
			ConsumerGeneration: "busy-consumer", Outcome: StateFailed,
			Reason: ReasonAdapterTargetBusy, Actor: f.principal.Token,
		})
		if err != nil {
			t.Fatalf("record busy %d: %v", attempt, err)
		}
		if busy.State != StateFailed || busy.LastReason != ReasonAdapterTargetBusy || busy.NextRetryAt == nil {
			t.Fatalf("busy state %d = %+v", attempt, busy)
		}
	}

	if _, err := workitems.NewService(f.pool, f.writer).Yield(
		f.ctx, f.assignment.WorkItemID, f.assignment.AssignmentEventID, f.principal.Token,
	); err != nil {
		t.Fatalf("yield assignment: %v", err)
	}
	if _, err := svc.Begin(f.ctx, BeginInput{
		ActivationID: a.ID, ConsumerGeneration: "busy-consumer", Actor: f.principal.Token,
	}); !errors.Is(err, ErrNoActiveAssignment) {
		t.Fatalf("begin after assignment release err=%v, want ErrNoActiveAssignment", err)
	}
}

func TestActivationIDPinsAssignmentBindingAndAttempt(t *testing.T) {
	assignment := uuid.New()
	a := ActivationID(assignment, "binding-a", 1)
	if a != ActivationID(assignment, "binding-a", 1) {
		t.Fatal("activation id is not deterministic")
	}
	if a == ActivationID(assignment, "binding-b", 1) || a == ActivationID(assignment, "binding-a", 2) {
		t.Fatal("activation id failed to bind generation or attempt")
	}
}
