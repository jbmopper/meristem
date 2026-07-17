package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/projections"
	registrydomain "github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestClaimLedgerAtomicLifecycleAndProjectorReplay(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, actorB := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	item := createClaimableItem(t, ctx, svc, actorA, "claim lifecycle")

	first, err := svc.Claim(ctx, item.ID, actorA)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if first.HolderTokenID != actorA.ID || first.Mode != domain.WorkItemAssignmentClaim {
		t.Fatalf("first assignment = %+v", first)
	}
	if got := first.ExpiresAt.Sub(first.ClaimedAt); got != 24*time.Hour {
		t.Fatalf("steady fallback lease = %s, want 24h", got)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("assigned events = %d, want 1", got)
	}

	// Same-holder retry returns the canonical row byte-for-byte and neither
	// extends the lease nor appends another event.
	retry, err := svc.Claim(ctx, item.ID, actorA)
	if err != nil {
		t.Fatalf("same-holder retry: %v", err)
	}
	if !reflect.DeepEqual(retry, first) {
		t.Fatalf("same-holder retry changed assignment:\nfirst=%+v\nretry=%+v", first, retry)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("assigned events after retry = %d, want 1", got)
	}

	_, err = svc.Claim(ctx, item.ID, actorB)
	var held *ClaimHeldError
	if !errors.As(err, &held) || held.HolderTokenID != actorA.ID || held.AssignmentEventID != first.AssignmentEventID || !held.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("different-holder conflict = %#v (%v)", held, err)
	}
	if _, err := svc.Yield(ctx, item.ID, root); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("root yield error = %v, want ErrClaimUnavailable", err)
	}

	assignedEvent := loadAssignmentEvent(t, ctx, pool, first.AssignmentEventID)
	earlyExpiry := domain.Event{
		ID: uuid.New(), Seq: assignedEvent.Seq + 1,
		OccurredAt: first.ClaimedAt, ActorTokenID: &actorB.ID, Source: actorB.Source,
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemAssignmentReleased,
		Payload: map[string]any{
			"payload_version": 1, "assignment_event_id": first.AssignmentEventID,
			"assignee_token_id": actorA.ID, "mode": domain.WorkItemAssignmentClaim,
			"reason": domain.AssignmentReleaseExpired, "released_at": first.ClaimedAt,
		},
	}
	applyAssignmentProjector(t, ctx, pool, assignmentReleasedProjector{}, earlyExpiry, true)
	stillHeld, err := svc.GetAssignment(ctx, item.ID)
	if err != nil || stillHeld.AssignmentEventID != first.AssignmentEventID {
		t.Fatalf("early expiry changed assignment: %+v err=%v", stillHeld, err)
	}

	// The assigned projector accepts exact re-application.
	applyAssignmentProjector(t, ctx, pool, assignedProjector{}, assignedEvent, false)

	if _, err := svc.Yield(ctx, item.ID, actorB); !errors.Is(err, ErrAssignmentNotHeld) {
		t.Fatalf("non-holder yield error = %v, want ErrAssignmentNotHeld", err)
	}
	if _, err := svc.Yield(ctx, item.ID, actorA); err != nil {
		t.Fatalf("holder yield: %v", err)
	}
	if _, err := svc.GetAssignment(ctx, item.ID); !errors.Is(err, ErrAssignmentNotFound) {
		t.Fatalf("GetAssignment after yield error = %v", err)
	}
	releaseEvent := loadLatestAssignmentEvent(t, ctx, pool, item.ID, domain.EventWorkItemAssignmentReleased)
	applyAssignmentProjector(t, ctx, pool, assignmentReleasedProjector{}, releaseEvent, false)

	second, err := svc.Claim(ctx, item.ID, actorB)
	if err != nil {
		t.Fatalf("claim after yield: %v", err)
	}
	if second.AssignmentEventID == first.AssignmentEventID || second.HolderTokenID != actorB.ID {
		t.Fatalf("replacement assignment = %+v, first = %+v", second, first)
	}

	// Re-applying either stale epoch after a replacement is an idempotent
	// no-op and leaves the newer holder intact.
	applyAssignmentProjector(t, ctx, pool, assignmentReleasedProjector{}, releaseEvent, false)
	applyAssignmentProjector(t, ctx, pool, assignedProjector{}, assignedEvent, false)
	current, err := svc.GetAssignment(ctx, item.ID)
	if err != nil || current.AssignmentEventID != second.AssignmentEventID {
		t.Fatalf("stale projector changed replacement: %+v err=%v", current, err)
	}

	if _, err := svc.Claim(ctx, item.ID, root); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("root claim error = %v, want ErrClaimUnavailable", err)
	}
}

func TestClaimOpportunisticallyExpiresStaleEpoch(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, actorB := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	cultivar := defineOneSecondCultivar(t, ctx, pool, writer, root)
	item, err := svc.Create(ctx, CreateInput{
		Title: "stale claim", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"expired claim replaced"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   cultivar, Actor: actorA,
	})
	if err != nil {
		t.Fatalf("create short-lease item: %v", err)
	}
	first, err := svc.Claim(ctx, item.ID, actorA)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}
	if wait := time.Until(first.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	second, err := svc.Claim(ctx, item.ID, actorB)
	if err != nil {
		t.Fatalf("claim B after expiry: %v", err)
	}
	if second.HolderTokenID != actorB.ID || second.AssignmentEventID == first.AssignmentEventID {
		t.Fatalf("second assignment = %+v", second)
	}
	var released int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2 AND payload->>'reason'=$3`, item.ID, domain.EventWorkItemAssignmentReleased, domain.AssignmentReleaseExpired).Scan(&released); err != nil {
		t.Fatalf("count opportunistic expiry: %v", err)
	}
	if released != 1 {
		t.Fatalf("opportunistic expired releases = %d, want 1", released)
	}
}

func TestClaimYieldChurnConsumesLifecycleEventBudget(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actor, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	cultivar := defineTightLifecycleClaimCultivar(t, ctx, pool, writer, root)
	item, err := svc.Create(ctx, CreateInput{
		Title: "bounded claim yield churn", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"claim churn exhausts lifecycle budget"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   cultivar, Actor: actor,
	})
	if err != nil {
		t.Fatalf("create budgeted item: %v", err)
	}
	for epoch := 0; epoch < 2; epoch++ {
		if _, err := svc.Claim(ctx, item.ID, actor); err != nil {
			t.Fatalf("claim epoch %d: %v", epoch+1, err)
		}
		if _, err := svc.Yield(ctx, item.ID, actor); err != nil {
			t.Fatalf("yield epoch %d: %v", epoch+1, err)
		}
	}
	if _, err := svc.Claim(ctx, item.ID, actor); !errors.Is(err, ErrXylemBudgetExhausted) {
		t.Fatalf("third claim error = %v, want ErrXylemBudgetExhausted", err)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 2 {
		t.Fatalf("assigned events = %d, want 2 before exhaustion", got)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssignmentReleased); got != 2 {
		t.Fatalf("released events = %d, want 2 before exhaustion", got)
	}
}

func TestDirectTerminalTransitionReleasesAssignment(t *testing.T) {
	for _, terminal := range []domain.WorkItemState{domain.WorkItemDone, domain.WorkItemFailed, domain.WorkItemCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			ctx := context.Background()
			pool, writer, _, actor, _ := newAssignmentTestStack(t, ctx)
			svc := NewService(pool, writer)
			item := createClaimableItem(t, ctx, svc, actor, "terminal "+string(terminal))
			if _, err := svc.Claim(ctx, item.ID, actor); err != nil {
				t.Fatalf("claim: %v", err)
			}

			tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			transitionID, _, err := writer.Append(ctx, tx, events.Spec{
				SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
				Kind: domain.EventWorkItemTransitioned, Source: actor.Source, ActorTokenID: &actor.ID,
				Discriminator: "direct-terminal-" + string(terminal),
				Payload:       map[string]any{"from": item.State, "to": terminal, "reason": "direct event proof"},
			})
			if err != nil {
				t.Fatalf("append direct transition: %v", err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.GetAssignment(ctx, item.ID); !errors.Is(err, ErrAssignmentNotFound) {
				t.Fatalf("assignment survived %s: %v", terminal, err)
			}
			var reason string
			var gotTerminal string
			var addressee uuid.UUID
			var stateEventID uuid.UUID
			if err := pool.QueryRow(ctx, `
				SELECT last_release_reason, terminal_state,
				       terminal_addressee_token_id, state_event_id
				FROM work_item_assignment_state
				WHERE work_item_id=$1
			`, item.ID).Scan(&reason, &gotTerminal, &addressee, &stateEventID); err != nil {
				t.Fatalf("read terminal assignment state: %v", err)
			}
			if reason != string(domain.AssignmentReleaseDone) || gotTerminal != string(terminal) || addressee != actor.ID || stateEventID != transitionID {
				t.Fatalf("terminal assignment state = reason %q terminal %q addressee %s state_event %s", reason, gotTerminal, addressee, stateEventID)
			}
			if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssignmentReleased); got != 0 {
				t.Fatalf("terminal transition emitted %d assignment release events, want 0", got)
			}

			// Exact projector replay preserves the terminal address even though the
			// active holder fields were cleared by the first fold.
			transitionEvent := loadAssignmentEvent(t, ctx, pool, transitionID)
			applyAssignmentProjector(t, ctx, pool, transitionedProjector{}, transitionEvent, false)
			var replayedAddressee uuid.UUID
			if err := pool.QueryRow(ctx, `SELECT terminal_addressee_token_id FROM work_item_assignment_state WHERE work_item_id=$1`, item.ID).Scan(&replayedAddressee); err != nil || replayedAddressee != actor.ID {
				t.Fatalf("replayed terminal addressee = %s, %v; want %s", replayedAddressee, err, actor.ID)
			}

			// A later legal terminal same-state event is a lifecycle no-op. It
			// must not replace the entering-terminal event identity, or a later
			// assigned-feed snapshot could no longer find the handback.
			if _, err := svc.Transition(ctx, item.ID, terminal, "terminal same-state no-op", actor); err != nil {
				t.Fatalf("same-state terminal transition: %v", err)
			}
			var afterNoopAddressee uuid.UUID
			var afterNoopEventID uuid.UUID
			if err := pool.QueryRow(ctx, `
				SELECT terminal_addressee_token_id, state_event_id
				FROM work_item_assignment_state WHERE work_item_id=$1
			`, item.ID).Scan(&afterNoopAddressee, &afterNoopEventID); err != nil {
				t.Fatalf("read assignment state after terminal no-op: %v", err)
			}
			if afterNoopAddressee != actor.ID || afterNoopEventID != transitionID {
				t.Fatalf("terminal no-op moved handback: addressee=%s state_event=%s; want %s/%s", afterNoopAddressee, afterNoopEventID, actor.ID, transitionID)
			}
		})
	}
}

func TestTerminalTransitionAddressUsesAssignmentNotTransitionActor(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)

	assigned := createClaimableItem(t, ctx, svc, holder, "other actor terminal handback")
	if _, err := svc.Claim(ctx, assigned.ID, holder); err != nil {
		t.Fatalf("claim assigned item: %v", err)
	}
	if _, err := svc.Transition(ctx, assigned.ID, domain.WorkItemDone, "closed by coordinator", closer); err != nil {
		t.Fatalf("terminalize assigned item: %v", err)
	}
	transition := loadLatestAssignmentEvent(t, ctx, pool, assigned.ID, domain.EventWorkItemTransitioned)
	if transition.ActorTokenID == nil || *transition.ActorTokenID != closer.ID {
		t.Fatalf("transition attribution = %v, want closer %s", transition.ActorTokenID, closer.ID)
	}
	var addressee uuid.UUID
	var stateEventID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, assigned.ID).Scan(&addressee, &stateEventID); err != nil {
		t.Fatalf("read assigned terminal address: %v", err)
	}
	if addressee != holder.ID || stateEventID != transition.ID {
		t.Fatalf("assigned terminal address = %s at %s, want holder %s at transition %s", addressee, stateEventID, holder.ID, transition.ID)
	}

	unassigned := createClaimableItem(t, ctx, svc, holder, "unassigned terminal")
	if _, err := svc.Transition(ctx, unassigned.ID, domain.WorkItemDone, "no holder", closer); err != nil {
		t.Fatalf("terminalize unassigned item: %v", err)
	}
	var noAddressee *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT terminal_addressee_token_id FROM work_item_assignment_state WHERE work_item_id=$1`, unassigned.ID).Scan(&noAddressee); err != nil {
		t.Fatalf("read unassigned terminal address: %v", err)
	}
	if noAddressee != nil {
		t.Fatalf("unassigned terminal address = %s, want NULL", *noAddressee)
	}
}

func TestTerminalAtCreateRemainsUnaddressedAcrossSameStateTransition(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, actor, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	item, err := svc.Create(ctx, CreateInput{
		Title: "terminal at create", State: domain.WorkItemDone,
		SuggestedConvergenceChecks: []string{"already terminal"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      actor,
	})
	if err != nil {
		t.Fatalf("create terminal item: %v", err)
	}
	created := loadLatestAssignmentEvent(t, ctx, pool, item.ID, domain.EventWorkItemCreated)
	if _, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "terminal create no-op", actor); err != nil {
		t.Fatalf("terminal same-state transition: %v", err)
	}
	var addressee *uuid.UUID
	var stateEventID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, item.ID).Scan(&addressee, &stateEventID); err != nil {
		t.Fatalf("read terminal-at-create assignment state: %v", err)
	}
	if addressee != nil || stateEventID != created.ID {
		t.Fatalf("terminal-at-create state = addressee %v event %s, want NULL/%s", addressee, stateEventID, created.ID)
	}
}

func TestTerminalPayloadCannotForgeAssignmentLifecycle(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	item := createClaimableItem(t, ctx, svc, holder, "forged terminal payload")
	if _, err := svc.Claim(ctx, item.ID, holder); err != nil {
		t.Fatalf("claim fixture: %v", err)
	}

	for _, tc := range []struct {
		name string
		to   domain.WorkItemState
	}{
		{name: "terminal to nonterminal", to: domain.WorkItemRunning},
		{name: "terminal to different terminal", to: domain.WorkItemFailed},
		{name: "terminal same-state while projection running", to: domain.WorkItemDone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			_, _, err = writer.Append(ctx, tx, events.Spec{
				SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
				Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
				Discriminator: "forged-terminal-payload:" + tc.name,
				Payload: map[string]any{
					"from":   domain.WorkItemDone,
					"to":     tc.to,
					"reason": "caller payload must not override projected lifecycle",
				},
			})
			if err == nil {
				t.Fatal("forged terminal payload was accepted")
			}
		})
	}

	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned); got != 0 {
		t.Fatalf("forged terminal payload committed %d transitions, want 0", got)
	}
	current, err := svc.GetAssignment(ctx, item.ID)
	if err != nil {
		t.Fatalf("read assignment after forged transitions: %v", err)
	}
	if current.HolderTokenID != holder.ID {
		t.Fatalf("forged terminal payload changed holder to %s, want %s", current.HolderTokenID, holder.ID)
	}
	workItem, err := svc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("read work item after forged transitions: %v", err)
	}
	if workItem.State != domain.WorkItemRunning {
		t.Fatalf("forged terminal payload changed work item state to %s", workItem.State)
	}

	legitimate, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "legitimate terminal entry", closer)
	if err != nil {
		t.Fatalf("terminalize fixture: %v", err)
	}
	if legitimate.State != domain.WorkItemDone {
		t.Fatalf("legitimate terminal state = %s", legitimate.State)
	}
	terminalEntry := loadLatestAssignmentEvent(t, ctx, pool, item.ID, domain.EventWorkItemTransitioned)
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
		Discriminator: "forged-nonterminal-from-terminal",
		Payload: map[string]any{
			"from": domain.WorkItemRunning, "to": domain.WorkItemPlanned,
			"reason": "event history, not caller payload, owns from",
		},
	}); err == nil {
		t.Fatal("forged nonterminal from escaped terminal event history")
	}
	afterForgery, err := svc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("read work item after terminal escape attempt: %v", err)
	}
	if afterForgery.State != domain.WorkItemDone {
		t.Fatalf("terminal escape attempt changed work item state to %s", afterForgery.State)
	}
	var addressee uuid.UUID
	var stateEventID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, item.ID).Scan(&addressee, &stateEventID); err != nil {
		t.Fatalf("read assignment after terminal escape attempt: %v", err)
	}
	if addressee != holder.ID || stateEventID != terminalEntry.ID {
		t.Fatalf("terminal escape attempt changed handback to %s/%s, want %s/%s", addressee, stateEventID, holder.ID, terminalEntry.ID)
	}
}

func TestLegacyTransitionWithoutFromUsesEventHistory(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	item := createClaimableItem(t, ctx, svc, holder, "legacy missing-from transition")
	if _, err := svc.Claim(ctx, item.ID, holder); err != nil {
		t.Fatalf("claim fixture: %v", err)
	}

	appendLegacy := func(discriminator, reason string) domain.Event {
		t.Helper()
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		eventID, _, err := writer.Append(ctx, tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
			Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
			Discriminator: discriminator,
			Payload:       map[string]any{"to": domain.WorkItemDone, "reason": reason},
		})
		if err != nil {
			t.Fatalf("append legacy transition: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit legacy transition: %v", err)
		}
		return loadAssignmentEvent(t, ctx, pool, eventID)
	}

	terminalEntry := appendLegacy("legacy-missing-from-terminal-entry", "legacy terminal entry")
	applyAssignmentProjector(t, ctx, pool, transitionedProjector{}, terminalEntry, false)
	appendLegacy("legacy-missing-from-terminal-noop", "legacy terminal no-op")

	workItem, err := svc.Get(ctx, item.ID)
	if err != nil {
		t.Fatalf("read legacy transition result: %v", err)
	}
	if workItem.State != domain.WorkItemDone {
		t.Fatalf("legacy transition state = %s", workItem.State)
	}
	var addressee uuid.UUID
	var stateEventID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, item.ID).Scan(&addressee, &stateEventID); err != nil {
		t.Fatalf("read legacy transition handback: %v", err)
	}
	if addressee != holder.ID || stateEventID != terminalEntry.ID {
		t.Fatalf("legacy transition handback = %s/%s, want %s/%s", addressee, stateEventID, holder.ID, terminalEntry.ID)
	}
}

func TestTerminalAssignmentProjectorMissingVsUnassignedPlaceholder(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, actor, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	item := createClaimableItem(t, ctx, svc, actor, "unassigned terminal")

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	eventID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemTransitioned, Source: actor.Source, ActorTokenID: &actor.ID,
		Discriminator: "unassigned-terminal-sentinel",
		Payload:       map[string]any{"from": item.State, "to": domain.WorkItemDone, "reason": "terminal sentinel"},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	event := loadAssignmentEvent(t, ctx, pool, eventID)
	applyAssignmentProjector(t, ctx, pool, transitionedProjector{}, event, false)
	var terminal string
	if err := pool.QueryRow(ctx, `SELECT terminal_state FROM work_item_assignment_state WHERE work_item_id=$1`, item.ID).Scan(&terminal); err != nil || terminal != string(domain.WorkItemDone) {
		t.Fatalf("terminal sentinel = %q, %v", terminal, err)
	}
	laterAssignment := domain.Event{
		ID: uuid.New(), Seq: event.Seq + 1, OccurredAt: event.OccurredAt.Add(time.Second),
		ActorTokenID: &actor.ID, Source: actor.Source, SubjectKind: domain.SubjectWorkItem,
		SubjectID: item.ID, Kind: domain.EventWorkItemAssigned,
		Payload: map[string]any{
			"payload_version": 1, "assignee_token_id": actor.ID,
			"mode": domain.WorkItemAssignmentClaim, "lease_seconds": 60,
			"claimed_at": event.OccurredAt.Add(time.Second),
			"expires_at": event.OccurredAt.Add(61 * time.Second),
		},
	}
	applyAssignmentProjector(t, ctx, pool, assignedProjector{}, laterAssignment, true)

	missing := event
	missing.ID = uuid.New()
	missing.SubjectID = uuid.New()
	applyAssignmentProjector(t, ctx, pool, transitionedProjector{}, missing, true)
}

func TestClaimLeaseResolutionUsesHeldTransactionAtMaxConnsOne(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cultivar bool
		want     time.Duration
	}{
		{name: "active policy fallback", want: 24 * time.Hour},
		{name: "cultivar max wall", cultivar: true, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pool, writer, root, actor, _ := newAssignmentTestStack(t, ctx)
			cultivar := ""
			if tc.cultivar {
				cultivar = defineOneSecondCultivar(t, ctx, pool, writer, root)
			}
			item, err := NewService(pool, writer).Create(ctx, CreateInput{
				Title: "max-conns-one " + tc.name, State: domain.WorkItemRunning,
				SuggestedConvergenceChecks: []string{"claim completes"},
				HumanReviewStatus:          domain.HumanReviewWavedThrough,
				Cultivar:                   cultivar, Actor: actor,
			})
			if err != nil {
				t.Fatalf("create item: %v", err)
			}

			config := pool.Config()
			config.MaxConns = 1
			config.MinConns = 0
			single, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatalf("single-connection pool: %v", err)
			}
			t.Cleanup(single.Close)
			claimCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			assignment, err := NewService(single, writer).Claim(claimCtx, item.ID, actor)
			if err != nil {
				t.Fatalf("Claim with MaxConns=1: %v", err)
			}
			if got := assignment.ExpiresAt.Sub(assignment.ClaimedAt); got != tc.want {
				t.Fatalf("lease = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClaimRechecksDatabaseClockAfterLockWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, writer, root, actorA, actorB := newAssignmentTestStack(t, ctx)
	cultivar := defineClaimLeaseCultivar(t, ctx, pool, writer, root, "claim-lock-boundary", 3)
	svc := NewService(pool, writer)
	item, err := svc.Create(ctx, CreateInput{
		Title: "claim lock boundary", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"waiting claimant rechecks database clock"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   cultivar, Actor: actorA,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	first, err := svc.Claim(ctx, item.ID, actorA)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	blocker, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `SELECT id FROM work_items WHERE id=$1 FOR UPDATE`, item.ID); err != nil {
		t.Fatalf("lock work item: %v", err)
	}
	appName := "meristem-claim-boundary-" + uuid.NewString()
	contender := assignmentTestPoolWithAppName(t, ctx, pool, appName, 1)
	type claimResult struct {
		assignment domain.WorkItemAssignment
		err        error
	}
	result := make(chan claimResult, 1)
	go func() {
		assignment, err := NewService(contender, writer).Claim(ctx, item.ID, actorB)
		result <- claimResult{assignment: assignment, err: err}
	}()
	waitForAssignmentLockWaiters(t, ctx, pool, appName, 1)
	if !time.Now().UTC().Before(first.ExpiresAt) {
		t.Fatalf("claim waiter was not established before expiry %s", first.ExpiresAt)
	}
	if wait := time.Until(first.ExpiresAt) + 50*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
	got := <-result
	if got.err != nil {
		t.Fatalf("replacement claim after lock wait: %v", got.err)
	}
	if got.assignment.HolderTokenID != actorB.ID || got.assignment.ClaimedAt.Before(first.ExpiresAt) {
		t.Fatalf("replacement assignment = %+v, first expiry=%s", got.assignment, first.ExpiresAt)
	}
	if !time.Now().UTC().Before(got.assignment.ExpiresAt) {
		t.Fatalf("replacement lease was born expired: %+v", got.assignment)
	}
	if got.assignment.ExpiresAt.Sub(got.assignment.ClaimedAt) != 3*time.Second {
		t.Fatalf("replacement lease duration = %s", got.assignment.ExpiresAt.Sub(got.assignment.ClaimedAt))
	}
}

func TestExpireAssignmentRechecksDatabaseClockAfterLockWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, writer, root, actor, _ := newAssignmentTestStack(t, ctx)
	system := createAssignmentToken(t, ctx, pool, writer, "assignment-clock-system", domain.SourceSystem, false, root)
	cultivar := defineClaimLeaseCultivar(t, ctx, pool, writer, root, "expiry-lock-boundary", 3)
	svc := NewService(pool, writer)
	item, err := svc.Create(ctx, CreateInput{
		Title: "expiry lock boundary", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"waiting expiry rechecks database clock"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   cultivar, Actor: actor,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	assignment, err := svc.Claim(ctx, item.ID, actor)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	blocker, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `SELECT id FROM work_items WHERE id=$1 FOR UPDATE`, item.ID); err != nil {
		t.Fatalf("lock work item: %v", err)
	}
	appName := "meristem-expiry-boundary-" + uuid.NewString()
	contender := assignmentTestPoolWithAppName(t, ctx, pool, appName, 1)
	type expiryResult struct {
		expired bool
		err     error
	}
	result := make(chan expiryResult, 1)
	go func() {
		expired, err := NewService(contender, writer).ExpireAssignment(ctx, item.ID, system)
		result <- expiryResult{expired: expired, err: err}
	}()
	waitForAssignmentLockWaiters(t, ctx, pool, appName, 1)
	if !time.Now().UTC().Before(assignment.ExpiresAt) {
		t.Fatalf("expiry waiter was not established before expiry %s", assignment.ExpiresAt)
	}
	if wait := time.Until(assignment.ExpiresAt) + 50*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
	got := <-result
	if got.err != nil || !got.expired {
		t.Fatalf("expiry after lock wait = %v, %v", got.expired, got.err)
	}
	if _, err := svc.GetAssignment(ctx, item.ID); !errors.Is(err, ErrAssignmentNotFound) {
		t.Fatalf("assignment survived expiry: %v", err)
	}
	var releasedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT (payload->>'released_at')::timestamptz
		FROM events
		WHERE subject_id=$1 AND kind=$2 AND payload->>'reason'=$3
		ORDER BY seq DESC LIMIT 1
	`, item.ID, domain.EventWorkItemAssignmentReleased, domain.AssignmentReleaseExpired).Scan(&releasedAt); err != nil {
		t.Fatalf("read expiry release clock: %v", err)
	}
	if releasedAt.Before(assignment.ExpiresAt) {
		t.Fatalf("released_at=%s before expires_at=%s", releasedAt, assignment.ExpiresAt)
	}
}

func TestConcurrentFirstClaimHasOneWinnerAndTypedLoser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, writer, _, actorA, actorB := newAssignmentTestStack(t, ctx)
	item := createClaimableItem(t, ctx, NewService(pool, writer), actorA, "concurrent first claim")

	// Hold the work_item row while both claim transactions enter through their
	// own connections. Releasing this blocker makes them contend on the real
	// work_item -> assignment placeholder lock path rather than serializing at
	// the pool. The separate MaxConns=1 regression above covers connection
	// starvation/deadlock safety.
	contentionAppName := "meristem-claim-contention-" + uuid.NewString()
	contended := assignmentTestPoolWithAppName(t, ctx, pool, contentionAppName, 3)
	svc := NewService(contended, writer)
	blocker, err := contended.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin row blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	var lockedID uuid.UUID
	if err := blocker.QueryRow(ctx, `SELECT id FROM work_items WHERE id=$1 FOR UPDATE`, item.ID).Scan(&lockedID); err != nil {
		t.Fatalf("lock work item: %v", err)
	}

	type outcome struct {
		actor      domain.Token
		assignment domain.WorkItemAssignment
		err        error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, actor := range []domain.Token{actorA, actorB} {
		go func(actor domain.Token) {
			<-start
			assignment, err := svc.Claim(ctx, item.ID, actor)
			results <- outcome{actor: actor, assignment: assignment, err: err}
		}(actor)
	}
	close(start)
	// Observe both claim sessions actually waiting on the row blocker before
	// releasing it. This makes the regression prove database lock contention,
	// rather than relying on scheduler timing or a blind sleep.
	waitForAssignmentLockWaiters(t, ctx, pool, contentionAppName, 2)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release row blocker: %v", err)
	}
	first := <-results
	second := <-results
	outcomes := []outcome{first, second}
	var winner *outcome
	var loser *outcome
	for i := range outcomes {
		if outcomes[i].err == nil {
			winner = &outcomes[i]
		} else if errors.Is(outcomes[i].err, ErrClaimHeld) {
			loser = &outcomes[i]
		} else {
			t.Fatalf("unexpected claim outcome: actor=%s err=%v", outcomes[i].actor.ID, outcomes[i].err)
		}
	}
	if winner == nil || loser == nil {
		t.Fatalf("winner=%+v loser=%+v outcomes=%+v", winner, loser, outcomes)
	}
	if winner.assignment.HolderTokenID != winner.actor.ID {
		t.Fatalf("winner assignment = %+v actor=%s", winner.assignment, winner.actor.ID)
	}
	var conflict *ClaimHeldError
	if !errors.As(loser.err, &conflict) || conflict.HolderTokenID != winner.actor.ID || conflict.AssignmentEventID != winner.assignment.AssignmentEventID {
		t.Fatalf("typed loser conflict = %#v err=%v winner=%+v", conflict, loser.err, winner.assignment)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("concurrent assigned events = %d, want 1", got)
	}
}

func assignmentTestPoolWithAppName(t *testing.T, ctx context.Context, source *pgxpool.Pool, applicationName string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config := source.Config()
	config.MaxConns = maxConns
	config.MinConns = 0
	runtimeParams := make(map[string]string, len(config.ConnConfig.RuntimeParams)+1)
	for key, value := range config.ConnConfig.RuntimeParams {
		runtimeParams[key] = value
	}
	runtimeParams["application_name"] = applicationName
	config.ConnConfig.RuntimeParams = runtimeParams
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create assignment test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func waitForAssignmentLockWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, applicationName string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
			  AND wait_event_type = 'Lock'
		`, applicationName).Scan(&waiting); err != nil {
			t.Fatalf("observe assignment lock waiters: %v", err)
		}
		if waiting >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("observed %d assignment lock waiters, want %d", waiting, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newAssignmentTestStack(t *testing.T, ctx context.Context) (*pgxpool.Pool, *events.Writer, domain.Token, domain.Token, domain.Token) {
	t.Helper()
	pool := pgtest.NewPool(t, "meristem_assignment")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	projectorRegistry := projections.NewRegistry()
	auth.RegisterProjectors(projectorRegistry)
	RegisterProjectors(projectorRegistry)
	registrydomain.RegisterProjectors(projectorRegistry)
	writer := events.NewWriter(projectorRegistry)
	rootResult, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	root := rootResult.Token
	actorA := createAssignmentToken(t, ctx, pool, writer, "assignment-a", domain.SourceAgent, false, root)
	actorB := createAssignmentToken(t, ctx, pool, writer, "assignment-b", domain.SourceAgent, false, root)
	return pool, writer, root, actorA, actorB
}

func defineOneSecondCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) string {
	return defineClaimLeaseCultivar(t, ctx, pool, writer, actor, "claim-one-second", 1)
}

func defineClaimLeaseCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token, name string, seconds int) string {
	t.Helper()
	svc := registrydomain.NewService(pool, writer)
	if _, _, err := svc.DefineTropism(ctx, actor, registrydomain.DefineTropismInput{
		Name: name + "-checklist", Version: 1,
		Reducer:     registrydomain.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      json.RawMessage(`{"budget":{"max_attempts":1,"escalation":"hand_to_human"}}`),
		Description: "claim lease test reducer",
	}); err != nil {
		t.Fatalf("define claim test tropism: %v", err)
	}
	if _, _, err := svc.DefineCultivar(ctx, actor, registrydomain.DefineCultivarInput{
		Name: name, Version: 1,
		Tropism: registrydomain.TropismRef{Name: name + "-checklist", Version: 1},
		Profile: registrydomain.Profile{Briefing: "briefings/claim-test.md", ScopesTemplate: []string{"work_items.read", "work_items.write"}},
		Xylem:   registrydomain.Xylem{MaxAttempts: 1, MaxWallSeconds: seconds, MaxDepth: 0},
		Phloem:  "projection:work-item-brief", Description: "short claim lease",
	}); err != nil {
		t.Fatalf("define %s cultivar: %v", name, err)
	}
	return name + "@1"
}

func defineTightLifecycleClaimCultivar(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, actor domain.Token) string {
	t.Helper()
	svc := registrydomain.NewService(pool, writer)
	if _, _, err := svc.DefineTropism(ctx, actor, registrydomain.DefineTropismInput{
		Name: "claim-budget-checklist", Version: 1,
		Reducer:     registrydomain.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:      json.RawMessage(`{"budget":{"max_attempts":1,"escalation":"hand_to_human"}}`),
		Description: "claim churn budget test reducer",
	}); err != nil {
		t.Fatalf("define claim budget tropism: %v", err)
	}
	if _, _, err := svc.DefineCultivar(ctx, actor, registrydomain.DefineCultivarInput{
		Name: "claim-tight-lifecycle", Version: 1,
		Tropism: registrydomain.TropismRef{Name: "claim-budget-checklist", Version: 1},
		Profile: registrydomain.Profile{Briefing: "briefings/claim-budget.md", ScopesTemplate: []string{"work_items.read", "work_items.write"}},
		Xylem: registrydomain.Xylem{
			MaxAttempts: 1, MaxWallSeconds: 60, MaxDepth: 0,
			MaxEventsPerItemPerHourByClass: map[string]int{feed.KindClassLifecycle: 4},
		},
		Phloem: "projection:work-item-brief", Description: "tight claim lifecycle budget",
	}); err != nil {
		t.Fatalf("define tight claim lifecycle cultivar: %v", err)
	}
	return "claim-tight-lifecycle@1"
}

func createAssignmentToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, name string, source domain.Source, root bool, actor domain.Token) domain.Token {
	t.Helper()
	result, err := auth.NewService(pool, writer).CreateToken(ctx, auth.CreateTokenInput{Name: name, IsRoot: root, Source: source, Actor: &actor})
	if err != nil {
		t.Fatalf("create token %s: %v", name, err)
	}
	return result.Token
}

func createClaimableItem(t *testing.T, ctx context.Context, svc *Service, actor domain.Token, title string) domain.WorkItem {
	t.Helper()
	item, err := svc.Create(ctx, CreateInput{Title: title, State: domain.WorkItemRunning, SuggestedConvergenceChecks: []string{"manual claim-ledger test"}, HumanReviewStatus: domain.HumanReviewWavedThrough, Actor: actor})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	return item
}

func countAssignmentEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, kind string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, id, kind).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	return count
}

func loadLatestAssignmentEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, kind string) domain.Event {
	t.Helper()
	var eventID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM events WHERE subject_id=$1 AND kind=$2 ORDER BY seq DESC LIMIT 1`, id, kind).Scan(&eventID); err != nil {
		t.Fatalf("latest %s: %v", kind, err)
	}
	return loadAssignmentEvent(t, ctx, pool, eventID)
}

func loadAssignmentEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) domain.Event {
	t.Helper()
	var event domain.Event
	var source string
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT id, seq, occurred_at, actor_token_id, source, subject_kind, subject_id, kind, payload FROM events WHERE id=$1`, id).Scan(&event.ID, &event.Seq, &event.OccurredAt, &event.ActorTokenID, &source, &event.SubjectKind, &event.SubjectID, &event.Kind, &raw); err != nil {
		t.Fatalf("load event %s: %v", id, err)
	}
	event.Source = domain.Source(source)
	if err := json.Unmarshal(raw, &event.Payload); err != nil {
		t.Fatalf("decode event %s: %v", id, err)
	}
	return event
}

type assignmentProjector interface {
	Apply(context.Context, pgx.Tx, domain.Event) error
}

func applyAssignmentProjector(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projector assignmentProjector, event domain.Event, wantErr bool) {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = projector.Apply(ctx, tx, event)
	if wantErr {
		if err == nil {
			t.Fatalf("projector %T unexpectedly accepted stale/missing event", projector)
		}
		return
	}
	if err != nil {
		t.Fatalf("projector %T: %v", projector, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
