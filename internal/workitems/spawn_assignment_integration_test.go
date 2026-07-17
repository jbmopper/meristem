package workitems

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// AssignSpawned is the spawner-authored half of the reviewer binding
// (ee916614 slice 2): a non-root system executor binds a provisioned agent
// identity before the reviewing mind starts. It must mirror Claim's
// guarantees — idempotent same-assignee, typed conflict, no takeover — while
// never wielding the assignee's credential.
func TestAssignSpawnedBindsProvisionedReviewer(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, _ := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	spawner := createAssignmentToken(t, ctx, pool, writer, "spawner", domain.SourceSystem, false, root)
	reviewerX := createAssignmentToken(t, ctx, pool, writer, "reviewer-x", domain.SourceAgent, false, root)
	reviewerY := createAssignmentToken(t, ctx, pool, writer, "reviewer-y", domain.SourceAgent, false, root)
	human := createAssignmentToken(t, ctx, pool, writer, "human-reviewer", domain.SourceHuman, false, root)

	item := createClaimableItem(t, ctx, svc, actorA, "spawn binding")

	first, err := svc.AssignSpawned(ctx, item.ID, reviewerX.ID, spawner)
	if err != nil {
		t.Fatalf("spawn binding: %v", err)
	}
	if first.HolderTokenID != reviewerX.ID || first.Mode != domain.WorkItemAssignmentSpawn {
		t.Fatalf("spawn assignment = %+v, want holder %s mode spawn", first, reviewerX.ID)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("assigned events = %d, want 1", got)
	}

	// Idempotent same-assignee: canonical row unchanged, no second event.
	retry, err := svc.AssignSpawned(ctx, item.ID, reviewerX.ID, spawner)
	if err != nil {
		t.Fatalf("same-assignee retry: %v", err)
	}
	if !reflect.DeepEqual(retry, first) {
		t.Fatalf("same-assignee retry changed assignment:\nfirst=%+v\nretry=%+v", first, retry)
	}
	if got := countAssignmentEvents(t, ctx, pool, item.ID, domain.EventWorkItemAssigned); got != 1 {
		t.Fatalf("assigned events after retry = %d, want 1", got)
	}

	// A live binding refuses every competitor identically to Claim: another
	// spawn, and the would-be volunteer's own claim.
	var held *ClaimHeldError
	if _, err := svc.AssignSpawned(ctx, item.ID, reviewerY.ID, spawner); !errors.As(err, &held) || held.HolderTokenID != reviewerX.ID {
		t.Fatalf("competing spawn = %v, want ClaimHeldError holder %s", err, reviewerX.ID)
	}
	if _, err := svc.Claim(ctx, item.ID, reviewerY); !errors.As(err, &held) || held.HolderTokenID != reviewerX.ID {
		t.Fatalf("volunteer claim against spawn binding = %v, want ClaimHeldError holder %s", err, reviewerX.ID)
	}

	// The spawner must be a dedicated non-root system identity.
	if _, err := svc.AssignSpawned(ctx, item.ID, reviewerY.ID, actorA); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("agent-actor spawn = %v, want ErrInvalidRequest", err)
	}
	if _, err := svc.AssignSpawned(ctx, item.ID, reviewerY.ID, root); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("root-actor spawn = %v, want ErrInvalidRequest", err)
	}

	// The assignee must be a live non-root agent token.
	fresh := createClaimableItem(t, ctx, svc, actorA, "spawn assignee validation")
	if _, err := svc.AssignSpawned(ctx, fresh.ID, human.ID, spawner); !errors.Is(err, ErrSpawnAssigneeInvalid) {
		t.Fatalf("human assignee = %v, want ErrSpawnAssigneeInvalid", err)
	}
	if _, err := svc.AssignSpawned(ctx, fresh.ID, uuid.New(), spawner); !errors.Is(err, ErrSpawnAssigneeInvalid) {
		t.Fatalf("missing assignee = %v, want ErrSpawnAssigneeInvalid", err)
	}
	if _, err := svc.AssignSpawned(ctx, fresh.ID, root.ID, spawner); !errors.Is(err, ErrSpawnAssigneeInvalid) {
		t.Fatalf("root assignee = %v, want ErrSpawnAssigneeInvalid", err)
	}
}

// The verdict-authority gate fences review.verdict_recorded to the binding
// GENERATION in the one canonical AppendEvent path: only the holder, citing
// the exact current work_item.assigned event, may record a verdict; expiry,
// release, and rebinding all fence a stale process out; items that never used
// bindings keep legacy latest-verdict-wins.
func TestVerdictAuthorityGenerationFencing(t *testing.T) {
	ctx := context.Background()
	pool, writer, root, actorA, actorB := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)
	spawner := createAssignmentToken(t, ctx, pool, writer, "verdict-spawner", domain.SourceSystem, false, root)
	reviewerX := createAssignmentToken(t, ctx, pool, writer, "verdict-x", domain.SourceAgent, false, root)
	reviewerY := createAssignmentToken(t, ctx, pool, writer, "verdict-y", domain.SourceAgent, false, root)

	verdict := func(generation uuid.UUID) map[string]any {
		p := map[string]any{"verdict": "accepted", "summary": "fencing test"}
		if generation != uuid.Nil {
			p["assignment_event_id"] = generation.String()
		}
		return p
	}

	// Never-bound item: legacy latest-verdict-wins is untouched, but a
	// generation claim that cannot possibly be current is refused, not
	// ignored.
	legacy := createClaimableItem(t, ctx, svc, actorA, "legacy verdict item")
	if err := svc.AppendEvent(ctx, legacy.ID, ReviewVerdictInnerKind, verdict(uuid.New()), actorB); !errors.Is(err, ErrVerdictStaleGeneration) {
		t.Fatalf("legacy item with generation claim = %v, want ErrVerdictStaleGeneration", err)
	}
	if err := svc.AppendEvent(ctx, legacy.ID, ReviewVerdictInnerKind, verdict(uuid.Nil), actorB); err != nil {
		t.Fatalf("legacy item plain verdict: %v", err)
	}

	// Bound item, generation g1.
	item := createClaimableItem(t, ctx, svc, actorA, "bound verdict item")
	bound, err := svc.AssignSpawned(ctx, item.ID, reviewerX.ID, spawner)
	if err != nil {
		t.Fatalf("bind X: %v", err)
	}
	g1 := bound.AssignmentEventID

	if err := svc.AppendEvent(ctx, item.ID, ReviewVerdictInnerKind, verdict(g1), reviewerY); !errors.Is(err, ErrVerdictNotFromBoundReviewer) {
		t.Fatalf("non-holder verdict = %v, want ErrVerdictNotFromBoundReviewer", err)
	}
	if err := svc.AppendEvent(ctx, item.ID, ReviewVerdictInnerKind, verdict(uuid.Nil), reviewerX); !errors.Is(err, ErrVerdictGenerationRequired) {
		t.Fatalf("holder verdict without generation = %v, want ErrVerdictGenerationRequired", err)
	}
	if err := svc.AppendEvent(ctx, item.ID, ReviewVerdictInnerKind, verdict(uuid.New()), reviewerX); !errors.Is(err, ErrVerdictStaleGeneration) {
		t.Fatalf("holder verdict with wrong generation = %v, want ErrVerdictStaleGeneration", err)
	}
	if err := svc.AppendEvent(ctx, item.ID, ReviewVerdictInnerKind, verdict(g1), reviewerX); err != nil {
		t.Fatalf("holder verdict with current generation: %v", err)
	}

	// Real lease expiry (one-second cultivar): the holder's authority ends
	// with the lease — its own generation no longer carries a verdict.
	cultivar := defineOneSecondCultivar(t, ctx, pool, writer, root)
	shortItem, err := svc.Create(ctx, CreateInput{
		Title: "expiring verdict item", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{ReviewVerdictCheck},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Cultivar:                   cultivar, Actor: actorA,
	})
	if err != nil {
		t.Fatalf("create short-lease item: %v", err)
	}
	shortBound, err := svc.AssignSpawned(ctx, shortItem.ID, reviewerX.ID, spawner)
	if err != nil {
		t.Fatalf("bind short-lease: %v", err)
	}
	if wait := time.Until(shortBound.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerX); !errors.Is(err, ErrVerdictBindingRequired) {
		t.Fatalf("expired holder verdict = %v, want ErrVerdictBindingRequired", err)
	}

	// Rebinding to Y (opportunistically releasing the expired epoch) fences
	// the stale process completely: old holder refused, old generation
	// refused, and only Y citing g2 lands.
	rebound, err := svc.AssignSpawned(ctx, shortItem.ID, reviewerY.ID, spawner)
	if err != nil {
		t.Fatalf("rebind Y after expiry: %v", err)
	}
	g2 := rebound.AssignmentEventID
	if g2 == shortBound.AssignmentEventID {
		t.Fatal("rebind reused the expired generation")
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerX); !errors.Is(err, ErrVerdictNotFromBoundReviewer) {
		t.Fatalf("stale holder after rebind = %v, want ErrVerdictNotFromBoundReviewer", err)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerY); !errors.Is(err, ErrVerdictStaleGeneration) {
		t.Fatalf("new holder citing old generation = %v, want ErrVerdictStaleGeneration", err)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(g2), reviewerY); err != nil {
		t.Fatalf("new holder with current generation: %v", err)
	}

	// Yield releases the binding; the gap fails closed until a rebind.
	yielded := createClaimableItem(t, ctx, svc, actorA, "yielded verdict item")
	yBound, err := svc.AssignSpawned(ctx, yielded.ID, reviewerX.ID, spawner)
	if err != nil {
		t.Fatalf("bind for yield: %v", err)
	}
	if _, err := svc.Yield(ctx, yielded.ID, reviewerX); err != nil {
		t.Fatalf("yield: %v", err)
	}
	if err := svc.AppendEvent(ctx, yielded.ID, ReviewVerdictInnerKind, verdict(yBound.AssignmentEventID), reviewerX); !errors.Is(err, ErrVerdictBindingRequired) {
		t.Fatalf("verdict after yield = %v, want ErrVerdictBindingRequired", err)
	}
	if err := svc.AppendEvent(ctx, yielded.ID, ReviewVerdictInnerKind, verdict(uuid.Nil), actorB); !errors.Is(err, ErrVerdictBindingRequired) {
		t.Fatalf("third-party verdict after yield = %v, want ErrVerdictBindingRequired", err)
	}
}
