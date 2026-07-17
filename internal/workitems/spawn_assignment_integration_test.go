package workitems

import (
	"context"
	"encoding/json"
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
	reviewerS := createAssignmentToken(t, ctx, pool, writer, "verdict-short-lease", domain.SourceAgent, false, root)
	shortBound, err := svc.AssignSpawned(ctx, shortItem.ID, reviewerS.ID, spawner)
	if err != nil {
		t.Fatalf("bind short-lease: %v", err)
	}
	if wait := time.Until(shortBound.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerS); !errors.Is(err, ErrVerdictBindingRequired) {
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
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerS); !errors.Is(err, ErrVerdictNotFromBoundReviewer) {
		t.Fatalf("stale holder after rebind = %v, want ErrVerdictNotFromBoundReviewer", err)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(shortBound.AssignmentEventID), reviewerY); !errors.Is(err, ErrVerdictStaleGeneration) {
		t.Fatalf("new holder citing old generation = %v, want ErrVerdictStaleGeneration", err)
	}
	if err := svc.AppendEvent(ctx, shortItem.ID, ReviewVerdictInnerKind, verdict(g2), reviewerY); err != nil {
		t.Fatalf("new holder with current generation: %v", err)
	}

	// Round fencing (codex round-2 P1): while ONE binding generation stays
	// active, the artifact must not be able to move under it. A new
	// implementation.ready_for_review postdating the binding voids its
	// verdict authority — the executor must rebind per round — and the
	// verdict must name the round's exact commit.
	rounds := createClaimableItem(t, ctx, svc, actorA, "round fenced item")
	roundA := map[string]any{"commit": "aaaa111", "branch": "claude/demo"}
	if err := svc.AppendEvent(ctx, rounds.ID, "implementation.ready_for_review", roundA, actorA); err != nil {
		t.Fatalf("declare round A: %v", err)
	}
	reviewerR1 := createAssignmentToken(t, ctx, pool, writer, "verdict-round-a", domain.SourceAgent, false, root)
	roundBound, err := svc.AssignSpawned(ctx, rounds.ID, reviewerR1.ID, spawner)
	if err != nil {
		t.Fatalf("bind for round A: %v", err)
	}
	gr := roundBound.AssignmentEventID
	withCommit := func(generation uuid.UUID, commit string) map[string]any {
		p := verdict(generation)
		p["reviewed_commit"] = commit
		return p
	}
	// Wrong artifact refused; right artifact lands.
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(gr, "wrong000"), reviewerR1); !errors.Is(err, ErrVerdictWrongArtifact) {
		t.Fatalf("wrong-commit verdict = %v, want ErrVerdictWrongArtifact", err)
	}
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(gr, "aaaa111"), reviewerR1); err != nil {
		t.Fatalf("round A verdict: %v", err)
	}
	// The artifact moves A -> B with the SAME binding generation active: the
	// old generation must not carry a verdict for the new round, with the old
	// commit or the new one.
	roundB := map[string]any{"commit": "bbbb222", "branch": "claude/demo"}
	if err := svc.AppendEvent(ctx, rounds.ID, "implementation.ready_for_review", roundB, actorA); err != nil {
		t.Fatalf("declare round B: %v", err)
	}
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(gr, "aaaa111"), reviewerR1); !errors.Is(err, ErrVerdictStaleRound) {
		t.Fatalf("same-generation stale-round verdict (old commit) = %v, want ErrVerdictStaleRound", err)
	}
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(gr, "bbbb222"), reviewerR1); !errors.Is(err, ErrVerdictStaleRound) {
		t.Fatalf("same-generation stale-round verdict (new commit) = %v, want ErrVerdictStaleRound", err)
	}
	// Rebinding against round B restores authority for exactly round B. The
	// expired-epoch release path needs the incumbent gone first: the holder
	// yields, then the spawner rebinds.
	if _, err := svc.Yield(ctx, rounds.ID, reviewerR1); err != nil {
		t.Fatalf("yield round-A binding: %v", err)
	}
	reviewerR2 := createAssignmentToken(t, ctx, pool, writer, "verdict-round-b", domain.SourceAgent, false, root)
	reboundRound, err := svc.AssignSpawned(ctx, rounds.ID, reviewerR2.ID, spawner)
	if err != nil {
		t.Fatalf("rebind for round B: %v", err)
	}
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(reboundRound.AssignmentEventID, "aaaa111"), reviewerR2); !errors.Is(err, ErrVerdictWrongArtifact) {
		t.Fatalf("rebound verdict citing old commit = %v, want ErrVerdictWrongArtifact", err)
	}
	if err := svc.AppendEvent(ctx, rounds.ID, ReviewVerdictInnerKind, withCommit(reboundRound.AssignmentEventID, "bbbb222"), reviewerR2); err != nil {
		t.Fatalf("rebound verdict for round B: %v", err)
	}

	// The live MCP handoff records the ready-for-review inner payload as a
	// JSON-encoded STRING; the artifact fence must decode it identically to
	// the object form, never silently disable (finding on 8ef04eb). And a
	// round that declares no valid commit carries no verdict authority.
	liveShape := createClaimableItem(t, ctx, svc, actorA, "string-shaped round item")
	encodedRound, err := json.Marshal(map[string]any{"commit": "cccc333", "branch": "claude/live-shape"})
	if err != nil {
		t.Fatalf("encode live-shape round: %v", err)
	}
	// AppendEvent with a string payload stores inner as a JSON string — the
	// exact shape live MCP deliveries produce.
	if err := svc.AppendEvent(ctx, liveShape.ID, "implementation.ready_for_review", string(encodedRound), actorA); err != nil {
		t.Fatalf("declare string-shaped round: %v", err)
	}
	reviewerLv := createAssignmentToken(t, ctx, pool, writer, "verdict-live-shape", domain.SourceAgent, false, root)
	liveBound, err := svc.AssignSpawned(ctx, liveShape.ID, reviewerLv.ID, spawner)
	if err != nil {
		t.Fatalf("bind string-shaped round: %v", err)
	}
	if err := svc.AppendEvent(ctx, liveShape.ID, ReviewVerdictInnerKind, withCommit(liveBound.AssignmentEventID, "wrong999"), reviewerLv); !errors.Is(err, ErrVerdictWrongArtifact) {
		t.Fatalf("string-shape wrong-commit verdict = %v, want ErrVerdictWrongArtifact", err)
	}
	if err := svc.AppendEvent(ctx, liveShape.ID, ReviewVerdictInnerKind, withCommit(liveBound.AssignmentEventID, "cccc333"), reviewerLv); err != nil {
		t.Fatalf("string-shape correct verdict: %v", err)
	}

	// A declared round with no commit at all fails closed for every verdict.
	noCommit := createClaimableItem(t, ctx, svc, actorA, "commitless round item")
	if err := svc.AppendEvent(ctx, noCommit.ID, "implementation.ready_for_review", map[string]any{"branch": "claude/no-commit"}, actorA); err != nil {
		t.Fatalf("declare commitless round: %v", err)
	}
	reviewerNc := createAssignmentToken(t, ctx, pool, writer, "verdict-commitless", domain.SourceAgent, false, root)
	ncBound, err := svc.AssignSpawned(ctx, noCommit.ID, reviewerNc.ID, spawner)
	if err != nil {
		t.Fatalf("bind commitless round: %v", err)
	}
	if err := svc.AppendEvent(ctx, noCommit.ID, ReviewVerdictInnerKind, withCommit(ncBound.AssignmentEventID, "anything"), reviewerNc); !errors.Is(err, ErrVerdictRoundArtifactInvalid) {
		t.Fatalf("commitless-round verdict = %v, want ErrVerdictRoundArtifactInvalid", err)
	}

	// Conflicting commit vs exact_commit declarations also fail closed.
	conflicted := createClaimableItem(t, ctx, svc, actorA, "conflicting round item")
	if err := svc.AppendEvent(ctx, conflicted.ID, "implementation.ready_for_review", map[string]any{"commit": "dddd444", "exact_commit": "eeee555"}, actorA); err != nil {
		t.Fatalf("declare conflicting round: %v", err)
	}
	reviewerCf := createAssignmentToken(t, ctx, pool, writer, "verdict-conflicted", domain.SourceAgent, false, root)
	cfBound, err := svc.AssignSpawned(ctx, conflicted.ID, reviewerCf.ID, spawner)
	if err != nil {
		t.Fatalf("bind conflicting round: %v", err)
	}
	if err := svc.AppendEvent(ctx, conflicted.ID, ReviewVerdictInnerKind, withCommit(cfBound.AssignmentEventID, "dddd444"), reviewerCf); !errors.Is(err, ErrVerdictRoundArtifactInvalid) {
		t.Fatalf("conflicting-round verdict = %v, want ErrVerdictRoundArtifactInvalid", err)
	}

	// A missing assignment placeholder is a corrupted projection, never a
	// legacy item (migration 0035 backfills every work item): the gate fails
	// closed instead of waving the verdict past holder/generation authority
	// (finding e0165213).
	corrupted := createClaimableItem(t, ctx, svc, actorA, "corrupted placeholder item")
	if _, err := pool.Exec(ctx, `DELETE FROM work_item_assignment_state WHERE work_item_id = $1`, corrupted.ID); err != nil {
		t.Fatalf("corrupt placeholder: %v", err)
	}
	if err := svc.AppendEvent(ctx, corrupted.ID, ReviewVerdictInnerKind, verdict(uuid.Nil), actorB); !errors.Is(err, ErrAssignmentStateMissing) {
		t.Fatalf("verdict on corrupted placeholder = %v, want fail-closed ErrAssignmentStateMissing", err)
	}

	// Yield releases the binding; the gap fails closed until a rebind.
	// (Reviewer identities are single-use since slice 3a, so every fresh
	// binding in this test provisions a fresh token.)
	yielded := createClaimableItem(t, ctx, svc, actorA, "yielded verdict item")
	reviewerYd := createAssignmentToken(t, ctx, pool, writer, "verdict-yield", domain.SourceAgent, false, root)
	yBound, err := svc.AssignSpawned(ctx, yielded.ID, reviewerYd.ID, spawner)
	if err != nil {
		t.Fatalf("bind for yield: %v", err)
	}
	if _, err := svc.Yield(ctx, yielded.ID, reviewerYd); err != nil {
		t.Fatalf("yield: %v", err)
	}
	if err := svc.AppendEvent(ctx, yielded.ID, ReviewVerdictInnerKind, verdict(yBound.AssignmentEventID), reviewerYd); !errors.Is(err, ErrVerdictBindingRequired) {
		t.Fatalf("verdict after yield = %v, want ErrVerdictBindingRequired", err)
	}
	if err := svc.AppendEvent(ctx, yielded.ID, ReviewVerdictInnerKind, verdict(uuid.Nil), actorB); !errors.Is(err, ErrVerdictBindingRequired) {
		t.Fatalf("third-party verdict after yield = %v, want ErrVerdictBindingRequired", err)
	}
}
