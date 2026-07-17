package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/workitems"
)

// The lens vocabulary is contract-only in this slice, so these tests exercise
// the feed service directly with normalized ReadFilters rather than wire params.
func lensRead(t *testing.T, fixture assignedFeedFixture, predicates ...feed.Predicate) string {
	t.Helper()
	filter, err := feed.NormalizeReadFilter(feed.ReadFilter{Predicates: predicates})
	if err != nil {
		t.Fatalf("normalize lens filter: %v", err)
	}
	items, err := feed.NewService(fixture.pool).ListWithReadFilter(fixture.ctx, filter, 200)
	if err != nil {
		t.Fatalf("lens read: %v", err)
	}
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal lens read: %v", err)
	}
	return string(raw)
}

func TestLensContractAnchorActorAndKindPredicatesIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	appendQuietSelfNote(t, fixture, "assignedA", "lens-by-actor-alpha", fixture.actorA.Token)
	appendQuietSelfNote(t, fixture, "assignedB", "lens-by-actor-beta", fixture.actorB.Token)
	appendQuietSelfNote(t, fixture, "unassigned", "lens-by-root", fixture.root.Token)
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.outside.ID, "agent.quiet_self_test",
		map[string]any{"marker": "lens-outside-tree"}, fixture.root.Token); err != nil {
		t.Fatalf("append outside note: %v", err)
	}

	body := lensRead(t, fixture, feed.Predicate{Kind: feed.PredicateActor, TokenID: fixture.actorB.Token.ID})
	if !strings.Contains(body, "lens-by-actor-beta") {
		t.Fatalf("actor lens missing B's event: %s", body)
	}
	for _, hidden := range []string{"lens-by-actor-alpha", "lens-by-root"} {
		if strings.Contains(body, hidden) {
			t.Fatalf("actor lens leaked %q: %s", hidden, body)
		}
	}

	body = lensRead(t, fixture, feed.Predicate{Kind: feed.PredicateWorkItem, WorkItemID: fixture.assignedA.ID})
	if !strings.Contains(body, "lens-by-actor-alpha") {
		t.Fatalf("work_item lens missing anchored event: %s", body)
	}
	for _, hidden := range []string{"lens-by-actor-beta", "lens-by-root", "lens-outside-tree"} {
		if strings.Contains(body, hidden) {
			t.Fatalf("work_item lens leaked %q: %s", hidden, body)
		}
	}

	body = lensRead(t, fixture, feed.Predicate{Kind: feed.PredicateWorkItemTree, WorkItemID: fixture.tree.ID})
	for _, visible := range []string{"lens-by-actor-alpha", "lens-by-actor-beta", "lens-by-root"} {
		if !strings.Contains(body, visible) {
			t.Fatalf("tree lens missing in-tree event %q: %s", visible, body)
		}
	}
	if strings.Contains(body, "lens-outside-tree") {
		t.Fatalf("tree lens leaked outside event: %s", body)
	}

	body = lensRead(t, fixture, feed.Predicate{Kind: feed.PredicateKindInclude, EventKinds: []string{domain.EventWorkItemEventAppended}})
	if !strings.Contains(body, "lens-by-actor-alpha") || strings.Contains(body, domain.EventWorkItemCreated) {
		t.Fatalf("kind_include did not narrow to the named kind: %s", body)
	}

	body = lensRead(t, fixture, feed.Predicate{Kind: feed.PredicateKindExclude, EventKinds: []string{domain.EventWorkItemEventAppended}})
	if strings.Contains(body, "lens-by-actor-alpha") || !strings.Contains(body, domain.EventWorkItemCreated) {
		t.Fatalf("kind_exclude did not remove the named kind: %s", body)
	}
}

func TestLensContractKindNarrowingKeepsWakeSignalsIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	if _, err := fixture.work.Claim(fixture.ctx, fixture.sseItem.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim sse item: %v", err)
	}
	handback, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
		Title: "lens-handback", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"handback survives kind narrowing"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn handback item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, handback.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim handback item: %v", err)
	}
	if _, err := fixture.work.Transition(fixture.ctx, handback.ID, domain.WorkItemDone, "lens-terminal-handback-reason", fixture.root.Token); err != nil {
		t.Fatalf("root terminalize: %v", err)
	}
	if _, err := fixture.work.Transition(fixture.ctx, fixture.assignedA.ID, domain.WorkItemTriaged, "lens-nonterminal-reason", fixture.root.Token); err != nil {
		t.Fatalf("non-terminal transition: %v", err)
	}
	appendQuietSelfNote(t, fixture, "assignedA", "lens-included-note", fixture.actorA.Token)

	assigned := feed.Predicate{Kind: feed.PredicateAssignedOrAddressed, TokenID: fixture.actorA.Token.ID}

	body := lensRead(t, fixture, assigned, feed.Predicate{Kind: feed.PredicateKindInclude, EventKinds: []string{domain.EventWorkItemEventAppended}})
	for _, visible := range []string{domain.EventWorkItemAssigned, "lens-terminal-handback-reason", "lens-included-note"} {
		if !strings.Contains(body, visible) {
			t.Fatalf("kind_include swallowed wake signal or included content %q: %s", visible, body)
		}
	}
	if strings.Contains(body, "lens-nonterminal-reason") {
		t.Fatalf("kind_include kept a non-addressed transitioned event: %s", body)
	}

	body = lensRead(t, fixture, assigned, feed.Predicate{Kind: feed.PredicateKindExclude, EventKinds: []string{domain.EventWorkItemTransitioned}})
	if !strings.Contains(body, "lens-terminal-handback-reason") {
		t.Fatalf("kind_exclude swallowed the terminal handback: %s", body)
	}
	if strings.Contains(body, "lens-nonterminal-reason") {
		t.Fatalf("kind_exclude kept a non-addressed transitioned event: %s", body)
	}

	// Explicit-addressee protection under kind narrowing: a root-authored
	// generic event explicitly addressed to A survives kind sets that do not
	// name work_item.event_appended, in both include and exclude form.
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedA.ID, "agent.quiet_self_test",
		map[string]any{"marker": "lens-explicit-addressed", "addressee_token_id": fixture.actorA.Token.ID}, fixture.root.Token); err != nil {
		t.Fatalf("append explicitly addressed note: %v", err)
	}
	body = lensRead(t, fixture, assigned, feed.Predicate{Kind: feed.PredicateKindInclude, EventKinds: []string{domain.EventMessageCaptured}})
	if !strings.Contains(body, "lens-explicit-addressed") {
		t.Fatalf("kind_include swallowed an explicitly addressed event: %s", body)
	}
	body = lensRead(t, fixture, assigned, feed.Predicate{Kind: feed.PredicateKindExclude, EventKinds: []string{domain.EventWorkItemEventAppended}})
	if !strings.Contains(body, "lens-explicit-addressed") {
		t.Fatalf("kind_exclude swallowed an explicitly addressed event: %s", body)
	}

	// The wake-bridge case: a listener lensed to one author (its allowlist)
	// must still receive directed signals other actors send it. The
	// root-authored terminal handback survives an actor lens for A; root's
	// non-addressed transition does not.
	body = lensRead(t, fixture, assigned, feed.Predicate{Kind: feed.PredicateActor, TokenID: fixture.actorA.Token.ID})
	for _, visible := range []string{"lens-terminal-handback-reason", "lens-included-note"} {
		if !strings.Contains(body, visible) {
			t.Fatalf("actor lens swallowed %q: %s", visible, body)
		}
	}
	if strings.Contains(body, "lens-nonterminal-reason") {
		t.Fatalf("actor lens kept root's non-addressed transition: %s", body)
	}
}
