package main

import (
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
)

func strptr(s string) *string { return &s }

// The route section must show exactly what the sender's pure selection rule
// would walk right now: direct first, then queue hosts in relay_via order,
// and an explicit reason when there is no plan at all.
func TestBuildRoutePlansMirrorsSelect(t *testing.T) {
	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	registry := []domain.Node{
		{NodeID: "hub", DirectURL: strptr("https://hub.example"), Status: domain.NodeStatusActive},
		{NodeID: "m4", RelayVia: []string{"hub"}, Status: domain.NodeStatusActive},
		{NodeID: "island", Status: domain.NodeStatusActive},
		{NodeID: "retired", DirectURL: strptr("https://retired.example"), Status: domain.NodeStatusDisabled},
	}

	plans := buildRoutePlans(registry, now)
	if len(plans) != 4 {
		t.Fatalf("plans = %d, want one per registered node", len(plans))
	}
	byID := map[string]routePlanReport{}
	for _, p := range plans {
		byID[p.TargetNodeID] = p
	}

	if got := byID["hub"].Plan; len(got) != 1 || got[0] != "direct https://hub.example" {
		t.Fatalf("hub plan = %v, want the direct candidate", got)
	}
	if got := byID["m4"].Plan; len(got) != 1 || got[0] != "queue via hub https://hub.example" {
		t.Fatalf("m4 plan = %v, want the queue-host candidate", got)
	}
	if p := byID["island"]; len(p.Plan) != 0 || p.Error == "" {
		t.Fatalf("island = %+v, want no plan and an explicit error", p)
	}
	if p := byID["retired"]; len(p.Plan) != 0 || p.Error == "" {
		t.Fatalf("retired = %+v, want no plan for a disabled node", p)
	}

	// The plan is the same walk Select emits — assert against it directly so
	// this test fails if selection semantics move under the diagnostics.
	candidates, err := crossnode.Select(registry, "m4", nil, now)
	if err != nil || len(candidates) != 1 || candidates[0].Kind != crossnode.KindQueue {
		t.Fatalf("Select(m4) = (%v, %v); diagnostics and selection disagree", candidates, err)
	}
}

// A legacy-replayed origin that current validation rejects (userinfo here)
// must never be printed or presented as a usable route.
func TestBuildRoutePlansRedactsInvalidLegacyOrigins(t *testing.T) {
	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	registry := []domain.Node{
		{NodeID: "legacy", DirectURL: strptr("https://user:secret@legacy.example"), Status: domain.NodeStatusActive},
	}
	plans := buildRoutePlans(registry, now)
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	got := strings.Join(plans[0].Plan, "; ")
	if strings.Contains(got, "secret") || strings.Contains(got, "user:") {
		t.Fatalf("plan leaked legacy userinfo: %q", got)
	}
	if !strings.Contains(got, redactedOrigin) {
		t.Fatalf("plan = %q, want %q for an invalid legacy origin", got, redactedOrigin)
	}
}

// --target for a node the registry does not know must surface the selection
// rule's unknown-target refusal, never an empty (or "no nodes") screen.
func TestFilterRoutesSurfacesUnknownTarget(t *testing.T) {
	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	registry := []domain.Node{
		{NodeID: "hub", DirectURL: strptr("https://hub.example"), Status: domain.NodeStatusActive},
	}
	plans := buildRoutePlans(registry, now)

	got := filterRoutes(plans, registry, "ghost", now)
	if len(got) != 1 || got[0].TargetNodeID != "ghost" {
		t.Fatalf("filterRoutes(ghost) = %v, want one ghost report", got)
	}
	if got[0].Error != crossnode.ErrUnknownTarget.Error() {
		t.Fatalf("ghost error = %q, want Select's unknown-target refusal", got[0].Error)
	}

	kept := filterRoutes(plans, registry, "hub", now)
	if len(kept) != 1 || kept[0].TargetNodeID != "hub" || kept[0].Error != "" {
		t.Fatalf("filterRoutes(hub) = %v, want the existing hub plan", kept)
	}
}

// The text rendering must carry every diagnostic the operator needs on one
// screen: route plan, the pending retryable/exhausted/due split with attempt
// budget and expiry eligibility, the last terminal outcome AND the last
// failure (a later success must not hide it), reconciler cursor, and spoke
// bookmark.
func TestRenderNodeStatusShowsFailureAndRetryFacts(t *testing.T) {
	now := time.Date(2026, 7, 17, 1, 2, 3, 0, time.UTC)
	oldest := now.Add(-90 * time.Minute)
	lastAttempt := now.Add(-5 * time.Minute)
	failureAttempt := now.Add(-65 * time.Minute)
	expires := now.Add(22 * time.Hour)
	failCode := 502
	doneCode := 201

	report := nodeStatusReport{
		GeneratedAt: now,
		Routes: []routePlanReport{
			{TargetNodeID: "m4", Status: "active", Plan: []string{"queue via hub https://hub.example"}},
			{TargetNodeID: "island", Status: "active", Error: crossnode.ErrNoRoute.Error()},
		},
		Queue: []crossnode.QueueTargetStatus{{
			TargetNodeID:       "m4",
			Pending:            3,
			PendingRetryable:   1,
			PendingExhausted:   1,
			PendingDue:         1,
			OldestQueuedAt:     &oldest,
			EarliestDeadlineAt: &expires,
			MaxAttempts:        5,
			LastAttemptAt:      &lastAttempt,
			Done:               4,
			Failed:             1,
			LastTerminal: &crossnode.TerminalOutcome{
				CommandPath: "/v1/work-items/y/transition",
				State:       "done",
				StatusCode:  &doneCode,
				At:          now.Add(-30 * time.Minute),
			},
			LastFailure: &crossnode.TerminalOutcome{
				CommandPath:   "/v1/work-items/x/transition",
				State:         "failed",
				StatusCode:    &failCode,
				At:            now.Add(-time.Hour),
				LastAttemptAt: &failureAttempt,
			},
		}},
		Outcomes: []crossnode.OutcomeHostStatus{{
			QueueHostNodeID: "hub", OriginNodeID: "slab",
			CursorSeq: 41, CursorUpdatedAt: now, Observations: 2,
			LastObserved: &crossnode.ObservedOutcome{
				TargetNodeID: "m4", Outcome: "expired", RemoteOccurredAt: now.Add(-time.Minute),
			},
		}},
		SpokeCursors: []crossnode.SpokeCursor{{Key: "hub_feed_cursor", Value: "17", UpdatedAt: now}},
	}

	var b strings.Builder
	renderNodeStatus(&b, report)
	out := b.String()

	for _, want := range []string{
		"queue via hub https://hub.example",
		"no plan: " + crossnode.ErrNoRoute.Error(),
		"pending=3 (retryable=1 exhausted=1 due=1)",
		"attempts=5/5",
		"oldest=" + oldest.Format(time.RFC3339),
		"last_attempt=" + lastAttempt.Format(time.RFC3339),
		"deadline=" + expires.Format(time.RFC3339),
		"last_terminal=done status=201",
		// The last terminal has no recorded attempt; it must say so rather
		// than omit the field.
		"last_attempt=- path=/v1/work-items/y/transition",
		"last_failure=failed status=502",
		// The last failure carries its own final attempt time (P2, round 2).
		"last_attempt=" + failureAttempt.Format(time.RFC3339) + " path=/v1/work-items/x/transition",
		"cursor_seq=41",
		"last=expired target=m4",
		"hub_feed_cursor\t17",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered status missing %q:\n%s", want, out)
		}
	}
}

// Empty sections must say why they are empty instead of vanishing: an
// operator diagnosing a quiet node needs "nothing here" stated, not implied.
func TestRenderNodeStatusNamesEmptySections(t *testing.T) {
	var b strings.Builder
	renderNodeStatus(&b, nodeStatusReport{GeneratedAt: time.Unix(0, 0).UTC()})
	out := b.String()
	for _, want := range []string{
		"no nodes registered",
		"queue empty",
		"sync-outcomes has not run here",
		"only pull-only nodes advance these",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("empty rendering missing %q:\n%s", want, out)
		}
	}
}
