package mcp

// MCP mirrors of the canonical assignment transports (listener control
// plane, slice 1). REST is canonical; these tests pin request/response
// parity, the pure-refusal classification of claim conflicts, and that the
// new tools stay invisible to credentials without assignment authority.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestAssignmentToolsParityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-mcp-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	root := rootResult.Token
	workSvc := workitems.NewService(pool, writer)
	tree, err := workSvc.Create(ctx, workitems.CreateInput{Title: "assignment-mcp-tree", Actor: root})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	item, err := workSvc.SpawnChild(ctx, tree.ID, workitems.CreateInput{
		Title: "assignment-mcp-item", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"event:assignment_mcp"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      root,
	})
	if err != nil {
		t.Fatalf("spawn item: %v", err)
	}

	newAgentServer := func(name string) (*Server, auth.CreateTokenResult) {
		result, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
			Name:   name,
			Source: domain.SourceAgent,
			Scopes: []string{
				access.ScopeWorkItemsRead,
				access.ScopeWorkItemsWrite,
				"work_items.tree:" + tree.ID.String(),
			},
			Actor: &root,
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		s := New(Deps{
			Auth:        authSvc,
			Access:      access.NewService(pool),
			Idempotency: idempotency.NewMiddleware(pool, writer),
			WorkItems:   workSvc,
			Feed:        feed.NewService(pool),
		}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
		if err := s.Authenticate(ctx, result.Secret); err != nil {
			t.Fatalf("authenticate %s: %v", name, err)
		}
		return s, result
	}

	holderServer, holder := newAgentServer("assignment-mcp-holder")
	rivalServer, rival := newAgentServer("assignment-mcp-rival")

	// Claim through MCP; response mirrors the REST assignment object.
	isError, text := callToolForTest(t, holderServer, "work_items.claim", map[string]any{
		"id": item.ID.String(), "idempotency_key": uuid.NewString(),
	})
	if isError {
		t.Fatalf("claim failed: %s", text)
	}
	for _, field := range []string{"assignment_event_id", "holder_token_id", "claimed_at", "expires_at", holder.Token.ID.String()} {
		if !strings.Contains(text, field) {
			t.Fatalf("claim response missing %q: %s", field, text)
		}
	}
	var claimResult struct {
		Assignment struct {
			AssignmentEventID string `json:"assignment_event_id"`
		} `json:"assignment"`
	}
	if err := json.Unmarshal([]byte(text), &claimResult); err != nil || claimResult.Assignment.AssignmentEventID == "" {
		t.Fatalf("decode claim generation: %v (%s)", err, text)
	}
	generation := claimResult.Assignment.AssignmentEventID

	// Rival conflict is a typed pure refusal: tool error carrying the holder,
	// no event appended, and the SAME idempotency key stays usable.
	rivalKey := uuid.NewString()
	before := eventCount(t, pool, domain.EventWorkItemAssigned)
	isError, text = callToolForTest(t, rivalServer, "work_items.claim", map[string]any{
		"id": item.ID.String(), "idempotency_key": rivalKey,
	})
	if !isError || !strings.Contains(text, "claim held") {
		t.Fatalf("rival claim: isError=%t text=%q, want claim-held tool error", isError, text)
	}
	if after := eventCount(t, pool, domain.EventWorkItemAssigned); after != before {
		t.Fatalf("conflicting claim appended events: before=%d after=%d", before, after)
	}

	// Parity read.
	isError, text = callToolForTest(t, rivalServer, "work_items.get_assignment", map[string]any{"id": item.ID.String()})
	if isError || !strings.Contains(text, holder.Token.ID.String()) {
		t.Fatalf("get_assignment: isError=%t text=%q", isError, text)
	}

	// Yield by non-holder refuses purely; a stale generation refuses purely;
	// the holder naming the exact generation succeeds.
	if isError, text = callToolForTest(t, rivalServer, "work_items.yield", map[string]any{
		"id": item.ID.String(), "assignment_event_id": generation, "idempotency_key": uuid.NewString(),
	}); !isError || !strings.Contains(text, "held by another token") {
		t.Fatalf("rival yield: isError=%t text=%q", isError, text)
	}
	if isError, text = callToolForTest(t, holderServer, "work_items.yield", map[string]any{
		"id": item.ID.String(), "assignment_event_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
	}); !isError || !strings.Contains(text, "stale assignment generation") {
		t.Fatalf("stale-generation yield: isError=%t text=%q", isError, text)
	}
	if isError, text = callToolForTest(t, holderServer, "work_items.yield", map[string]any{
		"id": item.ID.String(), "assignment_event_id": generation, "idempotency_key": uuid.NewString(),
	}); isError {
		t.Fatalf("holder yield failed: %s", text)
	}

	// Released: get_assignment is a pure not-found now.
	if isError, text = callToolForTest(t, rivalServer, "work_items.get_assignment", map[string]any{"id": item.ID.String()}); !isError || !strings.Contains(text, "assignment_not_found") {
		t.Fatalf("post-yield get_assignment: isError=%t text=%q", isError, text)
	}

	// The rival's conflicted key was never consumed: reusing it now wins.
	if isError, text = callToolForTest(t, rivalServer, "work_items.claim", map[string]any{
		"id": item.ID.String(), "idempotency_key": rivalKey,
	}); isError || !strings.Contains(text, rival.Token.ID.String()) {
		t.Fatalf("rival re-claim with preserved key: isError=%t text=%q", isError, text)
	}
}

// TestAssignmentToolAdvertisementScopes pins the authority boundary: claim
// and yield are work-item WRITE capabilities, never tracker-write, so sealed
// provider tracker profiles cannot gain lease authority from the tool
// catalog growing.
func TestAssignmentToolAdvertisementScopes(t *testing.T) {
	treeID := uuid.New()
	writeAgent := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{
		access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite, "work_items.tree:" + treeID.String(),
	}}
	trackerAgent := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{
		access.ScopeWorkItemsRead, access.ScopeWorkItemsTrackerWrite, "work_items.tree:" + treeID.String(),
	}}
	readAgent := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{
		access.ScopeWorkItemsRead, "work_items.tree:" + treeID.String(),
	}}

	for _, tool := range []string{"work_items.claim", "work_items.yield"} {
		if !access.ToolVisible(writeAgent, tool) {
			t.Errorf("write-scoped agent missing %s", tool)
		}
		if access.ToolVisible(trackerAgent, tool) {
			t.Errorf("tracker-write agent must not see %s", tool)
		}
		if access.ToolVisible(readAgent, tool) {
			t.Errorf("read-only agent must not see %s", tool)
		}
	}
	if !access.ToolVisible(readAgent, "work_items.get_assignment") {
		t.Errorf("read-scoped agent missing work_items.get_assignment")
	}
}
