package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

type oceFixture struct {
	ctx    context.Context
	auth   *auth.Service
	work   *workitems.Service
	deps   Deps
	root   auth.CreateTokenResult
	actorA auth.CreateTokenResult
	actorB auth.CreateTokenResult
	broad  auth.CreateTokenResult
	tree   domain.WorkItem
	itemA  domain.WorkItem
	itemB  domain.WorkItem
}

func newOCEFixture(t *testing.T) oceFixture {
	t.Helper()
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oce-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	tree, err := workSvc.Create(ctx, workitems.CreateInput{Title: "oce-tree", Actor: root.Token})
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	spawn := func(title string) domain.WorkItem {
		item, err := workSvc.SpawnChild(ctx, tree.ID, workitems.CreateInput{Title: title, Actor: root.Token})
		if err != nil {
			t.Fatalf("spawn %s: %v", title, err)
		}
		return item
	}
	itemA := spawn("oce-item-a")
	itemB := spawn("oce-item-b")
	assignedScopes := []string{access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned, "work_items.tree:" + tree.ID.String()}
	actorA, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oce-a", Source: domain.SourceAgent, Scopes: assignedScopes, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create actor A: %v", err)
	}
	actorB, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "oce-b", Source: domain.SourceAgent, Scopes: assignedScopes, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create actor B: %v", err)
	}
	broad, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "oce-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead, access.ScopeWorkItemsReadAll},
		Actor:  &root.Token,
	})
	if err != nil {
		t.Fatalf("create broad: %v", err)
	}
	if _, err := workSvc.Claim(ctx, itemA.ID, actorA.Token); err != nil {
		t.Fatalf("claim A: %v", err)
	}
	if _, err := workSvc.Claim(ctx, itemB.ID, actorB.Token); err != nil {
		t.Fatalf("claim B: %v", err)
	}
	deps := Deps{
		Auth:        authSvc,
		Access:      access.NewService(pool),
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workSvc,
		Feed:        feed.NewService(pool),
	}
	return oceFixture{ctx: ctx, auth: authSvc, work: workSvc, deps: deps, root: root,
		actorA: actorA, actorB: actorB, broad: broad, tree: tree, itemA: itemA, itemB: itemB}
}

func (f oceFixture) server(t *testing.T, secret string) *Server {
	t.Helper()
	s := New(f.deps, ServerInfo{Name: "oce-test", Version: "test"}, nil)
	if err := s.Authenticate(f.ctx, secret); err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	return s
}

func (f oceFixture) note(t *testing.T, item domain.WorkItem, marker string, actor domain.Token) {
	t.Helper()
	if err := f.work.AppendEvent(f.ctx, item.ID, "agent.oce_test", map[string]any{"marker": marker}, actor); err != nil {
		t.Fatalf("append %s: %v", marker, err)
	}
}

func feedReadForTest(t *testing.T, s *Server, args map[string]any) (bool, string) {
	t.Helper()
	return callToolForTest(t, s, "feed.read", args)
}

func nextCursorFromText(t *testing.T, text string) string {
	t.Helper()
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(text), &page); err != nil || page.NextCursor == "" {
		t.Fatalf("no next_cursor in tool result: err=%v text=%s", err, text)
	}
	return page.NextCursor
}

func TestMCPFeedReadAssignedLaneParityIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	fixture.note(t, fixture.itemA, "oce-assigned-a-visible", fixture.root.Token)
	fixture.note(t, fixture.itemB, "oce-other-assignee-hidden", fixture.root.Token)

	// An assigned-only bearer with no scope argument is normalized onto the
	// reducing lane, exactly as REST normalizes it.
	sA := fixture.server(t, fixture.actorA.Secret)
	isErr, text := feedReadForTest(t, sA, map[string]any{"limit": 100})
	if isErr {
		t.Fatalf("assigned-only default read errored: %s", text)
	}
	if !strings.Contains(text, "oce-assigned-a-visible") || strings.Contains(text, "oce-other-assignee-hidden") {
		t.Fatalf("assigned lane parity broken: %s", text)
	}

	// Explicit scope=assigned works for a full feed.read authority.
	sBroad := fixture.server(t, fixture.broad.Secret)
	isErr, text = feedReadForTest(t, sBroad, map[string]any{"limit": 100, "scope": "assigned"})
	if isErr {
		t.Fatalf("broad scope=assigned errored: %s", text)
	}
	if strings.Contains(text, "oce-assigned-a-visible") {
		t.Fatalf("broad reader's assigned lane leaked another actor's lane: %s", text)
	}

	// Invalid scope fails closed; a token without feed authority is refused.
	if isErr, text = feedReadForTest(t, sBroad, map[string]any{"scope": "future"}); !isErr || !strings.Contains(text, "invalid_feed_scope") {
		t.Fatalf("invalid scope not rejected: err=%t %s", isErr, text)
	}

	// The legacy plain-snapshot branch keeps the access reduction: a token
	// with no feed scope at all is denied there, never handed the raw log.
	noFeed, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "oce-no-feed", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, "work_items.tree:" + fixture.tree.ID.String()},
		Actor:  &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create no-feed token: %v", err)
	}
	sNoFeed := fixture.server(t, noFeed.Secret)
	if isErr, text = feedReadForTest(t, sNoFeed, map[string]any{"limit": 10}); !isErr || !strings.Contains(text, "insufficient_scope") {
		t.Fatalf("plain snapshot without feed scope not denied: err=%t %s", isErr, text)
	}
}

func TestMCPFeedReadExcludeActorParityIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	fixture.note(t, fixture.itemA, "oce-by-alpha", fixture.actorA.Token)
	fixture.note(t, fixture.itemB, "oce-by-beta", fixture.actorB.Token)
	fixture.note(t, fixture.itemA, "oce-by-broad", fixture.broad.Token)
	s := fixture.server(t, fixture.broad.Secret)

	isErr, text := feedReadForTest(t, s, map[string]any{"limit": 100, "exclude_actor": []string{fixture.actorA.Token.ID.String()}})
	if isErr || strings.Contains(text, "oce-by-alpha") || !strings.Contains(text, "oce-by-beta") {
		t.Fatalf("exclude by id broken: err=%t %s", isErr, text)
	}
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100, "exclude_actor": []string{"self"}})
	if isErr || strings.Contains(text, "oce-by-broad") || !strings.Contains(text, "oce-by-alpha") {
		t.Fatalf("exclude self broken: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"exclude_actor": []string{"not-a-uuid"}}); !isErr || !strings.Contains(text, "invalid_exclude_actor") {
		t.Fatalf("malformed exclusion not rejected: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"exclude_actor": []string{"00000000-0000-0000-0000-000000000000"}}); !isErr || !strings.Contains(text, "invalid_exclude_actor") {
		t.Fatalf("nil exclusion not rejected: err=%t %s", isErr, text)
	}
}

func TestMCPFeedReadCursorIdentityAndLossProofReconnectIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	s := fixture.server(t, fixture.broad.Secret)
	exclude := []string{fixture.actorB.Token.ID.String()}

	// Bootstrap a filtered watcher cursor at head.
	isErr, text := feedReadForTest(t, s, map[string]any{"wait": "0s", "exclude_actor": exclude})
	if isErr {
		t.Fatalf("filtered bootstrap errored: %s", text)
	}
	cursor := nextCursorFromText(t, text)

	// Identity mismatches fail closed in every direction.
	if isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": cursor}); !isErr || !strings.Contains(text, "cursor_filter_mismatch") {
		t.Fatalf("filtered cursor on plain read not rejected: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": cursor, "exclude_actor": []string{"self"}}); !isErr || !strings.Contains(text, "cursor_filter_mismatch") {
		t.Fatalf("cross-filter cursor not rejected: err=%t %s", isErr, text)
	}
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s"})
	if isErr {
		t.Fatalf("plain bootstrap errored: %s", text)
	}
	plainCursor := nextCursorFromText(t, text)
	if isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": plainCursor, "exclude_actor": exclude}); !isErr || !strings.Contains(text, "cursor_filter_mismatch") {
		t.Fatalf("plain cursor on filtered read not rejected: err=%t %s", isErr, text)
	}

	// Loss-proof reconnect: events land while the watcher is disconnected;
	// resuming with the same identity misses nothing it is entitled to see.
	fixture.note(t, fixture.itemA, "oce-missed-one", fixture.actorA.Token)
	fixture.note(t, fixture.itemB, "oce-excluded-noise", fixture.actorB.Token)
	fixture.note(t, fixture.itemA, "oce-missed-two", fixture.root.Token)
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": cursor, "exclude_actor": exclude})
	if isErr {
		t.Fatalf("same-identity resume errored: %s", text)
	}
	for _, missed := range []string{"oce-missed-one", "oce-missed-two"} {
		if !strings.Contains(text, missed) {
			t.Fatalf("reconnect lost %q: %s", missed, text)
		}
	}
	if strings.Contains(text, "oce-excluded-noise") {
		t.Fatalf("reconnect leaked excluded traffic: %s", text)
	}

	// Continuity: the resumed page's cursor carries the identity forward.
	next := nextCursorFromText(t, text)
	fixture.note(t, fixture.itemA, "oce-missed-three", fixture.root.Token)
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": next, "exclude_actor": exclude})
	if isErr || !strings.Contains(text, "oce-missed-three") || strings.Contains(text, "oce-missed-two") {
		t.Fatalf("cursor chain continuity broken: err=%t %s", isErr, text)
	}
}
