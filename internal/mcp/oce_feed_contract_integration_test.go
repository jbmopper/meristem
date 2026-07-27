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

func TestMCPFeedReadListenForParityIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	fixture.note(t, fixture.itemA, "oce-listen-a-hidden", fixture.root.Token)
	fixture.note(t, fixture.itemB, "oce-listen-b-visible", fixture.root.Token)
	target := fixture.actorB.Token.ID.String()

	sBroad := fixture.server(t, fixture.broad.Secret)
	isErr, text := feedReadForTest(t, sBroad, map[string]any{
		"limit": 100, "listen_for": target,
	})
	if isErr || !strings.Contains(text, "oce-listen-b-visible") || strings.Contains(text, "oce-listen-a-hidden") {
		t.Fatalf("broad listen_for lane broken: err=%t %s", isErr, text)
	}

	delegated, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "oce-listen-delegated", Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			access.WorkItemTreeScope(fixture.tree.ID),
			access.FeedListenForScope(fixture.actorB.Token.ID),
		},
		Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create delegated listener: %v", err)
	}
	sDelegated := fixture.server(t, delegated.Secret)
	isErr, text = feedReadForTest(t, sDelegated, map[string]any{
		"limit": 100, "scope": "assigned", "listen_for": target,
	})
	if isErr || !strings.Contains(text, "oce-listen-b-visible") || strings.Contains(text, "oce-listen-a-hidden") {
		t.Fatalf("delegated listen_for lane broken: err=%t %s", isErr, text)
	}

	sDenied := fixture.server(t, fixture.actorA.Secret)
	if isErr, text = feedReadForTest(t, sDenied, map[string]any{"listen_for": target}); !isErr || !strings.Contains(text, "insufficient_scope") {
		t.Fatalf("undelegated listen_for was not denied: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, sBroad, map[string]any{"listen_for": "not-a-uuid"}); !isErr || !strings.Contains(text, "invalid_listen_for") {
		t.Fatalf("malformed listen_for was not rejected: err=%t %s", isErr, text)
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

func TestMCPFeedReadContentPredicateParityIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	fixture.note(t, fixture.itemA, "oce-content-by-alpha", fixture.actorA.Token)
	fixture.note(t, fixture.itemB, "oce-content-by-beta", fixture.actorB.Token)
	outside, err := fixture.work.Create(fixture.ctx, workitems.CreateInput{Title: "oce-content-outside", Actor: fixture.root.Token})
	if err != nil {
		t.Fatalf("create outside item: %v", err)
	}
	fixture.note(t, outside, "oce-content-outside-note", fixture.root.Token)
	s := fixture.server(t, fixture.broad.Secret)

	// kinds: only the requested event kind. The tree's work_item.created
	// events exist, so their absence under the kinds filter is meaningful.
	isErr, text := feedReadForTest(t, s, map[string]any{"limit": 100, "kinds": []string{"work_item.event_appended"}})
	if isErr || !strings.Contains(text, "oce-content-by-alpha") || strings.Contains(text, "work_item.created") {
		t.Fatalf("kinds filter broken: err=%t %s", isErr, text)
	}

	// exclude_kinds: the excluded kind is gone.
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100, "exclude_kinds": []string{"work_item.event_appended"}})
	if isErr || strings.Contains(text, "oce-content-by-alpha") || !strings.Contains(text, "work_item.created") {
		t.Fatalf("exclude_kinds filter broken: err=%t %s", isErr, text)
	}

	// actors: only the named author's events.
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100, "actors": []string{fixture.actorB.Token.ID.String()}})
	if isErr || !strings.Contains(text, "oce-content-by-beta") || strings.Contains(text, "oce-content-by-alpha") {
		t.Fatalf("actors filter broken: err=%t %s", isErr, text)
	}

	// Two actors are a UNION — REST parity for the repeated actor param.
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100,
		"actors": []string{fixture.actorA.Token.ID.String(), fixture.actorB.Token.ID.String()}})
	if isErr || !strings.Contains(text, "oce-content-by-alpha") || !strings.Contains(text, "oce-content-by-beta") {
		t.Fatalf("actors union broken: err=%t %s", isErr, text)
	}
	if strings.Contains(text, "oce-content-outside-note") {
		t.Fatalf("actors union leaked a root-authored event: %s", text)
	}

	// work_item_tree: subtree anchoring keeps tree events, drops outside.
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100, "work_item_tree": fixture.tree.ID.String()})
	if isErr || !strings.Contains(text, "oce-content-by-alpha") || strings.Contains(text, "oce-content-outside-note") {
		t.Fatalf("work_item_tree filter broken: err=%t %s", isErr, text)
	}

	// work_item: exact anchoring only.
	isErr, text = feedReadForTest(t, s, map[string]any{"limit": 100, "work_item": fixture.itemA.ID.String()})
	if isErr || !strings.Contains(text, "oce-content-by-alpha") || strings.Contains(text, "oce-content-by-beta") {
		t.Fatalf("work_item filter broken: err=%t %s", isErr, text)
	}

	// Fail-closed argument validation, same vocabulary authority as REST.
	if isErr, text = feedReadForTest(t, s, map[string]any{"kinds": []string{"not.a.known.kind"}}); !isErr || !strings.Contains(text, "invalid_filter") {
		t.Fatalf("unknown kind not rejected: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"actors": []string{"not-a-uuid"}}); !isErr || !strings.Contains(text, "invalid_feed_actor") {
		t.Fatalf("malformed actors entry not rejected: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"work_item": "not-a-uuid"}); !isErr || !strings.Contains(text, "invalid_feed_work_item") {
		t.Fatalf("malformed work_item not rejected: err=%t %s", isErr, text)
	}

	// The content filter participates in cursor identity: a cursor minted
	// under one kinds set fails closed under any other filter.
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "kinds": []string{"work_item.event_appended"}})
	if isErr {
		t.Fatalf("filtered page errored: %s", text)
	}
	cursor := nextCursorFromText(t, text)
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": cursor, "kinds": []string{"work_item.event_appended"}})
	if isErr {
		t.Fatalf("same-filter cursor resume errored: %s", text)
	}
	for _, mismatched := range []map[string]any{
		{"wait": "0s", "cursor": cursor},
		{"wait": "0s", "cursor": cursor, "kinds": []string{"work_item.created"}},
		{"wait": "0s", "cursor": cursor, "kinds": []string{"work_item.event_appended"}, "actors": []string{fixture.actorA.Token.ID.String()}},
	} {
		if isErr, text = feedReadForTest(t, s, mismatched); !isErr || !strings.Contains(text, "cursor_filter_mismatch") {
			t.Fatalf("mismatched filter identity not rejected: err=%t %s", isErr, text)
		}
	}
}

func TestMCPFeedReadBootstrapHeadParityIntegration(t *testing.T) {
	fixture := newOCEFixture(t)
	s := fixture.server(t, fixture.broad.Secret)

	// Bootstrap mints an identity-bound head cursor with no items.
	isErr, text := feedReadForTest(t, s, map[string]any{"bootstrap": "head", "kinds": []string{"work_item.event_appended"}})
	if isErr {
		t.Fatalf("bootstrap errored: %s", text)
	}
	var boot struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(text), &boot); err != nil || boot.NextCursor == "" {
		t.Fatalf("decode bootstrap: err=%v text=%s", err, text)
	}
	if len(boot.Items) != 0 {
		t.Fatalf("bootstrap must not return items, got %d", len(boot.Items))
	}

	// An event appended after the mint is delivered from the minted cursor.
	fixture.note(t, fixture.itemA, "oce-bootstrap-race-wake", fixture.root.Token)
	isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": boot.NextCursor, "kinds": []string{"work_item.event_appended"}})
	if isErr || !strings.Contains(text, "oce-bootstrap-race-wake") {
		t.Fatalf("event after bootstrap not delivered: err=%t %s", isErr, text)
	}

	// Identity binding and argument validation fail closed.
	if isErr, text = feedReadForTest(t, s, map[string]any{"wait": "0s", "cursor": boot.NextCursor}); !isErr || !strings.Contains(text, "cursor_filter_mismatch") {
		t.Fatalf("bootstrap cursor not identity-bound: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"bootstrap": "tail"}); !isErr || !strings.Contains(text, "invalid_bootstrap") {
		t.Fatalf("invalid bootstrap value not rejected: err=%t %s", isErr, text)
	}
	if isErr, text = feedReadForTest(t, s, map[string]any{"bootstrap": "head", "wait": "0s"}); !isErr || !strings.Contains(text, "invalid_bootstrap") {
		t.Fatalf("bootstrap+wait not rejected: err=%t %s", isErr, text)
	}
}
