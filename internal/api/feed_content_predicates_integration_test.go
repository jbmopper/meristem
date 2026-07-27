package api

// REST wiring for the content predicates (kind / exclude_kind / actor /
// work_item / work_item_tree query params). The predicate semantics
// themselves are pinned by internal/feed tests; these tests pin the
// transport contract: param parsing fails closed, the normalized filter is
// applied to snapshot, page, and SSE reads, cursors are bound to the filter
// identity, and the assigned lane's directed-signal protection survives kind
// exclusion at this surface.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/workitems"
)

func feedItemsForTest(t *testing.T, rec *httptest.ResponseRecorder) []feedItemProbe {
	t.Helper()
	var envelope struct {
		Items []feedItemProbe `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode feed response: %v body=%s", err, rec.Body.String())
	}
	return envelope.Items
}

type feedItemProbe struct {
	Kind      string          `json:"kind"`
	SubjectID string          `json:"subject_id"`
	Payload   json.RawMessage `json:"payload"`
}

func TestFeedContentPredicateParamsIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "content-predicates-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad reader: %v", err)
	}

	appendAssignedFeedNote(t, fixture, fixture.assignedA.ID, "kind-filter-note-a", uuid.Nil)
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedB.ID, "agent.assigned_feed_test",
		map[string]any{"marker": "actor-filter-note-b"}, fixture.actorB.Token); err != nil {
		t.Fatalf("append as actor B: %v", err)
	}

	// kind include: only the requested event kind comes back.
	rec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&kind=work_item.event_appended", broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	items := feedItemsForTest(t, rec)
	if len(items) == 0 {
		t.Fatalf("kind filter returned no items")
	}
	for _, item := range items {
		if item.Kind != "work_item.event_appended" {
			t.Fatalf("kind filter leaked %q", item.Kind)
		}
	}

	// exclude_kind: the excluded kind is gone, others remain.
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&exclude_kind=work_item.event_appended", broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	items = feedItemsForTest(t, rec)
	if len(items) == 0 {
		t.Fatalf("exclude_kind filter returned no items")
	}
	for _, item := range items {
		if item.Kind == "work_item.event_appended" {
			t.Fatalf("exclude_kind leaked the excluded kind")
		}
	}

	// actor: only events authored by the named token.
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&actor="+fixture.actorB.Token.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "actor-filter-note-b") {
		t.Fatalf("actor filter omitted the author's event: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "kind-filter-note-a") {
		t.Fatalf("actor filter leaked another author's event")
	}

	// Repeated actor params are a UNION, not an AND that selects nothing:
	// both authors' events come back, other authors' stay out.
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedA.ID, "agent.assigned_feed_test",
		map[string]any{"marker": "actor-union-note-a"}, fixture.actorA.Token); err != nil {
		t.Fatalf("append as actor A: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&actor="+fixture.actorA.Token.ID.String()+"&actor="+fixture.actorB.Token.ID.String(),
		broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, visible := range []string{"actor-union-note-a", "actor-filter-note-b"} {
		if !strings.Contains(rec.Body.String(), visible) {
			t.Fatalf("actor union omitted %q: %s", visible, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), "kind-filter-note-a") {
		t.Fatalf("actor union leaked a root-authored event")
	}

	// work_item: exact anchoring; work_item_tree: subtree anchoring.
	appendAssignedFeedNote(t, fixture, fixture.outside.ID, "outside-tree-note", uuid.Nil)
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&work_item="+fixture.assignedA.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "kind-filter-note-a") {
		t.Fatalf("work_item filter omitted the anchored event")
	}
	for _, leaked := range []string{"actor-filter-note-b", "outside-tree-note"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("work_item filter leaked %q", leaked)
		}
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&work_item_tree="+fixture.tree.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, visible := range []string{"kind-filter-note-a", "actor-filter-note-b"} {
		if !strings.Contains(rec.Body.String(), visible) {
			t.Fatalf("work_item_tree filter omitted %q", visible)
		}
	}
	if strings.Contains(rec.Body.String(), "outside-tree-note") {
		t.Fatalf("work_item_tree filter leaked an outside-tree event")
	}

	// Fail-closed param validation, on the snapshot and the stream alike.
	for _, tc := range []struct {
		query string
		code  string
	}{
		{"kind=not.a.known.kind", "invalid_feed_predicate"},
		{"exclude_kind=", "invalid_feed_predicate"},
		{"actor=not-a-uuid", "invalid_feed_actor"},
		{"actor=00000000-0000-0000-0000-000000000000", "invalid_feed_actor"},
		{"work_item=not-a-uuid", "invalid_feed_work_item"},
		{"work_item_tree=not-a-uuid", "invalid_feed_work_item"},
		{"work_item=" + fixture.tree.ID.String() + "&work_item=" + fixture.assignedA.ID.String(), "invalid_feed_work_item"},
	} {
		rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?"+tc.query, broad.Secret, "", nil)
		assertRESTStatus(t, rec, http.StatusBadRequest)
		assertErrorCode(t, rec, tc.code)

		streamRec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed/stream?"+tc.query, broad.Secret, "", nil)
		assertRESTStatus(t, streamRec, http.StatusBadRequest)
		assertErrorCode(t, streamRec, tc.code)
	}
}

func TestFeedContentFilteredCursorIdentityIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "content-cursor-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad reader: %v", err)
	}

	page := func(query string) (*httptest.ResponseRecorder, string) {
		rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?wait=0s&"+query, broad.Secret, "", nil)
		var decoded struct {
			NextCursor string `json:"next_cursor"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &decoded)
		return rec, decoded.NextCursor
	}

	rec, cursor := page("kind=work_item.event_appended")
	assertRESTStatus(t, rec, http.StatusOK)
	if cursor == "" {
		t.Fatalf("filtered page returned no cursor: %s", rec.Body.String())
	}

	// Same filter: the cursor resumes.
	rec, _ = page("kind=work_item.event_appended&cursor=" + cursor)
	assertRESTStatus(t, rec, http.StatusOK)

	// Any change to the content filter is a different identity: fail closed.
	for _, mismatched := range []string{
		"cursor=" + cursor,
		"kind=work_item.created&cursor=" + cursor,
		"kind=work_item.event_appended&exclude_actor=self&cursor=" + cursor,
		"kind=work_item.event_appended&work_item_tree=" + fixture.tree.ID.String() + "&cursor=" + cursor,
	} {
		rec, _ = page(mismatched)
		assertRESTStatus(t, rec, http.StatusBadRequest)
		assertErrorCode(t, rec, "cursor_filter_mismatch")
	}
}

func TestFeedStreamContentFilterDeliversAndOmitsIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()

	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "content-stream-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad reader: %v", err)
	}

	streamCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frames := make(chan sseFrameForTest, 16)
	ready := make(chan error, 1)
	go consumeSSEForTestReady(t, streamCtx,
		httpServer.URL+"/v1/feed/stream?kind=work_item.event_appended&actor="+fixture.actorB.Token.ID.String(),
		broad.Secret, "", ready, frames)
	if err := <-ready; err != nil {
		t.Fatalf("open filtered stream: %v", err)
	}

	// A non-matching author, then a non-matching kind, then the match. Only
	// the match may arrive; receiving it proves the earlier events were
	// filtered (SSE delivers in seq order).
	appendAssignedFeedNote(t, fixture, fixture.assignedA.ID, "stream-wrong-author", uuid.Nil)
	if _, err := fixture.work.Create(fixture.ctx, workitems.CreateInput{Title: "stream-wrong-kind", Actor: fixture.root.Token}); err != nil {
		t.Fatalf("create decoy item: %v", err)
	}
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedB.ID, "agent.assigned_feed_test",
		map[string]any{"marker": "stream-match"}, fixture.actorB.Token); err != nil {
		t.Fatalf("append matching event: %v", err)
	}

	select {
	case frame, ok := <-frames:
		if !ok {
			t.Fatalf("stream closed before delivering the matching event")
		}
		if !strings.Contains(frame.data, "stream-match") {
			t.Fatalf("first delivered frame is not the match: %s", frame.data)
		}
		if frame.id == "" {
			t.Fatalf("filtered frame carries no cursor id")
		}
	case <-streamCtx.Done():
		t.Fatalf("timed out waiting for the matching event")
	}

	select {
	case frame, ok := <-frames:
		if ok {
			t.Fatalf("filtered stream delivered an unexpected extra frame: %s", frame.data)
		}
	case <-time.After(500 * time.Millisecond):
	}
}

func TestFeedAssignedLaneKindExclusionKeepsWakesIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	// The reader excludes the very kind its wake arrives as. The directed
	// signal must survive: kind predicates are content filters and never
	// remove an event the assigned lane matched as addressed.
	appendAssignedFeedNote(t, fixture, fixture.addressed.ID, "excluded-kind-wake", fixture.actorA.Token.ID)
	appendAssignedFeedNote(t, fixture, fixture.assignedA.ID, "excluded-kind-quiet", uuid.Nil)

	rec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&scope=assigned&exclude_kind=work_item.event_appended", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "excluded-kind-wake") {
		t.Fatalf("kind exclusion swallowed an addressed wake: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "excluded-kind-quiet") {
		t.Fatalf("kind exclusion failed to drop a non-addressed event of the excluded kind")
	}
}
