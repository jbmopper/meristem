package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestAssignedFeedSnapshotAndAccessIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	appendAssignedFeedNote(t, fixture, fixture.addressed.ID, "addressed-in-tree", fixture.actorA.Token.ID)
	appendAssignedFeedNote(t, fixture, fixture.unassigned.ID, "unassigned-hidden", uuid.Nil)
	appendAssignedFeedNote(t, fixture, fixture.assignedA.ID, "assigned-a-visible", uuid.Nil)
	for i := 0; i < 3; i++ {
		appendAssignedFeedNote(t, fixture, fixture.assignedB.ID, "other-assignee-hidden-"+string(rune('a'+i)), uuid.Nil)
	}
	appendAssignedFeedNote(t, fixture, fixture.outside.ID, "addressed-outside-hidden", fixture.actorA.Token.ID)
	appendConflictingAddressEvent(t, fixture, fixture.assignedA.ID, "conflicting-address-hidden", fixture.actorA.Token.ID, fixture.actorB.Token.ID)

	// An assigned-only bearer that omits scope is normalized to the reducing
	// lane. Newer unauthorized/other-assignee traffic cannot consume limit=1.
	rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=1", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "assigned-a-visible") {
		t.Fatalf("limit scan did not reach newest authorized assigned event: %s", rec.Body.String())
	}
	for _, hidden := range []string{"other-assignee-hidden", "addressed-outside-hidden", "unassigned-hidden", "conflicting-address-hidden"} {
		if strings.Contains(rec.Body.String(), hidden) {
			t.Fatalf("assigned snapshot leaked %q: %s", hidden, rec.Body.String())
		}
	}

	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, visible := range []string{"assigned-a-visible", "addressed-in-tree"} {
		if !strings.Contains(rec.Body.String(), visible) {
			t.Errorf("assigned snapshot omitted %q: %s", visible, rec.Body.String())
		}
	}
	for _, hidden := range []string{"other-assignee-hidden", "addressed-outside-hidden", "unassigned-hidden", "conflicting-address-hidden"} {
		if strings.Contains(rec.Body.String(), hidden) {
			t.Errorf("assigned snapshot leaked %q: %s", hidden, rec.Body.String())
		}
	}

	// Full feed.read authority may explicitly request the reducing preset.
	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "assigned-feed-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad reader: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned&limit=20", broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)

	noFeed, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "assigned-feed-denied", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead, "work_items.tree:" + fixture.tree.ID.String()}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create denied reader: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned", noFeed.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusForbidden)
	assertErrorCode(t, rec, "insufficient_scope")

	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=future", fixture.root.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, "invalid_feed_scope")
}

func TestAssignedFeedListenForDelegatedLaneIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	appendAssignedFeedNote(t, fixture, fixture.assignedA.ID, "listen-for-a-hidden", uuid.Nil)
	appendAssignedFeedNote(t, fixture, fixture.assignedB.ID, "listen-for-b-assigned", uuid.Nil)
	appendAssignedFeedNote(t, fixture, fixture.addressed.ID, "listen-for-b-addressed", fixture.actorB.Token.ID)
	appendAssignedFeedNote(t, fixture, fixture.outside.ID, "listen-for-b-outside", fixture.actorB.Token.ID)

	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "listen-for-broad", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad listener: %v", err)
	}
	target := fixture.actorB.Token.ID.String()
	rec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&listen_for="+target, broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, visible := range []string{"listen-for-b-assigned", "listen-for-b-addressed", "listen-for-b-outside"} {
		if !strings.Contains(rec.Body.String(), visible) {
			t.Errorf("broad target lane omitted %q: %s", visible, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), "listen-for-a-hidden") {
		t.Errorf("target lane leaked another assignee: %s", rec.Body.String())
	}

	delegated, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "listen-for-delegated", Source: domain.SourceAgent,
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
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?scope=assigned&listen_for="+target+"&limit=100", delegated.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, visible := range []string{"listen-for-b-assigned", "listen-for-b-addressed"} {
		if !strings.Contains(rec.Body.String(), visible) {
			t.Errorf("delegated target lane omitted %q: %s", visible, rec.Body.String())
		}
	}
	for _, hidden := range []string{"listen-for-a-hidden", "listen-for-b-outside"} {
		if strings.Contains(rec.Body.String(), hidden) {
			t.Errorf("delegated target lane leaked %q: %s", hidden, rec.Body.String())
		}
	}

	denied, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "listen-for-denied", Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			access.WorkItemTreeScope(fixture.tree.ID),
		},
		Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create denied listener: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?listen_for="+target, denied.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusForbidden)
	assertErrorCode(t, rec, "insufficient_scope")

	for _, malformed := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		rec = doREST(t, fixture.server.Handler(), http.MethodGet,
			"/v1/feed?listen_for="+malformed, broad.Secret, "", nil)
		assertRESTStatus(t, rec, http.StatusBadRequest)
		assertErrorCode(t, rec, "invalid_listen_for")
	}

	cursorRec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?wait=0s&listen_for="+target, broad.Secret, "", nil)
	assertRESTStatus(t, cursorRec, http.StatusOK)
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(cursorRec.Body.Bytes(), &page); err != nil || page.NextCursor == "" {
		t.Fatalf("decode target cursor: err=%v body=%s", err, cursorRec.Body.String())
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?wait=0s&cursor="+page.NextCursor+"&listen_for="+fixture.actorA.Token.ID.String(),
		broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusBadRequest)
	assertErrorCode(t, rec, "cursor_filter_mismatch")
}

func TestAssignedFeedLongPollIgnoresReducedTrafficIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	cursor := fetchHeadCursor(t, fixture.server.Handler(), fixture.actorA.Secret)

	type result struct {
		code int
		body string
	}
	done := make(chan result, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/feed?scope=assigned&cursor="+cursor+"&wait=4s", nil)
		req.Header.Set("Authorization", "Bearer "+fixture.actorA.Secret)
		rec := httptest.NewRecorder()
		fixture.server.Handler().ServeHTTP(rec, req)
		done <- result{code: rec.Code, body: rec.Body.String()}
	}()

	time.Sleep(300 * time.Millisecond)
	appendAssignedFeedNote(t, fixture, fixture.assignedB.ID, "long-poll-other-assignee", uuid.Nil)
	appendAssignedFeedNote(t, fixture, fixture.outside.ID, "long-poll-outside-address", fixture.actorA.Token.ID)
	select {
	case got := <-done:
		t.Fatalf("reduced traffic ended long-poll early: status=%d body=%s", got.code, got.body)
	case <-time.After(600 * time.Millisecond):
	}

	appendAssignedFeedNote(t, fixture, fixture.addressed.ID, "long-poll-addressed-wakeup", fixture.actorA.Token.ID)
	select {
	case got := <-done:
		if got.code != http.StatusOK || !strings.Contains(got.body, "long-poll-addressed-wakeup") {
			t.Fatalf("addressed wake result: status=%d body=%s", got.code, got.body)
		}
		for _, hidden := range []string{"long-poll-other-assignee", "long-poll-outside-address"} {
			if strings.Contains(got.body, hidden) {
				t.Errorf("wake page leaked %q: %s", hidden, got.body)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("assigned long-poll did not wake on explicit in-tree address")
	}
}

func TestAssignedFeedSSEResumeAndExactAssignmentControlsIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	cursor := fetchHeadCursor(t, fixture.server.Handler(), fixture.actorA.Secret)

	claimA, err := fixture.work.Claim(fixture.ctx, fixture.sseItem.ID, fixture.actorA.Token)
	if err != nil {
		t.Fatalf("claim SSE item for A: %v", err)
	}
	if _, err := fixture.work.Yield(fixture.ctx, fixture.sseItem.ID, claimA.AssignmentEventID, fixture.actorA.Token); err != nil {
		t.Fatalf("yield SSE item for A: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, fixture.sseItem.ID, fixture.actorB.Token); err != nil {
		t.Fatalf("claim SSE item for B: %v", err)
	}

	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()
	streamCtx, cancel := context.WithTimeout(fixture.ctx, 4*time.Second)
	defer cancel()
	frames := make(chan sseFrameForTest, 16)
	go consumeSSEForTest(t, streamCtx, httpServer.URL+"/v1/feed/stream?scope=assigned", fixture.actorA.Secret, cursor, frames)

	var sawAssigned, sawReleased bool
	deadline := time.After(2500 * time.Millisecond)
	for !(sawAssigned && sawReleased) {
		select {
		case frame, ok := <-frames:
			if !ok {
				t.Fatal("assigned SSE closed before replaying A controls")
			}
			if strings.Contains(frame.data, fixture.actorB.Token.ID.String()) &&
				(strings.Contains(frame.data, domain.EventWorkItemAssigned) || strings.Contains(frame.data, domain.EventWorkItemAssignmentReleased)) {
				t.Fatalf("A stream received B assignment control: %s", frame.data)
			}
			if strings.Contains(frame.data, fixture.actorA.Token.ID.String()) && strings.Contains(frame.data, domain.EventWorkItemAssigned) {
				sawAssigned = true
			}
			if strings.Contains(frame.data, fixture.actorA.Token.ID.String()) && strings.Contains(frame.data, domain.EventWorkItemAssignmentReleased) {
				sawReleased = true
			}
		case <-deadline:
			t.Fatalf("assigned SSE resume missing controls: assigned=%t released=%t", sawAssigned, sawReleased)
		}
	}
	cancel()

	// B now holds the item. Its snapshot may see ordinary history, but never
	// A's stale assignment/release controls through the current-holder clause.
	rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", fixture.actorB.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	var body struct {
		Items []struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode B feed: %v", err)
	}
	for _, item := range body.Items {
		if item.Kind != domain.EventWorkItemAssigned && item.Kind != domain.EventWorkItemAssignmentReleased {
			continue
		}
		var payload struct {
			AssigneeTokenID uuid.UUID `json:"assignee_token_id"`
		}
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			t.Fatalf("decode assignment control: %v", err)
		}
		if payload.AssigneeTokenID != fixture.actorB.Token.ID {
			t.Fatalf("B feed received stale control for %s: kind=%s payload=%s", payload.AssigneeTokenID, item.Kind, item.Payload)
		}
	}
}

func TestAssignedFeedSSEIdleWakeAndScannedResumeIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()

	streamCtx, cancel := context.WithTimeout(fixture.ctx, 5*time.Second)
	frames := make(chan sseFrameForTest, 16)
	ready := make(chan error, 1)
	go consumeSSEForTestReady(t, streamCtx, httpServer.URL+"/v1/feed/stream?scope=assigned", fixture.actorA.Secret, "", ready, frames)
	select {
	case err := <-ready:
		if err != nil {
			cancel()
			t.Fatalf("open idle SSE stream: %v", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("idle SSE stream did not commit response headers")
	}

	appendAssignedFeedNote(t, fixture, fixture.assignedB.ID, "idle-sse-other-assignee", uuid.Nil)
	appendAssignedFeedNote(t, fixture, fixture.outside.ID, "idle-sse-outside-address", fixture.actorA.Token.ID)
	select {
	case frame := <-frames:
		cancel()
		t.Fatalf("reduced traffic emitted an SSE frame: %s", frame.data)
	case <-time.After(500 * time.Millisecond):
	}

	if _, err := fixture.work.Claim(fixture.ctx, fixture.sseItem.ID, fixture.actorA.Token); err != nil {
		cancel()
		t.Fatalf("claim idle SSE item: %v", err)
	}
	var assignmentFrame sseFrameForTest
	select {
	case assignmentFrame = <-frames:
		if assignmentFrame.id == "" || !strings.Contains(assignmentFrame.data, domain.EventWorkItemAssigned) ||
			!strings.Contains(assignmentFrame.data, fixture.actorA.Token.ID.String()) {
			cancel()
			t.Fatalf("idle SSE did not wake on exact assignment: %+v", assignmentFrame)
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("idle SSE did not wake on assignment")
	}
	cancel()

	appendAssignedFeedNote(t, fixture, fixture.sseItem.ID, "idle-sse-resumed-note", uuid.Nil)
	resumeCtx, resumeCancel := context.WithTimeout(fixture.ctx, 4*time.Second)
	defer resumeCancel()
	resumed := make(chan sseFrameForTest, 8)
	go consumeSSEForTest(t, resumeCtx, httpServer.URL+"/v1/feed/stream?scope=assigned", fixture.actorA.Secret, assignmentFrame.id, resumed)
	select {
	case frame := <-resumed:
		if !strings.Contains(frame.data, "idle-sse-resumed-note") {
			t.Fatalf("resumed SSE returned wrong frame: %s", frame.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("assigned SSE did not resume after scanned/reduced traffic")
	}
}

func TestAssignedFeedExpiredUnsweptTruthAndExpiryControlIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	registryService := registry.NewService(fixture.pool, fixture.writer)
	if _, _, err := registryService.DefineTropism(fixture.ctx, fixture.actorA.Token, registry.DefineTropismInput{
		Name: "assigned-feed-one-second-checklist", Version: 1,
		Reducer: registry.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:  json.RawMessage(`{"budget":{"max_attempts":1,"escalation":"hand_to_human"}}`),
	}); err != nil {
		t.Fatalf("define short-lease tropism: %v", err)
	}
	if _, _, err := registryService.DefineCultivar(fixture.ctx, fixture.actorA.Token, registry.DefineCultivarInput{
		Name: "assigned-feed-one-second", Version: 1,
		Tropism: registry.TropismRef{Name: "assigned-feed-one-second-checklist", Version: 1},
		Profile: registry.Profile{Briefing: "briefings/assigned-feed-test.md", ScopesTemplate: []string{"work_items.read", "work_items.write"}},
		Xylem:   registry.Xylem{MaxAttempts: 1, MaxWallSeconds: 1, MaxDepth: 0},
		Phloem:  "projection:work-item-brief", Description: "short lease for assigned feed integration",
	}); err != nil {
		t.Fatalf("define short-lease cultivar: %v", err)
	}
	item, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
		Title: "expired-unswept", Cultivar: "assigned-feed-one-second@1", Actor: fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn short-lease item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, item.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim short-lease item: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	appendAssignedFeedNote(t, fixture, item.ID, "expired-but-unswept-visible", uuid.Nil)
	rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "expired-but-unswept-visible") {
		t.Fatalf("read path clock-expired event-sourced assignment: %s", rec.Body.String())
	}

	systemActor, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "assigned-feed-expiry-system", Source: domain.SourceSystem, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create expiry actor: %v", err)
	}
	expired, err := fixture.work.ExpireAssignment(fixture.ctx, item.ID, systemActor.Token)
	if err != nil || !expired {
		t.Fatalf("expire assignment: expired=%t err=%v", expired, err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), domain.EventWorkItemAssignmentReleased) ||
		!strings.Contains(rec.Body.String(), fixture.actorA.Token.ID.String()) ||
		!strings.Contains(rec.Body.String(), string(domain.AssignmentReleaseExpired)) {
		t.Fatalf("expired release was not delivered to exact former assignee: %s", rec.Body.String())
	}
}

func TestAssignedFeedTerminalHandbackIsExactAndTreeScopedIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	spawnRunning := func(title string) domain.WorkItem {
		t.Helper()
		item, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
			Title: title, State: domain.WorkItemRunning,
			SuggestedConvergenceChecks: []string{"terminal handback is visible"},
			HumanReviewStatus:          domain.HumanReviewWavedThrough,
			Actor:                      fixture.root.Token,
		})
		if err != nil {
			t.Fatalf("spawn %s: %v", title, err)
		}
		return item
	}
	coordinator, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "terminal-handback-coordinator", Source: domain.SourceHuman,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			"work_items.tree:" + fixture.tree.ID.String(),
		},
		Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}

	handback := spawnRunning("terminal-handback-other-actor")
	if _, err := fixture.work.Claim(fixture.ctx, handback.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim handback item: %v", err)
	}
	appendAssignedFeedNote(t, fixture, handback.ID, "terminal-handback-prior-history", uuid.Nil)
	path := "/v1/work-items/" + handback.ID.String() + "/transition"
	body := []byte(`{"to":"done","reason":"terminal-handback-other-actor"}`)
	first := doREST(t, fixture.server.Handler(), http.MethodPost, path, coordinator.Secret, "terminal-handback-transition", body)
	assertRESTStatus(t, first, http.StatusOK)
	replay := doREST(t, fixture.server.Handler(), http.MethodPost, path, coordinator.Secret, "terminal-handback-transition", body)
	assertRESTStatus(t, replay, http.StatusOK)
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("terminal handback retry was not replayed: headers=%v", replay.Header())
	}

	var transitions int
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT count(*) FROM events
		WHERE subject_id=$1 AND kind=$2 AND payload->>'reason'=$3
	`, handback.ID, domain.EventWorkItemTransitioned, "terminal-handback-other-actor").Scan(&transitions); err != nil {
		t.Fatalf("count terminal transitions: %v", err)
	}
	if transitions != 1 {
		t.Fatalf("terminal handback transitions = %d, want 1", transitions)
	}
	var releases int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM events WHERE subject_id=$1 AND kind=$2`, handback.ID, domain.EventWorkItemAssignmentReleased).Scan(&releases); err != nil {
		t.Fatalf("count terminal releases: %v", err)
	}
	if releases != 0 {
		t.Fatalf("terminal handback emitted %d assignment releases, want 0", releases)
	}
	noOp := doREST(
		t,
		fixture.server.Handler(),
		http.MethodPost,
		path,
		coordinator.Secret,
		"terminal-handback-later-noop",
		[]byte(`{"to":"done","reason":"terminal-handback-later-noop"}`),
	)
	assertRESTStatus(t, noOp, http.StatusOK)

	assignedA := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned&limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, assignedA, http.StatusOK)
	if !strings.Contains(assignedA.Body.String(), "terminal-handback-other-actor") {
		t.Fatalf("former holder feed omitted other-actor terminal handback: %s", assignedA.Body.String())
	}
	for _, widened := range []string{"terminal-handback-prior-history", "terminal-handback-later-noop"} {
		if strings.Contains(assignedA.Body.String(), widened) {
			t.Fatalf("terminal address widened to %q: %s", widened, assignedA.Body.String())
		}
	}
	assignedB := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned&limit=100", fixture.actorB.Secret, "", nil)
	assertRESTStatus(t, assignedB, http.StatusOK)
	for _, hidden := range []string{"terminal-handback-other-actor", "terminal-handback-later-noop"} {
		if strings.Contains(assignedB.Body.String(), hidden) {
			t.Fatalf("other token received %q: %s", hidden, assignedB.Body.String())
		}
	}

	self := spawnRunning("terminal-handback-same-actor-item")
	if _, err := fixture.work.Claim(fixture.ctx, self.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim same-actor item: %v", err)
	}
	if _, err := fixture.work.Transition(fixture.ctx, self.ID, domain.WorkItemDone, "terminal-handback-same-actor", fixture.actorA.Token); err != nil {
		t.Fatalf("same actor terminalize: %v", err)
	}
	assignedA = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned&limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, assignedA, http.StatusOK)
	if !strings.Contains(assignedA.Body.String(), "terminal-handback-same-actor") {
		t.Fatalf("same holder feed omitted its terminal handback: %s", assignedA.Body.String())
	}

	outside, err := fixture.work.Create(fixture.ctx, workitems.CreateInput{
		Title: "terminal-handback-outside", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"outside remains hidden"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create outside item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, outside.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim outside item fixture: %v", err)
	}
	if _, err := fixture.work.Transition(fixture.ctx, outside.ID, domain.WorkItemDone, "terminal-handback-outside-hidden", fixture.root.Token); err != nil {
		t.Fatalf("terminalize outside item: %v", err)
	}
	if _, err := fixture.work.Transition(fixture.ctx, fixture.unassigned.ID, domain.WorkItemCanceled, "terminal-handback-unassigned-hidden", fixture.root.Token); err != nil {
		t.Fatalf("terminalize unassigned item: %v", err)
	}
	assignedA = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?scope=assigned&limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, assignedA, http.StatusOK)
	for _, hidden := range []string{"terminal-handback-outside-hidden", "terminal-handback-unassigned-hidden"} {
		if strings.Contains(assignedA.Body.String(), hidden) {
			t.Fatalf("former holder feed leaked %q: %s", hidden, assignedA.Body.String())
		}
	}
}

func TestAssignedFeedIncompleteScopeFailsBeforeSSEHeadersIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	incomplete, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "assigned-feed-no-tree", Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedReadAssigned}, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create incomplete assigned reader: %v", err)
	}
	rec := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed", incomplete.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusForbidden)
	assertErrorCode(t, rec, "insufficient_scope")

	req := httptest.NewRequest(http.MethodGet, "/v1/feed/stream?scope=assigned", nil)
	req.Header.Set("Authorization", "Bearer "+incomplete.Secret)
	streamRec := httptest.NewRecorder()
	fixture.server.Handler().ServeHTTP(streamRec, req)
	assertRESTStatus(t, streamRec, http.StatusForbidden)
	assertErrorCode(t, streamRec, "insufficient_scope")
	if strings.HasPrefix(streamRec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("incomplete scope committed SSE headers: %v", streamRec.Header())
	}
}

type assignedFeedFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	writer     *events.Writer
	auth       *auth.Service
	work       *workitems.Service
	server     *Server
	root       auth.CreateTokenResult
	actorA     auth.CreateTokenResult
	actorB     auth.CreateTokenResult
	tree       domain.WorkItem
	assignedA  domain.WorkItem
	assignedB  domain.WorkItem
	unassigned domain.WorkItem
	addressed  domain.WorkItem
	sseItem    domain.WorkItem
	outside    domain.WorkItem
}

func newAssignedFeedFixture(t *testing.T) assignedFeedFixture {
	t.Helper()
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assigned-feed-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	workSvc := workitems.NewService(pool, writer)
	tree, err := workSvc.Create(ctx, workitems.CreateInput{Title: "assigned-feed-tree", Actor: root.Token})
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
	assignedA := spawn("assigned-a")
	assignedB := spawn("assigned-b")
	unassigned := spawn("unassigned")
	addressed := spawn("addressed")
	sseItem := spawn("sse-item")
	outside, err := workSvc.Create(ctx, workitems.CreateInput{Title: "outside-tree", Actor: root.Token})
	if err != nil {
		t.Fatalf("create outside: %v", err)
	}
	actorScopes := func(rootID uuid.UUID) []string {
		return []string{access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned, "work_items.tree:" + rootID.String()}
	}
	actorA, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assigned-feed-a", Source: domain.SourceAgent, Scopes: actorScopes(tree.ID), Actor: &root.Token})
	if err != nil {
		t.Fatalf("create actor A: %v", err)
	}
	actorB, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "assigned-feed-b", Source: domain.SourceAgent, Scopes: actorScopes(tree.ID), Actor: &root.Token})
	if err != nil {
		t.Fatalf("create actor B: %v", err)
	}
	if _, err := workSvc.Claim(ctx, assignedA.ID, actorA.Token); err != nil {
		t.Fatalf("claim assigned A: %v", err)
	}
	if _, err := workSvc.Claim(ctx, assignedB.ID, actorB.Token); err != nil {
		t.Fatalf("claim assigned B: %v", err)
	}
	return assignedFeedFixture{
		ctx: ctx, pool: pool, writer: writer, auth: authSvc, work: workSvc, server: New(pool, nil), root: root,
		actorA: actorA, actorB: actorB, tree: tree, assignedA: assignedA,
		assignedB: assignedB, unassigned: unassigned, addressed: addressed,
		sseItem: sseItem, outside: outside,
	}
}

func appendConflictingAddressEvent(t *testing.T, fixture assignedFeedFixture, id uuid.UUID, marker string, topLevel, inner uuid.UUID) {
	t.Helper()
	tx, err := fixture.pool.Begin(fixture.ctx)
	if err != nil {
		t.Fatalf("begin conflicting address event: %v", err)
	}
	defer func() { _ = tx.Rollback(fixture.ctx) }()
	if _, _, err := fixture.writer.Append(fixture.ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   id,
		Kind:        domain.EventWorkItemEventAppended,
		Source:      domain.SourceAgent,
		ActorTokenID: func() *uuid.UUID {
			id := fixture.root.Token.ID
			return &id
		}(),
		Payload: map[string]any{
			"inner_kind":         "agent.conflicting_address_test",
			"addressee_token_id": topLevel,
			"inner": map[string]any{
				"marker":             marker,
				"addressee_token_id": inner,
			},
		},
	}); err != nil {
		t.Fatalf("append conflicting address event: %v", err)
	}
	if err := tx.Commit(fixture.ctx); err != nil {
		t.Fatalf("commit conflicting address event: %v", err)
	}
}

func appendAssignedFeedNote(t *testing.T, fixture assignedFeedFixture, id uuid.UUID, marker string, addressee uuid.UUID) {
	t.Helper()
	payload := map[string]any{"marker": marker}
	if addressee != uuid.Nil {
		payload["addressee_token_id"] = addressee
	}
	if err := fixture.work.AppendEvent(fixture.ctx, id, "agent.assigned_feed_test", payload, fixture.root.Token); err != nil {
		t.Fatalf("append %s: %v", marker, err)
	}
}
