package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/workitems"
)

func newQuietSelfBroadReader(t *testing.T, fixture assignedFeedFixture, name string) auth.CreateTokenResult {
	t.Helper()
	broad, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: name, Source: domain.SourceAgent,
		Scopes: []string{access.ScopeFeedRead, access.ScopeWorkItemsReadAll},
		Actor:  &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create broad reader: %v", err)
	}
	return broad
}

func appendQuietSelfNote(t *testing.T, fixture assignedFeedFixture, id, marker string, actor domain.Token) {
	t.Helper()
	item := fixture.assignedA
	switch id {
	case "assignedB":
		item = fixture.assignedB
	case "unassigned":
		item = fixture.unassigned
	}
	if err := fixture.work.AppendEvent(fixture.ctx, item.ID, "agent.quiet_self_test", map[string]any{"marker": marker}, actor); err != nil {
		t.Fatalf("append %s: %v", marker, err)
	}
}

func TestQuietSelfSnapshotExclusionIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	broad := newQuietSelfBroadReader(t, fixture, "quiet-self-broad")

	appendQuietSelfNote(t, fixture, "unassigned", "authored-by-actor-alpha", fixture.actorA.Token)
	appendQuietSelfNote(t, fixture, "unassigned", "authored-by-actor-beta", fixture.actorB.Token)
	appendQuietSelfNote(t, fixture, "unassigned", "authored-by-caller", broad.Token)

	baseline := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", broad.Secret, "", nil)
	assertRESTStatus(t, baseline, http.StatusOK)
	for _, marker := range []string{"authored-by-actor-alpha", "authored-by-actor-beta", "authored-by-caller"} {
		if !strings.Contains(baseline.Body.String(), marker) {
			t.Fatalf("baseline snapshot missing %q: %s", marker, baseline.Body.String())
		}
	}

	rec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&exclude_actor="+fixture.actorA.Token.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if strings.Contains(rec.Body.String(), "authored-by-actor-alpha") {
		t.Fatalf("excluded actor's event leaked: %s", rec.Body.String())
	}
	for _, marker := range []string{"authored-by-actor-beta", "authored-by-caller"} {
		if !strings.Contains(rec.Body.String(), marker) {
			t.Fatalf("exclusion of A removed %q: %s", marker, rec.Body.String())
		}
	}

	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100&exclude_actor=self", broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if strings.Contains(rec.Body.String(), "authored-by-caller") {
		t.Fatalf("exclude_actor=self leaked the caller's own event: %s", rec.Body.String())
	}
	for _, marker := range []string{"authored-by-actor-alpha", "authored-by-actor-beta"} {
		if !strings.Contains(rec.Body.String(), marker) {
			t.Fatalf("exclude_actor=self removed %q: %s", marker, rec.Body.String())
		}
	}

	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&exclude_actor="+fixture.actorA.Token.ID.String()+"&exclude_actor="+fixture.actorB.Token.ID.String(),
		broad.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, hidden := range []string{"authored-by-actor-alpha", "authored-by-actor-beta"} {
		if strings.Contains(rec.Body.String(), hidden) {
			t.Fatalf("stacked exclusion leaked %q: %s", hidden, rec.Body.String())
		}
	}
	if !strings.Contains(rec.Body.String(), "authored-by-caller") {
		t.Fatalf("stacked exclusion removed unrelated event: %s", rec.Body.String())
	}

	for _, malformed := range []string{"exclude_actor=not-a-uuid", "exclude_actor=", "exclude_actor=00000000-0000-0000-0000-000000000000"} {
		rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?"+malformed, broad.Secret, "", nil)
		assertRESTStatus(t, rec, http.StatusBadRequest)
		assertErrorCode(t, rec, "invalid_exclude_actor")
	}
}

func TestQuietSelfDirectedSignalsSurviveExclusionIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)

	// Expired-lease release: a system actor authors the release control that
	// is addressed to A. Excluding the system actor must not swallow it.
	registryService := registry.NewService(fixture.pool, fixture.writer)
	if _, _, err := registryService.DefineTropism(fixture.ctx, fixture.actorA.Token, registry.DefineTropismInput{
		Name: "quiet-self-one-second-checklist", Version: 1,
		Reducer: registry.ReducerRef{Identity: "all_pass_checklist", Version: 1},
		Params:  []byte(`{"budget":{"max_attempts":1,"escalation":"hand_to_human"}}`),
	}); err != nil {
		t.Fatalf("define short-lease tropism: %v", err)
	}
	if _, _, err := registryService.DefineCultivar(fixture.ctx, fixture.actorA.Token, registry.DefineCultivarInput{
		Name: "quiet-self-one-second", Version: 1,
		Tropism: registry.TropismRef{Name: "quiet-self-one-second-checklist", Version: 1},
		Profile: registry.Profile{Briefing: "briefings/quiet-self-test.md", ScopesTemplate: []string{"work_items.read", "work_items.write"}},
		Xylem:   registry.Xylem{MaxAttempts: 1, MaxWallSeconds: 1, MaxDepth: 0},
		Phloem:  "projection:work-item-brief", Description: "short lease for quiet-self integration",
	}); err != nil {
		t.Fatalf("define short-lease cultivar: %v", err)
	}
	leased, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
		Title: "quiet-self-leased", Cultivar: "quiet-self-one-second@1", Actor: fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn leased item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, leased.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim leased item: %v", err)
	}
	systemActor, err := fixture.auth.CreateToken(fixture.ctx, auth.CreateTokenInput{
		Name: "quiet-self-expiry-system", Source: domain.SourceSystem, Actor: &fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("create system actor: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	expired, err := fixture.work.ExpireAssignment(fixture.ctx, leased.ID, systemActor.Token)
	if err != nil || !expired {
		t.Fatalf("expire assignment: expired=%t err=%v", expired, err)
	}

	rec := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&exclude_actor="+systemActor.Token.ID.String(), fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), domain.EventWorkItemAssignmentReleased) ||
		!strings.Contains(rec.Body.String(), string(domain.AssignmentReleaseExpired)) {
		t.Fatalf("excluding the system actor swallowed A's release control: %s", rec.Body.String())
	}

	// Terminal handback authored by root stays visible to the former holder
	// when root is excluded; root's ordinary chatter on A's items does not.
	handback, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
		Title: "quiet-self-handback", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"handback survives exclusion"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn handback item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, handback.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim handback item: %v", err)
	}
	appendQuietSelfNote(t, fixture, "assignedA", "root-chatter-on-a", fixture.root.Token)
	if _, err := fixture.work.Transition(fixture.ctx, handback.ID, domain.WorkItemDone, "quiet-self-root-terminalizes", fixture.root.Token); err != nil {
		t.Fatalf("root terminalize: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?limit=100&exclude_actor="+fixture.root.Token.ID.String(), fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), "quiet-self-root-terminalizes") {
		t.Fatalf("excluding root swallowed A's terminal handback: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "root-chatter-on-a") {
		t.Fatalf("excluding root leaked root's ordinary chatter: %s", rec.Body.String())
	}

	// exclude_actor=self quiets A's own writes and self-addressed controls —
	// including a self-authored terminal handback — without touching the
	// directed signals other actors sent to A.
	selfItem, err := fixture.work.SpawnChild(fixture.ctx, fixture.tree.ID, workitems.CreateInput{
		Title: "quiet-self-own-terminal", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"self handback is quieted"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      fixture.root.Token,
	})
	if err != nil {
		t.Fatalf("spawn self-terminal item: %v", err)
	}
	if _, err := fixture.work.Claim(fixture.ctx, selfItem.ID, fixture.actorA.Token); err != nil {
		t.Fatalf("claim self-terminal item: %v", err)
	}
	appendQuietSelfNote(t, fixture, "assignedA", "a-own-note", fixture.actorA.Token)
	if _, err := fixture.work.Transition(fixture.ctx, selfItem.ID, domain.WorkItemDone, "quiet-self-own-terminalize", fixture.actorA.Token); err != nil {
		t.Fatalf("self terminalize: %v", err)
	}

	baseline := doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, baseline, http.StatusOK)
	for _, marker := range []string{"a-own-note", "quiet-self-own-terminalize", domain.EventWorkItemAssigned} {
		if !strings.Contains(baseline.Body.String(), marker) {
			t.Fatalf("baseline assigned feed missing %q: %s", marker, baseline.Body.String())
		}
	}
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedA.ID, "agent.quiet_self_test",
		map[string]any{"marker": "a-outbound-directed", "addressee_token_id": fixture.actorB.Token.ID}, fixture.actorA.Token); err != nil {
		t.Fatalf("append outbound directed note: %v", err)
	}
	rec = doREST(t, fixture.server.Handler(), http.MethodGet, "/v1/feed?limit=100&exclude_actor=self", fixture.actorA.Secret, "", nil)
	assertRESTStatus(t, rec, http.StatusOK)
	for _, hidden := range []string{"a-own-note", "quiet-self-own-terminalize", "a-outbound-directed"} {
		if strings.Contains(rec.Body.String(), hidden) {
			t.Fatalf("exclude_actor=self leaked %q: %s", hidden, rec.Body.String())
		}
	}
	if !strings.Contains(rec.Body.String(), string(domain.AssignmentReleaseExpired)) {
		t.Fatalf("exclude_actor=self swallowed the system-authored release: %s", rec.Body.String())
	}
}

func TestQuietSelfLongPollAndSSEIgnoreExcludedTrafficIntegration(t *testing.T) {
	fixture := newAssignedFeedFixture(t)
	broad := newQuietSelfBroadReader(t, fixture, "quiet-self-stream-broad")
	// Bootstrap under the same filter identity the watcher will use: cursor
	// identity now binds the canonical predicate fingerprint fail-closed.
	boot := doREST(t, fixture.server.Handler(), http.MethodGet,
		"/v1/feed?wait=0s&exclude_actor="+fixture.actorB.Token.ID.String(), broad.Secret, "", nil)
	assertRESTStatus(t, boot, http.StatusOK)
	var bootPage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(boot.Body.Bytes(), &bootPage); err != nil || bootPage.NextCursor == "" {
		t.Fatalf("bootstrap filtered cursor: err=%v body=%s", err, boot.Body.String())
	}
	cursor := bootPage.NextCursor

	type result struct {
		code int
		body string
	}
	done := make(chan result, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet,
			"/v1/feed?cursor="+cursor+"&wait=4s&exclude_actor="+fixture.actorB.Token.ID.String(), nil)
		req.Header.Set("Authorization", "Bearer "+broad.Secret)
		rec := httptest.NewRecorder()
		fixture.server.Handler().ServeHTTP(rec, req)
		done <- result{code: rec.Code, body: rec.Body.String()}
	}()

	time.Sleep(300 * time.Millisecond)
	appendQuietSelfNote(t, fixture, "assignedB", "long-poll-excluded-b", fixture.actorB.Token)
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedB.ID, "agent.quiet_self_test",
		map[string]any{"marker": "long-poll-excluded-directed", "addressee_token_id": fixture.root.Token.ID}, fixture.actorB.Token); err != nil {
		t.Fatalf("append excluded directed note: %v", err)
	}
	select {
	case got := <-done:
		t.Fatalf("excluded traffic ended long-poll early: status=%d body=%s", got.code, got.body)
	case <-time.After(600 * time.Millisecond):
	}

	appendQuietSelfNote(t, fixture, "unassigned", "long-poll-included-root", fixture.root.Token)
	select {
	case got := <-done:
		if got.code != http.StatusOK || !strings.Contains(got.body, "long-poll-included-root") {
			t.Fatalf("included wake result: status=%d body=%s", got.code, got.body)
		}
		for _, hidden := range []string{"long-poll-excluded-b", "long-poll-excluded-directed"} {
			if strings.Contains(got.body, hidden) {
				t.Fatalf("wake page leaked excluded event %q: %s", hidden, got.body)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll did not wake on non-excluded event")
	}

	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()
	streamCtx, cancel := context.WithTimeout(fixture.ctx, 5*time.Second)
	defer cancel()
	frames := make(chan sseFrameForTest, 16)
	ready := make(chan error, 1)
	go consumeSSEForTestReady(t, streamCtx,
		httpServer.URL+"/v1/feed/stream?exclude_actor="+fixture.actorB.Token.ID.String(),
		broad.Secret, "", ready, frames)
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("open SSE stream: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SSE stream did not commit response headers")
	}

	appendQuietSelfNote(t, fixture, "assignedB", "sse-excluded-b", fixture.actorB.Token)
	if err := fixture.work.AppendEvent(fixture.ctx, fixture.assignedB.ID, "agent.quiet_self_test",
		map[string]any{"marker": "sse-excluded-directed", "addressee_token_id": fixture.root.Token.ID}, fixture.actorB.Token); err != nil {
		t.Fatalf("append excluded directed sse note: %v", err)
	}
	select {
	case frame := <-frames:
		t.Fatalf("excluded traffic emitted an SSE frame: %s", frame.data)
	case <-time.After(500 * time.Millisecond):
	}

	appendQuietSelfNote(t, fixture, "unassigned", "sse-included-root", fixture.root.Token)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-frames:
			if strings.Contains(frame.data, "sse-excluded-b") {
				t.Fatalf("SSE leaked excluded event: %s", frame.data)
			}
			if strings.Contains(frame.data, "sse-included-root") {
				return
			}
		case <-deadline:
			t.Fatal("SSE did not deliver non-excluded event")
		}
	}
}
