package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/buildguard"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// TestIdempotencyAdvisoryLockSerializesConcurrentRequests fires N
// concurrent POSTs that share the same (token, scope, key, body) at
// the live Postgres-backed API. The advisory lock in
// internal/idempotency/middleware.go must serialize them: exactly one
// runs the inner handler (one signal row, one work_item, one
// idempotency.recorded event), and all N responses must be
// byte-identical so callers converge regardless of which one their
// retry latched onto.
//
// Pre-lock this would intermittently produce two cache rows worth of
// races; today the events layer absorbs the duplicate event id but
// the loser used to send back its own buffered response which can
// differ in any non-deterministic field. With pg_advisory_lock all N
// callers get the winner's bytes.
func TestIdempotencyAdvisoryLockSerializesConcurrentRequests(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "idempotency-concurrent",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	body := signalRequestBody(t, "integration:idem-lock:retry")
	const concurrency = 8
	const sharedKey = "concurrent-key"

	type resp struct {
		status int
		body   []byte
	}
	results := make([]resp, concurrency)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			r := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, body)
			results[i] = resp{status: r.Code, body: append([]byte(nil), r.Body.Bytes()...)}
		}()
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.status != http.StatusCreated {
			t.Fatalf("worker %d: want 201, got %d body=%s", i, r.status, string(r.body))
		}
		if !bytes.Equal(r.body, results[0].body) {
			t.Fatalf("worker %d body diverged from worker 0:\nworker 0=%s\nworker %d=%s", i, string(results[0].body), i, string(r.body))
		}
	}

	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "signals", 1)
	assertTableCount(t, pool, "work_items", 1)
}

// TestIdempotencyAdvisoryLockRejectsConflictingBodies confirms that
// two same-key requests with different bodies serialize on the lock
// and the second is rejected as a 422 conflict before its handler
// runs (so it produces no events / projection rows of its own).
func TestIdempotencyAdvisoryLockRejectsConflictingBodies(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "idempotency-conflict",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	bodyA := signalRequestBody(t, "integration:idem-lock:body-a")
	bodyB := signalRequestBody(t, "integration:idem-lock:body-b")
	const sharedKey = "conflict-key"

	first := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, bodyA)
	if first.Code != http.StatusCreated {
		t.Fatalf("first body: want 201, got %d body=%s", first.Code, first.Body.String())
	}

	second := postSignal(t, server.Handler(), tokenResult.Secret, sharedKey, bodyB)
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("second body (conflict): want 422, got %d body=%s", second.Code, second.Body.String())
	}

	// The conflict must be rejected before the inner handler runs —
	// no extra signal/work_item rows, no extra events.
	assertEventCount(t, pool, domain.EventSignalReceived, 1)
	assertEventCount(t, pool, domain.EventWorkItemCreated, 1)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "signals", 1)
	assertTableCount(t, pool, "work_items", 1)
}

// TestIdempotencyRejectionDoesNotConsumeKey pins the REST half of the
// key-consumption contract: a 4xx rejection is never recorded, so the
// same key retried with a corrected body executes instead of surfacing
// the same-key/different-body 422 conflict.
func TestIdempotencyRejectionDoesNotConsumeKey(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)

	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name:   "idempotency-rejection",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	server := New(pool, nil)
	const sharedKey = "rejection-keep-key"
	invalid := []byte(`{"title": ""}`)
	corrected := []byte(`{"title": "recovered after rejection"}`)

	post := func(body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/work-items", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tokenResult.Secret)
		req.Header.Set("Idempotency-Key", sharedKey)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		return rec
	}

	// A rejection re-derives on every same-body retry and records nothing.
	for i := 0; i < 2; i++ {
		if rec := post(invalid); rec.Code != http.StatusBadRequest {
			t.Fatalf("invalid call %d: want 400, got %d body=%s", i+1, rec.Code, rec.Body.String())
		}
	}
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 0)
	assertTableCount(t, pool, "work_items", 0)

	// The corrected body reuses the SAME key and must execute.
	first := post(corrected)
	if first.Code != http.StatusCreated {
		t.Fatalf("corrected retry: want 201, got %d body=%s", first.Code, first.Body.String())
	}
	if first.Header().Get("Idempotency-Replayed") == "true" {
		t.Fatalf("corrected retry must execute fresh, not replay")
	}
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)
	assertTableCount(t, pool, "work_items", 1)

	// The committed conclusion replays byte-for-byte under the same key...
	replay := post(corrected)
	if replay.Code != http.StatusCreated || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay: want 201 replayed, got %d replayed=%q", replay.Code, replay.Header().Get("Idempotency-Replayed"))
	}
	if !bytes.Equal(replay.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("replay bytes diverged:\nfirst=%s\nreplay=%s", first.Body.String(), replay.Body.String())
	}
	assertTableCount(t, pool, "work_items", 1)

	// ...and only now does a different body conflict.
	if rec := post(invalid); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("post-commit different body: want 422 conflict, got %d body=%s", rec.Code, rec.Body.String())
	}
}

// TestStatefulRefusalConsumesKeyAndReplays pins the other half of the cache
// disposition contract (review finding IDEM-B1): a refusal that COMMITS
// authoritative events before returning — xylem budget exhaustion appends
// xylem.exhausted plus an escalation and blocks the parent — is a stateful
// conclusion. It must consume its key: a same-body retry replays the recorded
// 409 without appending a second refusal set, and a changed body under the
// same key conflicts instead of being admitted as a new authoritative action.
func TestStatefulRefusalConsumesKeyAndReplays(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "stateful-refusal-root", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	defineSingleChildCultivar(t, ctx, pool, writer, rootResult.Token)
	workSvc := workitems.NewService(pool, writer)
	parent, err := workSvc.Create(ctx, workitems.CreateInput{
		Title: "stateful refusal parent", Actor: rootResult.Token, Cultivar: "single-child-worker@1",
	})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	server := New(pool, nil)
	spawnPath := "/v1/work-items/" + parent.ID.String() + "/children"

	first := doREST(t, server.Handler(), http.MethodPost, spawnPath, rootResult.Secret, "first-child", []byte(`{"title":"first child"}`))
	assertRESTStatus(t, first, http.StatusCreated)

	// The over-budget refusal commits one xylem.exhausted + escalation set
	// and must be recorded under its key.
	overBudget := []byte(`{"title":"second child"}`)
	refused := doREST(t, server.Handler(), http.MethodPost, spawnPath, rootResult.Secret, "stateful-409", overBudget)
	assertRESTStatus(t, refused, http.StatusConflict)
	assertErrorCode(t, refused, "xylem_budget_exhausted")
	assertEventCount(t, pool, domain.EventXylemExhausted, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)

	// Same key, same body: replay the recorded refusal, append NOTHING new.
	replay := doREST(t, server.Handler(), http.MethodPost, spawnPath, rootResult.Secret, "stateful-409", overBudget)
	assertRESTStatus(t, replay, http.StatusConflict)
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("stateful refusal retry must replay, not re-execute")
	}
	assertEventCount(t, pool, domain.EventXylemExhausted, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)

	// Same key, changed body: one logical key cannot identify a second
	// authoritative action — conflict, and still exactly one refusal set.
	changed := doREST(t, server.Handler(), http.MethodPost, spawnPath, rootResult.Secret, "stateful-409", []byte(`{"title":"second child renamed"}`))
	assertRESTStatus(t, changed, http.StatusUnprocessableEntity)
	assertErrorCode(t, changed, "idempotency_key_conflict")
	assertEventCount(t, pool, domain.EventXylemExhausted, 1)
	assertEventCount(t, pool, domain.EventEscalationRequested, 1)
}

// TestUnmarkedRefusalIsConservativelyRecorded pins the fail-conservative
// default: a 4xx no handler explicitly marked as pure (here a signals
// validation rejection, whose handler predates the disposition seam) is
// recorded, so a changed body under the same key conflicts rather than
// executing. If a future handler wants key-preserving validation it must
// opt in via MarkRefusalUnconsumed, never get it by default.
func TestUnmarkedRefusalIsConservativelyRecorded(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "unmarked-refusal", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	server := New(pool, nil)

	invalid := []byte(`{"kind":"not-a-real-signal-kind","dedupe_key":"conservative-1"}`)
	rec := postSignal(t, server.Handler(), tokenResult.Secret, "conservative-key", invalid)
	assertRESTStatus(t, rec, http.StatusBadRequest)
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)

	// The unmarked rejection consumed the key: same body replays...
	replay := postSignal(t, server.Handler(), tokenResult.Secret, "conservative-key", invalid)
	assertRESTStatus(t, replay, http.StatusBadRequest)
	if replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("unmarked refusal retry must replay the recorded response")
	}
	// ...and a changed body conflicts instead of executing.
	valid := signalRequestBody(t, "conservative-2")
	conflict := postSignal(t, server.Handler(), tokenResult.Secret, "conservative-key", valid)
	assertRESTStatus(t, conflict, http.StatusUnprocessableEntity)
	assertEventCount(t, pool, domain.EventSignalReceived, 0)
}

// TestWrapStatefulRefusalRecordSurvivesPinAdvance is the HTTP-middleware twin
// of the MCP IDEM-B4 regression: a handler commits an authoritative event
// while admitted/current, the pin advances, and the handler returns an
// unmarked 409. The idempotency record must be durably written across the
// advance (admission-fenced record append), the stale process refuses
// outward with build_pin, and once a current build returns the key replays
// and conflicts instead of admitting a second authoritative action.
func TestWrapStatefulRefusalRecordSurvivesPinAdvance(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	current := buildguard.Status{
		State: buildguard.StateCurrent, CompiledCommit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpectedCommit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CompiledMetadata: buildguard.CompiledValid,
		Reason: "current",
	}
	mismatched := buildguard.Status{
		State: buildguard.StateMismatch, CompiledCommit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpectedCommit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CompiledMetadata: buildguard.CompiledValid,
		Reason: "advanced",
	}
	status := current
	provider := buildguard.ProviderFunc(func() buildguard.Status { return status })
	writer := app.NewGuardedEventWriter(provider)

	authSvc := auth.NewService(pool, writer)
	tokenResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name: "wrap-cutover", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	middleware := idempotency.NewMiddlewareWithGuard(pool, writer, func() error {
		return buildguard.RequireNonBlocking(provider)
	})

	subject := uuid.New()
	calls := 0
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		tx, err := pool.Begin(r.Context())
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(r.Context()) }()
		actorID := tokenResult.Token.ID
		if _, _, err := writer.Append(r.Context(), tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem, SubjectID: subject,
			Kind: domain.EventWorkItemEventAppended, Source: domain.SourceHuman,
			ActorTokenID: &actorID,
			Payload:      map[string]any{"inner_kind": "test.wrap_cutover_refusal", "inner": map[string]any{"attempt": calls}},
		}); err != nil {
			t.Fatalf("append while current: %v", err)
		}
		if err := tx.Commit(r.Context()); err != nil {
			t.Fatalf("commit: %v", err)
		}
		status = mismatched
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"stateful_refusal","message":"refused after commit"}}`))
	})
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.ServeHTTP(w, r)
	}))

	do := func(key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/test-stateful", bytes.NewReader([]byte(body)))
		req = req.WithContext(auth.WithToken(req.Context(), tokenResult.Token))
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	countEvents := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE subject_id = $1`, subject).Scan(&n); err != nil {
			t.Fatalf("count events: %v", err)
		}
		return n
	}

	first := do("wrap-cutover", `{"note":"a"}`)
	if first.Code != http.StatusServiceUnavailable || !strings.Contains(first.Body.String(), "build_pin") {
		t.Fatalf("stale process should refuse outward with build_pin, got %d body=%s", first.Code, first.Body.String())
	}
	if got := countEvents(); got != 1 {
		t.Fatalf("expected one committed event, got %d", got)
	}
	assertEventCount(t, pool, domain.EventIdempotencyRecorded, 1)

	// Current build restored: same body replays, changed body conflicts,
	// no second authoritative event set, handler never re-runs.
	status = current
	replay := do("wrap-cutover", `{"note":"a"}`)
	if replay.Code != http.StatusConflict || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("post-cutover same-body retry should replay the 409, got %d replayed=%q", replay.Code, replay.Header().Get("Idempotency-Replayed"))
	}
	changed := do("wrap-cutover", `{"note":"b"}`)
	if changed.Code != http.StatusUnprocessableEntity {
		t.Fatalf("post-cutover changed body should conflict, got %d body=%s", changed.Code, changed.Body.String())
	}
	if calls != 1 {
		t.Fatalf("handler re-executed across cutover: %d calls", calls)
	}
	if got := countEvents(); got != 1 {
		t.Fatalf("cutover allowed a second authoritative event set: %d", got)
	}
}

// TestAdmittedRecordAppendIsRestricted pins review finding IDEM-B5: the
// admitted-record path is an enforced narrow completion fence, not a generic
// guard bypass. Under an already-mismatched pin, the idempotency completion
// event is the ONLY spec it admits; an arbitrary event pushed through the
// same method is rejected outright, and the ordinary Append path keeps the
// full pre-append refusal.
func TestAdmittedRecordAppendIsRestricted(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tokenResult, err := auth.NewService(pool, app.NewEventWriter()).CreateToken(ctx, auth.CreateTokenInput{
		Name: "restricted-fence", IsRoot: true, Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	mismatched := buildguard.Status{
		State: buildguard.StateMismatch, CompiledCommit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ExpectedCommit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CompiledMetadata: buildguard.CompiledValid,
		Reason: "stale",
	}
	provider := buildguard.ProviderFunc(func() buildguard.Status { return mismatched })
	writer := app.NewGuardedEventWriter(provider)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	subject := uuid.New()
	arbitrary := events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: subject,
		Kind: domain.EventWorkItemEventAppended, Source: domain.SourceSystem,
		Payload: map[string]any{"inner_kind": "review_probe.arbitrary_stale_write"},
	}

	// The review's probe: an arbitrary stale event through the admitted path
	// must be rejected by the fence itself, not admitted.
	if _, _, err := writer.AppendAdmittedIdempotencyRecord(ctx, tx, arbitrary); err == nil || !strings.Contains(err.Error(), "restricted") {
		t.Fatalf("arbitrary event through the admitted path must be rejected, got err=%v", err)
	}

	// The ordinary path keeps the full pre-append refusal under a stale pin.
	if _, _, err := writer.Append(ctx, tx, arbitrary); err == nil || !strings.Contains(err.Error(), "pre-append check") {
		t.Fatalf("ordinary append under a stale pin must be refused, got err=%v", err)
	}

	// The one admitted spec — the idempotency completion event — succeeds
	// across the stale pin (the IDEM-B4 property, here at the writer seam;
	// the end-to-end REST/MCP cutover tests exercise it through the
	// middleware).
	record := events.Spec{
		SubjectKind: domain.SubjectIdempotencyKey, SubjectID: uuid.New(),
		Kind: domain.EventIdempotencyRecorded, Source: domain.SourceSystem,
		Payload: map[string]any{
			"token_id":        tokenResult.Token.ID.String(),
			"scope":           "TEST restricted-fence",
			"key":             "k",
			"request_hash":    "aGFzaA==",
			"response_status": 409,
			"response_body":   map[string]any{"error": map[string]any{"code": "stateful_refusal"}},
		},
	}
	if _, _, err := writer.AppendAdmittedIdempotencyRecord(ctx, tx, record); err != nil {
		t.Fatalf("admitted idempotency completion must succeed across the stale pin: %v", err)
	}
}
