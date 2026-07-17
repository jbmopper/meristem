package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/escalations"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestRESTScopedWorkItemTreeAccessIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "rest-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	root := rootResult.Token

	workSvc := workitems.NewService(pool, writer)
	a, err := workSvc.Create(ctx, workitems.CreateInput{Title: "A", Actor: root})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	a1, err := workSvc.SpawnChild(ctx, a.ID, workitems.CreateInput{
		Title:                      "A1",
		SuggestedConvergenceChecks: []string{"event:scoped_access"},
		Actor:                      root,
	})
	if err != nil {
		t.Fatalf("spawn A1: %v", err)
	}
	b, err := workSvc.Create(ctx, workitems.CreateInput{Title: "B", Actor: root})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}

	scopedResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "rest-scoped-agent",
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + a.ID.String(),
		},
		Actor: &root,
	})
	if err != nil {
		t.Fatalf("create scoped token: %v", err)
	}
	if _, err := workSvc.Claim(ctx, a1.ID, scopedResult.Token); err != nil {
		t.Fatalf("claim A1 for assigned-feed actor: %v", err)
	}
	server := New(pool, nil)

	incompleteResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "rest-incomplete-scoped-agent",
		Source: domain.SourceAgent,
		Scopes: []string{access.ScopeWorkItemsRead},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create incomplete scoped token: %v", err)
	}
	incompleteList := doREST(t, server.Handler(), http.MethodGet, "/v1/work-items?limit=20", incompleteResult.Secret, "", nil)
	assertRESTStatus(t, incompleteList, http.StatusForbidden)
	assertErrorCode(t, incompleteList, "insufficient_scope")

	listRec := doREST(t, server.Handler(), http.MethodGet, "/v1/work-items?limit=20", scopedResult.Secret, "", nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list scoped work_items: %d %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), a.ID.String()) || !strings.Contains(listRec.Body.String(), a1.ID.String()) {
		t.Fatalf("scoped list omitted assigned tree items: %s", listRec.Body.String())
	}
	if strings.Contains(listRec.Body.String(), b.ID.String()) || strings.Contains(listRec.Body.String(), `"title":"B"`) {
		t.Fatalf("scoped list leaked out-of-tree B: %s", listRec.Body.String())
	}

	// Readiness accepts the old limit argument for compatibility, but scans the
	// complete projection before applying the same tree filter. A limit of one
	// must therefore neither hide A/A1 nor expose out-of-tree B.
	readinessRec := doREST(t, server.Handler(), http.MethodGet, "/v1/backlog/readiness?limit=1", scopedResult.Secret, "", nil)
	if readinessRec.Code != http.StatusOK {
		t.Fatalf("scoped backlog readiness: %d %s", readinessRec.Code, readinessRec.Body.String())
	}
	if !strings.Contains(readinessRec.Body.String(), a.ID.String()) || !strings.Contains(readinessRec.Body.String(), a1.ID.String()) {
		t.Fatalf("scoped readiness omitted assigned tree items: %s", readinessRec.Body.String())
	}
	if strings.Contains(readinessRec.Body.String(), b.ID.String()) || strings.Contains(readinessRec.Body.String(), `"title":"B"`) {
		t.Fatalf("scoped readiness leaked out-of-tree B: %s", readinessRec.Body.String())
	}
	if !strings.Contains(readinessRec.Body.String(), `"limit":0`) {
		t.Fatalf("scoped readiness did not report an unbounded scan: %s", readinessRec.Body.String())
	}

	assertRESTStatus(t, doREST(t, server.Handler(), http.MethodGet, "/v1/work-items/"+a.ID.String(), scopedResult.Secret, "", nil), http.StatusOK)
	assertRESTStatus(t, doREST(t, server.Handler(), http.MethodGet, "/v1/work-items/"+a1.ID.String(), scopedResult.Secret, "", nil), http.StatusOK)
	assertRESTStatus(t, doREST(t, server.Handler(), http.MethodGet, "/v1/work-items/"+b.ID.String(), scopedResult.Secret, "", nil), http.StatusNotFound)

	feedRec := doREST(t, server.Handler(), http.MethodGet, "/v1/feed?limit=50", scopedResult.Secret, "", nil)
	if feedRec.Code != http.StatusOK {
		t.Fatalf("scoped feed: %d %s", feedRec.Code, feedRec.Body.String())
	}
	if strings.Contains(feedRec.Body.String(), b.ID.String()) || strings.Contains(feedRec.Body.String(), `"title":"B"`) {
		t.Fatalf("scoped feed leaked out-of-tree B: %s", feedRec.Body.String())
	}
	if !strings.Contains(feedRec.Body.String(), a1.ID.String()) {
		t.Fatalf("scoped feed omitted in-tree child A1: %s", feedRec.Body.String())
	}

	// Non-work_item-subject events reach the tree-scoped feed through
	// their work_item anchors: an in-tree convergence verdict (subject
	// kind "convergence") and escalation (subject kind "escalation") are
	// visible, while their out-of-tree twins stay redacted.
	appendTestVerdict(t, ctx, pool, writer, a1.ID, "in-tree verdict marker")
	appendTestVerdict(t, ctx, pool, writer, b.ID, "out-of-tree verdict marker")
	escSvc := escalations.NewService(pool, writer)
	if _, err := escSvc.Request(ctx, escalations.RequestInput{WorkItemID: a1.ID, Reason: "in-tree-escalation-marker", Summary: "scoped feed regression", Actor: root}); err != nil {
		t.Fatalf("escalate a1: %v", err)
	}
	if _, err := escSvc.Request(ctx, escalations.RequestInput{WorkItemID: b.ID, Reason: "out-of-tree-escalation-marker", Summary: "scoped feed regression", Actor: root}); err != nil {
		t.Fatalf("escalate b: %v", err)
	}
	feedRec = doREST(t, server.Handler(), http.MethodGet, "/v1/feed?limit=50", scopedResult.Secret, "", nil)
	if feedRec.Code != http.StatusOK {
		t.Fatalf("scoped feed after anchored events: %d %s", feedRec.Code, feedRec.Body.String())
	}
	for _, want := range []string{"in-tree verdict marker", "in-tree-escalation-marker"} {
		if !strings.Contains(feedRec.Body.String(), want) {
			t.Fatalf("scoped feed omitted in-tree anchored event %q: %s", want, feedRec.Body.String())
		}
	}
	for _, leak := range []string{"out-of-tree verdict marker", "out-of-tree-escalation-marker"} {
		if strings.Contains(feedRec.Body.String(), leak) {
			t.Fatalf("scoped feed leaked out-of-tree anchored event %q: %s", leak, feedRec.Body.String())
		}
	}

	beforeDenied := totalEventCount(t, pool)
	deniedTransition := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+b.ID.String()+"/transition", scopedResult.Secret, "deny-b", []byte(`{"to":"running","reason":"should not happen"}`))
	assertRESTStatus(t, deniedTransition, http.StatusNotFound)
	if after := totalEventCount(t, pool); after != beforeDenied {
		t.Fatalf("denied transition appended events: before=%d after=%d body=%s", beforeDenied, after, deniedTransition.Body.String())
	}

	beforeCreate := totalEventCount(t, pool)
	deniedCreate := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items", scopedResult.Secret, "deny-create", []byte(`{"title":"should not happen"}`))
	assertRESTStatus(t, deniedCreate, http.StatusForbidden)
	assertErrorCode(t, deniedCreate, "insufficient_scope")
	if after := totalEventCount(t, pool); after != beforeCreate {
		t.Fatalf("denied top-level create appended events: before=%d after=%d body=%s", beforeCreate, after, deniedCreate.Body.String())
	}

	allowedTransition := doREST(t, server.Handler(), http.MethodPost, "/v1/work-items/"+a1.ID.String()+"/transition", scopedResult.Secret, "allow-a1", []byte(`{"to":"running","reason":"inside assigned tree"}`))
	assertRESTStatus(t, allowedTransition, http.StatusOK)
	if got := lastActorForRESTKind(t, pool, domain.EventWorkItemTransitioned); got != scopedResult.Token.ID {
		t.Fatalf("allowed transition actor = %s, want scoped token %s", got, scopedResult.Token.ID)
	}

	humanScopedResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "rest-human-no-inbox",
		Source: domain.SourceHuman,
		Scopes: []string{access.ScopeWorkItemsRead, "work_items.tree:" + a.ID.String()},
		Actor:  &root,
	})
	if err != nil {
		t.Fatalf("create human scoped token: %v", err)
	}
	beforeInbox := totalEventCount(t, pool)
	deniedInbox := doREST(t, server.Handler(), http.MethodPost, "/v1/inbox/messages", humanScopedResult.Secret, "deny-inbox", []byte(`{"text":"should not happen"}`))
	assertRESTStatus(t, deniedInbox, http.StatusForbidden)
	assertErrorCode(t, deniedInbox, "insufficient_scope")
	if after := totalEventCount(t, pool); after != beforeInbox {
		t.Fatalf("denied inbox capture appended events: before=%d after=%d body=%s", beforeInbox, after, deniedInbox.Body.String())
	}

	logRec := doREST(t, server.Handler(), http.MethodGet, "/v1/deterministic-errors", scopedResult.Secret, "", nil)
	assertRESTStatus(t, logRec, http.StatusForbidden)
	assertErrorCode(t, logRec, "insufficient_scope")
}

// appendTestVerdict records a minimal valid convergence.verdict_recorded
// event for workItemID, with marker as the verdict reason so feed assertions
// can distinguish in-tree from out-of-tree verdicts.
func appendTestVerdict(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *events.Writer, workItemID uuid.UUID, marker string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin verdict tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectConvergence,
		SubjectID:   workItemID,
		Kind:        domain.EventConvergenceVerdictRecorded,
		Source:      domain.SourceSystem,
		Payload: map[string]any{
			"reducer_identity": "majority_vote",
			"reducer_version":  1,
			"attempt":          1,
			"inputs_digest":    strings.Repeat("a", 64),
			"reducer_config":   map[string]any{"signal_kind": "grader.pass"},
			"verdict":          map[string]any{"disposition": "accept", "reason": marker},
			"signals":          []any{map[string]any{"kind": "grader.pass", "pass": true}},
		},
	}); err != nil {
		t.Fatalf("append verdict for %s: %v", workItemID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit verdict tx: %v", err)
	}
}

func doREST(t *testing.T, handler http.Handler, method, path, token, key string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func assertRESTStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("want status %d, got %d body=%s", want, rec.Code, rec.Body.String())
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode error body: %v body=%s", err, rec.Body.String())
	}
	if out.Error.Code != want {
		t.Fatalf("error code = %q, want %q body=%s", out.Error.Code, want, rec.Body.String())
	}
}

func totalEventCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return count
}

func lastActorForRESTKind(t *testing.T, pool *pgxpool.Pool, kind string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT actor_token_id
		FROM events
		WHERE kind = $1
		ORDER BY occurred_at DESC, id DESC
		LIMIT 1
	`, kind).Scan(&id); err != nil {
		t.Fatalf("last actor for %s: %v", kind, err)
	}
	return id
}
