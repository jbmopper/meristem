package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/worker"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestProviderTrackerHTTPMutationIdempotencyIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-tracker-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root token: %v", err)
	}
	actorAResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-tracker-a", Source: domain.SourceAgent, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor A: %v", err)
	}
	actorBResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-tracker-b", Source: domain.SourceAgent, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor B: %v", err)
	}
	actorA := actorAResult.Token
	actorB := actorBResult.Token
	workerActorResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "http-tracker-worker", Source: domain.SourceSystem, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create worker actor: %v", err)
	}
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	profile := ProviderTrackerHTTPProfile()

	args := map[string]any{
		"title":                        "HTTP tracker item",
		"body":                         "safe coordination only",
		"human_review_status":          "blocked",
		"suggested_convergence_checks": []string{"event:owner_tracker_reviewed"},
		"idempotency_key":              "same-key",
	}
	first := callHTTPTool(t, s, actorA, profile, "work_items.create", args)
	if first.IsError {
		t.Fatalf("first create failed: %s", first.Text)
	}
	replay := callHTTPTool(t, s, actorA, profile, "work_items.create", args)
	if replay.IsError {
		t.Fatalf("replayed create failed: %s", replay.Text)
	}
	if first.Text != replay.Text {
		t.Fatalf("replay body differs:\nfirst %s\nreplay %s", first.Text, replay.Text)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 1 {
		t.Fatalf("same actor/tool/key/args created %d items, want 1", got)
	}

	conflicting := cloneHTTPArgs(args)
	conflicting["title"] = "different arguments"
	conflict := callHTTPTool(t, s, actorA, profile, "work_items.create", conflicting)
	if !conflict.IsError || !strings.Contains(conflict.Text, "idempotency_key_conflict") {
		t.Fatalf("same key/different args did not conflict before mutation: %+v", conflict)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 1 {
		t.Fatalf("conflicting request mutated work items: created events=%d", got)
	}

	secondActor := callHTTPTool(t, s, actorB, profile, "work_items.create", args)
	if secondActor.IsError {
		t.Fatalf("second actor create failed: %s", secondActor.Text)
	}
	if secondActor.Text == first.Text {
		t.Fatalf("different actors received the same work item response: %s", first.Text)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 2 {
		t.Fatalf("different actors did not retain distinct idempotency identity: created events=%d", got)
	}

	w, err := worker.New(pool, writer, worker.Budgets{ByState: map[domain.WorkItemState]time.Duration{}}, &workerActorResult.Token.ID, nil)
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	workerResult, err := w.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("scan worker after provider tracker writes: %v", err)
	}
	if workerResult.DispatchCandidatesScanned != 0 || workerResult.DispatchesRequested != 0 || workerResult.ScribeCandidatesScanned != 0 {
		t.Fatalf("human-review-blocked provider items reached worker execution lanes: %+v", workerResult)
	}
	if got := eventCount(t, pool, domain.EventDispatchRequested); got != 0 {
		t.Fatalf("worker emitted %d dispatch.requested events for provider tracker items", got)
	}
	var queued int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_queue`).Scan(&queued); err != nil {
		t.Fatalf("count job_queue: %v", err)
	}
	if queued != 0 {
		t.Fatalf("tracker-only creates enqueued %d jobs", queued)
	}
}

// TestProviderTrackerRejectsLegacyRawIdempotencyReplay seeds the exact cache
// shape produced before provider-safe response-contract versioning: the same
// tool, arguments, key, and old request hash paired with an ordinary DTO. The
// provider retry must conflict under the unchanged logical key, not replay the
// raw body and not execute the mutation again.
func TestProviderTrackerRejectsLegacyRawIdempotencyReplay(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "provider-legacy-cache")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	args := map[string]any{
		"title":                        "Legacy raw cache canary",
		"body":                         "ordinary operator DTO response",
		"human_review_status":          "blocked",
		"suggested_convergence_checks": []string{"event:legacy-cache-reviewed"},
		"idempotency_key":              "legacy-provider-cache-key",
	}

	// An unrestricted call uses the pre-discriminator request hash and stores the
	// ordinary work-item DTO, faithfully modeling a legacy provider cache row
	// without bypassing the event-backed idempotency writer in the test.
	legacy := callHTTPTool(t, s, actor, nil, "work_items.create", args)
	if legacy.IsError || legacy.TransportError != "" {
		t.Fatalf("seed legacy cache row: %+v", legacy)
	}
	if !strings.Contains(legacy.Text, "created_by") || strings.Contains(legacy.Text, ProviderSafeWorkItemsContract) {
		t.Fatalf("seed response was not an ordinary legacy DTO: %s", legacy.Text)
	}

	retry := callHTTPTool(t, s, actor, ProviderTrackerHTTPProfile(), "work_items.create", args)
	if !retry.IsError || !strings.Contains(retry.Text, "idempotency_key_conflict") {
		t.Fatalf("provider retry did not fail closed on the legacy row: %+v", retry)
	}
	if strings.Contains(retry.Text, "created_by") || strings.Contains(retry.Text, "ordinary operator DTO response") {
		t.Fatalf("provider conflict leaked the cached legacy body: %s", retry.Text)
	}
	if got := eventCount(t, pool, domain.EventWorkItemCreated); got != 1 {
		t.Fatalf("legacy provider retry re-executed mutation: created events=%d", got)
	}
	if got := idempotencyKeyCount(t, pool, actor.ID, "MCP:work_items.create", "legacy-provider-cache-key"); got != 1 {
		t.Fatalf("legacy cache conflict changed idempotency rows: got %d", got)
	}
}

func TestProviderTrackerHTTPHiddenCallsHaveNoDurableEffectsIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	actor := createMCPTestActor(t, ctx, pool, writer, "http-tracker-hidden")
	s := New(Deps{
		Idempotency: idempotency.NewMiddleware(pool, writer),
		WorkItems:   workitems.NewService(pool, writer),
	}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	profile := ProviderTrackerHTTPProfile()

	before := durableEffectCounts(t, ctx, pool)
	for _, name := range []string{
		"inbox.capture",
		"policy_profile.switch",
		"registry.define_tropism",
		"registry.define_cultivar",
		"registry.activate_cultivar",
		"projections.define",
		"approvals.request",
		"approvals.decide",
		"connectors.http_request",
		"convergence.propose_checks",
	} {
		result := callHTTPTool(t, s, actor, profile, name, map[string]any{"idempotency_key": "hidden-" + name})
		if result.TransportError == "" || !strings.Contains(result.TransportError, "not enabled on this HTTP MCP profile") {
			t.Fatalf("hidden tool %s was not rejected at the HTTP profile boundary: %+v", name, result)
		}
	}
	after := durableEffectCounts(t, ctx, pool)
	if after != before {
		t.Fatalf("hidden calls changed durable state: before=%+v after=%+v", before, after)
	}
}

type httpToolCallResult struct {
	IsError        bool
	Text           string
	TransportError string
}

func callHTTPTool(t *testing.T, s *Server, actor domain.Token, profile *HTTPToolProfile, name string, args map[string]any) httpToolCallResult {
	t.Helper()
	result, err := doHTTPToolCall(s, actor, profile, name, args)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// doHTTPToolCall is safe to call from a worker goroutine because it reports
// all decode/setup failures to its caller instead of invoking testing.FailNow.
func doHTTPToolCall(s *Server, actor domain.Token, profile *HTTPToolProfile, name string, args map[string]any) (httpToolCallResult, error) {
	params, err := json.Marshal(map[string]any{"name": name, "arguments": args})
	if err != nil {
		return httpToolCallResult{}, fmt.Errorf("encode HTTP MCP params: %w", err)
	}
	raw := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":10,"method":"tools/call","params":%s}`, params))
	resp := s.HandleHTTPMessageWithOptions(context.Background(), raw, actor, HTTPOptions{Profile: profile})
	var envelope struct {
		Error  *rpcError       `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &envelope); err != nil {
		return httpToolCallResult{}, fmt.Errorf("decode HTTP MCP response: %w body=%s", err, resp.Body)
	}
	if envelope.Error != nil {
		return httpToolCallResult{TransportError: envelope.Error.Message}, nil
	}
	var toolResult struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(envelope.Result, &toolResult); err != nil {
		return httpToolCallResult{}, fmt.Errorf("decode HTTP MCP tool result: %w result=%s", err, envelope.Result)
	}
	text := ""
	if len(toolResult.Content) > 0 {
		text = toolResult.Content[0].Text
	}
	return httpToolCallResult{IsError: toolResult.IsError, Text: text}, nil
}

func cloneHTTPArgs(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type durableCounts struct {
	Events int
	Jobs   int
	Outbox int
}

func durableEffectCounts(t *testing.T, ctx context.Context, q *pgxpool.Pool) durableCounts {
	t.Helper()
	var got durableCounts
	if err := q.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&got.Events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM job_queue`).Scan(&got.Jobs); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&got.Outbox); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return got
}
