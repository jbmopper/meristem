package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

func TestProviderSafeHTTPContextAllVersusTreeAndNoEventPayloadLeakage(t *testing.T) {
	ctx := context.Background()
	pool := newMCPIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, discardLogger()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	rootResult, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{
		Name:   "provider-safe-root",
		IsRoot: true,
		Source: domain.SourceHuman,
	})
	if err != nil {
		t.Fatal(err)
	}
	workSvc := workitems.NewService(pool, writer)
	rootActor := rootResult.Token
	a, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:                      "Visible project A",
		Body:                       "ordinary tracker instructions for A",
		SuggestedConvergenceChecks: []string{"event:safe-a"},
		Actor:                      rootActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	a1, err := workSvc.SpawnChild(ctx, a.ID, workitems.CreateInput{
		Title:                      "Visible child A1",
		Body:                       "ordinary child instructions",
		SuggestedConvergenceChecks: []string{"event:safe-a1"},
		Actor:                      rootActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := workSvc.Create(ctx, workitems.CreateInput{
		Title:                      "Visible project B",
		Body:                       "ordinary tracker instructions for B",
		SuggestedConvergenceChecks: []string{"event:safe-b"},
		Actor:                      rootActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workSvc.Transition(ctx, a1.ID, domain.WorkItemBlocked, "PRIVATE-STATE-REASON", rootActor); err != nil {
		t.Fatal(err)
	}
	if err := workSvc.AppendEvent(ctx, a.ID, "coordination.private_test", map[string]any{
		"message_text":      "PRIVATE-MESSAGE-TEXT",
		"signal_payload":    "PRIVATE-SIGNAL-PAYLOAD",
		"approval_request":  "PRIVATE-APPROVAL-REQUEST",
		"connector_request": "PRIVATE-CONNECTOR-REQUEST",
		"encrypted_payload": "PRIVATE-ENCRYPTED-PAYLOAD",
		"token":             "PRIVATE-TOKEN-MATERIAL",
	}, rootActor); err != nil {
		t.Fatal(err)
	}

	s := New(Deps{
		Access:    access.NewService(pool),
		WorkItems: workSvc,
		Feed:      feed.NewService(pool),
	}, ServerInfo{Name: "provider-safe-test", Version: "test"}, nil)

	ownerAuthority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: ownerAuthority.Scopes}
	ownerFeed := providerHTTPToolText(t, s, owner, "feed.read", map[string]any{"limit": 50})
	for _, id := range []uuid.UUID{a.ID, a1.ID, b.ID} {
		if !strings.Contains(ownerFeed, id.String()) {
			t.Errorf("owner feed missing %s: %s", id, ownerFeed)
		}
	}
	assertProviderTextSafe(t, ownerFeed)
	if !strings.Contains(ownerFeed, feed.ProviderSafeContract) {
		t.Fatalf("owner feed missing contract: %s", ownerFeed)
	}

	delegatedAuthority, err := access.ReduceProviderAuthority(access.ProviderDelegatedTreeReadV1, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	delegated := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: delegatedAuthority.Scopes}
	delegatedFeed := providerHTTPToolText(t, s, delegated, "feed.read", map[string]any{"limit": 50})
	if !strings.Contains(delegatedFeed, a.ID.String()) || !strings.Contains(delegatedFeed, a1.ID.String()) {
		t.Fatalf("delegated feed missing assigned tree: %s", delegatedFeed)
	}
	if strings.Contains(delegatedFeed, b.ID.String()) {
		t.Fatalf("delegated feed leaked out-of-tree B: %s", delegatedFeed)
	}
	assertProviderTextSafe(t, delegatedFeed)

	ownerList := providerHTTPToolText(t, s, owner, "work_items.list", map[string]any{"limit": 50})
	for _, required := range []string{"provider_safe_work_items.v1", "Visible project A", "ordinary tracker instructions for A", "event:safe-a", "Visible project B"} {
		if !strings.Contains(ownerList, required) {
			t.Errorf("owner work-item list missing %q: %s", required, ownerList)
		}
	}
	for _, omitted := range []string{"PRIVATE-STATE-REASON", "created_by", "state_reason"} {
		if strings.Contains(ownerList, omitted) {
			t.Errorf("owner work-item list leaked %q: %s", omitted, ownerList)
		}
	}

	delegatedList := providerHTTPToolText(t, s, delegated, "work_items.list", map[string]any{"limit": 50})
	if !strings.Contains(delegatedList, "Visible project A") || !strings.Contains(delegatedList, "Visible child A1") {
		t.Fatalf("delegated list missing assigned tree: %s", delegatedList)
	}
	if strings.Contains(delegatedList, "Visible project B") {
		t.Fatalf("delegated list leaked out-of-tree B: %s", delegatedList)
	}

	ownerTrackerAuthority, err := access.ReduceProviderAuthority(access.ProviderOwnerTrackerWriteV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	ownerTracker := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: ownerTrackerAuthority.Scopes}
	ownerTrackerList := providerHTTPToolTextWithOptions(t, s, ownerTracker, "work_items.list", map[string]any{"limit": 50}, HTTPOptions{Profile: ProviderTrackerHTTPProfile()})
	if !strings.Contains(ownerTrackerList, ProviderSafeWorkItemsContract) {
		t.Fatalf("tracker profile did not select provider-safe work-item DTO: %s", ownerTrackerList)
	}
	assertProviderTextSafe(t, ownerTrackerList)

	// backlog.readiness emits backlog.Item, not the provider-safe work-item
	// DTO, and is advertised by both production profiles; its summary must be
	// reduced under the provider-safe context like every other read.
	ownerReadiness := providerHTTPToolTextWithOptions(t, s, owner, "backlog.readiness", map[string]any{}, HTTPOptions{Profile: ProviderSafeReadHTTPProfile()})
	if !strings.Contains(ownerReadiness, backlog.Contract) {
		t.Fatalf("readiness summary missing contract: %s", ownerReadiness)
	}
	if !strings.Contains(ownerReadiness, a1.ID.String()) {
		t.Fatalf("readiness summary missing blocked child A1: %s", ownerReadiness)
	}
	assertProviderTextSafe(t, ownerReadiness)

	trackerReadiness := providerHTTPToolTextWithOptions(t, s, ownerTracker, "backlog.readiness", map[string]any{}, HTTPOptions{Profile: ProviderTrackerHTTPProfile()})
	assertProviderTextSafe(t, trackerReadiness)
}

func providerHTTPToolText(t *testing.T, s *Server, actor domain.Token, name string, args map[string]any) string {
	t.Helper()
	return providerHTTPToolTextWithOptions(t, s, actor, name, args, HTTPOptions{AllowedTools: ReadOnlyHTTPTools()})
}

func providerHTTPToolTextWithOptions(t *testing.T, s *Server, actor domain.Token, name string, args map[string]any, opts HTTPOptions) string {
	t.Helper()
	arguments, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":` + quoteJSON(t, name) + `,"arguments":` + string(arguments) + `}}`)
	resp := s.HandleHTTPMessageWithOptions(context.Background(), request, actor, opts)
	var rpc rpcMessage
	if err := json.Unmarshal(resp.Body, &rpc); err != nil {
		t.Fatalf("decode HTTP RPC: %v body=%s", err, resp.Body)
	}
	if rpc.Error != nil {
		t.Fatalf("HTTP RPC error: %+v", rpc.Error)
	}
	result := decodeToolResult(t, rpc)
	if result.IsError || len(result.Content) == 0 {
		t.Fatalf("tool %s failed: %+v", name, result)
	}
	return result.Content[0].Text
}

func assertProviderTextSafe(t *testing.T, text string) {
	t.Helper()
	for _, forbidden := range []string{
		`"payload"`, `"actor_token_id"`, "PRIVATE-STATE-REASON", "PRIVATE-MESSAGE-TEXT",
		"PRIVATE-SIGNAL-PAYLOAD", "PRIVATE-APPROVAL-REQUEST", "PRIVATE-CONNECTOR-REQUEST",
		"PRIVATE-ENCRYPTED-PAYLOAD", "PRIVATE-TOKEN-MATERIAL",
	} {
		if strings.Contains(text, forbidden) {
			t.Errorf("provider response leaked %q: %s", forbidden, text)
		}
	}
}
