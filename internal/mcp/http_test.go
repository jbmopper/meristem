package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestHandleHTTPMessageInitializeReturnsJSONRPCResponse(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent}

	resp := s.HandleHTTPMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`), actor)

	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", resp.Status, resp.Body)
	}
	if resp.ContentType != contentTypeJSON {
		t.Fatalf("content type = %q", resp.ContentType)
	}
	var out rpcMessage
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Error != nil {
		t.Fatalf("unexpected json-rpc error: %+v", out.Error)
	}
	if string(out.ID) != "1" {
		t.Fatalf("id = %s, want 1", out.ID)
	}
}

func TestHandleHTTPMessageNotificationReturnsAcceptedNoBody(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent}

	resp := s.HandleHTTPMessage(context.Background(), []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`), actor)

	if resp.Status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", resp.Status, resp.Body)
	}
	if len(resp.Body) != 0 {
		t.Fatalf("notification response body = %q, want empty", resp.Body)
	}
}

func TestHandleHTTPMessageUsesProvidedActorForToolVisibility(t *testing.T) {
	root := uuid.New()
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + root.String(),
		},
	}

	resp := s.HandleHTTPMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`), actor)

	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", resp.Status, resp.Body)
	}
	if strings.Contains(string(resp.Body), "inbox.capture") {
		t.Fatalf("scoped HTTP actor saw hidden tool: %s", resp.Body)
	}
	if !strings.Contains(string(resp.Body), "work_items.get") {
		t.Fatalf("scoped HTTP actor did not see readable work item tool: %s", resp.Body)
	}
}

func TestHandleHTTPMessageWithOptionsFiltersHTTPTools(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}

	resp := s.HandleHTTPMessageWithOptions(
		context.Background(),
		[]byte(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`),
		actor,
		HTTPOptions{AllowedTools: ReadOnlyHTTPTools()},
	)

	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", resp.Status, resp.Body)
	}
	body := string(resp.Body)
	if strings.Contains(body, "work_items.create") || strings.Contains(body, "work_items.transition") || strings.Contains(body, "convergence.propose_checks") {
		t.Fatalf("HTTP read-only tool list leaked write tools: %s", body)
	}
	if !strings.Contains(body, "feed.read") || !strings.Contains(body, "work_items.get") {
		t.Fatalf("HTTP read-only tool list omitted expected read tools: %s", body)
	}
	for _, forbidden := range []string{"backlog.readiness", "approvals.get", "approvals.list_for_work_item", "deterministic_errors.get", "registry.get", "projections.get"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("provider-safe HTTP tool list leaked %q: %s", forbidden, body)
		}
	}
}

func TestHandleHTTPMessageWithOptionsRejectsHTTPWriteTool(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}

	resp := s.HandleHTTPMessageWithOptions(
		context.Background(),
		[]byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"work_items.create","arguments":{"title":"nope"}}}`),
		actor,
		HTTPOptions{AllowedTools: ReadOnlyHTTPTools()},
	)

	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", resp.Status, resp.Body)
	}
	if !strings.Contains(string(resp.Body), "not enabled on this HTTP MCP profile") {
		t.Fatalf("expected HTTP write tool rejection, got %s", resp.Body)
	}
}

func TestProviderTrackerHTTPProfileAdvertisesOnlySafeTrackerTools(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}
	profile := ProviderTrackerHTTPProfile()

	resp := s.HandleHTTPMessageWithOptions(
		context.Background(),
		[]byte(`{"jsonrpc":"2.0","id":4,"method":"tools/list"}`),
		actor,
		HTTPOptions{Profile: profile},
	)
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", resp.Status, resp.Body)
	}
	var envelope struct {
		Result struct {
			Tools []struct {
				Name        string              `json:"name"`
				Annotations httpToolAnnotations `json:"annotations"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &envelope); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	want := map[string]bool{
		"backlog.readiness":          true,
		"work_items.list":            true,
		"work_items.get":             true,
		"work_items.create":          true,
		"work_items.spawn_child":     true,
		"work_items.append_event":    true,
		"work_items.update_metadata": true,
		"work_items.transition":      true,
	}
	if len(envelope.Result.Tools) != len(want) {
		t.Fatalf("tools/list count = %d, want %d: %s", len(envelope.Result.Tools), len(want), resp.Body)
	}
	for _, tool := range envelope.Result.Tools {
		if !want[tool.Name] {
			t.Errorf("unexpected provider tracker tool %q", tool.Name)
		}
		delete(want, tool.Name)
		if strings.HasPrefix(tool.Name, "work_items.") && tool.Name != "work_items.list" && tool.Name != "work_items.get" {
			if !tool.Annotations.IdempotentHint || tool.Annotations.OpenWorldHint || tool.Annotations.ReadOnlyHint {
				t.Errorf("mutation annotations for %s = %+v", tool.Name, tool.Annotations)
			}
		} else if !tool.Annotations.ReadOnlyHint || tool.Annotations.OpenWorldHint {
			t.Errorf("read annotations for %s = %+v", tool.Name, tool.Annotations)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing provider tracker tools: %v", want)
	}
}

func TestProviderTrackerHTTPProfileRejectsHiddenToolsBeforeDispatch(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}
	profile := ProviderTrackerHTTPProfile()
	hidden := []string{
		"inbox.capture",
		"feed.read",
		"registry.list",
		"registry.define_tropism",
		"projections.define",
		"policy_profile.switch",
		"approvals.request",
		"approvals.decide",
		"connectors.http_request",
		"convergence.propose_checks",
		"registry.activate_cultivar",
	}
	for _, name := range hidden {
		t.Run(name, func(t *testing.T) {
			resp := s.HandleHTTPMessageWithOptions(
				context.Background(),
				[]byte(`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"`+name+`","arguments":{}}}`),
				actor,
				HTTPOptions{Profile: profile},
			)
			if !strings.Contains(string(resp.Body), "not enabled on this HTTP MCP profile") {
				t.Fatalf("hidden call was not rejected before dispatch: %s", resp.Body)
			}
		})
	}
}

func TestProviderTrackerHTTPProfileRejectsLatentExecutionAuthority(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceHuman}
	profile := ProviderTrackerHTTPProfile()
	cases := []struct {
		name string
		call string
	}{
		{"create waved through", `{"name":"work_items.create","arguments":{"title":"x","human_review_status":"waved_through","idempotency_key":"x"}}`},
		{"create cultivar", `{"name":"work_items.create","arguments":{"title":"x","human_review_status":"blocked","cultivar":"worker@1","idempotency_key":"x"}}`},
		{"create running", `{"name":"work_items.create","arguments":{"title":"x","state":"running","human_review_status":"blocked","idempotency_key":"x"}}`},
		{"metadata wave through", `{"name":"work_items.update_metadata","arguments":{"id":"00000000-0000-0000-0000-000000000001","suggested_convergence_checks":[],"human_review_status":"waved_through","idempotency_key":"x"}}`},
		{"transition running", `{"name":"work_items.transition","arguments":{"id":"00000000-0000-0000-0000-000000000001","to":"running","idempotency_key":"x"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.HandleHTTPMessageWithOptions(context.Background(), []byte(`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":`+tc.call+`}`), actor, HTTPOptions{Profile: profile})
			if !strings.Contains(string(resp.Body), "tracker_execution_authority_denied") {
				t.Fatalf("execution-shaped call was not rejected: %s", resp.Body)
			}
		})
	}
}

func TestAcceptsStreamableHTTPPostRequiresJSONAndSSE(t *testing.T) {
	if !AcceptsStreamableHTTPPost("application/json, text/event-stream") {
		t.Fatal("expected application/json + text/event-stream to be accepted")
	}
	if AcceptsStreamableHTTPPost("application/json") {
		t.Fatal("expected missing text/event-stream to be rejected")
	}
}
