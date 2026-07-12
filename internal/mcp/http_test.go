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
	if !strings.Contains(string(resp.Body), "not enabled on HTTP MCP transport") {
		t.Fatalf("expected HTTP write tool rejection, got %s", resp.Body)
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
