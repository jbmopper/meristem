package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/mcp"
)

func TestHandleMCPPostDispatchesAuthenticatedActor(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content type = %q", got)
	}
	if got := rec.Header().Get(mcp.HeaderProtocolVersion); got != "2025-06-18" {
		t.Fatalf("protocol header = %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"serverInfo"`) {
		t.Fatalf("initialize response missing serverInfo: %s", rec.Body.String())
	}
}

func TestHandleMCPPostRejectsMissingStreamableAccept(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Accept", "application/json")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want 406 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid_accept") {
		t.Fatalf("expected invalid_accept error, got %s", rec.Body.String())
	}
}

func TestHandleMCPPostExposesReadOnlyToolSurface(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceHuman}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "work_items.create") || strings.Contains(body, "work_items.transition") || strings.Contains(body, "convergence.propose_checks") {
		t.Fatalf("HTTP /mcp leaked write tools before idempotency contract: %s", body)
	}
	if !strings.Contains(body, "feed.read") || !strings.Contains(body, "work_items.get") {
		t.Fatalf("HTTP /mcp omitted expected read tools: %s", body)
	}
}

func TestHandleMCPGetReturns405UntilSSEExists(t *testing.T) {
	s := New(nil, nil)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req = req.WithContext(auth.WithToken(req.Context(), domain.Token{ID: uuid.New(), Source: domain.SourceAgent}))
	rec := httptest.NewRecorder()

	s.handleMCP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "mcp_sse_unavailable") {
		t.Fatalf("expected mcp_sse_unavailable error, got %s", rec.Body.String())
	}
}
