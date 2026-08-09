package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/mcp"
)

// MCP26-B3-R2 / MCP26-B4-R2 regressions: requests rejected BEFORE era
// classification (JSON parse, invalid jsonrpc, missing method) must still be
// labeled with the era the transport header establishes, and invalid id-less
// messages must receive an HTTP error, never 202. These run at the API level
// so the full header-in/header-out path is exercised.

func mcpEarlyErrorPost(t *testing.T, body string, modernHeader bool) *httptest.ResponseRecorder {
	t.Helper()
	s := New(nil, nil)
	allowMCPWriteDeadlines(s)
	s.mcpServer = mcp.New(mcp.Deps{}, mcp.ServerInfo{Name: "meristem-test", Version: "test"}, nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Accept", "application/json, text/event-stream")
	if modernHeader {
		req.Header.Set(mcp.HeaderProtocolVersion, "2026-07-28")
		req.Header.Set("Mcp-Method", "server/discover")
	}
	req = req.WithContext(auth.WithToken(req.Context(), providerActor(t, access.ProviderOwnerTrackerReadV1)))
	rec := httptest.NewRecorder()
	s.handleMCP(rec, req)
	return rec
}

func TestMCPEarlyErrorsCarryModernVersionHeader(t *testing.T) {
	cases := map[string]string{
		"malformed JSON":  `{"jsonrpc":`,
		"invalid jsonrpc": `{"jsonrpc":"1.0","id":7,"method":"server/discover"}`,
		"missing method":  `{"jsonrpc":"2.0","id":8}`,
	}
	for name, body := range cases {
		rec := mcpEarlyErrorPost(t, body, true)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
		if got := rec.Header().Get(mcp.HeaderProtocolVersion); got != "2026-07-28" {
			t.Errorf("%s: response version = %q, want 2026-07-28", name, got)
		}
	}
}

func TestMCPEarlyErrorsDefaultLegacyVersionWithoutModernHeader(t *testing.T) {
	rec := mcpEarlyErrorPost(t, `{"jsonrpc":`, false)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := rec.Header().Get(mcp.HeaderProtocolVersion); got != "2025-06-18" {
		t.Errorf("response version = %q, want legacy default", got)
	}
}

func TestMCPInvalidIDLessMessageIsHTTPErrorNot202(t *testing.T) {
	// jsonrpc 1.0, no id, modern header and valid modern metadata: an invalid
	// message, not a notification. 202 is reserved for accepted notifications.
	body := `{"jsonrpc":"1.0","method":"notifications/x","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	rec := mcpEarlyErrorPost(t, body, true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := rec.Header().Get(mcp.HeaderProtocolVersion); got != "2026-07-28" {
		t.Errorf("response version = %q, want 2026-07-28", got)
	}
	if !strings.Contains(rec.Body.String(), "jsonrpc must be") {
		t.Errorf("expected invalid-request error body, got %s", rec.Body.String())
	}
	// The same invalid shape without any modern signal is still an HTTP
	// error on the legacy-labeled path.
	legacy := mcpEarlyErrorPost(t, `{"jsonrpc":"1.0","method":"notifications/x"}`, false)
	if legacy.Code != http.StatusBadRequest {
		t.Errorf("legacy-path invalid id-less message: status = %d, want 400", legacy.Code)
	}
}
