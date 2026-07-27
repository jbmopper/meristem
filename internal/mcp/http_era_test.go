package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func modernTransport(method, name string) *HTTPTransportContext {
	t := &HTTPTransportContext{
		ProtocolVersion:    modernProtocolVersion,
		HasProtocolVersion: true,
		McpMethod:          method,
		HasMcpMethod:       true,
	}
	if name != "" {
		t.McpName = name
		t.HasMcpName = true
	}
	return t
}

func httpEra(t *testing.T, s *Server, body string, transport *HTTPTransportContext) HTTPResponse {
	t.Helper()
	return s.HandleHTTPMessageWithOptions(context.Background(), []byte(body), s.actorToken(), HTTPOptions{Transport: transport})
}

func decodeHTTPBody(t *testing.T, resp HTTPResponse) rpcMessage {
	t.Helper()
	var msg rpcMessage
	if err := json.Unmarshal(resp.Body, &msg); err != nil {
		t.Fatalf("decode http body: %v\nraw: %s", err, resp.Body)
	}
	return msg
}

func TestHTTPModernRequestServed(t *testing.T) {
	s := newTestServer(t)
	resp := httpEra(t, s, modernRequest(1, "server/discover", ""), modernTransport("server/discover", ""))
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	if resp.ProtocolVersion != modernProtocolVersion {
		t.Errorf("response protocol version = %q", resp.ProtocolVersion)
	}
	msg := decodeHTTPBody(t, resp)
	result := decodeResult(t, msg)
	if result["resultType"] != "complete" {
		t.Errorf("resultType = %v", result["resultType"])
	}
}

func TestHTTPModernMetaWithoutHeaderRejected(t *testing.T) {
	s := newTestServer(t)
	resp := httpEra(t, s, modernRequest(1, "server/discover", ""), &HTTPTransportContext{
		McpMethod: "server/discover", HasMcpMethod: true,
	})
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.Status)
	}
	msg := decodeHTTPBody(t, resp)
	if msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("expected -32020, got %+v", msg.Error)
	}
}

func TestHTTPHeaderBodyVersionMismatchRejected(t *testing.T) {
	s := newTestServer(t)
	transport := modernTransport("server/discover", "")
	transport.ProtocolVersion = "2025-06-18"
	resp := httpEra(t, s, modernRequest(1, "server/discover", ""), transport)
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.Status)
	}
	msg := decodeHTTPBody(t, resp)
	if msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("expected -32020 on header/body mismatch, got %+v", msg.Error)
	}
}

func TestHTTPModernMcpMethodHeaderEnforced(t *testing.T) {
	s := newTestServer(t)
	// Missing Mcp-Method.
	transport := modernTransport("server/discover", "")
	transport.HasMcpMethod = false
	transport.McpMethod = ""
	resp := httpEra(t, s, modernRequest(1, "server/discover", ""), transport)
	msg := decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("missing Mcp-Method: status=%d err=%+v", resp.Status, msg.Error)
	}
	// Mismatched Mcp-Method.
	resp = httpEra(t, s, modernRequest(2, "server/discover", ""), modernTransport("tools/list", ""))
	msg = decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("mismatched Mcp-Method: status=%d err=%+v", resp.Status, msg.Error)
	}
}

func TestHTTPModernMcpNameRequiredOnToolsCall(t *testing.T) {
	s := newTestServer(t)
	body := modernRequest(1, "tools/call", `"name":"work_items.list","arguments":{}`)
	resp := httpEra(t, s, body, modernTransport("tools/call", ""))
	msg := decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("missing Mcp-Name: status=%d err=%+v", resp.Status, msg.Error)
	}
	resp = httpEra(t, s, body, modernTransport("tools/call", "other.tool"))
	msg = decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeHeaderMismatch {
		t.Fatalf("mismatched Mcp-Name: status=%d err=%+v", resp.Status, msg.Error)
	}
}

func TestHTTPModernHeaderWithoutMetaRejected(t *testing.T) {
	s := newTestServer(t)
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, modernTransport("tools/list", ""))
	msg := decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeInvalidParams {
		t.Fatalf("modern header without _meta: status=%d err=%+v", resp.Status, msg.Error)
	}
}

func TestHTTPLegacyHeaderServesLegacy(t *testing.T) {
	s := newTestServer(t)
	transport := &HTTPTransportContext{ProtocolVersion: "2025-06-18", HasProtocolVersion: true}
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, transport)
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	if resp.ProtocolVersion != "2025-06-18" {
		t.Errorf("response protocol version = %q", resp.ProtocolVersion)
	}
	msg := decodeHTTPBody(t, resp)
	result := decodeResult(t, msg)
	if _, ok := result["resultType"]; ok {
		t.Error("legacy-headed request must not receive a modern envelope")
	}
}

func TestHTTPUnknownVersionHeaderRejected(t *testing.T) {
	s := newTestServer(t)
	transport := &HTTPTransportContext{ProtocolVersion: "2025-03-26", HasProtocolVersion: true}
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, transport)
	msg := decodeHTTPBody(t, resp)
	if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeUnsupportedProtocol {
		t.Fatalf("unknown version header: status=%d err=%+v", resp.Status, msg.Error)
	}
}

func TestHTTPInitializeWithoutHeaderOpensLegacy(t *testing.T) {
	s := newTestServer(t)
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`, &HTTPTransportContext{})
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	if resp.ProtocolVersion != "2025-11-25" {
		t.Errorf("negotiated response version = %q", resp.ProtocolVersion)
	}
}

func TestHTTPBareRequestWithoutHeaderGrandfathered(t *testing.T) {
	// The current provider clients send no MCP-Protocol-Version on tool
	// calls; they are grandfathered onto the default legacy version exactly
	// like the stdio bare-request rule. Telemetry tags them "(absent)" so the
	// removal gate can measure headerless traffic; modern strictness is
	// untouched.
	s := newTestServer(t)
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, &HTTPTransportContext{})
	if resp.Status != http.StatusOK {
		t.Fatalf("headerless legacy request: status=%d body=%s", resp.Status, resp.Body)
	}
	if resp.ProtocolVersion != "2025-06-18" {
		t.Errorf("grandfathered response version = %q", resp.ProtocolVersion)
	}
	msg := decodeHTTPBody(t, resp)
	result := decodeResult(t, msg)
	if _, ok := result["resultType"]; ok {
		t.Error("grandfathered request must keep the legacy envelope")
	}
}

func TestHTTPModernErrorResponsesCarryModernVersion(t *testing.T) {
	// MCP26-B3: an era established as modern (by header or by body metadata)
	// labels even its error responses with the modern version.
	s := newTestServer(t)
	// Modern header, missing _meta.
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, modernTransport("tools/list", ""))
	if resp.Status != http.StatusBadRequest || resp.ProtocolVersion != modernProtocolVersion {
		t.Errorf("modern header without _meta: status=%d version=%q", resp.Status, resp.ProtocolVersion)
	}
	// Modern _meta, missing header.
	resp = httpEra(t, s, modernRequest(2, "server/discover", ""), &HTTPTransportContext{McpMethod: "server/discover", HasMcpMethod: true})
	if resp.Status != http.StatusBadRequest || resp.ProtocolVersion != modernProtocolVersion {
		t.Errorf("modern _meta without header: status=%d version=%q", resp.Status, resp.ProtocolVersion)
	}
	// Malformed modern metadata.
	resp = httpEra(t, s, `{"jsonrpc":"2.0","id":3,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":"broken"}}}`, modernTransport("server/discover", ""))
	if resp.Status != http.StatusBadRequest || resp.ProtocolVersion != modernProtocolVersion {
		t.Errorf("malformed modern metadata: status=%d version=%q", resp.Status, resp.ProtocolVersion)
	}
}

func TestHTTPRejectedModernNotificationIsHTTPError(t *testing.T) {
	// MCP26-B4: a notification the server cannot accept is an HTTP-level
	// error with no JSON-RPC body; 202 is reserved for accepted ones.
	s := newTestServer(t)
	// Malformed metadata (missing clientCapabilities), no id -> notification.
	badMeta := `{"jsonrpc":"2.0","method":"notifications/something","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`
	resp := httpEra(t, s, badMeta, modernTransport("notifications/something", ""))
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("rejected notification status = %d, want 400", resp.Status)
	}
	if len(resp.Body) != 0 {
		t.Errorf("rejected notification must have no JSON-RPC body, got %s", resp.Body)
	}
	if resp.ProtocolVersion != modernProtocolVersion {
		t.Errorf("rejected modern notification version = %q", resp.ProtocolVersion)
	}
	// Malformed header (missing version header on modern-shaped notification).
	noHeader := `{"jsonrpc":"2.0","method":"notifications/something","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	resp = httpEra(t, s, noHeader, &HTTPTransportContext{McpMethod: "notifications/something", HasMcpMethod: true})
	if resp.Status != http.StatusBadRequest || len(resp.Body) != 0 {
		t.Errorf("headerless modern notification: status=%d body=%s", resp.Status, resp.Body)
	}
	// An accepted modern notification still gets 202.
	ok := modernRequestNotification("notifications/something")
	resp = httpEra(t, s, ok, modernTransport("notifications/something", ""))
	if resp.Status != http.StatusAccepted {
		t.Errorf("accepted notification status = %d, want 202", resp.Status)
	}
}

func modernRequestNotification(method string) string {
	return `{"jsonrpc":"2.0","method":"` + method + `","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
}

func TestHTTPHeaderlessGrandfatherHonorsNarrowing(t *testing.T) {
	// MCP26-B1: the headerless compatibility default is the oldest SERVED
	// version, not a hard-coded one; an empty served set fails closed.
	t.Setenv(EnvLegacyVersions, "2025-11-25")
	s := newTestServer(t)
	resp := httpEra(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, &HTTPTransportContext{})
	if resp.Status != http.StatusOK || resp.ProtocolVersion != "2025-11-25" {
		t.Errorf("narrowed grandfather: status=%d version=%q", resp.Status, resp.ProtocolVersion)
	}
}

func TestHTTPHeaderlessFailsClosedWhenLegacyDisabled(t *testing.T) {
	t.Setenv(EnvLegacyVersions, "2025-03-26")
	s := newTestServer(t)
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`,
	} {
		resp := httpEra(t, s, body, &HTTPTransportContext{})
		msg := decodeHTTPBody(t, resp)
		if resp.Status != http.StatusBadRequest || msg.Error == nil || msg.Error.Code != errCodeUnsupportedProtocol {
			t.Errorf("legacy-disabled request: status=%d err=%+v", resp.Status, msg.Error)
		}
	}
}

func TestHTTPModernUnknownMethodIs404(t *testing.T) {
	s := newTestServer(t)
	resp := httpEra(t, s, modernRequest(1, "resources/list", ""), modernTransport("resources/list", ""))
	if resp.Status != http.StatusNotFound {
		t.Fatalf("status = %d", resp.Status)
	}
	msg := decodeHTTPBody(t, resp)
	if msg.Error == nil || msg.Error.Code != errCodeMethodNotFound {
		t.Fatalf("expected -32601, got %+v", msg.Error)
	}
}

func TestHTTPNilTransportKeepsLegacyContract(t *testing.T) {
	// In-process callers that predate the era classifier see the exact
	// pre-2026 behavior: no header requirements, no envelopes, empty
	// response version (the transport's historical default applies).
	s := newTestServer(t)
	resp := s.HandleHTTPMessageWithOptions(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`), s.actorToken(), HTTPOptions{})
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d", resp.Status)
	}
	if resp.ProtocolVersion != "" {
		t.Errorf("nil-transport response version = %q, want empty", resp.ProtocolVersion)
	}
	msg := decodeHTTPBody(t, resp)
	result := decodeResult(t, msg)
	if _, ok := result["resultType"]; ok {
		t.Error("nil-transport requests must keep the legacy envelope")
	}
}
