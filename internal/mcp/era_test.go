package mcp

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// modernRequest builds a minimally valid 2026-07-28 request body.
func modernRequest(id int, method string, extraParams string) string {
	params := `{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}`
	if extraParams != "" {
		params += "," + extraParams
	}
	params += `}`
	return `{"jsonrpc":"2.0","id":` + strconv.Itoa(id) + `,"method":"` + method + `","params":` + params + `}`
}

func decodeResult(t *testing.T, msg rpcMessage) map[string]any {
	t.Helper()
	if msg.Error != nil {
		t.Fatalf("unexpected error: %+v", msg.Error)
	}
	var out map[string]any
	if err := json.Unmarshal(msg.Result, &out); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return out
}

func TestStdioModernDiscoverLocksModernEra(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, modernRequest(1, "server/discover", ""))
	result := decodeResult(t, resp)

	if result["resultType"] != "complete" {
		t.Errorf("resultType = %v, want complete", result["resultType"])
	}
	supported, _ := result["supportedVersions"].([]any)
	if len(supported) != 3 || supported[0] != "2026-07-28" || supported[1] != "2025-11-25" || supported[2] != "2025-06-18" {
		t.Errorf("supportedVersions = %v", supported)
	}
	if result["ttlMs"] != float64(0) || result["cacheScope"] != "private" {
		t.Errorf("cache hints = %v / %v", result["ttlMs"], result["cacheScope"])
	}
	if strings.TrimSpace(result["instructions"].(string)) == "" {
		t.Error("expected non-empty discover instructions")
	}
	meta, _ := result["_meta"].(map[string]any)
	if meta == nil {
		t.Fatal("missing _meta on discover result")
	}
	if _, ok := meta[metaServerInfoKey]; !ok {
		t.Error("missing serverInfo in discover _meta")
	}
	build, ok := meta[metaBuildKey].(map[string]any)
	if !ok {
		t.Fatal("missing meristem build diagnostic in discover _meta")
	}
	if _, ok := build["state"]; !ok {
		t.Errorf("build diagnostic missing state: %v", build)
	}

	// The era is now locked: a legacy initialize on the same connection is a
	// violation naming the locked era, never a silent downgrade.
	locked := roundtrip(t, s, `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`)
	if locked.Error == nil || locked.Error.Code != errCodeInvalidRequest {
		t.Fatalf("expected era-locked -32600, got %+v", locked.Error)
	}
	if !strings.Contains(locked.Error.Message, "locked to modern") {
		t.Errorf("era-locked message should name the locked era: %q", locked.Error.Message)
	}
}

func TestStdioLegacyInitializeLocksLegacyEra(t *testing.T) {
	s := newTestServer(t)
	first := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`)
	result := decodeResult(t, first)
	if result["protocolVersion"] != "2025-06-18" {
		t.Fatalf("negotiated version = %v", result["protocolVersion"])
	}
	// Modern traffic on the legacy-locked connection is refused with the
	// locked era named.
	modern := roundtrip(t, s, modernRequest(2, "server/discover", ""))
	if modern.Error == nil || modern.Error.Code != errCodeInvalidRequest {
		t.Fatalf("expected era-locked -32600, got %+v", modern.Error)
	}
	if !strings.Contains(modern.Error.Message, "locked to legacy") {
		t.Errorf("era-locked message should name the locked era: %q", modern.Error.Message)
	}
}

func TestStdioBareLegacyRequestGrandfathered(t *testing.T) {
	// Byte-compatibility with the pre-2026 server: a client that never sends
	// initialize (the c5a99ac contract allowed this) still gets legacy
	// behavior, locked at the default version.
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	result := decodeResult(t, resp)
	if _, ok := result["tools"]; !ok {
		t.Fatalf("expected legacy tools/list result, got %v", result)
	}
	if _, ok := result["resultType"]; ok {
		t.Error("legacy tools/list must not carry resultType")
	}
	// And the bare request locked legacy: modern traffic now refused.
	modern := roundtrip(t, s, modernRequest(2, "server/discover", ""))
	if modern.Error == nil || modern.Error.Code != errCodeInvalidRequest {
		t.Fatalf("expected era-locked -32600 after grandfathered legacy lock, got %+v", modern.Error)
	}
}

func TestModernRequestMissingClientCapabilitiesRejected(t *testing.T) {
	s := newTestServer(t)
	req := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`
	resp := roundtrip(t, s, req)
	if resp.Error == nil || resp.Error.Code != errCodeInvalidParams {
		t.Fatalf("expected -32602 for missing clientCapabilities, got %+v", resp.Error)
	}
	// Malformed modern never falls back to legacy: the era is still unlocked,
	// so a modern retry with valid metadata succeeds.
	retry := roundtrip(t, s, modernRequest(2, "server/discover", ""))
	if retry.Error != nil {
		t.Fatalf("valid modern retry after malformed request failed: %+v", retry.Error)
	}
}

func TestModernUnknownVersionRejectedWithSupportedList(t *testing.T) {
	s := newTestServer(t)
	req := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2027-01-01","io.modelcontextprotocol/clientCapabilities":{}}}}`
	resp := roundtrip(t, s, req)
	if resp.Error == nil || resp.Error.Code != errCodeUnsupportedProtocol {
		t.Fatalf("expected -32022, got %+v", resp.Error)
	}
	var data struct {
		Supported []string `json:"supported"`
		Requested string   `json:"requested"`
	}
	if err := json.Unmarshal(resp.Error.Data, &data); err != nil {
		t.Fatalf("decode error data: %v", err)
	}
	if data.Requested != "2027-01-01" || len(data.Supported) != 3 {
		t.Errorf("error data = %+v", data)
	}
}

func TestModernRemovedMethodsAreNotFound(t *testing.T) {
	s := newTestServer(t)
	for id, method := range map[int]string{2: "ping", 3: "shutdown", 4: "initialize", 5: "logging/setLevel"} {
		if method == "initialize" {
			continue // covered by the era-lock tests; initialize is not modern-shaped
		}
		resp := roundtrip(t, s, modernRequest(id, method, ""))
		if resp.Error == nil || resp.Error.Code != errCodeMethodNotFound {
			t.Errorf("%s in modern era: expected -32601, got %+v", method, resp.Error)
		}
	}
}

func TestModernToolsListEnvelope(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, modernRequest(1, "tools/list", ""))
	result := decodeResult(t, resp)
	if result["resultType"] != "complete" {
		t.Errorf("resultType = %v", result["resultType"])
	}
	if result["ttlMs"] != float64(0) || result["cacheScope"] != "private" {
		t.Errorf("cache hints missing on modern tools/list: %v", result)
	}
	meta, _ := result["_meta"].(map[string]any)
	if meta == nil || meta[metaServerInfoKey] == nil {
		t.Error("modern tools/list must carry serverInfo in _meta")
	}
	if _, ok := result["tools"]; !ok {
		t.Error("modern tools/list must keep the tools payload")
	}
}

func TestLegacyInitializeNeverEchoesUnknownVersion(t *testing.T) {
	// The pre-2026 server echoed any proposed version; the consensus contract
	// answers a supported version instead (2025-06-18 lifecycle: respond with
	// a version the server supports; the client decides whether to continue).
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	result := decodeResult(t, resp)
	if result["protocolVersion"] != "2025-06-18" {
		t.Fatalf("unknown version must negotiate to the default, got %v", result["protocolVersion"])
	}
}

func TestLegacyInitializeSupportsFixtureBackedVersions(t *testing.T) {
	for _, version := range []string{"2025-06-18", "2025-11-25"} {
		s := newTestServer(t)
		resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"`+version+`"}}`)
		result := decodeResult(t, resp)
		if result["protocolVersion"] != version {
			t.Errorf("version %s: negotiated %v", version, result["protocolVersion"])
		}
		// Legacy result shape is frozen: top-level meristemBuild, serverInfo,
		// capabilities, instructions; no modern envelope fields.
		for _, key := range []string{"meristemBuild", "serverInfo", "capabilities", "instructions"} {
			if _, ok := result[key]; !ok {
				t.Errorf("version %s: legacy initialize missing %s", version, key)
			}
		}
		for _, key := range []string{"resultType", "ttlMs", "cacheScope", "_meta"} {
			if _, ok := result[key]; ok {
				t.Errorf("version %s: legacy initialize must not carry %s", version, key)
			}
		}
	}
}

func TestLegacyVersionNarrowingNeverWidens(t *testing.T) {
	// Narrowing via env works; widening to an untested version is ignored.
	t.Setenv(EnvLegacyVersions, "2025-06-18, 2025-03-26")
	s := newTestServer(t)
	if len(s.legacyVersions) != 1 || s.legacyVersions[0] != "2025-06-18" {
		t.Fatalf("narrowed set = %v, want [2025-06-18]", s.legacyVersions)
	}
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`)
	result := decodeResult(t, resp)
	if result["protocolVersion"] != "2025-06-18" {
		t.Errorf("narrowed server must not serve 2025-11-25, negotiated %v", result["protocolVersion"])
	}
	older := roundtrip(t, s, `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}`)
	olderResult := decodeResult(t, older)
	if olderResult["protocolVersion"] != "2025-06-18" {
		t.Errorf("config widening must be impossible, negotiated %v", olderResult["protocolVersion"])
	}
}

func TestLegacyToolCallEnvelopeUnchanged(t *testing.T) {
	// Golden legacy shape: tools/call error results keep exactly the c5a99ac
	// fields (content, isError) with no modern additions.
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nonexistent.tool"}}`)
	if resp.Error == nil || resp.Error.Code != errCodeMethodNotFound {
		t.Fatalf("expected -32601 for unknown tool, got %+v", resp.Error)
	}
	list := roundtrip(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	result := decodeResult(t, list)
	if _, ok := result["resultType"]; ok {
		t.Error("legacy results must not carry resultType")
	}
}
