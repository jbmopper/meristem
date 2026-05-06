package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// roundtrip runs a single request through the dispatcher without going
// through a real stdio loop. It reads back the one-line response.
func roundtrip(t *testing.T, s *Server, req string) rpcMessage {
	t.Helper()
	out := &bytes.Buffer{}
	if err := s.Run(context.Background(), strings.NewReader(req+"\n"), out); err != nil {
		t.Fatalf("Run returned: %v", err)
	}
	line := strings.TrimRight(out.String(), "\n")
	if line == "" {
		t.Fatalf("expected response, got empty stdout")
	}
	if strings.Contains(line, "\n") {
		t.Fatalf("expected exactly one response line, got %q", line)
	}
	var msg rpcMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		t.Fatalf("response is not valid JSON: %v\nraw: %s", err, line)
	}
	return msg
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	s.actor = domain.Token{ID: uuid.New(), Source: domain.SourceHuman, IsRoot: false, Name: "test"}
	return s
}

func TestServer_Initialize_EchoesProtocolVersion(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	if resp.Error != nil {
		t.Fatalf("initialize returned error: %+v", resp.Error)
	}
	var result struct {
		ProtocolVersion string         `json:"protocolVersion"`
		ServerInfo      map[string]any `json:"serverInfo"`
		Capabilities    map[string]any `json:"capabilities"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.ProtocolVersion != "2024-11-05" {
		t.Errorf("expected echoed protocolVersion 2024-11-05, got %q", result.ProtocolVersion)
	}
	if result.ServerInfo["name"] != "meristem-test" {
		t.Errorf("unexpected serverInfo.name: %v", result.ServerInfo["name"])
	}
	if _, ok := result.Capabilities["tools"]; !ok {
		t.Errorf("missing tools capability: %+v", result.Capabilities)
	}
}

func TestServer_Initialize_FallsBackToServerVersion(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(resp.Result, &result)
	if result.ProtocolVersion != protocolVersion {
		t.Errorf("expected fallback %q, got %q", protocolVersion, result.ProtocolVersion)
	}
}

func TestServer_NotificationsProduceNoResponse(t *testing.T) {
	s := newTestServer(t)
	out := &bytes.Buffer{}
	in := strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n")
	if err := s.Run(context.Background(), in, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("notification produced response: %q", out.String())
	}
}

func TestServer_ToolsList_AdvertisesAllTools(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if resp.Error != nil {
		t.Fatalf("tools/list returned error: %+v", resp.Error)
	}
	var result struct {
		Tools []toolDescriptor `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}

	expected := []string{
		"inbox.capture",
		"feed.read",
		"work_items.list",
		"work_items.get",
		"work_items.create",
		"work_items.spawn_child",
		"work_items.append_event",
		"work_items.update_metadata",
		"work_items.transition",
	}
	if len(result.Tools) != len(expected) {
		t.Fatalf("expected %d tools, got %d (%v)", len(expected), len(result.Tools), toolNames(result.Tools))
	}
	got := make(map[string]toolDescriptor, len(result.Tools))
	for _, tool := range result.Tools {
		got[tool.Name] = tool
	}
	for _, name := range expected {
		tool, ok := got[name]
		if !ok {
			t.Errorf("missing expected tool %q", name)
			continue
		}
		if tool.Description == "" {
			t.Errorf("tool %q has no description", name)
		}
		if tool.InputSchema == nil {
			t.Errorf("tool %q has no input schema", name)
		}
	}
}

func TestServer_ToolsList_CursorModeAdvertisesUnderscoreAliases(t *testing.T) {
	s := newTestServer(t)
	s.SetToolNameMode(ToolNameModeCursor)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if resp.Error != nil {
		t.Fatalf("tools/list returned error: %+v", resp.Error)
	}
	var result struct {
		Tools []toolDescriptor `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}

	expected := []string{
		"inbox_capture",
		"feed_read",
		"work_items_list",
		"work_items_get",
		"work_items_create",
		"work_items_spawn_child",
		"work_items_append_event",
		"work_items_update_metadata",
		"work_items_transition",
	}
	if len(result.Tools) != len(expected) {
		t.Fatalf("expected %d tools, got %d (%v)", len(expected), len(result.Tools), toolNames(result.Tools))
	}
	got := make(map[string]bool, len(result.Tools))
	for _, tool := range result.Tools {
		got[tool.Name] = true
		if strings.Contains(tool.Name, ".") {
			t.Errorf("Cursor-compatible tool name still contains dot: %q", tool.Name)
		}
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("missing Cursor-compatible tool %q", name)
		}
	}
}

func TestServer_CallTool_AcceptsCursorAlias(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"feed_read","arguments":{}}}`)
	if resp.Error != nil {
		t.Fatalf("expected transport success, got error %+v", resp.Error)
	}
	var result struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected service-missing tool error after alias dispatch, got %+v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "feed service not configured") {
		t.Errorf("alias did not route to feed.read handler: %+v", result.Content)
	}
}

func TestServer_UnknownMethod_ReturnsMethodNotFound(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":3,"method":"does/not/exist"}`)
	if resp.Error == nil {
		t.Fatalf("expected error response, got result %s", string(resp.Result))
	}
	if resp.Error.Code != errCodeMethodNotFound {
		t.Errorf("expected code %d, got %d", errCodeMethodNotFound, resp.Error.Code)
	}
}

func TestServer_ParseError_ReturnsParseError(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{not valid json`)
	if resp.Error == nil {
		t.Fatalf("expected error response, got result %s", string(resp.Result))
	}
	if resp.Error.Code != errCodeParse {
		t.Errorf("expected code %d, got %d", errCodeParse, resp.Error.Code)
	}
	if string(resp.ID) != "null" {
		t.Errorf("expected null id on parse error, got %s", string(resp.ID))
	}
}

func TestServer_CallToolWithoutAuth_ReturnsToolError(t *testing.T) {
	// New() doesn't set an actor; Authenticate is normally called between
	// New and Run. Verify that skipping it surfaces as an isError tool
	// result, not a transport-level success that silently appends events
	// without attribution.
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"feed.read","arguments":{}}}`)
	if resp.Error != nil {
		t.Fatalf("expected transport success, got error %+v", resp.Error)
	}
	var result struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if !result.IsError {
		t.Errorf("expected isError=true when unauthenticated, got %+v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "authenticated") {
		t.Errorf("unexpected error text: %+v", result.Content)
	}
}

func TestServer_CallToolUnknown_ReturnsMethodNotFound(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"nope","arguments":{}}}`)
	if resp.Error == nil {
		t.Fatalf("expected error response, got result %s", string(resp.Result))
	}
	if resp.Error.Code != errCodeMethodNotFound {
		t.Errorf("expected method-not-found code %d, got %d", errCodeMethodNotFound, resp.Error.Code)
	}
}

func TestServer_CallTool_ServiceMissing_ReturnsToolError(t *testing.T) {
	// All Deps are nil; tool handlers should refuse with isError=true.
	// This proves the dispatcher routes to the handler and that handlers
	// guard against missing dependencies (which is the wiring failure
	// mode if cmd/meristem forgets a service).
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"feed.read","arguments":{}}}`)
	if resp.Error != nil {
		t.Fatalf("expected transport success, got error %+v", resp.Error)
	}
	var result struct {
		IsError bool `json:"isError"`
	}
	_ = json.Unmarshal(resp.Result, &result)
	if !result.IsError {
		t.Errorf("expected isError=true when feed service is nil")
	}
}

func TestServer_DecodeArgs_RejectsUnknownFields(t *testing.T) {
	var args struct {
		Text string `json:"text"`
	}
	err := decodeArgs(json.RawMessage(`{"text":"hi","extra":true}`), &args)
	if err == nil {
		t.Fatalf("expected decode error for unknown field")
	}
	if !strings.Contains(err.Error(), "extra") {
		t.Errorf("expected error to mention unknown field name, got %v", err)
	}
}

func TestServer_DecodeArgs_AcceptsEmpty(t *testing.T) {
	var args struct {
		Text string `json:"text"`
	}
	if err := decodeArgs(nil, &args); err != nil {
		t.Errorf("expected nil error for empty raw, got %v", err)
	}
}

func TestServer_ParseUUID_RejectsBadInput(t *testing.T) {
	if _, err := parseUUID("not-a-uuid", "id"); err == nil {
		t.Fatalf("expected parse error")
	}
	if _, err := parseUUID(uuid.New().String(), "id"); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestAsTransportError_PassesThroughRPCError(t *testing.T) {
	want := rpcErrorf(errCodeInvalidRequest, "boom")
	got := asTransportError(want)
	if got != want {
		t.Errorf("expected pass-through, got %+v", got)
	}
	wrapped := errors.New("plain error")
	got = asTransportError(wrapped)
	if got == nil || got.Code != errCodeInternal {
		t.Errorf("expected internal error wrap, got %+v", got)
	}
}

func toolNames(tools []toolDescriptor) []string {
	out := make([]string, len(tools))
	for i, t := range tools {
		out[i] = t.Name
	}
	return out
}
