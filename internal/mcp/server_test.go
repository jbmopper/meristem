package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
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

// TestServer_Run_AcceptsMultilineJSON verifies that pretty-printed (multiline)
// JSON-RPC requests are parsed correctly. Some MCP clients — particularly
// small models running through Codex — send tool call arguments with embedded
// newlines; the scanner-based reader would break on these.
func TestServer_Run_AcceptsMultilineJSON(t *testing.T) {
	s := newTestServer(t)
	// Pretty-printed request spanning multiple lines.
	multiline := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 1,\n  \"method\": \"initialize\",\n  \"params\": {\n    \"protocolVersion\": \"2024-11-05\"\n  }\n}\n"
	out := &bytes.Buffer{}
	if err := s.Run(context.Background(), strings.NewReader(multiline), out); err != nil {
		t.Fatalf("Run returned error on multiline JSON: %v", err)
	}
	line := strings.TrimRight(out.String(), "\n")
	if line == "" {
		t.Fatal("expected response, got empty stdout")
	}
	// Response must itself be a single compact line (no embedded newlines).
	if strings.Contains(line, "\n") {
		t.Fatalf("response contains embedded newline (not compact): %q", line)
	}
	var msg rpcMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		t.Fatalf("response is not valid JSON: %v\nraw: %s", err, line)
	}
	if msg.Error != nil {
		t.Fatalf("initialize returned error: %+v", msg.Error)
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
		"policy_profile.switch",
		"inbox.capture",
		"feed.read",
		"backlog.readiness",
		"projections.list",
		"projections.get",
		"projections.define",
		"registry.list",
		"registry.get",
		"registry.define_tropism",
		"registry.define_cultivar",
		"registry.activate_cultivar",
		"deterministic_errors.list",
		"deterministic_errors.get",
		"work_items.list",
		"work_items.get",
		"approvals.list_for_work_item",
		"approvals.get",
		"work_items.create",
		"work_items.spawn_child",
		"work_items.append_event",
		"approvals.request",
		"approvals.decide",
		"connectors.http_request",
		"convergence.propose_checks",
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
		"policy_profile_switch",
		"inbox_capture",
		"feed_read",
		"backlog_readiness",
		"projections_list",
		"projections_get",
		"projections_define",
		"registry_list",
		"registry_get",
		"registry_define_tropism",
		"registry_define_cultivar",
		"registry_activate_cultivar",
		"deterministic_errors_list",
		"deterministic_errors_get",
		"work_items_list",
		"work_items_get",
		"approvals_list_for_work_item",
		"approvals_get",
		"work_items_create",
		"work_items_spawn_child",
		"work_items_append_event",
		"approvals_request",
		"approvals_decide",
		"connectors_http_request",
		"convergence_propose_checks",
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
		if _, ok := tool.InputSchema["properties"].(map[string]any); !ok {
			t.Fatalf("tool %s has non-object properties schema: %#v", tool.Name, tool.InputSchema["properties"])
		}
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("missing Cursor-compatible tool %q", name)
		}
	}
}

func TestServer_ToolsList_MutationSchemasRequireIdempotencyKey(t *testing.T) {
	s := newTestServer(t)
	mutations := map[string]bool{
		"policy_profile.switch":      true,
		"inbox.capture":              true,
		"projections.define":         true,
		"registry.define_tropism":    true,
		"registry.define_cultivar":   true,
		"registry.activate_cultivar": true,
		"approvals.request":          true,
		"approvals.decide":           true,
		"connectors.http_request":    true,
		"work_items.create":          true,
		"work_items.spawn_child":     true,
		"work_items.append_event":    true,
		"convergence.propose_checks": true,
		"work_items.update_metadata": true,
		"work_items.transition":      true,
	}
	for _, tool := range s.tools {
		required := schemaRequiredSet(tool.InputSchema)
		props, _ := tool.InputSchema["properties"].(map[string]any)
		_, hasProperty := props["idempotency_key"]
		if mutations[tool.Name] {
			if !required["idempotency_key"] || !hasProperty {
				t.Fatalf("mutation tool %s does not require idempotency_key: schema=%v", tool.Name, tool.InputSchema)
			}
			continue
		}
		if required["idempotency_key"] || hasProperty {
			t.Fatalf("read tool %s should not expose idempotency_key: schema=%v", tool.Name, tool.InputSchema)
		}
	}
}

func TestServer_ToolsList_FiltersScopedWorkerTools(t *testing.T) {
	root := uuid.New()
	s := newTestServer(t)
	s.actor = domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Name:   "scoped-worker",
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			access.ScopeFeedReadAssigned,
			"work_items.tree:" + root.String(),
		},
	}
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
	got := toolNameSet(result.Tools)
	for _, want := range []string{
		"feed.read",
		"backlog.readiness",
		"projections.list",
		"projections.get",
		"work_items.list",
		"work_items.get",
		"approvals.list_for_work_item",
		"approvals.get",
		"work_items.spawn_child",
		"work_items.append_event",
		"approvals.request",
		"connectors.http_request",
		"convergence.propose_checks",
		"registry.activate_cultivar",
		"work_items.update_metadata",
		"work_items.transition",
	} {
		if !got[want] {
			t.Errorf("missing scoped worker tool %q; got %v", want, toolNames(result.Tools))
		}
	}
	for _, hidden := range []string{
		"policy_profile.switch",
		"inbox.capture",
		"deterministic_errors.list",
		"deterministic_errors.get",
		"projections.define",
		"work_items.create",
		"approvals.decide",
	} {
		if got[hidden] {
			t.Errorf("scoped worker should not see %q; got %v", hidden, toolNames(result.Tools))
		}
	}
}

func TestServer_CallMutationTool_RequiresIdempotencyKey(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"work_items.create","arguments":{"title":"nope"}}}`)
	if resp.Error != nil {
		t.Fatalf("expected transport success, got error %+v", resp.Error)
	}
	result := decodeToolResult(t, resp)
	if !result.IsError {
		t.Fatalf("expected isError=true, got %+v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "idempotency_key_required") {
		t.Fatalf("expected idempotency_key_required, got %+v", result.Content)
	}
}

func TestServer_CallMutationTool_RequiresIdempotencyExecutor(t *testing.T) {
	s := newTestServer(t)
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"work_items.create","arguments":{"title":"nope","idempotency_key":"idem-1"}}}`)
	if resp.Error != nil {
		t.Fatalf("expected transport success, got error %+v", resp.Error)
	}
	result := decodeToolResult(t, resp)
	if !result.IsError {
		t.Fatalf("expected isError=true from missing idempotency executor, got %+v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "idempotency executor not configured") {
		t.Fatalf("expected idempotency executor guard, got %+v", result.Content)
	}
}

func TestServer_MutationIdempotencyContextCanonicalizesArguments(t *testing.T) {
	s := newTestServer(t)
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent}
	tool := s.toolsByName["work_items.create"]

	ctx1, stripped1, err := mcpCallContext(context.Background(), actor, tool, json.RawMessage(`{"idempotency_key":"idem-1","title":"same","body":"body"}`))
	if err != nil {
		t.Fatalf("mcpCallContext first: %v", err)
	}
	ctx2, stripped2, err := mcpCallContext(context.Background(), actor, tool, json.RawMessage(`{"body":"body","title":"same","idempotency_key":"idem-1"}`))
	if err != nil {
		t.Fatalf("mcpCallContext second: %v", err)
	}
	id1, ok := idempotency.SubjectID(ctx1, "work_item")
	if !ok {
		t.Fatal("first context did not contain idempotency request")
	}
	id2, ok := idempotency.SubjectID(ctx2, "work_item")
	if !ok {
		t.Fatal("second context did not contain idempotency request")
	}
	if id1 != id2 {
		t.Fatalf("same logical MCP mutation derived different subject ids: %s vs %s", id1, id2)
	}
	if strings.Contains(string(stripped1), "idempotency_key") || strings.Contains(string(stripped2), "idempotency_key") {
		t.Fatalf("stripped arguments still contain idempotency_key: %s / %s", stripped1, stripped2)
	}

	ctx3, _, err := mcpCallContext(context.Background(), actor, tool, json.RawMessage(`{"idempotency_key":"idem-1","title":"different","body":"body"}`))
	if err != nil {
		t.Fatalf("mcpCallContext third: %v", err)
	}
	id3, ok := idempotency.SubjectID(ctx3, "work_item")
	if !ok {
		t.Fatal("third context did not contain idempotency request")
	}
	if id3 == id1 {
		t.Fatalf("different logical MCP mutation reused subject id %s", id3)
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

func TestServer_CallTool_DeniesHiddenScopedToolBeforeHandler(t *testing.T) {
	root := uuid.New()
	s := newTestServer(t)
	s.actor = domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Name:   "scoped-worker",
		Scopes: []string{
			access.ScopeWorkItemsRead,
			access.ScopeWorkItemsWrite,
			"work_items.tree:" + root.String(),
		},
	}
	resp := roundtrip(t, s, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"work_items.create","arguments":{"title":"should not happen"}}}`)
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
		t.Fatalf("expected isError=true, got %+v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "insufficient_scope") {
		t.Fatalf("expected insufficient_scope denial before handler, got %+v", result.Content)
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

func TestMutationToolErrorStatus_DoesNotTreatBareNotFoundAsSemantic(t *testing.T) {
	if got := mutationToolErrorStatus(errors.New("pgx: prepared statement not found")); got != http.StatusInternalServerError {
		t.Fatalf("bare not found infrastructure error status = %d, want %d", got, http.StatusInternalServerError)
	}
	if got := mutationToolErrorStatus(replayableToolErr(errors.New("work item 00000000-0000-0000-0000-000000000000 not found"))); got != http.StatusOK {
		t.Fatalf("wrapped replayable not-found status = %d, want %d", got, http.StatusOK)
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

type decodedToolResult struct {
	IsError bool `json:"isError"`
	Content []struct {
		Text string `json:"text"`
	} `json:"content"`
}

func decodeToolResult(t *testing.T, resp rpcMessage) decodedToolResult {
	t.Helper()
	var result decodedToolResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	return result
}

func schemaRequiredSet(schema map[string]any) map[string]bool {
	out := map[string]bool{}
	switch required := schema["required"].(type) {
	case []string:
		for _, field := range required {
			out[field] = true
		}
	case []any:
		for _, raw := range required {
			if field, ok := raw.(string); ok {
				out[field] = true
			}
		}
	}
	return out
}

func toolNames(tools []toolDescriptor) []string {
	out := make([]string, len(tools))
	for i, t := range tools {
		out[i] = t.Name
	}
	return out
}

func toolNameSet(tools []toolDescriptor) map[string]bool {
	out := make(map[string]bool, len(tools))
	for _, tool := range tools {
		out[tool.Name] = true
	}
	return out
}
