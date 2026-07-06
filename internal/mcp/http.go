package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jbmopper/meristem/internal/domain"
)

const (
	HeaderProtocolVersion = "MCP-Protocol-Version"
	HeaderSessionID       = "Mcp-Session-Id"

	contentTypeJSON = "application/json; charset=utf-8"
)

// HTTPResponse is one Streamable HTTP response for a single JSON-RPC message.
// This first slice is stateless: POST requests return one JSON response for
// JSON-RPC requests, notifications/responses are accepted with 202, and GET is
// mounted by the API as 405 until server-initiated SSE is implemented.
type HTTPResponse struct {
	Status      int
	ContentType string
	Body        []byte
}

// HTTPOptions controls the subset of MCP behavior exposed by an HTTP route.
// It lets the API ship read-only Streamable HTTP before the mutation
// idempotency contract for HTTP MCP writes is finalized.
type HTTPOptions struct {
	AllowedTools map[string]bool
}

func ReadOnlyHTTPTools() map[string]bool {
	return map[string]bool{
		"backlog.readiness":            true,
		"feed.read":                    true,
		"projections.list":             true,
		"projections.get":              true,
		"registry.list":                true,
		"registry.get":                 true,
		"work_items.list":              true,
		"work_items.get":               true,
		"approvals.list_for_work_item": true,
		"approvals.get":                true,
		"deterministic_errors.list":    true,
		"deterministic_errors.get":     true,
	}
}

// HandleHTTPMessage dispatches one Streamable HTTP POST body using the actor
// resolved by the HTTP auth middleware. It deliberately does not read headers
// or perform authentication; the API transport owns that context.
func (s *Server) HandleHTTPMessage(ctx context.Context, raw []byte, actor domain.Token) HTTPResponse {
	return s.HandleHTTPMessageWithOptions(ctx, raw, actor, HTTPOptions{})
}

func (s *Server) HandleHTTPMessageWithOptions(ctx context.Context, raw []byte, actor domain.Token, opts HTTPOptions) HTTPResponse {
	var msg rpcMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return jsonRPCHTTPResponse(http.StatusBadRequest, rpcMessage{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   rpcErrorf(errCodeParse, "invalid JSON: "+err.Error()),
		})
	}
	if msg.JSONRPC != "2.0" {
		if msg.isNotification() {
			return HTTPResponse{Status: http.StatusAccepted}
		}
		return jsonRPCHTTPResponse(http.StatusBadRequest, rpcMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   rpcErrorf(errCodeInvalidRequest, "jsonrpc must be \"2.0\""),
		})
	}
	if isJSONRPCResponse(msg) {
		return HTTPResponse{Status: http.StatusAccepted}
	}
	if msg.Method == "" {
		return jsonRPCHTTPResponse(http.StatusBadRequest, rpcMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   rpcErrorf(errCodeInvalidRequest, "method is required"),
		})
	}

	if msg.isNotification() {
		if msg.Method == "tools/call" {
			if rerr := s.checkHTTPToolAllowed(msg.Params, opts.AllowedTools); rerr != nil {
				s.logger.Warn("mcp http notification rejected",
					"method", msg.Method,
					"error", rerr.Message)
				return HTTPResponse{Status: http.StatusAccepted}
			}
		}
		if _, rerr := s.dispatchWithActor(ctx, msg, actor); rerr != nil {
			s.logger.Warn("mcp http notification handler failed",
				"method", msg.Method,
				"error", rerr.Message)
		}
		return HTTPResponse{Status: http.StatusAccepted}
	}

	if msg.Method == "tools/list" {
		result, rerr := s.handleListToolsFiltered(actor, opts.AllowedTools)
		return s.httpRPCResult(msg, result, rerr)
	}
	if msg.Method == "tools/call" {
		if rerr := s.checkHTTPToolAllowed(msg.Params, opts.AllowedTools); rerr != nil {
			return s.httpRPCResult(msg, nil, rerr)
		}
	}

	result, rerr := s.dispatchWithActor(ctx, msg, actor)
	return s.httpRPCResult(msg, result, rerr)
}

func (s *Server) httpRPCResult(msg rpcMessage, result any, rerr *rpcError) HTTPResponse {
	resp := rpcMessage{JSONRPC: "2.0", ID: msg.ID}
	if rerr != nil {
		resp.Error = rerr
	} else {
		encoded, err := json.Marshal(result)
		if err != nil {
			resp.Error = rpcErrorf(errCodeInternal, "marshal result: "+err.Error())
		} else {
			resp.Result = encoded
		}
	}
	return jsonRPCHTTPResponse(http.StatusOK, resp)
}

func (s *Server) handleListToolsFiltered(actor domain.Token, allowed map[string]bool) (any, *rpcError) {
	result, rerr := s.handleListTools(actor)
	if rerr != nil || len(allowed) == 0 {
		return result, rerr
	}
	body, ok := result.(map[string]any)
	if !ok {
		return result, nil
	}
	descs, ok := body["tools"].([]toolDescriptor)
	if !ok {
		return result, nil
	}
	filtered := make([]toolDescriptor, 0, len(descs))
	for _, desc := range descs {
		canonical := s.canonicalToolName(desc.Name)
		if allowed[canonical] {
			filtered = append(filtered, desc)
		}
	}
	return map[string]any{"tools": filtered}, nil
}

func (s *Server) checkHTTPToolAllowed(raw json.RawMessage, allowed map[string]bool) *rpcError {
	if len(allowed) == 0 {
		return nil
	}
	var params callToolParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &params); err != nil {
			return rpcErrorf(errCodeInvalidParams, "invalid tools/call params: "+err.Error())
		}
	}
	canonical := s.canonicalToolName(params.Name)
	if allowed[canonical] {
		return nil
	}
	return rpcErrorf(errCodeMethodNotFound, "tool not enabled on HTTP MCP transport until mutation idempotency is specified: "+params.Name)
}

func (s *Server) canonicalToolName(name string) string {
	if tool, ok := s.toolsByName[name]; ok {
		return tool.Name
	}
	return name
}

func isJSONRPCResponse(msg rpcMessage) bool {
	return msg.Method == "" && (len(msg.Result) > 0 || msg.Error != nil)
}

func jsonRPCHTTPResponse(status int, msg rpcMessage) HTTPResponse {
	encoded, err := json.Marshal(msg)
	if err != nil {
		encoded = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"marshal response"}}`)
		status = http.StatusInternalServerError
	}
	encoded = append(encoded, '\n')
	return HTTPResponse{
		Status:      status,
		ContentType: contentTypeJSON,
		Body:        encoded,
	}
}

// AcceptsStreamableHTTPPost reports whether the client advertised the response
// formats required by the Streamable HTTP transport for POST requests.
func AcceptsStreamableHTTPPost(accept string) bool {
	return headerListContains(accept, "application/json") && headerListContains(accept, "text/event-stream")
}

func headerListContains(header string, want string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if strings.EqualFold(part, want) || part == "*/*" {
			return true
		}
	}
	return false
}
