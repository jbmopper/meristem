// Package mcp implements the meristem MCP server.
//
// Transport is newline-delimited JSON-RPC 2.0 over stdio (the MCP standard
// transport supported by Cursor and other current MCP clients). Each
// message is one JSON object on one line; framing is purely the newline.
//
// The server resolves a single bearer token at startup (MERISTEM_TOKEN),
// then attributes every event it appends to that token. Each MCP-connected
// agent (each Cursor instance, each custom worker) holds its own token row
// per docs/v0.md, so attribution stays clean across concurrent agents.
package mcp

import (
	"encoding/json"
	"errors"
)

// MCP protocol version this server speaks. The current spec revision is
// "2025-06-18"; we accept a client-proposed version unchanged in the
// initialize response, falling back to this constant if none is offered.
const protocolVersion = "2025-06-18"

// JSON-RPC 2.0 message shape. Per the spec, requests carry an `id`,
// notifications do not. We accept either string or number ids and echo
// them back verbatim in the response.
type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// JSON-RPC 2.0 standard error codes used here. Tool-level failures travel
// inside a successful tools/call response with isError=true (per MCP), not
// as transport errors.
const (
	errCodeParse          = -32700
	errCodeInvalidRequest = -32600
	errCodeMethodNotFound = -32601
	errCodeInvalidParams  = -32602
	errCodeInternal       = -32603
)

func (e *rpcError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// rpcErrorf builds an rpcError without a data payload. Tool handlers
// return this from validate-time failures so the transport layer can
// translate to either a JSON-RPC error frame (transport-level mistake)
// or a tool-result with isError=true (handler-level mistake).
func rpcErrorf(code int, message string) *rpcError {
	return &rpcError{Code: code, Message: message}
}

// isNotification reports whether m is a JSON-RPC notification (no id).
// JSON-RPC requires that we send no response at all in that case.
func (m rpcMessage) isNotification() bool {
	return len(m.ID) == 0 || string(m.ID) == "null"
}

// asTransportError unwraps an error into an rpcError if possible, falling
// back to a generic internal error. Used at the dispatcher seam.
func asTransportError(err error) *rpcError {
	if err == nil {
		return nil
	}
	var re *rpcError
	if errors.As(err, &re) {
		return re
	}
	return rpcErrorf(errCodeInternal, err.Error())
}
