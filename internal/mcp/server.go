package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/workitems"
)

// ToolNameMode controls the names advertised through tools/list. Canonical
// names stay dot-namespaced per docs/v0.md; Cursor currently filters tools
// containing dots, so Cursor compatibility mode advertises underscore aliases
// while still accepting canonical calls.
type ToolNameMode string

const (
	ToolNameModeCanonical ToolNameMode = "canonical"
	ToolNameModeCursor    ToolNameMode = "cursor"
)

// Deps bundles the domain services the MCP tools wrap. They are the same
// services the HTTP transport calls into; MCP is one more translation
// layer per docs/v0.md, never an alternate execution path.
type Deps struct {
	Auth      *auth.Service
	Inbox     *inbox.Service
	WorkItems *workitems.Service
	Feed      *feed.Service
	// MaxFeedWait caps feed.read watcher wait (mirrors GET /v1/feed). Zero
	// falls back to safety.DefaultPolicy().MaxFeedWait in the tool.
	MaxFeedWait time.Duration
}

// ServerInfo identifies this server to clients in the initialize
// handshake. Version travels with the binary.
type ServerInfo struct {
	Name    string
	Version string
}

// Server is the MCP server. Construct with New, resolve the actor with
// Authenticate, then drive Run until the client disconnects or ctx ends.
type Server struct {
	deps   Deps
	logger *slog.Logger
	info   ServerInfo

	mu    sync.RWMutex
	actor domain.Token

	toolNameMode ToolNameMode
	tools        []Tool
	toolsByName  map[string]Tool
}

// New builds an unauthenticated server. Authenticate must be called before
// Run, otherwise tool dispatch refuses every request.
func New(deps Deps, info ServerInfo, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if info.Name == "" {
		info.Name = "meristem"
	}
	if info.Version == "" {
		info.Version = "dev"
	}
	s := &Server{
		deps:        deps,
		logger:      logger,
		info:        info,
		toolsByName: make(map[string]Tool),
	}
	s.tools = s.buildTools()
	for _, t := range s.tools {
		s.toolsByName[t.Name] = t
		s.toolsByName[cursorToolName(t.Name)] = t
	}
	return s
}

// SetToolNameMode changes only the names advertised in tools/list. Dispatch
// accepts both canonical names and compatibility aliases either way, so a
// client changing modes mid-session does not break in-flight calls.
func (s *Server) SetToolNameMode(mode ToolNameMode) {
	switch mode {
	case ToolNameModeCursor:
		s.toolNameMode = mode
	default:
		s.toolNameMode = ToolNameModeCanonical
	}
}

// Authenticate resolves the bearer secret to a token row and stores it as
// the actor for every subsequent tool call. Each MCP-connected agent gets
// its own token row per the spec, so this attribution is per-process, not
// per-call.
func (s *Server) Authenticate(ctx context.Context, secret string) error {
	if s.deps.Auth == nil {
		return errors.New("mcp: auth service is not configured")
	}
	tok, err := s.deps.Auth.Authenticate(ctx, secret)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.actor = tok
	s.mu.Unlock()
	return nil
}

// actorToken returns a copy of the resolved bearer token. The empty Token
// (zero ID) means Authenticate was never called; tool handlers refuse in
// that case rather than appending events without attribution.
func (s *Server) actorToken() domain.Token {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.actor
}

// Run reads JSON-RPC messages from in, dispatches them, and writes
// responses to out. It returns when in reaches EOF, ctx is cancelled, or
// a write error occurs. Read errors other than EOF are returned; EOF is
// the normal disconnect path.
//
// Each line is one message. We reject oversized lines explicitly rather
// than silently truncating (bufio.Scanner's default 64 KiB cap is too
// small for tool payloads but we still want a hard ceiling).
func (s *Server) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	writer := newSyncWriter(out)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Copy because scanner reuses the buffer between Scan calls and
		// we may pass the bytes through to background goroutines later.
		buf := make([]byte, len(line))
		copy(buf, line)
		if err := s.handleRaw(ctx, buf, writer); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (s *Server) handleRaw(ctx context.Context, raw []byte, w *syncWriter) error {
	var msg rpcMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		// Parse error has no id to echo back; per JSON-RPC 2.0 the id is
		// null in this case.
		return w.write(rpcMessage{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   rpcErrorf(errCodeParse, "invalid JSON: "+err.Error()),
		})
	}
	if msg.JSONRPC != "2.0" {
		if msg.isNotification() {
			return nil
		}
		return w.write(rpcMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   rpcErrorf(errCodeInvalidRequest, "jsonrpc must be \"2.0\""),
		})
	}
	result, rerr := s.dispatch(ctx, msg)
	if msg.isNotification() {
		// Errors on notifications are logged; the protocol forbids a
		// response.
		if rerr != nil {
			s.logger.Warn("mcp notification handler failed",
				slog.String("method", msg.Method),
				slog.String("error", rerr.Message))
		}
		return nil
	}
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
	return w.write(resp)
}

func (s *Server) dispatch(ctx context.Context, msg rpcMessage) (any, *rpcError) {
	switch msg.Method {
	case "initialize":
		return s.handleInitialize(msg.Params)
	case "notifications/initialized", "initialized":
		return nil, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return s.handleListTools()
	case "tools/call":
		return s.handleCallTool(ctx, msg.Params)
	case "shutdown":
		return map[string]any{}, nil
	default:
		return nil, rpcErrorf(errCodeMethodNotFound, fmt.Sprintf("method not found: %s", msg.Method))
	}
}

type initializeParams struct {
	ProtocolVersion string          `json:"protocolVersion"`
	ClientInfo      json.RawMessage `json:"clientInfo,omitempty"`
	Capabilities    json.RawMessage `json:"capabilities,omitempty"`
}

func (s *Server) handleInitialize(raw json.RawMessage) (any, *rpcError) {
	var params initializeParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, rpcErrorf(errCodeInvalidParams, "invalid initialize params: "+err.Error())
		}
	}
	version := params.ProtocolVersion
	if version == "" {
		version = protocolVersion
	}
	return map[string]any{
		"protocolVersion": version,
		"serverInfo": map[string]any{
			"name":    s.info.Name,
			"version": s.info.Version,
		},
		"capabilities": map[string]any{
			"tools": map[string]any{
				"listChanged": false,
			},
		},
	}, nil
}

func (s *Server) handleListTools() (any, *rpcError) {
	descs := make([]toolDescriptor, 0, len(s.tools))
	for _, t := range s.tools {
		descs = append(descs, toolDescriptor{
			Name:        s.advertisedToolName(t.Name),
			Description: t.Description,
			InputSchema: t.InputSchema,
		})
	}
	return map[string]any{"tools": descs}, nil
}

func (s *Server) advertisedToolName(canonical string) string {
	if s.toolNameMode == ToolNameModeCursor {
		return cursorToolName(canonical)
	}
	return canonical
}

func cursorToolName(canonical string) string {
	return strings.ReplaceAll(canonical, ".", "_")
}

type callToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func (s *Server) handleCallTool(ctx context.Context, raw json.RawMessage) (any, *rpcError) {
	var params callToolParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, rpcErrorf(errCodeInvalidParams, "invalid tools/call params: "+err.Error())
		}
	}
	tool, ok := s.toolsByName[params.Name]
	if !ok {
		return nil, rpcErrorf(errCodeMethodNotFound, "no such tool: "+params.Name)
	}
	actor := s.actorToken()
	if actor.ID == (domain.Token{}).ID {
		return toolErrorResult("mcp server is not authenticated; set MERISTEM_TOKEN before launching"), nil
	}
	result, err := tool.Handler(ctx, actor, params.Arguments)
	if err != nil {
		// Tool-level errors travel inside a successful response with
		// isError=true (per MCP spec), not as JSON-RPC errors. The
		// distinction is "the transport worked, the tool didn't".
		return toolErrorResult(err.Error()), nil
	}
	return toolSuccessResult(result), nil
}

type toolDescriptor struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

func toolSuccessResult(payload any) map[string]any {
	text := ""
	if payload != nil {
		if encoded, err := json.Marshal(payload); err == nil {
			text = string(encoded)
		}
	}
	out := map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": text},
		},
		"isError": false,
	}
	if payload != nil {
		out["structuredContent"] = payload
	}
	return out
}

func toolErrorResult(message string) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": message},
		},
		"isError": true,
	}
}

// syncWriter serializes writes to the underlying stream so concurrent
// dispatch (future-proofing) cannot interleave bytes mid-message.
type syncWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func newSyncWriter(out io.Writer) *syncWriter {
	return &syncWriter{out: out}
}

func (s *syncWriter) write(msg rpcMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	encoded, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = s.out.Write(encoded)
	return err
}
