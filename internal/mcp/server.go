package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/approvals"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/httpconnector"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/inbox"
	"github.com/jbmopper/meristem/internal/oauth"
	"github.com/jbmopper/meristem/internal/policyprofile"
	"github.com/jbmopper/meristem/internal/projectiondefs"
	"github.com/jbmopper/meristem/internal/registry"
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
	Auth                *auth.Service
	Access              *access.Service
	Idempotency         *idempotency.Middleware
	Inbox               *inbox.Service
	OAuthClientAdmin    *oauth.ClientAdminService
	WorkItems           *workitems.Service
	Approvals           *approvals.Service
	HTTPConnector       *httpconnector.Service
	CheckProposals      *convergence.ChecksProposalService
	CultivarActivations *cultivaractivation.Service
	DeterministicErrors *errorreporting.Service
	Feed                *feed.Service
	PolicyProfiles      *policyprofile.Service
	Projections         *projectiondefs.Service
	Registry            *registry.Service
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
// Messages may arrive as compact single-line JSON or pretty-printed
// multi-line JSON; json.Decoder handles both. The server always writes
// compact single-line responses (one message per '\n'-terminated line).
func (s *Server) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	writer := newSyncWriter(out)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			// JSON syntax/type error: send a parse error per JSON-RPC 2.0 and
			// stop. The decoder stream state is undefined after a bad token so
			// further reads are unreliable.
			var synErr *json.SyntaxError
			var unmarshalErr *json.UnmarshalTypeError
			if errors.As(err, &synErr) || errors.As(err, &unmarshalErr) {
				_ = writer.write(rpcMessage{
					JSONRPC: "2.0",
					ID:      json.RawMessage("null"),
					Error:   rpcErrorf(errCodeParse, "invalid JSON: "+err.Error()),
				})
				return nil
			}
			return err
		}
		if len(raw) == 0 {
			continue
		}
		if err := s.handleRaw(ctx, []byte(raw), writer); err != nil {
			return err
		}
	}
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
	return s.dispatchWithActor(ctx, msg, s.actorToken())
}

func (s *Server) dispatchWithActor(ctx context.Context, msg rpcMessage, actor domain.Token) (any, *rpcError) {
	switch msg.Method {
	case "initialize":
		return s.handleInitialize(msg.Params)
	case "notifications/initialized", "initialized":
		return nil, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return s.handleListTools(actor)
	case "tools/call":
		return s.handleCallTool(ctx, actor, msg.Params)
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
		// Onboarding text injected into the connecting agent's system prompt by
		// compliant clients. Shared by stdio and the HTTP /mcp gateway; see
		// instructions.go for why the same text is safe across profiles.
		"instructions": serverInstructions,
	}, nil
}

func (s *Server) handleListTools(actor domain.Token) (any, *rpcError) {
	descs := make([]toolDescriptor, 0, len(s.tools))
	for _, t := range s.tools {
		if !access.ToolVisible(actor, t.Name) {
			continue
		}
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

func (s *Server) handleCallTool(ctx context.Context, actor domain.Token, raw json.RawMessage) (any, *rpcError) {
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
	if actor.ID == (domain.Token{}).ID {
		return toolErrorResult("mcp server is not authenticated; set MERISTEM_TOKEN before launching"), nil
	}
	if !access.ToolVisible(actor, tool.Name) {
		return toolErrorResult("insufficient_scope: token cannot use " + tool.Name), nil
	}
	if !tool.Mutates {
		result, err := tool.Handler(ctx, actor, params.Arguments)
		if err != nil {
			// Tool-level errors travel inside a successful response with
			// isError=true (per MCP spec), not as JSON-RPC errors. The
			// distinction is "the transport worked, the tool didn't".
			return toolErrorResult(err.Error()), nil
		}
		return toolSuccessResult(result), nil
	}
	result, err := s.handleIdempotentMutationTool(ctx, actor, tool, params.Arguments)
	if err != nil {
		return toolErrorResult(err.Error()), nil
	}
	return result, nil
}

func (s *Server) handleIdempotentMutationTool(ctx context.Context, actor domain.Token, tool Tool, raw json.RawMessage) (map[string]any, error) {
	req, arguments, err := mcpIdempotencyRequest(actor, tool, raw)
	if err != nil {
		return nil, err
	}
	if s.deps.Idempotency == nil {
		return nil, fmt.Errorf("idempotency executor not configured")
	}
	result, err := s.deps.Idempotency.Execute(ctx, idempotency.ExecuteInput{
		Token:       actor,
		Scope:       req.Scope,
		Key:         req.Key,
		RequestHash: req.RequestHash,
		Run: func(callCtx context.Context) (int, []byte, error) {
			payload, err := tool.Handler(callCtx, actor, arguments)
			status := http.StatusOK
			var toolResult map[string]any
			if err != nil {
				status = mutationToolErrorStatus(err)
				toolResult = toolErrorResult(err.Error())
			} else {
				toolResult = toolSuccessResult(payload)
			}
			encoded, err := json.Marshal(toolResult)
			if err != nil {
				return 0, nil, fmt.Errorf("marshal MCP tool result: %w", err)
			}
			return status, encoded, nil
		},
	})
	if err != nil {
		return nil, err
	}
	var replayed map[string]any
	if err := json.Unmarshal(result.Body, &replayed); err != nil {
		return nil, fmt.Errorf("decode idempotent MCP result: %w", err)
	}
	return replayed, nil
}

func mcpCallContext(ctx context.Context, actor domain.Token, tool Tool, raw json.RawMessage) (context.Context, json.RawMessage, error) {
	req, stripped, err := mcpIdempotencyRequest(actor, tool, raw)
	if err != nil {
		return ctx, raw, err
	}
	return idempotency.WithRequest(ctx, req), stripped, nil
}

func mcpIdempotencyRequest(actor domain.Token, tool Tool, raw json.RawMessage) (idempotency.Request, json.RawMessage, error) {
	if !tool.Mutates {
		return idempotency.Request{}, raw, nil
	}
	var args map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &args); err != nil {
			return idempotency.Request{}, raw, fmt.Errorf("invalid arguments: %w", err)
		}
	}
	keyRaw, ok := args["idempotency_key"]
	if !ok {
		return idempotency.Request{}, raw, fmt.Errorf("idempotency_key_required: %s requires idempotency_key", tool.Name)
	}
	var key string
	if err := json.Unmarshal(keyRaw, &key); err != nil {
		return idempotency.Request{}, raw, fmt.Errorf("idempotency_key must be a string")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return idempotency.Request{}, raw, fmt.Errorf("idempotency_key_required: idempotency_key must be non-empty")
	}

	stripped := make(map[string]json.RawMessage, len(args))
	for name, value := range args {
		if name == "idempotency_key" {
			continue
		}
		stripped[name] = value
	}
	strippedRaw, err := events.CanonicalJSON(stripped)
	if err != nil {
		return idempotency.Request{}, raw, fmt.Errorf("canonicalize MCP arguments: %w", err)
	}
	requestPayload := map[string]any{
		"tool":      tool.Name,
		"arguments": stripped,
	}
	canonicalRequest, err := events.CanonicalJSON(requestPayload)
	if err != nil {
		return idempotency.Request{}, raw, fmt.Errorf("canonicalize MCP idempotency request: %w", err)
	}
	hash := sha256.Sum256(canonicalRequest)
	req := idempotency.Request{
		TokenID:     actor.ID,
		Scope:       "MCP:" + tool.Name,
		Key:         key,
		RequestHash: hash[:],
	}
	return req, json.RawMessage(strippedRaw), nil
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

type replayableToolError struct {
	err error
}

func (e replayableToolError) Error() string {
	return e.err.Error()
}

func (e replayableToolError) Unwrap() error {
	return e.err
}

func replayableToolErr(err error) error {
	if err == nil {
		return nil
	}
	return replayableToolError{err: err}
}

func mutationToolErrorStatus(err error) int {
	if err == nil || isReplayableToolError(err) || looksSemanticToolError(err) {
		return http.StatusOK
	}
	return http.StatusInternalServerError
}

func isReplayableToolError(err error) bool {
	var replayable replayableToolError
	return errors.As(err, &replayable)
}

// looksSemanticToolError recognizes refusals that mean "the tool reached a
// deterministic conclusion" and are therefore safe to pin under the caller's
// idempotency key. Unknown errors are treated as infrastructure failures so a
// same-key retry can re-execute after transient database/projector trouble.
func looksSemanticToolError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	switch {
	case strings.HasPrefix(message, "invalid arguments:"),
		strings.Contains(message, "unknown field"),
		strings.Contains(message, "must be a valid uuid"),
		strings.HasPrefix(message, "payload:"),
		strings.HasPrefix(message, "insufficient_scope:"):
		return true
	default:
		return false
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
