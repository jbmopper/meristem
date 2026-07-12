package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/safety"
)

// crossnodeCommandRequest is the envelope a peer posts to
// POST /v1/crossnode/commands: the home-node call to run, minus the
// idempotency and routing metadata, which travel in headers.
type crossnodeCommandRequest struct {
	CommandPath string          `json:"command_path"`
	CommandBody json.RawMessage `json:"command_body"`
}

// handleCrossnodeCommand durably queues one allowlisted canonical REST
// mutation for an inboundless peer. Direct delivery calls the canonical REST
// endpoint itself; this queue endpoint never pretends to execute a local call.
func (s *Server) handleCrossnodeCommand(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeCrossnodeCommandRequest(w, r)
	if !ok {
		return
	}

	queueFor := strings.TrimSpace(r.Header.Get(crossnode.HeaderQueueFor))
	if !validateCrossnodeQueueRequest(w, s.nodeID, actor, queueFor, req.CommandPath, r.Header) {
		return
	}
	if s.crossnode == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "crossnode_unavailable",
			"cross-node queue service is not configured")
		return
	}
	result, err := s.crossnode.Enqueue(r.Context(), crossnode.EnqueueInput{
		TargetNodeID:         queueFor,
		OriginNodeID:         strings.TrimSpace(r.Header.Get(crossnode.HeaderOriginNode)),
		CommandPath:          req.CommandPath,
		CommandBody:          req.CommandBody,
		OriginIdempotencyKey: r.Header.Get(crossnode.HeaderIdempotencyKey),
		OriginActorTokenID:   &actor.ID,
		Source:               actor.Source,
	})
	if err != nil {
		if errors.Is(err, crossnode.ErrInvalidTargetNodeID) {
			writeAPIError(w, http.StatusBadRequest, "invalid_queue_target", err.Error())
			return
		}
		if errors.Is(err, crossnode.ErrInvalidOriginNodeID) {
			writeAPIError(w, http.StatusBadRequest, "invalid_origin_node", err.Error())
			return
		}
		if errors.Is(err, crossnode.ErrInvalidCommandPath) {
			writeAPIError(w, http.StatusBadRequest, "invalid_command_path", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "command_queue_failed",
			"could not queue cross-node command")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"queued":               true,
		"local":                false,
		"target_node_id":       queueFor,
		"command_queued_event": result.EventID,
	})
}

// handleCrossnodeCommandsList serves GET /v1/crossnode/commands?target=<node_id>&limit=N,
// the hub read a pull-only target polls to drain its durable command queue
// (docs/network-layer-spec.md §2 "Commands to nodes without inbound
// reachability"). It returns the target's still-pending rows oldest-first; the
// caller executes each locally and acks the outcome. Auth-only (no idempotency:
// a read), mirroring the other GET routes.
func (s *Server) handleCrossnodeCommandsList(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.crossnode == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "crossnode_unavailable",
			"cross-node queue service is not configured")
		return
	}
	target := strings.TrimSpace(r.URL.Query().Get("target"))
	if target == "" {
		writeAPIError(w, http.StatusBadRequest, "target_required", "target query parameter is required")
		return
	}
	if !domain.ValidNodeID(target) {
		writeAPIError(w, http.StatusBadRequest, "invalid_target", "target must be a DNS-safe node id")
		return
	}
	if err := crossnode.AuthorizeQueueDrain(actor, target); err != nil {
		writeCrossnodeAuthorizationError(w, err, "token cannot drain this target's cross-node queue")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	commands, err := s.crossnode.PendingForTarget(r.Context(), target, limit)
	if err != nil {
		if errors.Is(err, crossnode.ErrInvalidTargetNodeID) {
			writeAPIError(w, http.StatusBadRequest, "invalid_target", "target must be a DNS-safe node id")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "command_queue_read_failed",
			"could not read cross-node command queue")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": commands})
}

// crossnodeAckRequest is the structural outcome a target posts to
// POST /v1/crossnode/commands/{event_id}/ack after executing the command
// locally: the HTTP status its local api returned and whether it succeeded.
type crossnodeAckRequest struct {
	StatusCode int    `json:"status_code"`
	OK         *bool  `json:"ok,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
}

type crossnodeAttemptRequest struct {
	AttemptKey string `json:"attempt_key"`
}

// handleCrossnodeCommandAttempt records one logical local execution before a
// spoke calls its local API. The attributed queue-host event is the durable
// five-attempt budget; a refusal here means the spoke must not execute.
func (s *Server) handleCrossnodeCommandAttempt(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	eventID, err := uuid.Parse(strings.TrimSpace(r.PathValue("event_id")))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_event_id", "event_id must be a UUID")
		return
	}
	var req crossnodeAttemptRequest
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.AttemptKey) == "" {
		writeAPIError(w, http.StatusBadRequest, "attempt_key_required", "attempt_key is required")
		return
	}
	result, err := s.crossnode.RecordAttempt(r.Context(), crossnode.RecordAttemptInput{
		CommandQueueID: eventID,
		AttemptKey:     req.AttemptKey,
		Now:            time.Now().UTC(),
		ActorTokenID:   actor.ID,
		Source:         actor.Source,
	})
	if err != nil {
		switch {
		case errors.Is(err, crossnode.ErrUnknownCommand):
			writeAPIError(w, http.StatusNotFound, "unknown_command", "no queued command with that event_id")
		case errors.Is(err, crossnode.ErrCommandNotPending):
			writeAPIError(w, http.StatusConflict, "command_not_pending", err.Error())
		case errors.Is(err, crossnode.ErrCommandPatienceExhausted):
			writeAPIError(w, http.StatusConflict, "command_patience_exhausted", err.Error())
		case errors.Is(err, crossnode.ErrInvalidAttemptInput):
			writeAPIError(w, http.StatusBadRequest, "invalid_attempt", err.Error())
		default:
			writeAPIError(w, http.StatusInternalServerError, "command_attempt_failed", "could not record cross-node command attempt")
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"event_id":      result.EventID,
		"attempt_count": result.AttemptCount,
		"recorded":      result.Fresh,
	})
}

// handleCrossnodeCommandAck receives a target's acknowledgement of a drained
// command and folds the outcome onto the command_queue row (state pending ->
// done/failed). Runs behind the command (auth + idempotency) middleware so a
// replayed ack collapses.
func (s *Server) handleCrossnodeCommandAck(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.crossnode == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "crossnode_unavailable",
			"cross-node queue service is not configured")
		return
	}
	eventID, err := uuid.Parse(strings.TrimSpace(r.PathValue("event_id")))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_event_id", "event_id must be a UUID")
		return
	}
	req, ok := decodeCrossnodeAckRequest(w, r)
	if !ok {
		return
	}
	if req.StatusCode < 100 || req.StatusCode > 599 {
		writeAPIError(w, http.StatusBadRequest, "invalid_status_code", "status_code must be an HTTP status")
		return
	}
	if req.Outcome != "" {
		switch crossnode.CommandOutcome(req.Outcome) {
		case crossnode.CommandDone, crossnode.CommandRefused, crossnode.CommandFailed:
		default:
			writeAPIError(w, http.StatusBadRequest, "invalid_outcome", "outcome must be done, refused, or failed")
			return
		}
	}
	result, err := s.crossnode.Ack(r.Context(), crossnode.AckInput{
		CommandQueueID: eventID,
		StatusCode:     req.StatusCode,
		OK:             req.OK != nil && *req.OK,
		Outcome:        crossnode.CommandOutcome(req.Outcome),
		ActorTokenID:   &actor.ID,
		Source:         actor.Source,
	})
	if err != nil {
		if errors.Is(err, crossnode.ErrUnknownCommand) {
			writeAPIError(w, http.StatusNotFound, "unknown_command", "no queued command with that event_id")
			return
		}
		if errors.Is(err, crossnode.ErrCommandTerminalConflict) {
			writeAPIError(w, http.StatusConflict, "command_terminal_conflict", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "command_ack_failed",
			"could not acknowledge cross-node command")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"acked":               true,
		"event_id":            eventID,
		"target_node_id":      result.TargetNodeID,
		"command_acked_event": result.EventID,
	})
}

// crossnodeQueueCommand authenticates and authorizes before handing the
// request to idempotency. This ordering is load-bearing: a denied peer token
// must append neither a command event nor an idempotency event.
func (s *Server) crossnodeQueueCommand(next http.Handler) http.Handler {
	if s.authMiddleware == nil || s.idempotencyMiddleware == nil {
		return serviceUnavailableHandler()
	}
	inner := s.idempotencyMiddleware.Wrap(next)
	return s.authMiddleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actor, ok := authenticatedToken(w, r)
		if !ok {
			return
		}
		body, ok := readCrossnodeBodyForAuthorization(w, r)
		if !ok {
			return
		}
		var req crossnodeCommandRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
			return
		}
		queueFor := strings.TrimSpace(r.Header.Get(crossnode.HeaderQueueFor))
		if !validateCrossnodeQueueRequest(w, s.nodeID, actor, queueFor, req.CommandPath, r.Header) {
			return
		}
		inner.ServeHTTP(w, r)
	}))
}

// crossnodeAckCommand resolves the immutable command target and authorizes
// that target before idempotency can record the request.
func (s *Server) crossnodeAckCommand(next http.Handler) http.Handler {
	if s.authMiddleware == nil || s.idempotencyMiddleware == nil {
		return serviceUnavailableHandler()
	}
	inner := s.idempotencyMiddleware.Wrap(next)
	return s.authMiddleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actor, ok := authenticatedToken(w, r)
		if !ok {
			return
		}
		if s.crossnode == nil {
			writeAPIError(w, http.StatusServiceUnavailable, "crossnode_unavailable",
				"cross-node queue service is not configured")
			return
		}
		eventID, err := uuid.Parse(strings.TrimSpace(r.PathValue("event_id")))
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_event_id", "event_id must be a UUID")
			return
		}
		target, err := s.crossnode.TargetForCommand(r.Context(), eventID)
		if errors.Is(err, crossnode.ErrUnknownCommand) {
			writeAPIError(w, http.StatusNotFound, "unknown_command", "no queued command with that event_id")
			return
		}
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "command_queue_read_failed",
				"could not resolve cross-node command target")
			return
		}
		if err := crossnode.AuthorizeQueueAck(actor, target); err != nil {
			writeCrossnodeAuthorizationError(w, err, "token cannot acknowledge this target's cross-node queue")
			return
		}
		inner.ServeHTTP(w, r)
	}))
}

func validateCrossnodeQueueRequest(w http.ResponseWriter, localNodeID string, actor domain.Token, queueFor, commandPath string, headers http.Header) bool {
	if queueFor == "" {
		writeAPIError(w, http.StatusBadRequest, "queue_target_required",
			"X-Meristem-Queue-For is required; direct delivery uses canonical REST")
		return false
	}
	if !domain.ValidNodeID(queueFor) {
		writeAPIError(w, http.StatusBadRequest, "invalid_queue_target",
			"X-Meristem-Queue-For must be a DNS-safe node id")
		return false
	}
	if queueFor == localNodeID {
		writeAPIError(w, http.StatusBadRequest, "local_queue_target_forbidden",
			"local commands must use canonical REST, not the cross-node queue")
		return false
	}
	if headerIsTrue(headers.Get(crossnode.HeaderRelayed)) {
		writeAPIError(w, http.StatusConflict, "relay_refused_already_relayed",
			"an already-relayed command must not be queued or forwarded again")
		return false
	}
	originNodeID := strings.TrimSpace(headers.Get(crossnode.HeaderOriginNode))
	if !domain.ValidNodeID(originNodeID) {
		writeAPIError(w, http.StatusBadRequest, "invalid_origin_node",
			"X-Meristem-Origin-Node must name a DNS-safe originating node")
		return false
	}
	if err := crossnode.AuthorizeQueueWrite(actor, queueFor, commandPath); err != nil {
		if errors.Is(err, crossnode.ErrInvalidCommandPath) {
			writeAPIError(w, http.StatusBadRequest, "invalid_command_path",
				"command_path is not an allowlisted canonical REST mutation")
			return false
		}
		writeCrossnodeAuthorizationError(w, err, "token cannot queue this operation for the target")
		return false
	}
	return true
}

func readCrossnodeBodyForAuthorization(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	limited := http.MaxBytesReader(w, r.Body, safety.DefaultPolicy().MaxRequestBodyBytes)
	body, err := io.ReadAll(limited)
	_ = limited.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
			return nil, false
		}
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return nil, false
	}
	return body, true
}

func writeCrossnodeAuthorizationError(w http.ResponseWriter, err error, message string) {
	if errors.Is(err, crossnode.ErrCommandRootForbidden) {
		writeAPIError(w, http.StatusForbidden, "root_token_forbidden", "root token cannot authorize cross-node delivery")
		return
	}
	writeAPIError(w, http.StatusForbidden, "insufficient_scope", message)
}

func decodeCrossnodeAckRequest(w http.ResponseWriter, r *http.Request) (crossnodeAckRequest, bool) {
	defer func() { _ = r.Body.Close() }()
	r.Body = http.MaxBytesReader(w, r.Body, safety.DefaultPolicy().MaxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req crossnodeAckRequest
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
			return crossnodeAckRequest{}, false
		}
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return crossnodeAckRequest{}, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return crossnodeAckRequest{}, false
	}
	if req.OK == nil && strings.TrimSpace(req.Outcome) == "" {
		writeAPIError(w, http.StatusBadRequest, "outcome_required", "outcome or legacy ok is required")
		return crossnodeAckRequest{}, false
	}
	return req, true
}

func decodeCrossnodeCommandRequest(w http.ResponseWriter, r *http.Request) (crossnodeCommandRequest, bool) {
	defer func() { _ = r.Body.Close() }()
	r.Body = http.MaxBytesReader(w, r.Body, safety.DefaultPolicy().MaxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req crossnodeCommandRequest
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds resource safety limit")
			return crossnodeCommandRequest{}, false
		}
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return crossnodeCommandRequest{}, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object")
		return crossnodeCommandRequest{}, false
	}
	if strings.TrimSpace(req.CommandPath) == "" {
		writeAPIError(w, http.StatusBadRequest, "command_path_required", "command_path is required")
		return crossnodeCommandRequest{}, false
	}
	return req, true
}

// headerIsTrue reports whether an HTTP header carries the literal boolean true,
// case-insensitively. Absent or any other value is false.
func headerIsTrue(v string) bool {
	return strings.EqualFold(strings.TrimSpace(v), "true")
}
