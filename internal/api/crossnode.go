package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

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

// handleCrossnodeCommand receives a cross-node command and, per
// docs/network-layer-spec.md §2/§2b, does exactly one of three things:
//
//   - Refuse (relay_refused_already_relayed) when the request is already
//     relayed AND would need to be routed onward to another node. §2b: a node
//     never forwards an already-relayed request, so relay loops are impossible
//     structurally, not by TTL.
//   - Durably queue it (command.queued -> command_queue) when
//     X-Meristem-Queue-For names a node other than this one: the target has no
//     inbound route reachable from the sender and drains its queue by outbound
//     poll.
//   - Acknowledge local receipt when the command targets this node. Executing
//     it against this node's own log is out of scope for this slice.
func (s *Server) handleCrossnodeCommand(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	req, ok := decodeCrossnodeCommandRequest(w, r)
	if !ok {
		return
	}

	relayed := headerIsTrue(r.Header.Get(crossnode.HeaderRelayed))
	queueFor := strings.TrimSpace(r.Header.Get(crossnode.HeaderQueueFor))

	// A queue-for header naming a node other than this one asks us to route the
	// command onward (durably park it for that node). Anything else is bound
	// for this node.
	needsForwarding := queueFor != "" && queueFor != s.nodeID

	if needsForwarding {
		if relayed {
			writeAPIError(w, http.StatusConflict, "relay_refused_already_relayed",
				"an already-relayed command must not be queued or forwarded again")
			return
		}
		if !domain.ValidNodeID(queueFor) {
			writeAPIError(w, http.StatusBadRequest, "invalid_queue_target",
				"X-Meristem-Queue-For must be a DNS-safe node id")
			return
		}
		if s.crossnode == nil {
			writeAPIError(w, http.StatusServiceUnavailable, "crossnode_unavailable",
				"cross-node queue service is not configured")
			return
		}
		result, err := s.crossnode.Enqueue(r.Context(), crossnode.EnqueueInput{
			TargetNodeID:         queueFor,
			CommandPath:          req.CommandPath,
			CommandBody:          req.CommandBody,
			OriginIdempotencyKey: r.Header.Get(crossnode.HeaderIdempotencyKey),
			OriginActorTokenID:   &actor.ID,
		})
		if err != nil {
			if errors.Is(err, crossnode.ErrInvalidTargetNodeID) {
				writeAPIError(w, http.StatusBadRequest, "invalid_queue_target", err.Error())
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
		return
	}

	// The command targets this node. Executing it against this node's own log
	// (with req.CommandPath/req.CommandBody under the resolved actor token) is
	// the spoke-drain slice's job.
	// TODO(bc1da2c5): replace this {queued:false, local:true} placeholder with
	// local execution of the command against this node's event log.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"queued": false,
		"local":  true,
	})
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
