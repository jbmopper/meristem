package api

// REST surface for listener registrations (listener control plane, slice 2).
// REST is canonical; MCP mirrors these bodies. All business rules — admin
// separation of duties, self-narrowing, stale-policy conflicts — live in the
// listeners service; these handlers only authenticate, gate tool visibility,
// decode, and map refusals.

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/listeners"
)

func (s *Server) canUseListenerTool(tool string) accessGate {
	return func(w http.ResponseWriter, r *http.Request) bool {
		actor, ok := authenticatedToken(w, r)
		if !ok {
			return false
		}
		if !access.ToolVisible(actor, tool) {
			writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot administer listeners")
			return false
		}
		return true
	}
}

func toListenerResponse(reg listeners.Registration) map[string]any {
	out := map[string]any{
		"id":                         reg.ID,
		"name":                       reg.Name,
		"principal_token_id":         reg.PrincipalTokenID,
		"provider":                   reg.Provider,
		"capabilities":               reg.Capabilities,
		"max_concurrent_assignments": reg.MaxConcurrentAssignments,
		"created_at":                 reg.CreatedAt,
		"updated_at":                 reg.UpdatedAt,
	}
	if reg.Policy != nil {
		out["policy"] = reg.Policy
		out["policy_fingerprint"] = reg.PolicyFingerprint
		out["policy_event_id"] = reg.PolicyEventID
	}
	if reg.RetiredAt != nil {
		out["retired_at"] = reg.RetiredAt
	}
	return out
}

func (s *Server) handleCreateListener(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	var req struct {
		Name             string   `json:"name"`
		PrincipalTokenID string   `json:"principal_token_id"`
		Provider         string   `json:"provider"`
		Capabilities     []string `json:"capabilities"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	principal, err := uuid.Parse(req.PrincipalTokenID)
	if err != nil {
		idempotency.MarkRefusalUnconsumed(r.Context())
		writeAPIError(w, http.StatusBadRequest, "invalid_principal_token_id", "principal_token_id must be a uuid")
		return
	}
	reg, err := s.listeners.Register(r.Context(), listeners.RegisterInput{
		Name:             req.Name,
		PrincipalTokenID: principal,
		Provider:         req.Provider,
		Capabilities:     req.Capabilities,
		Actor:            actor,
	})
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"listener": toListenerResponse(reg)})
}

func (s *Server) handleListListeners(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !access.ToolVisible(actor, "listeners.list") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read listeners")
		return
	}
	includeRetired := r.URL.Query().Get("include_retired") == "true"
	regs, err := s.listeners.List(r.Context(), includeRetired)
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(regs))
	for _, reg := range regs {
		out = append(out, toListenerResponse(reg))
	}
	writeJSON(w, http.StatusOK, map[string]any{"listeners": out})
}

func (s *Server) handleGetListener(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !access.ToolVisible(actor, "listeners.get") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read listeners")
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	reg, err := s.listeners.Get(r.Context(), id)
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"listener": toListenerResponse(reg)})
}

func (s *Server) handleSetListenerPolicy(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Policy                listeners.Policy `json:"policy"`
		ObservedPolicyEventID string           `json:"observed_policy_event_id"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	var observed *uuid.UUID
	if req.ObservedPolicyEventID != "" {
		parsed, err := uuid.Parse(req.ObservedPolicyEventID)
		if err != nil {
			idempotency.MarkRefusalUnconsumed(r.Context())
			writeAPIError(w, http.StatusBadRequest, "invalid_observed_policy_event_id", "observed_policy_event_id must be a uuid")
			return
		}
		observed = &parsed
	}
	reg, err := s.listeners.SetPolicy(r.Context(), id, listeners.SetPolicyInput{
		Policy:                req.Policy,
		ObservedPolicyEventID: observed,
		Actor:                 actor,
		ActorIsAdminSurface:   hasListenersAdminScope(actor),
	})
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"listener": toListenerResponse(reg)})
}

func (s *Server) handleBindListenerCredential(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PrincipalTokenID string `json:"principal_token_id"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	principal, err := uuid.Parse(req.PrincipalTokenID)
	if err != nil {
		idempotency.MarkRefusalUnconsumed(r.Context())
		writeAPIError(w, http.StatusBadRequest, "invalid_principal_token_id", "principal_token_id must be a uuid")
		return
	}
	reg, err := s.listeners.BindCredential(r.Context(), id, principal, actor)
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"listener": toListenerResponse(reg)})
}

func (s *Server) handleRetireListener(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	reg, err := s.listeners.Retire(r.Context(), id, req.Reason, actor)
	if err != nil {
		writeListenerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"listener": toListenerResponse(reg)})
}

func hasListenersAdminScope(actor domain.Token) bool {
	for _, scope := range actor.Scopes {
		if scope == access.ScopeListenersAdmin {
			return true
		}
	}
	return false
}

// writeListenerError maps listener-service refusals. Every branch is a pure
// refusal — the service validates before appending and rolls back on every
// error path — so all of them preserve the caller's idempotency key. The
// stale-policy conflict is pure BY DESIGN (accepted design: "a stale policy
// revision is a pure conflict and appends no event").
func writeListenerError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, listeners.ErrNotFound),
		errors.Is(err, listeners.ErrNameTaken),
		errors.Is(err, listeners.ErrRetired),
		errors.Is(err, listeners.ErrStalePolicy),
		errors.Is(err, listeners.ErrNotAuthorized),
		errors.Is(err, listeners.ErrInvalidPolicy),
		errors.Is(err, listeners.ErrInvalidRequest):
		idempotency.MarkRefusalUnconsumed(r.Context())
	}
	switch {
	case errors.Is(err, listeners.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "listener_not_found", "listener not found")
	case errors.Is(err, listeners.ErrNameTaken):
		writeAPIError(w, http.StatusConflict, "listener_name_taken", err.Error())
	case errors.Is(err, listeners.ErrRetired):
		writeAPIError(w, http.StatusConflict, "listener_retired", err.Error())
	case errors.Is(err, listeners.ErrStalePolicy):
		writeAPIError(w, http.StatusConflict, "stale_policy_revision", err.Error())
	case errors.Is(err, listeners.ErrNotAuthorized):
		writeAPIError(w, http.StatusForbidden, "listener_operation_not_authorized", err.Error())
	case errors.Is(err, listeners.ErrInvalidPolicy), errors.Is(err, listeners.ErrInvalidRequest):
		writeAPIError(w, http.StatusBadRequest, "invalid_listener_request", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "listener_request_failed", err.Error())
	}
}
