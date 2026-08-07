package api

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/listeneractivation"
)

func toListenerActivationResponse(a listeneractivation.Activation) map[string]any {
	out := map[string]any{
		"id": a.ID, "listener_id": a.ListenerID, "work_item_id": a.WorkItemID,
		"assignment_event_id": a.AssignmentEventID, "demand_event_id": a.DemandEventID,
		"attempt": a.Attempt, "adapter_kind": a.AdapterKind,
		"binding_generation": a.BindingGeneration, "state": a.State,
		"dispatch_count": a.DispatchCount, "reconcile_count": a.ReconcileCount,
		"last_reason": a.LastReason, "last_outcome_event_id": a.LastOutcomeEventID,
		"state_event_id": a.StateEventID, "created_at": a.CreatedAt, "updated_at": a.UpdatedAt,
	}
	if a.DispatchMode != "" {
		out["dispatch_mode"] = a.DispatchMode
	}
	if a.ConsumerGeneration != "" {
		out["consumer_generation"] = a.ConsumerGeneration
	}
	if a.LeaseExpiresAt != nil {
		out["lease_expires_at"] = a.LeaseExpiresAt
	}
	if a.NextRetryAt != nil {
		out["next_retry_at"] = a.NextRetryAt
	}
	return out
}

func (s *Server) handleEnsureListenerActivation(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	listenerID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		AssignmentEventID string `json:"assignment_event_id"`
		BindingGeneration string `json:"binding_generation"`
		Attempt           int    `json:"attempt"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	assignmentID, err := uuid.Parse(req.AssignmentEventID)
	if err != nil {
		idempotency.MarkRefusalUnconsumed(r.Context())
		writeAPIError(w, http.StatusBadRequest, "invalid_assignment_event_id", "assignment_event_id must be a uuid")
		return
	}
	a, err := s.listenerActivations.Ensure(r.Context(), listeneractivation.EnsureInput{
		ListenerID: listenerID, AssignmentEventID: assignmentID,
		BindingGeneration: req.BindingGeneration, Attempt: req.Attempt, Actor: actor,
	})
	if err != nil {
		writeListenerActivationError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"activation": toListenerActivationResponse(a)})
}

func (s *Server) handleBeginListenerActivation(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		ConsumerGeneration string `json:"consumer_generation"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	result, err := s.listenerActivations.Begin(r.Context(), listeneractivation.BeginInput{
		ActivationID: id, ConsumerGeneration: req.ConsumerGeneration, Actor: actor,
	})
	if err != nil {
		writeListenerActivationError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"action": result.Action, "activation": toListenerActivationResponse(result.Activation),
	})
}

func (s *Server) handleListenerActivationReceipt(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		ObservedStateEventID string `json:"observed_state_event_id"`
		ConsumerGeneration   string `json:"consumer_generation"`
		Outcome              string `json:"outcome"`
		Reason               string `json:"reason"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	observed, err := uuid.Parse(req.ObservedStateEventID)
	if err != nil {
		idempotency.MarkRefusalUnconsumed(r.Context())
		writeAPIError(w, http.StatusBadRequest, "invalid_observed_state_event_id", "observed_state_event_id must be a uuid")
		return
	}
	a, err := s.listenerActivations.RecordReceipt(r.Context(), listeneractivation.ReceiptInput{
		ActivationID: id, ObservedStateEventID: observed,
		ConsumerGeneration: req.ConsumerGeneration,
		Outcome:            listeneractivation.State(req.Outcome), Reason: req.Reason, Actor: actor,
	})
	if err != nil {
		writeListenerActivationError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"activation": toListenerActivationResponse(a)})
}

func writeListenerActivationError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, listeneractivation.ErrInvalidRequest),
		errors.Is(err, listeneractivation.ErrNotFound),
		errors.Is(err, listeneractivation.ErrNotAuthorized),
		errors.Is(err, listeneractivation.ErrStaleState),
		errors.Is(err, listeneractivation.ErrNoActiveAssignment):
		idempotency.MarkRefusalUnconsumed(r.Context())
	}
	switch {
	case errors.Is(err, listeneractivation.ErrInvalidRequest):
		writeAPIError(w, http.StatusBadRequest, "invalid_listener_activation_request", err.Error())
	case errors.Is(err, listeneractivation.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "listener_activation_not_found", err.Error())
	case errors.Is(err, listeneractivation.ErrNotAuthorized):
		writeAPIError(w, http.StatusForbidden, "listener_activation_not_authorized", err.Error())
	case errors.Is(err, listeneractivation.ErrStaleState), errors.Is(err, listeneractivation.ErrNoActiveAssignment):
		writeAPIError(w, http.StatusConflict, "listener_activation_conflict", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "listener_activation_failed", err.Error())
	}
}
