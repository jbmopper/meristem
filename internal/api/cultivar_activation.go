package api

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/cultivaractivation"
	"github.com/jbmopper/meristem/internal/grants"
	"github.com/jbmopper/meristem/internal/registry"
)

type cultivarActivationResponse struct {
	ActivationID uuid.UUID                `json:"activation_id"`
	WorkItemID   uuid.UUID                `json:"work_item_id"`
	Disposition  grants.Disposition       `json:"disposition"`
	Reason       string                   `json:"reason"`
	Scopes       []string                 `json:"scopes,omitempty"`
	Cultivar     *registry.Cultivar       `json:"cultivar,omitempty"`
	Events       cultivarActivationEvents `json:"events"`
	Escalation   *subactorGrantEscalation `json:"escalation,omitempty"`
}

type cultivarActivationEvents struct {
	Requested uuid.UUID `json:"requested"`
	Outcome   uuid.UUID `json:"outcome"`
}

func (s *Server) handleActivateCultivar(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.cultivarActivations == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "service_unavailable", "cultivar activation service is not configured")
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req registry.DefineCultivarInput
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	result, err := s.cultivarActivations.Activate(r.Context(), cultivaractivation.ActivateInput{
		Actor:      actor,
		WorkItemID: id,
		Cultivar:   req,
	})
	if err != nil {
		writeCultivarActivationError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toCultivarActivationResponse(result))
}

func toCultivarActivationResponse(result cultivaractivation.Result) cultivarActivationResponse {
	resp := cultivarActivationResponse{
		ActivationID: result.ActivationID,
		WorkItemID:   result.WorkItemID,
		Disposition:  result.Disposition,
		Reason:       result.Reason,
		Scopes:       result.Scopes,
		Cultivar:     result.Cultivar,
		Events: cultivarActivationEvents{
			Requested: result.RequestEventID,
			Outcome:   result.OutcomeEventID,
		},
	}
	if result.EscalationID != uuid.Nil || result.HumanWorkItemID != uuid.Nil {
		resp.Escalation = &subactorGrantEscalation{
			ID:              result.EscalationID,
			HumanWorkItemID: result.HumanWorkItemID,
		}
	}
	return resp
}

func writeCultivarActivationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, grants.ErrWorkItemNotFound):
		writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
	case errors.Is(err, registry.ErrUnknownTropism), errors.Is(err, registry.ErrUnknownCultivar):
		writeAPIError(w, http.StatusNotFound, registryErrorCode(err), err.Error())
	case errors.Is(err, registry.ErrVersionConflict), errors.Is(err, registry.ErrRootstockImmutable):
		writeAPIError(w, http.StatusConflict, registryErrorCode(err), err.Error())
	case errors.Is(err, registry.ErrInvalidName), errors.Is(err, registry.ErrInvalidVersion), errors.Is(err, registry.ErrInvalidPayload):
		writeAPIError(w, http.StatusBadRequest, registryErrorCode(err), err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "cultivar_activation_failed", "cultivar activation failed")
	}
}
