package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jbmopper/meristem/internal/httpconnector"
)

func (s *Server) handleHTTPConnectorAction(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	workItemID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Mode   string          `json:"mode"`
		Method string          `json:"method"`
		URL    string          `json:"url"`
		Body   json.RawMessage `json:"body"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	result, err := s.httpConnector.Request(r.Context(), httpconnector.RequestInput{
		WorkItemID: workItemID,
		Mode:       httpconnector.Mode(req.Mode),
		Method:     req.Method,
		URL:        req.URL,
		Body:       req.Body,
		Actor:      actor,
	})
	if err != nil {
		writeHTTPConnectorError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Action.Mode == httpconnector.ModeRead {
		status = http.StatusOK
	}
	body := map[string]any{
		"action":   result.Action,
		"created":  result.Fresh,
		"event_id": result.EventID,
	}
	if result.Approval != nil {
		body["approval"] = result.Approval
		body["approval_event_id"] = result.ApprovalEvent
	}
	writeJSON(w, status, body)
}

func writeHTTPConnectorError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, httpconnector.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "work_item_not_found", "work item not found")
	case errors.Is(err, httpconnector.ErrInvalidMode):
		writeAPIError(w, http.StatusBadRequest, "invalid_connector_mode", "mode must be read or write")
	case errors.Is(err, httpconnector.ErrInvalidMethod):
		writeAPIError(w, http.StatusBadRequest, "invalid_connector_method", err.Error())
	case errors.Is(err, httpconnector.ErrInvalidURL):
		writeAPIError(w, http.StatusBadRequest, "invalid_connector_url", "url must be absolute http or https")
	case errors.Is(err, httpconnector.ErrUnsupportedRequest):
		writeAPIError(w, http.StatusBadRequest, "unsupported_connector_request", err.Error())
	default:
		writeAPIError(w, http.StatusBadRequest, "http_connector_failed", err.Error())
	}
}
