package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/approvals"
)

func (s *Server) handleCreateApproval(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	workItemID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Summary          string          `json:"summary"`
		Request          json.RawMessage `json:"request"`
		ExpiresInSeconds int             `json:"expires_in_seconds"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	var request any
	if len(req.Request) > 0 {
		if err := json.Unmarshal(req.Request, &request); err != nil {
			writeAPIError(w, http.StatusBadRequest, "malformed_json", "request must be valid JSON")
			return
		}
	}
	result, err := s.approvals.Create(r.Context(), approvals.CreateInput{
		WorkItemID: workItemID,
		Summary:    req.Summary,
		Request:    request,
		ExpiresIn:  time.Duration(req.ExpiresInSeconds) * time.Second,
		Actor:      actor,
	})
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"approval": result.Approval,
		"created":  result.Fresh,
		"event_id": result.EventID,
	})
}

func (s *Server) handleListApprovalsForWorkItem(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	workItemID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	if !s.canReadWorkItem(w, r, actor, workItemID) {
		return
	}
	items, err := s.approvals.ListForWorkItem(r.Context(), workItemID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "approval_read_failed", "could not read approvals")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleGetApproval(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	item, err := s.approvals.Get(r.Context(), id)
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	if !s.canReadWorkItem(w, r, actor, item.WorkItemID) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approval": item})
}

func (s *Server) handleDecideApproval(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	result, err := s.approvals.Decide(r.Context(), approvals.DecisionInput{
		ApprovalID: id,
		Decision:   approvals.Decision(req.Decision),
		Reason:     req.Reason,
		Actor:      actor,
	})
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"approval": result.Approval,
		"decided":  result.Fresh,
		"event_id": result.EventID,
	})
}

func (s *Server) canDecideApprovalPath(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if !access.ToolVisible(actor, "approvals.decide") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot decide approvals")
		return false
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return false
	}
	if _, err := s.approvals.Get(r.Context(), id); err != nil {
		writeApprovalError(w, err)
		return false
	}
	return true
}

func writeApprovalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, approvals.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "approval_not_found", "approval not found")
	case errors.Is(err, approvals.ErrHumanDecisionToken):
		writeAPIError(w, http.StatusForbidden, "human_decision_token_required", "approval decision requires a human non-root token")
	case errors.Is(err, approvals.ErrSeparationOfDuties):
		writeAPIError(w, http.StatusForbidden, "separation_of_duties", "requesting token cannot decide the same approval")
	case errors.Is(err, approvals.ErrAlreadyDecided):
		writeAPIError(w, http.StatusConflict, "approval_already_decided", err.Error())
	case errors.Is(err, approvals.ErrInvalidDecision):
		writeAPIError(w, http.StatusBadRequest, "invalid_decision", "decision must be approved or denied")
	default:
		writeAPIError(w, http.StatusBadRequest, "approval_failed", err.Error())
	}
}
