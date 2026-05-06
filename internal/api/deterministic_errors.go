package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/errorreporting"
)

type deterministicErrorResponse struct {
	ID         uuid.UUID                         `json:"id"`
	Component  string                            `json:"component"`
	Code       string                            `json:"code"`
	Message    string                            `json:"message"`
	Severity   domain.DeterministicErrorSeverity `json:"severity"`
	Details    json.RawMessage                   `json:"details"`
	ReportedBy *uuid.UUID                        `json:"reported_by,omitempty"`
	ReportedAt time.Time                         `json:"reported_at"`
	UpdatedAt  time.Time                         `json:"updated_at"`
	Masked     bool                              `json:"masked"`
	MaskReason *string                           `json:"mask_reason,omitempty"`
	MaskedBy   *uuid.UUID                        `json:"masked_by,omitempty"`
	MaskedAt   *time.Time                        `json:"masked_at,omitempty"`
}

func (s *Server) handleListDeterministicErrors(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.deterministicErrors == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	includeMasked, ok := parseIncludeMasked(w, r)
	if !ok {
		return
	}
	items, err := s.deterministicErrors.ListForAccessor(r.Context(), errorreporting.ListOptions{
		IncludeMasked: includeMasked,
		Limit:         limit,
	}, actor)
	if err != nil {
		writeDeterministicErrorError(w, err)
		return
	}
	out := make([]deterministicErrorResponse, 0, len(items))
	for _, item := range items {
		out = append(out, toDeterministicErrorResponse(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) handleGetDeterministicError(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.deterministicErrors == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	item, err := s.deterministicErrors.GetForAccessor(r.Context(), id, actor)
	if err != nil {
		writeDeterministicErrorError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deterministic_error": toDeterministicErrorResponse(item)})
}

func parseIncludeMasked(w http.ResponseWriter, r *http.Request) (bool, bool) {
	raw := r.URL.Query().Get("include_masked")
	if raw == "" {
		return false, true
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_include_masked", "include_masked must be a boolean")
		return false, false
	}
	return value, true
}

func writeDeterministicErrorError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errorreporting.ErrAccessDenied):
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token lacks deterministic log visibility scope")
	case errors.Is(err, errorreporting.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "not_found", "deterministic error not found")
	default:
		writeAPIError(w, http.StatusInternalServerError, "deterministic_error_read_failed", "could not read deterministic errors")
	}
}

func toDeterministicErrorResponse(item domain.DeterministicError) deterministicErrorResponse {
	details := json.RawMessage(item.Details)
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	return deterministicErrorResponse{
		ID:         item.ID,
		Component:  item.Component,
		Code:       item.Code,
		Message:    item.Message,
		Severity:   item.Severity,
		Details:    details,
		ReportedBy: item.ReportedBy,
		ReportedAt: item.ReportedAt,
		UpdatedAt:  item.UpdatedAt,
		Masked:     item.Masked,
		MaskReason: item.MaskReason,
		MaskedBy:   item.MaskedBy,
		MaskedAt:   item.MaskedAt,
	}
}
