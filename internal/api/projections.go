package api

import (
	"errors"
	"net/http"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projectiondefs"
)

func (s *Server) handleProjectionsList(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadProjections(w, actor) {
		return
	}
	if s.projections == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projections_unavailable", "projection service is not configured")
		return
	}
	snapshot, err := s.projections.List(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "projections_read_failed", "could not read projections")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleProjectionsGet(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadProjections(w, actor) {
		return
	}
	if s.projections == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projections_unavailable", "projection service is not configured")
		return
	}
	item, err := s.projections.Get(r.Context(), r.PathValue("name"))
	if err != nil {
		writeProjectionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"projection": item})
}

func (s *Server) handleProjectionsDefine(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.projections == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projections_unavailable", "projection service is not configured")
		return
	}
	var req projectiondefs.DefineInput
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, fresh, err := s.projections.Define(r.Context(), actor, req)
	if err != nil {
		writeProjectionError(w, err)
		return
	}
	status := http.StatusOK
	if fresh {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"projection": item, "defined": fresh})
}

func (s *Server) canReadProjections(w http.ResponseWriter, actor domain.Token) bool {
	if !access.ToolVisible(actor, "projections.list") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read projections")
		return false
	}
	return true
}

func writeProjectionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, projectiondefs.ErrUnknownProjection):
		writeAPIError(w, http.StatusNotFound, projectionErrorCode(err), err.Error())
	case errors.Is(err, projectiondefs.ErrVersionConflict), errors.Is(err, projectiondefs.ErrRootstockImmutable):
		writeAPIError(w, http.StatusConflict, projectionErrorCode(err), err.Error())
	case errors.Is(err, projectiondefs.ErrInvalidName), errors.Is(err, projectiondefs.ErrInvalidVersion), errors.Is(err, projectiondefs.ErrInvalidPayload),
		errors.Is(err, projectiondefs.ErrUnknownKind), errors.Is(err, projectiondefs.ErrUnknownKindClass), errors.Is(err, projectiondefs.ErrNotProjectable):
		writeAPIError(w, http.StatusBadRequest, projectionErrorCode(err), err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "projection_failed", "projection operation failed")
	}
}

func projectionErrorCode(err error) string {
	switch {
	case errors.Is(err, projectiondefs.ErrUnknownProjection):
		return "unknown_projection"
	case errors.Is(err, projectiondefs.ErrVersionConflict):
		return "version_conflict"
	case errors.Is(err, projectiondefs.ErrRootstockImmutable):
		return "rootstock_immutable"
	case errors.Is(err, projectiondefs.ErrInvalidName):
		return "invalid_name"
	case errors.Is(err, projectiondefs.ErrInvalidVersion):
		return "invalid_version"
	case errors.Is(err, projectiondefs.ErrUnknownKind):
		return "unknown_kind"
	case errors.Is(err, projectiondefs.ErrUnknownKindClass):
		return "unknown_kind_class"
	case errors.Is(err, projectiondefs.ErrNotProjectable):
		return "not_projectable"
	case errors.Is(err, projectiondefs.ErrInvalidPayload):
		return "invalid_payload"
	default:
		return "projection_failed"
	}
}
