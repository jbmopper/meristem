package api

import (
	"errors"
	"net/http"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
)

func (s *Server) handleRegistryList(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadRegistry(w, actor) {
		return
	}
	if s.registry == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_unavailable", "registry service is not configured")
		return
	}
	snapshot, err := s.registry.List(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "registry_read_failed", "could not read registry")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleRegistryGetTropism(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadRegistry(w, actor) {
		return
	}
	if s.registry == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_unavailable", "registry service is not configured")
		return
	}
	item, err := s.registry.GetTropism(r.Context(), r.PathValue("name"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tropism": item})
}

func (s *Server) handleRegistryGetCultivar(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if !s.canReadRegistry(w, actor) {
		return
	}
	if s.registry == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_unavailable", "registry service is not configured")
		return
	}
	item, err := s.registry.GetCultivar(r.Context(), r.PathValue("name"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cultivar": item})
}

func (s *Server) handleRegistryDefineTropism(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.registry == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_unavailable", "registry service is not configured")
		return
	}
	var req registry.DefineTropismInput
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, fresh, err := s.registry.DefineTropism(r.Context(), actor, req)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	status := http.StatusOK
	if fresh {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"tropism": item, "defined": fresh})
}

func (s *Server) handleRegistryDefineCultivar(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.registry == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_unavailable", "registry service is not configured")
		return
	}
	var req registry.DefineCultivarInput
	if !decodeJSONRequest(w, r, &req) {
		return
	}
	item, fresh, err := s.registry.DefineCultivar(r.Context(), actor, req)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	status := http.StatusOK
	if fresh {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"cultivar": item, "defined": fresh})
}

func (s *Server) canReadRegistry(w http.ResponseWriter, actor domain.Token) bool {
	if !access.ToolVisible(actor, "registry.list") {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read registry")
		return false
	}
	return true
}

func (s *Server) canWriteRegistry(tool string) accessGate {
	return func(w http.ResponseWriter, r *http.Request) bool {
		actor, ok := authenticatedToken(w, r)
		if !ok {
			return false
		}
		if !access.ToolVisible(actor, tool) {
			writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot write registry")
			return false
		}
		return true
	}
}

func writeRegistryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrUnknownTropism), errors.Is(err, registry.ErrUnknownCultivar):
		writeAPIError(w, http.StatusNotFound, registryErrorCode(err), err.Error())
	case errors.Is(err, registry.ErrVersionConflict), errors.Is(err, registry.ErrRootstockImmutable):
		writeAPIError(w, http.StatusConflict, registryErrorCode(err), err.Error())
	case errors.Is(err, registry.ErrInvalidName), errors.Is(err, registry.ErrInvalidVersion), errors.Is(err, registry.ErrInvalidPayload):
		writeAPIError(w, http.StatusBadRequest, registryErrorCode(err), err.Error())
	case errors.Is(err, registry.ErrUnknownReducer):
		writeAPIError(w, http.StatusBadRequest, "unknown_reducer", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "registry_failed", "registry operation failed")
	}
}

func registryErrorCode(err error) string {
	switch {
	case errors.Is(err, registry.ErrUnknownTropism):
		return "unknown_tropism"
	case errors.Is(err, registry.ErrUnknownCultivar):
		return "unknown_cultivar"
	case errors.Is(err, registry.ErrVersionConflict):
		return "version_conflict"
	case errors.Is(err, registry.ErrRootstockImmutable):
		return "rootstock_immutable"
	case errors.Is(err, registry.ErrInvalidName):
		return "invalid_name"
	case errors.Is(err, registry.ErrInvalidVersion):
		return "invalid_version"
	case errors.Is(err, registry.ErrInvalidPayload):
		return "invalid_payload"
	default:
		return "registry_failed"
	}
}
