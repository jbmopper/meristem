package api

import (
	"errors"
	"net/http"

	"github.com/jbmopper/meristem/internal/nodes"
)

func (s *Server) handleRegistrySnapshotRead(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.nodeSnapshots == nil || s.nodeID == "" || (s.registryHomeNodeID != "" && s.registryHomeNodeID != s.nodeID) {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_snapshot_unavailable", "registry snapshot service or local node id is not configured")
		return
	}
	snapshot, err := s.nodeSnapshots.Build(r.Context(), actor, s.nodeID)
	if err != nil {
		writeNodeSnapshotError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleRegistrySnapshotObserve(w http.ResponseWriter, r *http.Request) {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return
	}
	if s.nodeSnapshots == nil || s.registryHomeNodeID == "" {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_snapshot_unavailable", "registry snapshot service or pinned registry home is not configured")
		return
	}
	var snapshot nodes.RegistrySnapshot
	if !decodeJSONRequest(w, r, &snapshot) {
		return
	}
	accepted, fresh, err := s.nodeSnapshots.Observe(r.Context(), actor, s.registryHomeNodeID, snapshot)
	if err != nil {
		writeNodeSnapshotError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot": accepted, "observed": fresh})
}

func (s *Server) canObserveRegistrySnapshot(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if s.registryHomeNodeID == "" {
		writeAPIError(w, http.StatusServiceUnavailable, "registry_snapshot_unavailable", "pinned registry home is not configured")
		return false
	}
	if !nodes.CanObserveSnapshot(actor, s.registryHomeNodeID) {
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot observe this registry snapshot")
		return false
	}
	return true
}

func writeNodeSnapshotError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, nodes.ErrSnapshotDenied):
		writeAPIError(w, http.StatusForbidden, "insufficient_scope", "token cannot read or observe this registry snapshot")
	case errors.Is(err, nodes.ErrStaleSnapshot):
		writeAPIError(w, http.StatusConflict, "stale_registry_snapshot", err.Error())
	case errors.Is(err, nodes.ErrSnapshotConflict):
		writeAPIError(w, http.StatusConflict, "registry_snapshot_revision_conflict", err.Error())
	case errors.Is(err, nodes.ErrWrongSnapshotSource):
		writeAPIError(w, http.StatusBadRequest, "wrong_registry_snapshot_source", err.Error())
	case errors.Is(err, nodes.ErrInvalidSnapshot):
		writeAPIError(w, http.StatusBadRequest, "invalid_registry_snapshot", err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, "registry_snapshot_failed", "registry snapshot operation failed")
	}
}
