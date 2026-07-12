package api

import (
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/crossnode"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

// bindRemoteProvenance validates authenticated peer metadata after bearer
// authentication and before access/idempotency/domain code can append events.
func (s *Server) bindRemoteProvenance(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get(crossnode.HeaderOriginNode))
	originActor := strings.TrimSpace(r.Header.Get(crossnode.HeaderOriginActorToken))
	originSource := domain.Source(strings.TrimSpace(r.Header.Get(crossnode.HeaderOriginActorSource)))
	queueIDText := strings.TrimSpace(r.Header.Get(crossnode.HeaderQueueCommand))
	causeText := strings.TrimSpace(r.Header.Get(crossnode.HeaderCausingWorkItem))
	if originActor == "" && originSource == "" && queueIDText == "" && causeText == "" {
		return true
	}
	actor, ok := authenticatedToken(w, r)
	if !ok {
		return false
	}
	if !domain.ValidNodeID(origin) || originActor == "" || !originSource.Valid() {
		writeAPIError(w, http.StatusBadRequest, "invalid_remote_provenance", "origin node, actor token id, and actor source are required")
		return false
	}
	originActorID, err := uuid.Parse(originActor)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_remote_provenance", "origin actor token id must be a UUID")
		return false
	}
	if err := crossnode.AuthorizeTargetExecution(actor, s.nodeID, origin); err != nil {
		writeCrossnodeAuthorizationError(w, err, "token cannot execute remote commands for this target and origin")
		return false
	}
	var queueID, causeID *uuid.UUID
	if queueIDText != "" {
		parsed, err := uuid.Parse(queueIDText)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_remote_provenance", "queue command id must be a UUID")
			return false
		}
		queueID = &parsed
	}
	if causeText != "" {
		parsed, err := uuid.Parse(causeText)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_remote_provenance", "causing work item id must be a UUID")
			return false
		}
		causeID = &parsed
	}
	ctx := events.WithRemoteProvenance(r.Context(), events.RemoteProvenance{
		OriginNodeID: origin, OriginActorTokenID: &originActorID,
		OriginActorSource: originSource, QueueCommandID: queueID,
		CausingWorkItemID: causeID,
	})
	*r = *r.WithContext(ctx)
	return true
}
