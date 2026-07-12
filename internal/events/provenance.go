package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

type remoteProvenanceKey struct{}

// RemoteProvenance is authenticated routing metadata retained separately from
// the target-local token that authorized a mutation. It never changes an
// event's actor_token_id or source.
type RemoteProvenance struct {
	OriginNodeID       string        `json:"origin_node_id"`
	OriginActorTokenID *uuid.UUID    `json:"origin_actor_token_id,omitempty"`
	OriginActorSource  domain.Source `json:"origin_actor_source"`
	QueueCommandID     *uuid.UUID    `json:"queue_command_id,omitempty"`
	CausingWorkItemID  *uuid.UUID    `json:"causing_work_item_id,omitempty"`
}

func WithRemoteProvenance(ctx context.Context, provenance RemoteProvenance) context.Context {
	return context.WithValue(ctx, remoteProvenanceKey{}, provenance)
}

func remoteProvenanceFromContext(ctx context.Context) (RemoteProvenance, bool) {
	p, ok := ctx.Value(remoteProvenanceKey{}).(RemoteProvenance)
	return p, ok
}

func enrichSpecWithRemoteProvenance(ctx context.Context, spec Spec) (Spec, error) {
	p, ok := remoteProvenanceFromContext(ctx)
	if !ok {
		return spec, nil
	}
	raw, err := json.Marshal(spec.Payload)
	if err != nil {
		return Spec{}, fmt.Errorf("events: marshal payload for remote provenance: %w", err)
	}
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte(`{}`)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return Spec{}, fmt.Errorf("events: remote provenance requires an object payload")
	}
	if _, exists := payload["remote_provenance"]; exists {
		return Spec{}, fmt.Errorf("events: payload already contains reserved remote_provenance field")
	}
	payload["remote_provenance"] = p
	spec.Payload = payload
	return spec, nil
}
