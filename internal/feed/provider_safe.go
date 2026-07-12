package feed

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// ProviderSafeContract names the fail-closed provider feed wire shape. The
// version changes only when a consumer-visible field or event-kind policy
// changes.
const ProviderSafeContract = "provider_safe_feed.v1"

// ProviderSafeItem is the only event shape the provider-facing MCP surface
// emits. It deliberately has no payload field and no actor token id. Optional
// structure is reconstructed from an allowlist of lifecycle fields; arbitrary
// event payload values never cross this boundary.
type ProviderSafeItem struct {
	EventID    uuid.UUID               `json:"event_id"`
	OccurredAt time.Time               `json:"occurred_at"`
	Source     domain.Source           `json:"source"`
	Kind       string                  `json:"kind"`
	WorkItemID uuid.UUID               `json:"work_item_id"`
	Transition *ProviderSafeTransition `json:"transition,omitempty"`
	Relation   *ProviderSafeRelation   `json:"relation,omitempty"`
}

type ProviderSafeTransition struct {
	From domain.WorkItemState `json:"from"`
	To   domain.WorkItemState `json:"to"`
}

type ProviderSafeRelation struct {
	ParentID uuid.UUID `json:"parent_id"`
	ChildID  uuid.UUID `json:"child_id"`
}

// ProjectProviderSafeItem applies the provider event allowlist. Unknown kinds,
// non-work-item subjects, and malformed identity-bearing payloads fail closed.
// Unknown payload fields are never copied to the result.
func ProjectProviderSafeItem(item Item) (ProviderSafeItem, bool) {
	if item.SubjectKind != domain.SubjectWorkItem || item.SubjectID == uuid.Nil {
		return ProviderSafeItem{}, false
	}
	out := ProviderSafeItem{
		EventID:    item.EventID,
		OccurredAt: item.OccurredAt,
		Source:     item.Source,
		Kind:       item.Kind,
		WorkItemID: item.SubjectID,
	}
	switch item.Kind {
	case domain.EventWorkItemCreated,
		domain.EventWorkItemMetadataUpdated,
		domain.EventPatienceBreached,
		domain.EventXylemExhausted,
		domain.EventDispatchRequested,
		domain.EventConvergenceChecksProposed,
		domain.EventConvergenceVerdictRecorded:
		return out, true
	case domain.EventWorkItemTransitioned:
		var payload struct {
			From domain.WorkItemState `json:"from"`
			To   domain.WorkItemState `json:"to"`
		}
		if json.Unmarshal(item.Payload, &payload) != nil || !payload.From.Valid() || !payload.To.Valid() {
			return ProviderSafeItem{}, false
		}
		out.Transition = &ProviderSafeTransition{From: payload.From, To: payload.To}
		return out, true
	case domain.EventWorkItemRelationAdded:
		var payload struct {
			ParentID uuid.UUID `json:"parent_id"`
			ChildID  uuid.UUID `json:"child_id"`
		}
		if json.Unmarshal(item.Payload, &payload) != nil || payload.ParentID == uuid.Nil || payload.ChildID == uuid.Nil || payload.ParentID != item.SubjectID {
			return ProviderSafeItem{}, false
		}
		out.Relation = &ProviderSafeRelation{ParentID: payload.ParentID, ChildID: payload.ChildID}
		return out, true
	default:
		return ProviderSafeItem{}, false
	}
}

func ProjectProviderSafeItems(items []Item) []ProviderSafeItem {
	out := make([]ProviderSafeItem, 0, len(items))
	for _, item := range items {
		if projected, ok := ProjectProviderSafeItem(item); ok {
			out = append(out, projected)
		}
	}
	return out
}
