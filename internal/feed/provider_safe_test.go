package feed

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestProjectProviderSafeItemNeverCopiesRawPayload(t *testing.T) {
	workItemID := uuid.New()
	item := Item{
		EventID:     uuid.New(),
		OccurredAt:  time.Now().UTC(),
		Source:      domain.SourceAgent,
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   workItemID,
		Kind:        domain.EventWorkItemTransitioned,
		Payload:     json.RawMessage(`{"from":"triaged","to":"planned","reason":"PRIVATE-TRANSITION-REASON","token":"PRIVATE-TOKEN"}`),
	}
	got, ok := ProjectProviderSafeItem(item)
	if !ok {
		t.Fatal("expected transition to be projected")
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"payload", "actor_token_id", "PRIVATE-TRANSITION-REASON", "PRIVATE-TOKEN"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("provider item leaked %q: %s", forbidden, text)
		}
	}
	if got.Transition == nil || got.Transition.From != domain.WorkItemTriaged || got.Transition.To != domain.WorkItemPlanned {
		t.Fatalf("transition = %+v", got.Transition)
	}
}

func TestProjectProviderSafeItemFailsClosed(t *testing.T) {
	base := Item{
		EventID:     uuid.New(),
		OccurredAt:  time.Now().UTC(),
		Source:      domain.SourceHuman,
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   uuid.New(),
	}
	tests := []Item{
		withProviderKind(base, domain.EventMessageCaptured, `{"text":"PRIVATE-MESSAGE"}`),
		withProviderKind(base, domain.EventSignalReceived, `{"payload":"PRIVATE-SIGNAL"}`),
		withProviderKind(base, domain.EventWorkItemEventAppended, `{"payload":{"secret":"PRIVATE-EVENT"}}`),
		withProviderKind(base, domain.EventApprovalCreated, `{"request":{"secret":"PRIVATE-APPROVAL"}}`),
		withProviderKind(base, domain.EventHTTPConnectorActionRequested, `{"body":"PRIVATE-CONNECTOR"}`),
		withProviderKind(base, "future.unknown", `{"secret":"PRIVATE-UNKNOWN"}`),
		withProviderKind(base, domain.EventWorkItemTransitioned, `{"from":"triaged","to":"future_state"}`),
	}
	for _, item := range tests {
		if got, ok := ProjectProviderSafeItem(item); ok {
			t.Errorf("kind %q unexpectedly projected: %+v", item.Kind, got)
		}
	}
}

func TestProjectProviderSafeRelationRequiresMatchingSubject(t *testing.T) {
	parentID, childID := uuid.New(), uuid.New()
	item := Item{
		EventID:     uuid.New(),
		OccurredAt:  time.Now().UTC(),
		Source:      domain.SourceAgent,
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   parentID,
		Kind:        domain.EventWorkItemRelationAdded,
		Payload:     json.RawMessage(`{"parent_id":"` + parentID.String() + `","child_id":"` + childID.String() + `","secret":"PRIVATE"}`),
	}
	got, ok := ProjectProviderSafeItem(item)
	if !ok || got.Relation == nil || got.Relation.ParentID != parentID || got.Relation.ChildID != childID {
		t.Fatalf("relation = %+v ok=%t", got, ok)
	}
	item.SubjectID = uuid.New()
	if got, ok := ProjectProviderSafeItem(item); ok {
		t.Fatalf("mismatched relation projected: %+v", got)
	}
}

func withProviderKind(base Item, kind, payload string) Item {
	base.Kind = kind
	base.Payload = json.RawMessage(payload)
	return base
}
