package feed

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestNormalizeReadFilter(t *testing.T) {
	tokenID := uuid.New()
	filter, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{
		{Kind: PredicateAssignedOrAddressed, TokenID: tokenID},
		{Kind: PredicateAssignedOrAddressed, TokenID: tokenID},
	}})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(filter.Predicates) != 1 || filter.Predicates[0].TokenID != tokenID {
		t.Fatalf("normalized predicates = %+v", filter.Predicates)
	}

	if _, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed}}}); !errors.Is(err, ErrInvalidPredicate) {
		t.Fatalf("nil token error = %v, want ErrInvalidPredicate", err)
	}
	if _, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: "future", TokenID: tokenID}}}); !errors.Is(err, ErrUnknownPredicate) {
		t.Fatalf("unknown predicate error = %v, want ErrUnknownPredicate", err)
	}
}

func TestNormalizeReadFilterExcludeActor(t *testing.T) {
	caller := uuid.New()
	excludedA := uuid.New()
	excludedB := uuid.New()
	filter, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{
		{Kind: PredicateExcludeActor, TokenID: excludedB},
		{Kind: PredicateAssignedOrAddressed, TokenID: caller},
		{Kind: PredicateExcludeActor, TokenID: excludedA},
		{Kind: PredicateExcludeActor, TokenID: excludedA},
	}})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(filter.Predicates) != 3 {
		t.Fatalf("normalized predicates = %+v, want assigned + 2 deduped exclusions", filter.Predicates)
	}
	if filter.Predicates[0].Kind != PredicateAssignedOrAddressed {
		t.Fatalf("canonical order does not lead with %s: %+v", PredicateAssignedOrAddressed, filter.Predicates)
	}
	for _, predicate := range filter.Predicates[1:] {
		if predicate.Kind != PredicateExcludeActor {
			t.Fatalf("unexpected predicate kind in canonical order: %+v", filter.Predicates)
		}
	}

	if _, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateExcludeActor}}}); !errors.Is(err, ErrInvalidPredicate) {
		t.Fatalf("nil excluded token error = %v, want ErrInvalidPredicate", err)
	}
}

func TestReadFilterAssignmentControlKindsAreRuntimeOnly(t *testing.T) {
	tokenID := uuid.New()
	runtime := ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed, TokenID: tokenID}}}
	kinds := runtime.queryKinds()
	for _, kind := range []string{domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased} {
		if !slices.Contains(kinds, kind) {
			t.Errorf("runtime kinds omit %s", kind)
		}
		if slices.Contains(IncludedKinds, kind) {
			t.Errorf("runtime control kind %s leaked into default IncludedKinds", kind)
		}
	}

	projection := ProjectionFilter{Kinds: []string{domain.EventWorkItemCreated}}
	projected := ReadFilter{Projection: &projection, Predicates: runtime.Predicates}
	for _, kind := range []string{domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased} {
		if slices.Contains(projected.queryKinds(), kind) {
			t.Errorf("persisted projection admitted runtime control kind %s", kind)
		}
	}
}

func TestExplicitAddresseeTokenIDUsesOnlyCanonicalStructuredLocations(t *testing.T) {
	tokenID := uuid.New()
	otherID := uuid.New()
	tests := []struct {
		name string
		kind string
		body any
		want uuid.UUID
	}{
		{name: "native top-level", kind: domain.EventWorkItemCreated, body: map[string]any{"addressee_token_id": tokenID}, want: tokenID},
		{name: "generic event inner", kind: domain.EventWorkItemEventAppended, body: map[string]any{"inner": map[string]any{"addressee_token_id": tokenID}}, want: tokenID},
		{name: "generic event top-level spoof", kind: domain.EventWorkItemEventAppended, body: map[string]any{"addressee_token_id": tokenID}},
		{name: "nested spoof on native kind", kind: domain.EventWorkItemCreated, body: map[string]any{"inner": map[string]any{"addressee_token_id": tokenID}}},
		{name: "free text ignored", kind: domain.EventWorkItemEventAppended, body: map[string]any{"inner": map[string]any{"addressed_to": tokenID.String()}}},
		{name: "assignee spoof on ordinary kind", kind: domain.EventWorkItemCreated, body: map[string]any{"assignee_token_id": tokenID}},
		{name: "assignment control", kind: domain.EventWorkItemAssigned, body: map[string]any{"assignee_token_id": tokenID}, want: tokenID},
		{name: "assignment generic address spoof", kind: domain.EventWorkItemAssigned, body: map[string]any{"addressee_token_id": tokenID}},
		{name: "assignment conflicting generic address", kind: domain.EventWorkItemAssigned, body: map[string]any{"assignee_token_id": tokenID, "addressee_token_id": tokenID}},
		{name: "release control", kind: domain.EventWorkItemAssignmentReleased, body: map[string]any{"assignee_token_id": tokenID}, want: tokenID},
		{name: "conflicting generic identities", kind: domain.EventWorkItemEventAppended, body: map[string]any{"addressee_token_id": tokenID, "inner": map[string]any{"addressee_token_id": otherID}}},
		{name: "malformed", kind: domain.EventWorkItemEventAppended, body: json.RawMessage(`{"inner":`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			if explicit, ok := tc.body.(json.RawMessage); ok {
				raw = explicit
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := ExplicitAddresseeTokenID(Item{Kind: tc.kind, Payload: raw}); got != tc.want {
				t.Fatalf("addressee = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestReadFilterReducerFailsClosed(t *testing.T) {
	want := errors.New("policy unavailable")
	service := &Service{}
	_, err := service.matchingItems(context.Background(), ReadFilter{
		Reduce: func(context.Context, []Item) ([]Item, error) { return nil, want },
	}, []Item{{EventID: uuid.New(), Kind: domain.EventWorkItemCreated}})
	if !errors.Is(err, want) {
		t.Fatalf("matching error = %v, want wrapped reducer error", err)
	}
}
