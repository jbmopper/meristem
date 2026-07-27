package feed

// Issued cursors embed the filter fingerprint, so the canonical encoding of
// every pre-existing predicate shape is a wire compatibility surface: if a
// deploy changes any of these constants, every outstanding filtered cursor
// (production listener lanes included) is invalidated at once. The expected
// values below were computed at v1 c0a82e5, the last commit before the
// multi-actor set shape was introduced. Do NOT update them to make a failing
// build pass — a mismatch means the change breaks issued cursors.

import (
	"testing"

	"github.com/google/uuid"
)

func TestCanonicalFingerprintsAreStable(t *testing.T) {
	a := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	b := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	w := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	cases := []struct {
		name   string
		filter ReadFilter
		want   string
	}{
		{"assigned", ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed, TokenID: a}}}, "0ea64835c31c65692a8158c624830bf7"},
		{"assigned_exclude_self", ReadFilter{Predicates: []Predicate{{Kind: PredicateAssignedOrAddressed, TokenID: a}, {Kind: PredicateExcludeActor, TokenID: a}}}, "8e8e95285b7eb09b30e0159c317f0595"},
		{"exclude", ReadFilter{Predicates: []Predicate{{Kind: PredicateExcludeActor, TokenID: b}}}, "e046025e9d259956ab20335bf1bffbd9"},
		{"single_actor_legacy_field", ReadFilter{Predicates: []Predicate{{Kind: PredicateActor, TokenID: b}}}, "d18d5b4adda6101700dcf8f3fdead507"},
		{"single_actor_set_field", ReadFilter{Predicates: []Predicate{{Kind: PredicateActor, TokenIDs: []uuid.UUID{b}}}}, "d18d5b4adda6101700dcf8f3fdead507"},
		{"work_item", ReadFilter{Predicates: []Predicate{{Kind: PredicateWorkItem, WorkItemID: w}}}, "3128559bf689e87d3f51a7a9aca61237"},
		{"tree", ReadFilter{Predicates: []Predicate{{Kind: PredicateWorkItemTree, WorkItemID: w}}}, "7ac100a61ca51980d5bcad15231b64c3"},
		{"kinds", ReadFilter{Predicates: []Predicate{{Kind: PredicateKindInclude, EventKinds: []string{"work_item.created", "work_item.event_appended"}}}}, "ea823bc4a086d87c3c28b7928acbf88a"},
		{"kind_exclude", ReadFilter{Predicates: []Predicate{{Kind: PredicateKindExclude, EventKinds: []string{"patience.breached"}}}}, "5d2ee9362a854c298dce803b28d5d98c"},
	}
	for _, tc := range cases {
		normalized, err := NormalizeReadFilter(tc.filter)
		if err != nil {
			t.Fatalf("%s: normalize: %v", tc.name, err)
		}
		if got := normalized.FingerprintHash(); got != tc.want {
			t.Fatalf("%s: fingerprint drifted to %s (want %s) — this invalidates issued cursors", tc.name, got, tc.want)
		}
	}

	// The genuinely new shape must be a distinct identity from every
	// single-actor form, and order/duplication-insensitive within itself.
	multi, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateActor, TokenIDs: []uuid.UUID{a, b}}}})
	if err != nil {
		t.Fatalf("normalize multi: %v", err)
	}
	multiSwapped, err := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateActor, TokenIDs: []uuid.UUID{b, a, b}}}})
	if err != nil {
		t.Fatalf("normalize multi swapped: %v", err)
	}
	if multi.FingerprintHash() != multiSwapped.FingerprintHash() {
		t.Fatalf("multi-actor identity is order/duplication sensitive")
	}
	single, _ := NormalizeReadFilter(ReadFilter{Predicates: []Predicate{{Kind: PredicateActor, TokenID: b}}})
	if multi.FingerprintHash() == single.FingerprintHash() {
		t.Fatalf("multi-actor identity collided with a single-actor identity")
	}
}
