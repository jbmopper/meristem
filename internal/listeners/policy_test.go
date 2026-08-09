package listeners

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func normalizeForTest(t *testing.T, p Policy) Policy {
	t.Helper()
	normalized, _, err := NormalizePolicy(p, fixtureListenerID, []string{"review.complementary"})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return normalized
}

// TestNarrowsFocusOrdering is the LCP2-B2 regression: focus participates in
// the narrowing order. claimed_work_item_tree -> retain_base would let an
// assigned listener keep consuming base-lane demand, so it is a WIDENING and
// must refuse; the reverse move tightens and is allowed.
func TestNarrowsFocusOrdering(t *testing.T) {
	claimed := normalizeForTest(t, Policy{MaxConcurrentAssignments: 1, Focus: FocusClaimedWorkItemTree})
	retain := normalizeForTest(t, Policy{MaxConcurrentAssignments: 1, Focus: FocusRetainBase})

	if Narrows(claimed, retain) {
		t.Error("claimed_work_item_tree -> retain_base admitted: a principal could widen its own focus")
	}
	if !Narrows(retain, claimed) {
		t.Error("retain_base -> claimed_work_item_tree refused: tightening focus should narrow")
	}
	if !Narrows(claimed, claimed) || !Narrows(retain, retain) {
		t.Error("identical focus refused")
	}
}

// TestNormalizePolicyPinsDemandProjection is the LCP2-B1 projection gate: an
// absent projection defaults to the immutable dispatch demand lane and any
// other projection fails closed, so a listener policy can never select
// general activity.
func TestNormalizePolicyPinsDemandProjection(t *testing.T) {
	normalized := normalizeForTest(t, Policy{MaxConcurrentAssignments: 1})
	if normalized.Projection != DemandProjection {
		t.Errorf("defaulted projection = %q, want %q", normalized.Projection, DemandProjection)
	}
	if _, _, err := NormalizePolicy(Policy{Projection: "activity", MaxConcurrentAssignments: 1}, fixtureListenerID, []string{"review.complementary"}); !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("non-demand projection admitted: %v", err)
	}
}

func TestNormalizePolicyRefusesForeignListenerID(t *testing.T) {
	other := uuid.MustParse("99999999-9999-4999-8999-999999999999")
	if _, _, err := NormalizePolicy(Policy{ListenerID: other, MaxConcurrentAssignments: 1}, fixtureListenerID, []string{"review.complementary"}); !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("foreign listener_id admitted: %v", err)
	}
}
