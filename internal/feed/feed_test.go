package feed

import (
	"sort"
	"testing"

	"github.com/jbmopper/meristem/internal/domain"
)

// TestSignalReceivedIsFeedVisible is the headline test for the
// 2026-04-24 change that made signals show up in /v1/feed. If anyone
// removes signal.received from IncludedKinds without owning that policy
// flip in the coord doc + signals docs, this test fails first.
func TestSignalReceivedIsFeedVisible(t *testing.T) {
	if !contains(IncludedKinds, domain.EventSignalReceived) {
		t.Fatalf("signal.received must appear in IncludedKinds; /v1/feed would otherwise hide successful signals from operators")
	}
}

// TestExpectedKindsAreClassified pins the policy: every event kind we
// currently know about belongs to exactly one of the two lists. This is
// not the same as the drift guard below — this test names every kind, so
// a rename of any constant in internal/domain causes a compile-time
// failure here, not a silent reclassification.
func TestExpectedKindsAreClassified(t *testing.T) {
	expectedIncluded := []string{
		domain.EventMessageCaptured,
		domain.EventWorkItemCreated,
		domain.EventWorkItemTransitioned,
		domain.EventWorkItemEventAppended,
		domain.EventWorkItemRelationAdded,
		domain.EventWorkItemMetadataUpdated,
		domain.EventSignalReceived,
		domain.EventDeterministicErrorReported,
		domain.EventDeterministicErrorMasked,
		domain.EventDeterministicErrorUnmasked,
		domain.EventEscalationRequested,
		domain.EventSubactorGrantRequested,
		domain.EventSubactorGrantGranted,
		domain.EventSubactorGrantDenied,
		domain.EventSubactorGrantEscalated,
		domain.EventPatienceBreached,
		domain.EventConvergenceVerdictRecorded,
		domain.EventConvergenceChecksProposed,
		domain.EventDispatchRequested,
		domain.EventPolicyProfileSwitched,
		domain.EventTropismDefined,
		domain.EventCultivarDefined,
		domain.EventProjectionDefined,
	}
	for _, kind := range expectedIncluded {
		if !contains(IncludedKinds, kind) {
			t.Errorf("kind %q expected in IncludedKinds but missing", kind)
		}
	}

	expectedExcluded := []string{
		domain.EventTokenCreated,
		domain.EventTokenRevoked,
		domain.EventIdempotencyRecorded,
	}
	for _, kind := range expectedExcluded {
		if !contains(ExcludedKinds, kind) {
			t.Errorf("kind %q expected in ExcludedKinds but missing", kind)
		}
	}
}

func TestKindClassTaxonomyCoversEveryDomainKind(t *testing.T) {
	for _, kind := range domain.AllEventKinds {
		if _, _, ok := StaticKindClass(kind); !ok {
			t.Errorf("event kind %q has no R6 taxonomy class", kind)
		}
	}
}

func TestProjectionFilterRejectsAdminKindsAndClasses(t *testing.T) {
	for _, tc := range []ProjectionFilter{
		{Kinds: []string{domain.EventTokenCreated}},
		{Kinds: []string{domain.EventDeterministicErrorReported}},
		{KindClasses: []string{KindClassAdmin}},
	} {
		if _, err := NormalizeProjectionFilter(tc); err == nil {
			t.Fatalf("NormalizeProjectionFilter(%+v) succeeded; admin events must not be projectable", tc)
		}
	}
}

func TestProjectionFilterClassifiesWorkItemEventAppendedByInnerKind(t *testing.T) {
	progress, err := NormalizeProjectionFilter(ProjectionFilter{KindClasses: []string{KindClassProgress}})
	if err != nil {
		t.Fatalf("progress filter: %v", err)
	}
	decision, err := NormalizeProjectionFilter(ProjectionFilter{KindClasses: []string{KindClassDecision}})
	if err != nil {
		t.Fatalf("decision filter: %v", err)
	}
	progressItem := Item{Kind: domain.EventWorkItemEventAppended, Payload: []byte(`{"inner_kind":"agent.progress"}`)}
	coordinationItem := Item{Kind: domain.EventWorkItemEventAppended, Payload: []byte(`{"inner_kind":"coordination.claimed"}`)}
	if !progress.Matches(progressItem) || progress.Matches(coordinationItem) {
		t.Fatalf("progress filter did not split work_item.event_appended by inner_kind")
	}
	if !decision.Matches(coordinationItem) || decision.Matches(progressItem) {
		t.Fatalf("decision filter did not split work_item.event_appended by inner_kind")
	}
}

// TestNoKindIsBothIncludedAndExcluded keeps the partition honest. A kind
// in both lists would mean the policy is contradictory and the SQL filter
// would let it through (Included wins by being the one consulted) while
// the codebase claimed otherwise.
func TestNoKindIsBothIncludedAndExcluded(t *testing.T) {
	for _, kind := range IncludedKinds {
		if contains(ExcludedKinds, kind) {
			t.Errorf("kind %q is in both IncludedKinds and ExcludedKinds; pick one", kind)
		}
	}
}

// TestEveryDomainKindIsClassified is the drift guard. When someone adds a
// new event kind to internal/domain (and to AllEventKinds), this test
// fails until the kind is classified as either feed-visible or
// feed-excluded. The failure points the contributor at the policy
// decision instead of letting it default to "silently dropped from feed".
func TestEveryDomainKindIsClassified(t *testing.T) {
	classified := append([]string{}, IncludedKinds...)
	classified = append(classified, ExcludedKinds...)
	sort.Strings(classified)

	known := append([]string{}, domain.AllEventKinds...)
	sort.Strings(known)

	for _, kind := range known {
		if !contains(classified, kind) {
			t.Errorf("event kind %q is in domain.AllEventKinds but neither feed.IncludedKinds nor feed.ExcludedKinds — classify it", kind)
		}
	}
	for _, kind := range classified {
		if !contains(known, kind) {
			t.Errorf("event kind %q is classified by the feed but missing from domain.AllEventKinds — add it to the canonical enumeration", kind)
		}
	}
}

// TestNoiseKindsAreExcluded is a behaviour test masquerading as a
// classification test: even if someone "fixes" the drift guard by
// reclassifying token administration as feed-visible, this test pins the
// product call that those kinds are noise, not narrative. Flipping it
// requires a deliberate edit and a corresponding doc update.
func TestNoiseKindsAreExcluded(t *testing.T) {
	noise := []string{
		domain.EventTokenCreated,
		domain.EventTokenRevoked,
		domain.EventIdempotencyRecorded,
	}
	for _, kind := range noise {
		if contains(IncludedKinds, kind) {
			t.Errorf("kind %q would surface in /v1/feed; this is audit-log activity, not narrative", kind)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}
	return false
}
