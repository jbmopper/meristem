package listeners

// Outcome fixtures for the owner-visible listening contract (LCP2-B1). The
// four owner instructions are pinned twice: once as normalized policies with
// stable predicate fingerprints (the cursor-identity contract), and once as
// OUTCOMES — for each instruction, a representative matching demand, a
// nonmatching demand, and ordinary chatter run through the actual eligibility
// reduction. If normalization, the fingerprint contract, or the demand
// semantics drift, these break loudly.

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

var (
	fixtureListenerID     = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	fixtureFableToken     = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	fixtureNetworkingTree = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	fixtureOtherToken     = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	fixtureOtherTree      = uuid.MustParse("55555555-5555-4555-8555-555555555555")
	fixtureWorkItem       = uuid.MustParse("66666666-6666-4666-8666-666666666666")
)

// demand builds a well-formed dispatch demand envelope; overrides mutate it.
func demandEnvelope(overrides ...func(*DemandEnvelope)) DemandEnvelope {
	env := DemandEnvelope{
		Capability:    "review.complementary",
		EventKind:     domain.EventDispatchRequested,
		WorkItemID:    fixtureWorkItem,
		Lineage:       []uuid.UUID{fixtureWorkItem, fixtureNetworkingTree},
		OriginTokenID: fixtureFableToken,
	}
	for _, o := range overrides {
		o(&env)
	}
	return env
}

func TestOwnerInstructionOutcomeFixtures(t *testing.T) {
	registered := []string{"review.complementary"}

	fixtures := []struct {
		instruction string
		policy      Policy
		fingerprint string
		matching    DemandEnvelope
		nonmatching DemandEnvelope
	}{
		{
			instruction: "Listen for everything",
			policy:      Policy{Predicates: nil, MaxConcurrentAssignments: 1},
			fingerprint: "", // empty predicate set = plain demand lane, no fingerprint
			matching:    demandEnvelope(),
			// "Everything" means all eligible DEMAND, not all activity: the
			// only nonmatch is a capability nobody registered.
			nonmatching: demandEnvelope(func(e *DemandEnvelope) { e.Capability = "capability.unoffered" }),
		},
		{
			instruction: "Listen to Fable",
			policy: Policy{
				Predicates:               []PredicateWire{{Kind: "actor", TokenIDs: []string{fixtureFableToken.String()}}},
				MaxConcurrentAssignments: 1,
			},
			fingerprint: "d18d5b4adda6101700dcf8f3fdead507",
			// The demand event is SYSTEM-authored; the actor predicate must
			// match the originating principal carried on the envelope.
			matching:    demandEnvelope(),
			nonmatching: demandEnvelope(func(e *DemandEnvelope) { e.OriginTokenID = fixtureOtherToken }),
		},
		{
			instruction: "Listen for networking work",
			policy: Policy{
				Predicates: []PredicateWire{
					{Kind: "work_item_tree", WorkItemID: fixtureNetworkingTree.String()},
					{Kind: "kind_include", EventKinds: []string{domain.EventDispatchRequested}},
				},
				MaxConcurrentAssignments: 1,
			},
			fingerprint: "2a7409bb3fdfeecba9cf9608c0c4a977",
			matching:    demandEnvelope(),
			nonmatching: demandEnvelope(func(e *DemandEnvelope) {
				e.Lineage = []uuid.UUID{fixtureWorkItem, fixtureOtherTree}
			}),
		},
		{
			instruction: "Pick up one thing, finish it, then listen again",
			policy:      Policy{Predicates: nil, MaxConcurrentAssignments: 1, Focus: FocusClaimedWorkItemTree},
			fingerprint: "",
			matching:    demandEnvelope(),
			nonmatching: demandEnvelope(func(e *DemandEnvelope) { e.Capability = "capability.unoffered" }),
		},
	}
	for _, fixture := range fixtures {
		normalized, fingerprint, err := NormalizePolicy(fixture.policy, fixtureListenerID, registered)
		if err != nil {
			t.Errorf("%s: normalize: %v", fixture.instruction, err)
			continue
		}
		if fingerprint != fixture.fingerprint {
			t.Errorf("%s: fingerprint = %q, want pinned %q — predicate identity drifted", fixture.instruction, fingerprint, fixture.fingerprint)
		}
		if normalized.Projection != DemandProjection {
			t.Errorf("%s: projection = %q, want the pinned demand lane %q", fixture.instruction, normalized.Projection, DemandProjection)
		}
		if !EligibleDemand(&normalized, registered, fixture.matching) {
			t.Errorf("%s: matching demand refused: %+v", fixture.instruction, fixture.matching)
		}
		if EligibleDemand(&normalized, registered, fixture.nonmatching) {
			t.Errorf("%s: nonmatching demand admitted: %+v", fixture.instruction, fixture.nonmatching)
		}
		// Ordinary chatter — same work item, same origin, but not a demand
		// kind — is never eligible, whatever the predicates say. This is the
		// LCP2-B1 failure scenario: a listener configured for "everything"
		// must not react to unrelated activity.
		chatter := demandEnvelope(func(e *DemandEnvelope) { e.EventKind = "agent.status" })
		if EligibleDemand(&normalized, registered, chatter) {
			t.Errorf("%s: ordinary chatter admitted as demand", fixture.instruction)
		}
	}
}

func TestEligibleDemandPredicateSemantics(t *testing.T) {
	registered := []string{"review.complementary"}
	cases := []struct {
		name      string
		predicate PredicateWire
		env       DemandEnvelope
		want      bool
	}{
		{"exclude_actor passes other origins", PredicateWire{Kind: "exclude_actor", TokenID: fixtureOtherToken.String()}, demandEnvelope(), true},
		{"exclude_actor refuses the named origin", PredicateWire{Kind: "exclude_actor", TokenID: fixtureFableToken.String()}, demandEnvelope(), false},
		{"work_item exact match", PredicateWire{Kind: "work_item", WorkItemID: fixtureWorkItem.String()}, demandEnvelope(), true},
		{"work_item other item refused", PredicateWire{Kind: "work_item", WorkItemID: fixtureOtherTree.String()}, demandEnvelope(), false},
		{"kind_exclude refuses excluded demand kind", PredicateWire{Kind: "kind_exclude", EventKinds: []string{domain.EventDispatchRequested}}, demandEnvelope(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			policy := Policy{Predicates: []PredicateWire{c.predicate}, Capabilities: registered}
			if got := EligibleDemand(&policy, registered, c.env); got != c.want {
				t.Errorf("EligibleDemand = %v, want %v", got, c.want)
			}
		})
	}

	t.Run("unknown predicate vocabulary fails closed", func(t *testing.T) {
		policy := Policy{Predicates: []PredicateWire{{Kind: "vibes"}}, Capabilities: registered}
		if EligibleDemand(&policy, registered, demandEnvelope()) {
			t.Error("un-taught predicate vocabulary admitted demand")
		}
	})
	t.Run("nil policy listens to all eligible demand for registered capabilities", func(t *testing.T) {
		if !EligibleDemand(nil, registered, demandEnvelope()) {
			t.Error("nil-policy registration refused eligible demand")
		}
		if EligibleDemand(nil, registered, demandEnvelope(func(e *DemandEnvelope) { e.EventKind = "agent.status" })) {
			t.Error("nil-policy registration admitted chatter")
		}
		if EligibleDemand(nil, registered, demandEnvelope(func(e *DemandEnvelope) { e.Capability = "capability.unoffered" })) {
			t.Error("nil-policy registration admitted an unregistered capability")
		}
	})
}
