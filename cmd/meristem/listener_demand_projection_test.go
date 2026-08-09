package main

import (
	"slices"
	"testing"

	"github.com/jbmopper/meristem/internal/listeners"
)

// TestListenerDemandProjectionPinMatchesSeed pins the LCP2-B1 eligibility
// gate to the seeded rootstock definition: listener policies restrict to the
// dispatch demand lane, and listeners.DemandProjectionKinds is the constant
// the eligibility reduction trusts. If the seeded dispatch projection's kind
// set ever changes, this fails loudly instead of listeners silently widening
// or narrowing what counts as demand.
func TestListenerDemandProjectionPinMatchesSeed(t *testing.T) {
	for _, def := range projectionSeedDefinitions {
		if def.Name != listeners.DemandProjection {
			continue
		}
		seeded := slices.Clone(def.Filter.Kinds)
		pinned := slices.Clone(listeners.DemandProjectionKinds)
		slices.Sort(seeded)
		slices.Sort(pinned)
		if !slices.Equal(seeded, pinned) {
			t.Fatalf("dispatch projection kinds drifted: seeded %v, listener pin %v", seeded, pinned)
		}
		if def.Version != 1 {
			t.Fatalf("dispatch projection version = %d; the listener demand pin assumes the immutable v1 definition — revisit listeners.DemandProjectionKinds", def.Version)
		}
		return
	}
	t.Fatalf("no seeded projection named %q", listeners.DemandProjection)
}
