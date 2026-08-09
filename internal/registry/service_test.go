package registry

import (
	"fmt"
	"testing"
)

func TestNormalizeCultivarInputDispatchCapabilityCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		version   int
		rootstock bool
		explicit  string
		want      string
	}{
		{name: "checklist-worker", version: 1, rootstock: true, want: "work_items.execute_checks"},
		{name: "convergence-scribe", version: 1, rootstock: true, want: "convergence.propose_checks"},
		{name: "reviewer", version: 1, rootstock: true, want: "review.exact_artifact"},
		{name: "human-attention", version: 1, rootstock: true, want: "human.attention"},
		{name: "custom-worker", version: 2, want: "cultivar.custom-worker.v2"},
		{name: "custom-worker", version: 2, explicit: " custom.execute ", want: "custom.execute"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s-v%d-%s", tt.name, tt.version, tt.want), func(t *testing.T) {
			in := DefineCultivarInput{
				Name: tt.name, Version: tt.version, Rootstock: tt.rootstock,
				Tropism: TropismRef{Name: "checklist-all", Version: 1},
				Profile: Profile{
					Briefing: "briefings/test.md", ScopesTemplate: []string{"work_items.read"},
					DispatchCapability: tt.explicit,
				},
				Xylem:  Xylem{MaxAttempts: 1, MaxWallSeconds: 60, MaxDepth: 0},
				Phloem: "projection:test",
			}
			normalized, payload, err := normalizeCultivarInput(in)
			if err != nil {
				t.Fatalf("normalize cultivar: %v", err)
			}
			if got := normalized.Profile.DispatchCapability; got != tt.want {
				t.Fatalf("dispatch capability = %q, want %q", got, tt.want)
			}
			profile, ok := payload["profile"].(map[string]any)
			if !ok {
				t.Fatalf("payload profile type = %T", payload["profile"])
			}
			_, persisted := profile["dispatch_capability"]
			if wantPersisted := tt.explicit != ""; persisted != wantPersisted {
				t.Fatalf("persisted dispatch_capability = %t, want %t", persisted, wantPersisted)
			}
		})
	}
}
