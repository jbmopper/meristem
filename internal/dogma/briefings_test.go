package dogma

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// briefingArtifacts pins the committed artifact path for each rootstock
// cultivar. These paths are the same ones the registry seed fixtures declare
// in cmd/meristem/seed_registry.go (Profile.Briefing, relative to docs/);
// TestBriefingPathsMatchSeedFixtures keeps the two from drifting.
var briefingArtifacts = map[string]string{
	"convergence-scribe": "docs/briefings/convergence-scribe.md",
	"human-attention":    "docs/briefings/human-attention.md",
	"checklist-worker":   "docs/briefings/checklist-worker.md",
}

// TestRootstockBriefingsAreFreshProjections regenerates every briefing and
// requires the committed artifact to match byte-for-byte and stay under the
// R9 line ceiling. Run with UPDATE_BRIEFINGS=1 to rewrite the artifacts
// after changing the generator (golden-file convention).
func TestRootstockBriefingsAreFreshProjections(t *testing.T) {
	root := repoRoot(t)
	if err := VerifySourceSections(root); err != nil {
		t.Fatal(err)
	}
	for name, rel := range briefingArtifacts {
		generated, err := GenerateBriefing(name)
		if err != nil {
			t.Fatalf("generate %s: %v", name, err)
		}
		lines := strings.Count(generated, "\n")
		if lines > MaxBriefingLines {
			t.Errorf("briefing %s is %d lines; R9 ceiling is %d — a briefing a small model can hold", name, lines, MaxBriefingLines)
		}
		path := filepath.Join(root, rel)
		if os.Getenv("UPDATE_BRIEFINGS") == "1" {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir for %s: %v", rel, err)
			}
			if err := os.WriteFile(path, []byte(generated), 0o644); err != nil {
				t.Fatalf("update %s: %v", rel, err)
			}
			continue
		}
		committed, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s (run UPDATE_BRIEFINGS=1 go test ./internal/dogma/ to generate): %v", rel, err)
		}
		if string(committed) != generated {
			t.Errorf("briefing %s has drifted from its generator; run UPDATE_BRIEFINGS=1 go test ./internal/dogma/", rel)
		}
	}
}

// TestBriefingPathsMatchSeedFixtures asserts the registry seed declares
// exactly the briefing paths this package generates artifacts for, so the
// registry's Profile.Briefing pointers can never dangle.
func TestBriefingPathsMatchSeedFixtures(t *testing.T) {
	root := repoRoot(t)
	seedSource, err := os.ReadFile(filepath.Join(root, "cmd/meristem/seed_registry.go"))
	if err != nil {
		t.Fatalf("read seed_registry.go: %v", err)
	}
	for name, rel := range briefingArtifacts {
		declared := "briefings/" + name + ".md"
		if !strings.Contains(string(seedSource), `Briefing: "`+declared+`"`) {
			t.Errorf("seed fixture for %s does not declare Briefing %q; briefing artifacts and registry fixtures must stay aligned", name, declared)
		}
		if !strings.HasSuffix(rel, declared) {
			t.Errorf("artifact path %s does not correspond to declared briefing %s", rel, declared)
		}
	}
}
