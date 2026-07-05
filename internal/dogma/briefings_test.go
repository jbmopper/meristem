package dogma

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
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
	rootstockBriefings := rootstockSeedBriefings(t, filepath.Join(root, "cmd/meristem/seed_registry.go"))
	for name, rel := range briefingArtifacts {
		declared := "briefings/" + name + ".md"
		if rootstockBriefings[name] != declared {
			t.Errorf("seed fixture for %s does not declare Briefing %q; briefing artifacts and registry fixtures must stay aligned", name, declared)
		}
		if !strings.HasSuffix(rel, declared) {
			t.Errorf("artifact path %s does not correspond to declared briefing %s", rel, declared)
		}
	}
	for name, briefing := range rootstockBriefings {
		if _, ok := briefingArtifacts[name]; !ok {
			t.Errorf("rootstock cultivar %s declares Briefing %q but has no generated artifact in internal/dogma briefingArtifacts", name, briefing)
		}
	}
}

func rootstockSeedBriefings(t *testing.T, path string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) != 1 || valueSpec.Names[0].Name != "registrySeedCultivars" || len(valueSpec.Values) != 1 {
				continue
			}
			lit, ok := valueSpec.Values[0].(*ast.CompositeLit)
			if !ok {
				t.Fatalf("registrySeedCultivars is not a composite literal")
			}
			for _, elt := range lit.Elts {
				cultivar, ok := elt.(*ast.CompositeLit)
				if !ok {
					t.Fatalf("registrySeedCultivars contains non-composite element %T", elt)
				}
				name, briefing, rootstock := seedCultivarFields(t, cultivar)
				if rootstock {
					if name == "" || briefing == "" {
						t.Fatalf("rootstock seed cultivar missing name or briefing: name=%q briefing=%q", name, briefing)
					}
					out[name] = briefing
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no rootstock seed cultivars found")
	}
	return out
}

func seedCultivarFields(t *testing.T, cultivar *ast.CompositeLit) (name string, briefing string, rootstock bool) {
	t.Helper()
	for _, elt := range cultivar.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		switch key.Name {
		case "Name":
			name = stringLiteral(t, kv.Value)
		case "Rootstock":
			rootstock = boolLiteral(t, kv.Value)
		case "Profile":
			profile, ok := kv.Value.(*ast.CompositeLit)
			if !ok {
				t.Fatalf("Profile is not a composite literal")
			}
			briefing = profileBriefing(t, profile)
		}
	}
	return name, briefing, rootstock
}

func profileBriefing(t *testing.T, profile *ast.CompositeLit) string {
	t.Helper()
	for _, elt := range profile.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if ok && key.Name == "Briefing" {
			return stringLiteral(t, kv.Value)
		}
	}
	return ""
}

func stringLiteral(t *testing.T, expr ast.Expr) string {
	t.Helper()
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		t.Fatalf("expected string literal, got %T", expr)
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		t.Fatalf("unquote %s: %v", lit.Value, err)
	}
	return value
}

func boolLiteral(t *testing.T, expr ast.Expr) bool {
	t.Helper()
	ident, ok := expr.(*ast.Ident)
	if !ok || (ident.Name != "true" && ident.Name != "false") {
		t.Fatalf("expected bool literal, got %T", expr)
	}
	return ident.Name == "true"
}
