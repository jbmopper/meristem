package dogma

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
)

// briefingArtifacts pins the committed artifact path for each rootstock
// cultivar. These paths are the same ones the registry seed fixtures declare
// in cmd/meristem/seed_registry.go (Profile.Briefing, relative to docs/);
// TestBriefingPathsMatchSeedFixtures keeps the two from drifting.
var briefingArtifacts = map[string]string{
	"convergence-scribe": "docs/briefings/convergence-scribe.md",
	"human-attention":    "docs/briefings/human-attention.md",
	"checklist-worker":   "docs/briefings/checklist-worker.md",
	"reviewer":           "docs/briefings/reviewer.md",
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

func TestChecklistWorkerBriefingPinsReducerReadableEvidenceContract(t *testing.T) {
	generated, err := GenerateBriefing("checklist-worker")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"reviewed, assignment-fenced start path",
		"exact assignment generation to running",
		"That blocker does not transition or dispose the item",
		"inspect the available assigned feed/history",
		"same lowercase SHA-256 idempotency key",
		"up to 3 bounded local attempts",
		"Do not append checklist.item evidence for intermediate attempts",
		"exactly one final result for that CHECK",
		"kind checklist.item:<exact CHECK>",
		"boolean pass",
		"bounded string raw",
		"do not set pass=false",
		"checklist.blocked:<exact CHECK>",
		"blocker is audit evidence only",
		"running-state wall patience owns escalation",
		"irrevocable for the item",
		"do not append another checklist.item result for that CHECK",
		"never nest kind inside payload",
		"Never emit checks_passed/checks_failed",
		"deterministic verdict machinery owns lifecycle disposal",
	} {
		if !strings.Contains(generated, required) {
			t.Errorf("checklist-worker briefing does not pin %q", required)
		}
	}
	if !strings.Contains(generated, checklistEvidenceExampleArgs) {
		t.Fatal("checklist-worker briefing omits its pinned work_items.append_event example")
	}
	if !strings.Contains(generated, checklistBlockerExampleArgs) {
		t.Fatal("checklist-worker briefing omits its pinned cannot-run example")
	}
	if strings.Contains(generated, "fails or cannot run") {
		t.Fatal("checklist-worker briefing still conflates a final runnable failure with cannot-run evidence")
	}

	var args struct {
		ID             string `json:"id"`
		Kind           string `json:"kind"`
		IdempotencyKey string `json:"idempotency_key"`
		Payload        struct {
			Pass bool   `json:"pass"`
			Raw  string `json:"raw"`
		} `json:"payload"`
	}
	if err := json.Unmarshal([]byte(checklistEvidenceExampleArgs), &args); err != nil {
		t.Fatalf("pinned work_items.append_event example is not JSON: %v", err)
	}
	wantKind := "checklist.item:" + checklistEvidenceExampleCheck
	if args.Kind != wantKind {
		t.Fatalf("example kind = %q, want exact declared-check kind %q", args.Kind, wantKind)
	}
	if args.ID == "" || args.IdempotencyKey == "" || args.Payload.Raw == "" {
		t.Fatalf("example omits required routing/audit fields: %+v", args)
	}

	verdict, err := (convergence.AllPassChecklist{Required: []string{checklistEvidenceExampleCheck}}).Reduce([]convergence.Signal{{
		Kind: args.Kind,
		Pass: &args.Payload.Pass,
		Raw:  args.Payload.Raw,
	}})
	if err != nil {
		t.Fatalf("reduce pinned checklist signal: %v", err)
	}
	if verdict.Disposition != domain.VerdictAccept {
		t.Fatalf("pinned checklist signal is not reducer-readable: got %q (%s)", verdict.Disposition, verdict.Reason)
	}

	failed := false
	verdict, err = (convergence.AllPassChecklist{Required: []string{checklistEvidenceExampleCheck}}).Reduce([]convergence.Signal{{
		Kind: args.Kind,
		Pass: &failed,
		Raw:  "bounded audit-safe failure evidence",
	}})
	if err != nil {
		t.Fatalf("reduce pinned failing checklist signal: %v", err)
	}
	if verdict.Disposition != domain.VerdictReject {
		t.Fatalf("pinned checklist failure is not reducer-readable: got %q (%s)", verdict.Disposition, verdict.Reason)
	}

	// all_pass_checklist@1 is deliberately strict: once a false reading is
	// appended, a later true reading cannot heal it. This pins why local
	// transient attempts must never be emitted as checklist.item evidence.
	transientFalse := false
	finalTrue := true
	verdict, err = (convergence.AllPassChecklist{Required: []string{checklistEvidenceExampleCheck}}).Reduce([]convergence.Signal{
		{Kind: args.Kind, Pass: &transientFalse, Raw: "transient local failure"},
		{Kind: args.Kind, Pass: &finalTrue, Raw: "later local pass"},
	})
	if err != nil {
		t.Fatalf("reduce forbidden transient checklist evidence: %v", err)
	}
	if verdict.Disposition != domain.VerdictReject {
		t.Fatalf("strict reducer contract changed: transient false then true got %q, want reject", verdict.Disposition)
	}

	var blocker struct {
		ID             string                     `json:"id"`
		Kind           string                     `json:"kind"`
		IdempotencyKey string                     `json:"idempotency_key"`
		Payload        map[string]json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(checklistBlockerExampleArgs), &blocker); err != nil {
		t.Fatalf("pinned cannot-run example is not JSON: %v", err)
	}
	if blocker.Kind != "checklist.blocked:"+checklistEvidenceExampleCheck {
		t.Fatalf("cannot-run kind = %q, want typed non-check blocker", blocker.Kind)
	}
	if _, hasPass := blocker.Payload["pass"]; hasPass {
		t.Fatal("cannot-run evidence must not carry pass; that would poison the strict checklist reducer")
	}
	if len(blocker.Payload["raw"]) == 0 || blocker.ID == "" || blocker.IdempotencyKey == "" {
		t.Fatalf("cannot-run example omits required routing/audit fields: %+v", blocker)
	}
	verdict, err = (convergence.AllPassChecklist{Required: []string{checklistEvidenceExampleCheck}}).Reduce([]convergence.Signal{{
		Kind: blocker.Kind,
		Raw:  "cannot run: required tool is outside the assigned scope",
	}})
	if err != nil {
		t.Fatalf("reduce cannot-run evidence: %v", err)
	}
	if verdict.Disposition != domain.VerdictReject || !strings.Contains(verdict.Reason, "missing:") {
		t.Fatalf("cannot-run evidence must remain non-check evidence: got %q (%s)", verdict.Disposition, verdict.Reason)
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
