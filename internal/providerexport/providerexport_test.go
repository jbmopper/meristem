package providerexport

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/providercontext"
)

func testPolicy() providercontext.ContextPolicy {
	return providercontext.ContextPolicy{
		WorkItemID:        uuid.MustParse("accd39bb-eb95-493f-ade7-efc858ebe6d8"),
		ProviderID:        "cursor-cli",
		RepoPath:          ".",
		RepoRef:           "v1",
		AllowedPaths:      []string{"docs/", "internal/exports/"},
		DeniedPaths:       append([]string{"internal/auth/"}, providercontext.BuiltinSecretDenyList...),
		RedactionPolicyID: RedactionPolicyID,
		MCPToolAllowlist:  []string{"work_items.read"},
		LaunchMode:        providercontext.LaunchWorktree,
		RequiredReview:    string(domain.HumanReviewWavedThrough),
		ReducerID:         providercontext.ReducerID,
		PatienceSeconds:   3600,
		Message:           "narrative, excluded from policy hash",
	}
}

func TestPlanSelection(t *testing.T) {
	tree := []TreeMeta{
		{Path: "docs/spec.md", Mode: "100644"},
		{Path: "docs/.env", Mode: "100644"},                  // deny (builtin, nested)
		{Path: "docs/link.md", Mode: "120000"},               // symlink
		{Path: "docs/vendor", Mode: "160000"},                // submodule
		{Path: "internal/exports/walker.go", Mode: "100755"}, // exec blob
		{Path: "internal/auth/keys.go", Mode: "100644"},      // outside allow: silent
		{Path: "docs/keys/prod.token", Mode: "100644"},       // deny (*.token, nested)
		{Path: "docs/x/.meristem/api.token", Mode: "100644"}, // deny (.meristem/, nested)
	}
	included, omitted, err := Plan(testPolicy(), tree)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	wantIncluded := []string{"docs/spec.md", "internal/exports/walker.go"}
	if len(included) != len(wantIncluded) {
		t.Fatalf("included = %v", included)
	}
	for i, w := range wantIncluded {
		if included[i].Path != w {
			t.Fatalf("included[%d] = %q, want %q", i, included[i].Path, w)
		}
	}
	reasons := map[string]string{}
	for _, o := range omitted {
		reasons[o.Path] = o.Reason
	}
	for path, want := range map[string]string{
		"docs/.env":                  "denied_path",
		"docs/link.md":               "symlink",
		"docs/vendor":                "submodule",
		"docs/keys/prod.token":       "denied_path",
		"docs/x/.meristem/api.token": "denied_path",
	} {
		if reasons[path] != want {
			t.Errorf("omitted[%s] = %q, want %q", path, reasons[path], want)
		}
	}
	if _, leaked := reasons["internal/auth/keys.go"]; leaked {
		t.Error("path outside allowlist was enumerated in omitted — privacy leak")
	}
}

func TestPlanRefusesBadPolicy(t *testing.T) {
	p := testPolicy()
	p.DeniedPaths = []string{"internal/auth/"} // missing builtin coverage
	if _, _, err := Plan(p, nil); err == nil || !strings.Contains(err.Error(), "builtin secret") {
		t.Fatalf("err = %v, want builtin secret coverage error", err)
	}
	p2 := testPolicy()
	p2.AllowedPaths = []string{"../escape"}
	if _, _, err := Plan(p2, nil); err == nil || !strings.Contains(err.Error(), "traverses") {
		t.Fatalf("err = %v, want traversal error", err)
	}
}

func TestScanContent(t *testing.T) {
	cases := []struct {
		name    string
		content string
		rule    string
	}{
		{"clean go file", "package main\n\nfunc main() {}\n", ""},
		{"pem key", "-----BEGIN RSA PRIVATE KEY-----\nabc", "pem_private_key"},
		{"aws key", "cfg := \"AKIAIOSFODNN7EXAMPLE\"", "aws_access_key_id"},
		{"meristem token", "Bearer mrs_98HlzAZaZNHTPJhVE3H5VbMRZpZGiHgIS0dL", "meristem_bearer_token"},
		{"env secret line", "DB_PASSWORD=hunter2hunter2\n", "generic_env_secret"},
		{"secret word in prose", "the password rules are documented elsewhere", ""},
	}
	for _, tc := range cases {
		passed, rule := ScanContent("f.txt", []byte(tc.content))
		if (rule == "") != (tc.rule == "") || rule != tc.rule {
			t.Errorf("%s: passed=%v rule=%q, want rule %q", tc.name, passed, rule, tc.rule)
		}
	}
}

func TestBundleDigestDeterminism(t *testing.T) {
	files := []IncludedFile{
		{Path: "b.txt", Mode: "100644", SHA256: "sha256:bb", RedactionPassed: true},
		{Path: "a.txt", Mode: "100755", SHA256: "sha256:aa", RedactionPassed: true},
	}
	d1 := ComputeBundleDigest(files)
	reversed := []IncludedFile{files[1], files[0]}
	if d2 := ComputeBundleDigest(reversed); d2 != d1 {
		t.Fatalf("digest depends on input order: %s vs %s", d1, d2)
	}
	files[0].SHA256 = "sha256:bc"
	if d3 := ComputeBundleDigest(files); d3 == d1 {
		t.Fatal("digest did not change with content hash")
	}
}

func TestPolicyHashExcludesNarrative(t *testing.T) {
	p := testPolicy()
	h1 := PolicyHash(p)
	p.Message = "different narrative"
	if h2 := PolicyHash(p); h2 != h1 {
		t.Fatal("policy hash must ignore the narrative Message field")
	}
	p.AllowedPaths = append(p.AllowedPaths, "cmd/")
	if h3 := PolicyHash(p); h3 == h1 {
		t.Fatal("policy hash must change with structural fields")
	}
}

// TestContractParityWithReducer is the load-bearing test from the design:
// a planned+scanned export must be accepted by providercontext.Reduce.
func TestContractParityWithReducer(t *testing.T) {
	policy := testPolicy()
	tree := []TreeMeta{
		{Path: "docs/spec.md", Mode: "100644"},
		{Path: "internal/exports/walker.go", Mode: "100755"},
		{Path: "docs/.env", Mode: "100644"},
	}
	planned, _, err := Plan(policy, tree)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	var included []IncludedFile
	blobs := map[string]string{
		"docs/spec.md":               "# spec\n",
		"internal/exports/walker.go": "package exports\n",
	}
	for _, e := range planned {
		content := blobs[e.Path]
		passed, rule := ScanContent(e.Path, []byte(content))
		if !passed {
			t.Fatalf("scan failed on %s: %s", e.Path, rule)
		}
		included = append(included, IncludedFile{Path: e.Path, Mode: e.Mode, Size: int64(len(content)), SHA256: "sha256:deadbeef", RedactionPassed: true})
	}
	_, gen, entries := BuildManifest(policy, "f2a10317c9e4a5b0d3f6e8a1b2c3d4e5f6a7b8c9", included, nil)
	req := providercontext.Request{
		Policy:               policy,
		Generated:            gen,
		Manifest:             entries,
		AppliedRedactionID:   RedactionPolicyID,
		WorkItemFound:        true,
		WorkItemState:        domain.WorkItemRunning,
		HumanReviewStatus:    domain.HumanReviewWavedThrough,
		RegisteredGenerators: map[string][]string{"cursor-cli": {GeneratorID}},
		KnownRedactionIDs:    []string{RedactionPolicyID},
		GrantableTools:       []string{"work_items.read"},
	}
	got := providercontext.Reduce(req)
	if got.Disposition != domain.VerdictAccept {
		t.Fatalf("reducer rejected exporter output: %s (%s)", got.Disposition, got.Reason)
	}
}
