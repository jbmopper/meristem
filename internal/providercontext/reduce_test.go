package providercontext

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func validRequest() Request {
	return Request{
		Policy: ContextPolicy{
			WorkItemID:        uuid.MustParse("4daea3d0-d693-43dd-ab5b-a86e25fd6801"),
			ProviderID:        "cursor-cli",
			RepoPath:          ".",
			RepoRef:           "v1",
			AllowedPaths:      []string{"internal/exports/", "docs/spec.md"},
			DeniedPaths:       append([]string{"internal/auth/"}, BuiltinSecretDenyList...),
			RedactionPolicyID: "builtin:secret_deny@1",
			MCPToolAllowlist:  []string{"work_items.read", "feed.read"},
			LaunchMode:        LaunchWorktree,
			RequiredReview:    string(domain.HumanReviewWavedThrough),
			ReducerID:         ReducerID,
			PatienceSeconds:   3600,
		},
		Generated: Generated{
			GeneratorID:  "export@1",
			SourceCommit: "f2a1031",
			BundleDigest: "sha256:abc",
			PathCount:    2,
		},
		Manifest: []ManifestEntry{
			{Path: "internal/exports/walker.go", RedactionPassed: true},
			{Path: "docs/spec.md", RedactionPassed: true},
		},
		AppliedRedactionID:   "builtin:secret_deny@1",
		WorkItemFound:        true,
		WorkItemState:        domain.WorkItemRunning,
		HumanReviewStatus:    domain.HumanReviewWavedThrough,
		RegisteredGenerators: map[string][]string{"cursor-cli": {"export@1"}},
		KnownRedactionIDs:    []string{"builtin:secret_deny@1"},
		GrantableTools:       []string{"work_items.read", "work_items.transition", "feed.read"},
	}
}

func TestReduceAccepts(t *testing.T) {
	got := Reduce(validRequest())
	if got.Disposition != domain.VerdictAccept {
		t.Fatalf("disposition = %s (%s), want accept", got.Disposition, got.Reason)
	}
	want := []string{
		"anchor_work_item_open", "path_hygiene_ok", "deny_superset_ok",
		"paths_within_allowlist", "redaction_applied", "mcp_allowlist_grantable",
		"launch_mode_gated", "reducer_identity_ok",
	}
	if len(got.ChecksPassed) != len(want) {
		t.Fatalf("checks passed = %v, want %v", got.ChecksPassed, want)
	}
	for i := range want {
		if got.ChecksPassed[i] != want[i] {
			t.Fatalf("checks passed = %v, want %v", got.ChecksPassed, want)
		}
	}
}

func TestReduceRefusals(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*Request)
		verdict  domain.Verdict
		contains string
	}{
		{"missing work item", func(r *Request) { r.WorkItemFound = false }, domain.VerdictReject, "not found"},
		{"terminal work item", func(r *Request) { r.WorkItemState = domain.WorkItemDone }, domain.VerdictReject, "terminal"},
		{"review blocked", func(r *Request) { r.HumanReviewStatus = domain.HumanReviewBlocked }, domain.VerdictEscalate, "review blocked"},
		{"empty allowlist", func(r *Request) { r.Policy.AllowedPaths = nil }, domain.VerdictReject, "non-empty"},
		{"root allow entry", func(r *Request) { r.Policy.AllowedPaths = []string{"."} }, domain.VerdictReject, "root"},
		{"absolute allow entry", func(r *Request) { r.Policy.AllowedPaths = []string{"/etc"} }, domain.VerdictReject, "absolute"},
		{"traversal allow entry", func(r *Request) { r.Policy.AllowedPaths = []string{"a/../b"} }, domain.VerdictReject, "travers"},
		{"deny missing builtin", func(r *Request) { r.Policy.DeniedPaths = []string{"internal/auth/"} }, domain.VerdictReject, "builtin secret"},
		{"manifest hits deny", func(r *Request) {
			r.Manifest = append(r.Manifest, ManifestEntry{Path: ".meristem/api.token", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"manifest outside allow", func(r *Request) {
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "cmd/meristem/main.go", RedactionPassed: true})
		}, domain.VerdictReject, "outside allowed_paths"},
		{"unregistered generator", func(r *Request) { r.Generated.GeneratorID = "rogue@9" }, domain.VerdictReject, "not registered"},
		{"unknown redaction id", func(r *Request) {
			r.Policy.RedactionPolicyID = "mystery@1"
			r.AppliedRedactionID = "mystery@1"
		}, domain.VerdictReject, "unknown redaction"},
		{"redaction mismatch", func(r *Request) { r.AppliedRedactionID = "other@1" }, domain.VerdictReject, "does not match declared"},
		{"redaction failed on path", func(r *Request) { r.Manifest[0].RedactionPassed = false }, domain.VerdictReject, "redaction check failed"},
		{"path count drift", func(r *Request) { r.Generated.PathCount = 7 }, domain.VerdictReject, "path_count"},
		{"ungrantable tool", func(r *Request) {
			r.Policy.MCPToolAllowlist = []string{"tokens.create"}
		}, domain.VerdictReject, "not grantable"},
		{"invalid launch mode", func(r *Request) { r.Policy.LaunchMode = LaunchMode("yolo") }, domain.VerdictReject, "launch_mode"},
		{"direct without approval", func(r *Request) { r.Policy.LaunchMode = LaunchDirect }, domain.VerdictEscalate, "human-review approved"},
		{"direct with self-declared approval", func(r *Request) {
			r.Policy.LaunchMode = LaunchDirect
			r.Policy.RequiredReview = string(domain.HumanReviewApproved) // requester-authored; must not count
		}, domain.VerdictEscalate, "human-review approved"},
		{"nested env file hits deny", func(r *Request) {
			r.Policy.AllowedPaths = append(r.Policy.AllowedPaths, "services/")
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "services/api/.env", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"nested meristem dir hits deny", func(r *Request) {
			r.Policy.AllowedPaths = append(r.Policy.AllowedPaths, "vendor/")
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "vendor/x/.meristem/root.token", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"nested token file hits deny", func(r *Request) {
			r.Policy.AllowedPaths = append(r.Policy.AllowedPaths, "services/")
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "services/keys/prod.token", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"non-canonical env suffix hits deny", func(r *Request) {
			r.Policy.AllowedPaths = append(r.Policy.AllowedPaths, "config/")
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "config/database.env", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"env variant with suffix hits deny", func(r *Request) {
			r.Policy.AllowedPaths = append(r.Policy.AllowedPaths, "services/")
			r.Manifest = append(r.Manifest, ManifestEntry{Path: "services/api/prod.env.local", RedactionPassed: true})
		}, domain.VerdictReject, "denied_paths"},
		{"patience above ceiling", func(r *Request) { r.Policy.PatienceSeconds = 31 * 24 * 3600 }, domain.VerdictReject, "MaxPatienceBudget"},
		{"wrong reducer id", func(r *Request) { r.Policy.ReducerID = "provider_context_boundary@2" }, domain.VerdictReject, "this reducer"},
		{"zero patience", func(r *Request) { r.Policy.PatienceSeconds = 0 }, domain.VerdictReject, "patience"},
		{"nil work item id", func(r *Request) { r.Policy.WorkItemID = uuid.Nil }, domain.VerdictReject, "work_item_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			tc.mutate(&req)
			got := Reduce(req)
			if got.Disposition != tc.verdict {
				t.Fatalf("disposition = %s (%s), want %s", got.Disposition, got.Reason, tc.verdict)
			}
			if !strings.Contains(got.Reason, tc.contains) {
				t.Fatalf("reason = %q, want it to contain %q", got.Reason, tc.contains)
			}
			if got.Disposition != domain.VerdictAccept && len(got.ChecksPassed) != 0 {
				t.Fatalf("non-accept decision carried checks_passed %v", got.ChecksPassed)
			}
		})
	}
}

func TestReduceDirectWithApprovalAccepts(t *testing.T) {
	req := validRequest()
	req.Policy.LaunchMode = LaunchDirect
	req.HumanReviewStatus = domain.HumanReviewApproved
	got := Reduce(req)
	if got.Disposition != domain.VerdictAccept {
		t.Fatalf("disposition = %s (%s), want accept", got.Disposition, got.Reason)
	}
}

func TestReduceIsDeterministic(t *testing.T) {
	a := Reduce(validRequest())
	b := Reduce(validRequest())
	if a.Disposition != b.Disposition || a.Reason != b.Reason {
		t.Fatalf("reducer is not deterministic: %v vs %v", a, b)
	}
}

func TestMatchesPattern(t *testing.T) {
	cases := []struct {
		path, entry string
		want        bool
	}{
		{"internal/exports/walker.go", "internal/exports/", true},
		{"internal/exports", "internal/exports/", true},
		{"internal/exportsx/f.go", "internal/exports/", false},
		{"docs/spec.md", "docs/spec.md", true},
		{"docs/spec.md.bak", "docs/spec.md", false},
		{".meristem/api.token", ".meristem/", true},
		{"secrets/prod.token", "*.token", true},
		{".env.local", ".env.*", true},
		{".env", ".env", true},
	}
	for _, tc := range cases {
		if got := matchesPattern(tc.path, tc.entry); got != tc.want {
			t.Errorf("matchesPattern(%q, %q) = %v, want %v", tc.path, tc.entry, got, tc.want)
		}
	}
}
