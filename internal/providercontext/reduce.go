package providercontext

import (
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// Generated is the structural payload of provider_context.generated, as
// resolved by the caller.
type Generated struct {
	GeneratorID  string
	SourceCommit string
	BundleDigest string
	PathCount    int
}

// ManifestEntry is one exported path plus the redaction check result the
// generator recorded for it.
type ManifestEntry struct {
	Path            string
	RedactionPassed bool
}

// Request carries every fact the reducer needs, pre-resolved by the caller.
// The reducer never reads projections itself.
type Request struct {
	Policy             ContextPolicy
	Generated          Generated
	Manifest           []ManifestEntry
	AppliedRedactionID string
	// WorkItemFound etc. come from the work_items projection row for
	// Policy.WorkItemID.
	WorkItemFound     bool
	WorkItemState     domain.WorkItemState
	HumanReviewStatus domain.HumanReviewStatus
	// RegisteredGenerators maps provider id -> generator ids registered for
	// it. KnownRedactionIDs and GrantableTools come from the registry and
	// the requesting token's scope set respectively.
	RegisteredGenerators map[string][]string
	KnownRedactionIDs    []string
	GrantableTools       []string
}

// Decision mirrors the standard verdict mapping: accept appends verified,
// reject records the verdict only, escalate routes to escalation.requested.
type Decision struct {
	Disposition  domain.Verdict
	Reason       string
	ChecksPassed []string
}

func Reduce(req Request) Decision {
	var passed []string

	// 1. anchor work_item exists, is non-terminal, and is not review-blocked.
	if !req.WorkItemFound {
		return reject("anchor work_item not found")
	}
	if req.WorkItemState.Terminal() {
		return reject(fmt.Sprintf("anchor work_item is terminal (%s)", req.WorkItemState))
	}
	if req.HumanReviewStatus == domain.HumanReviewBlocked {
		return escalate("anchor work_item is human-review blocked")
	}
	passed = append(passed, "anchor_work_item_open")

	// 2. path hygiene: allowed non-empty, no root/absolute/traversal entries.
	if len(req.Policy.AllowedPaths) == 0 {
		return reject("allowed_paths must be explicit and non-empty")
	}
	for _, entry := range req.Policy.AllowedPaths {
		if !pathEntryClean(entry) {
			return reject(fmt.Sprintf("allowed_paths entry %q is root, absolute, or traverses", entry))
		}
	}
	for _, entry := range req.Policy.DeniedPaths {
		if !pathEntryClean(entry) {
			return reject(fmt.Sprintf("denied_paths entry %q is root, absolute, or traverses", entry))
		}
	}
	passed = append(passed, "path_hygiene_ok")

	// 3. deny set must cover the builtin secret deny list; deny wins on
	// overlap by construction of check 4.
	for _, builtin := range BuiltinSecretDenyList {
		if !denyCovers(req.Policy.DeniedPaths, builtin) {
			return reject(fmt.Sprintf("denied_paths must cover builtin secret entry %q", builtin))
		}
	}
	passed = append(passed, "deny_superset_ok")

	// 4. every manifest path inside allow, outside deny.
	for _, entry := range req.Manifest {
		if matchesAny(entry.Path, req.Policy.DeniedPaths) {
			return reject(fmt.Sprintf("manifest path %q matches denied_paths", entry.Path))
		}
		if !matchesAny(entry.Path, req.Policy.AllowedPaths) {
			return reject(fmt.Sprintf("manifest path %q is outside allowed_paths", entry.Path))
		}
	}
	passed = append(passed, "paths_within_allowlist")

	// 5. generator registered for provider; redaction id known, matches the
	// declared policy, and passed on every path.
	if !contains(req.RegisteredGenerators[req.Policy.ProviderID], req.Generated.GeneratorID) {
		return reject(fmt.Sprintf("generator %q is not registered for provider %q", req.Generated.GeneratorID, req.Policy.ProviderID))
	}
	if !contains(req.KnownRedactionIDs, req.Policy.RedactionPolicyID) {
		return reject(fmt.Sprintf("unknown redaction policy %q", req.Policy.RedactionPolicyID))
	}
	if req.AppliedRedactionID != req.Policy.RedactionPolicyID {
		return reject(fmt.Sprintf("applied redaction %q does not match declared %q", req.AppliedRedactionID, req.Policy.RedactionPolicyID))
	}
	for _, entry := range req.Manifest {
		if !entry.RedactionPassed {
			return reject(fmt.Sprintf("redaction check failed for %q", entry.Path))
		}
	}
	if req.Generated.PathCount != len(req.Manifest) {
		return reject(fmt.Sprintf("generated path_count %d does not match manifest length %d", req.Generated.PathCount, len(req.Manifest)))
	}
	passed = append(passed, "redaction_applied")

	// 6. MCP tool allowlist must be grantable under the requesting token.
	for _, tool := range req.Policy.MCPToolAllowlist {
		if !contains(req.GrantableTools, tool) {
			return reject(fmt.Sprintf("mcp tool %q is not grantable under the requesting token", tool))
		}
	}
	passed = append(passed, "mcp_allowlist_grantable")

	// 7. launch mode valid; direct requires approved review, else escalate
	// rather than verify.
	if !req.Policy.LaunchMode.Valid() {
		return reject(fmt.Sprintf("unknown launch_mode %q", req.Policy.LaunchMode))
	}
	if req.Policy.LaunchMode == LaunchDirect && domain.HumanReviewStatus(req.Policy.RequiredReview) != domain.HumanReviewApproved {
		return escalate("direct launch requires required_review=approved")
	}
	passed = append(passed, "launch_mode_gated")

	// 8. reducer self-check and bounded patience.
	if req.Policy.ReducerID != ReducerID {
		return reject(fmt.Sprintf("policy names reducer %q; this reducer is %q", req.Policy.ReducerID, ReducerID))
	}
	if req.Policy.PatienceSeconds <= 0 {
		return reject("patience_seconds must be positive (bounded patience)")
	}
	if req.Policy.WorkItemID == uuid.Nil {
		return reject("work_item_id is required")
	}
	passed = append(passed, "reducer_identity_ok")

	return Decision{
		Disposition:  domain.VerdictAccept,
		Reason:       "context policy validated against generated bundle",
		ChecksPassed: passed,
	}
}

// denyCovers reports whether any deny entry covers the builtin entry —
// exact match, or a directory entry that prefixes it.
func denyCovers(denied []string, builtin string) bool {
	for _, entry := range denied {
		if entry == builtin {
			return true
		}
		if strings.HasSuffix(entry, "/") && strings.HasPrefix(builtin, entry) {
			return true
		}
	}
	return false
}

func matchesAny(p string, entries []string) bool {
	for _, entry := range entries {
		if matchesPattern(p, entry) {
			return true
		}
	}
	return false
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func filepathMatch(pattern, name string) (bool, error) {
	return path.Match(pattern, name)
}

func reject(reason string) Decision {
	return Decision{Disposition: domain.VerdictReject, Reason: reason}
}

func escalate(reason string) Decision {
	return Decision{Disposition: domain.VerdictEscalate, Reason: reason}
}
