// Package providercontext contains the deterministic policy schema and
// reducer for the external-agent context boundary (work item 4daea3d0).
//
// It is intentionally pure, following internal/grants: callers resolve
// projection-dependent facts such as the anchor work_item row and the
// requesting token's grantable tools, then pass those facts in. The reducer
// decides accept, reject, or escalate without reading clocks, databases, or
// process state.
package providercontext

import (
	"strings"

	"github.com/google/uuid"
)

// Event kinds for the provider_context subject. Later events omit
// work_item_id/provider_id: the subject join to the requested event supplies
// them.
const (
	EventContextRequested      = "provider_context.requested"
	EventContextGenerated      = "provider_context.generated"
	EventContextVerified       = "provider_context.verified"
	EventContextLaunchApproved = "provider_context.launch_approved"
	EventContextLaunched       = "provider_context.launched"
	EventContextRevoked        = "provider_context.revoked"
	EventContextExpired        = "provider_context.expired"
)

// LaunchMode selects which boundary gate applies at launch time.
type LaunchMode string

const (
	// LaunchExportProxy hands the provider a scrubbed export bundle plus a
	// scoped MCP proxy; the provider never sees the raw workspace.
	LaunchExportProxy LaunchMode = "export_proxy"
	// LaunchWorktree launches the provider in a dedicated worktree. The
	// documented default for repo work.
	LaunchWorktree LaunchMode = "worktree"
	// LaunchDirect points the provider at a raw workspace. Exception path:
	// verified only when the policy requires approved human review.
	LaunchDirect LaunchMode = "direct"
)

func (m LaunchMode) Valid() bool {
	switch m {
	case LaunchExportProxy, LaunchWorktree, LaunchDirect:
		return true
	}
	return false
}

// ContextPolicy is the payload of provider_context.requested. Immutable per
// context: changing any field is a new request, never an update. All fields
// except Message are structural.
type ContextPolicy struct {
	PayloadVersion    int        `json:"payload_version,omitempty"`
	WorkItemID        uuid.UUID  `json:"work_item_id"`
	ProviderID        string     `json:"provider_id"`
	RepoPath          string     `json:"repo_path"`
	RepoRef           string     `json:"repo_ref"`
	AllowedPaths      []string   `json:"allowed_paths"`
	DeniedPaths       []string   `json:"denied_paths"`
	RedactionPolicyID string     `json:"redaction_policy_id"`
	MCPToolAllowlist  []string   `json:"mcp_tool_allowlist"`
	LaunchMode        LaunchMode `json:"launch_mode"`
	RequiredReview    string     `json:"required_review"`
	ReducerID         string     `json:"reducer_id"`
	PatienceSeconds   int        `json:"patience_seconds"`
	Message           string     `json:"message,omitempty"`
}

// ReducerID is the identity this package's reducer stamps into verdicts. A
// policy naming any other reducer id fails closed.
const ReducerID = "provider_context_boundary@1"

// BuiltinSecretDenyList is the deny superset every policy must include
// (directly or by covering pattern). Mirrors the AGENTS.md prompt-level
// secrets rule as a deterministic check.
var BuiltinSecretDenyList = []string{
	".meristem/",
	".env",
	".env.*",
	"*.env",
	"*.env.*",
	"*.token",
}

// MatchesPattern reports whether a path is covered by an allow-side policy
// entry (root-relative semantics). Exported so the exporter and the reducer
// share one matcher — divergent matchers would let an exporter include a
// path the reducer rejects, or vice versa.
func MatchesPattern(p, entry string) bool { return matchesPattern(p, entry) }

// MatchesAnyDeny reports whether a path is covered by any deny-side entry
// (widened segment/basename semantics at any depth).
func MatchesAnyDeny(p string, entries []string) bool { return matchesAnyDeny(p, entries) }

// MatchesAny reports whether a path is covered by any allow-side entry.
func MatchesAny(p string, entries []string) bool { return matchesAny(p, entries) }

// PathEntryClean reports whether a policy path entry is free of root,
// absolute, and traversal forms. Exported for the exporter's pre-flight
// hygiene check (design accd39bb step 1).
func PathEntryClean(entry string) bool { return pathEntryClean(entry) }

// pathEntryClean rejects entries that would let a policy escape its own
// declared boundary: absolute paths, parent traversal, or the repo root.
func pathEntryClean(entry string) bool {
	trimmed := strings.TrimSpace(entry)
	if trimmed == "" {
		return false
	}
	if trimmed == "." || trimmed == "/" || trimmed == "./" {
		return false
	}
	if strings.HasPrefix(trimmed, "/") {
		return false
	}
	for _, part := range strings.Split(strings.ReplaceAll(trimmed, "\\", "/"), "/") {
		if part == ".." {
			return false
		}
	}
	return true
}

// matchesPattern reports whether a manifest path is covered by a policy
// entry. Directory entries (trailing slash) cover their subtree; glob
// entries match by base name; exact entries match whole paths or prefixes
// at a path boundary.
func matchesPattern(path, entry string) bool {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return false
	}
	if strings.HasSuffix(entry, "/") {
		return strings.HasPrefix(path, entry) || path == strings.TrimSuffix(entry, "/")
	}
	if strings.ContainsAny(entry, "*?") {
		base := path
		if i := strings.LastIndex(path, "/"); i >= 0 {
			base = path[i+1:]
		}
		if ok, err := filepathMatch(entry, base); err == nil && ok {
			return true
		}
		ok, err := filepathMatch(entry, path)
		return err == nil && ok
	}
	return path == entry || strings.HasPrefix(path, entry+"/")
}

// matchesDenyPattern applies deny-side semantics: everything matchesPattern
// covers, plus subtree matching at any depth — a directory entry like
// ".meristem/" matches any path containing that segment, and a slash-free
// entry like ".env" matches any basename or segment. Deny widening is the
// fail-closed direction; allow entries keep the stricter root-relative
// semantics (review finding F2 on ca006d7: nested secrets must not slip
// past root-anchored deny entries).
func matchesDenyPattern(p, entry string) bool {
	if matchesPattern(p, entry) {
		return true
	}
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return false
	}
	if strings.HasSuffix(entry, "/") {
		seg := strings.TrimSuffix(entry, "/")
		for _, part := range strings.Split(p, "/") {
			if part == seg {
				return true
			}
		}
		return false
	}
	if !strings.Contains(entry, "/") {
		for _, part := range strings.Split(p, "/") {
			if part == entry {
				return true
			}
			if strings.ContainsAny(entry, "*?") {
				if ok, err := filepathMatch(entry, part); err == nil && ok {
					return true
				}
			}
		}
	}
	return false
}
