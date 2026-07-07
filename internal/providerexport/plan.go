// Package providerexport materializes approved target-project context for
// external agents (work item accd39bb): the deterministic workspace exporter
// behind the provider context boundary. This package is read-only against
// the repository — no events writer, no Postgres. Callers append the
// provider_context.generated event from the returned payload.
package providerexport

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jbmopper/meristem/internal/providercontext"
)

// GeneratorID is stamped into Generated.GeneratorID; the registry must list
// it under the target provider for the reducer's generator check to pass.
const GeneratorID = "workspace_export@1"

// TreeMeta is one entry of a git tree listing (ls-tree -r): repo-relative
// path plus the git mode string.
type TreeMeta struct {
	Path string
	Mode string
}

// Git modes the planner understands. Only blobs survive planning.
const (
	modeBlob     = "100644"
	modeBlobExec = "100755"
	modeSymlink  = "120000"
	modeGitlink  = "160000"
)

// OmittedPath records an allow-matched path that was excluded, with a
// structural reason. Paths outside the allowlist are never enumerated —
// naming them in a manifest would leak that they exist.
type OmittedPath struct {
	Path   string `json:"path"`
	Reason string `json:"reason"` // denied_path | symlink | submodule | non_utf8_path
}

// Plan is the pure selection pass: allow/deny over a tree listing, before
// any blob is read or file written. Deny wins over allow; denied blobs must
// never be read at all, so callers gate ReadBlob on the returned included
// set. The input listing order does not matter; output is in input order
// (callers pass sorted listings).
func Plan(policy providercontext.ContextPolicy, tree []TreeMeta) (included []TreeMeta, omitted []OmittedPath, err error) {
	if len(policy.AllowedPaths) == 0 {
		return nil, nil, fmt.Errorf("allowed_paths must be explicit and non-empty")
	}
	for _, entry := range append(append([]string{}, policy.AllowedPaths...), policy.DeniedPaths...) {
		if !providercontext.PathEntryClean(entry) {
			return nil, nil, fmt.Errorf("policy path entry %q is root, absolute, or traverses", entry)
		}
	}
	for _, builtin := range providercontext.BuiltinSecretDenyList {
		if !denyListCovers(policy.DeniedPaths, builtin) {
			return nil, nil, fmt.Errorf("denied_paths must cover builtin secret entry %q", builtin)
		}
	}
	for _, entry := range tree {
		if !providercontext.MatchesAny(entry.Path, policy.AllowedPaths) {
			continue // never enumerated
		}
		switch {
		case providercontext.MatchesAnyDeny(entry.Path, policy.DeniedPaths):
			omitted = append(omitted, OmittedPath{Path: entry.Path, Reason: "denied_path"})
		case entry.Mode == modeSymlink:
			omitted = append(omitted, OmittedPath{Path: entry.Path, Reason: "symlink"})
		case entry.Mode == modeGitlink:
			omitted = append(omitted, OmittedPath{Path: entry.Path, Reason: "submodule"})
		case !utf8.ValidString(entry.Path):
			omitted = append(omitted, OmittedPath{Path: entry.Path, Reason: "non_utf8_path"})
		case entry.Mode == modeBlob || entry.Mode == modeBlobExec:
			included = append(included, entry)
		default:
			omitted = append(omitted, OmittedPath{Path: entry.Path, Reason: "unsupported_mode"})
		}
	}
	return included, omitted, nil
}

// denyListCovers mirrors the reducer's builtin-coverage rule: exact entry or
// a directory entry prefixing the builtin.
func denyListCovers(denied []string, builtin string) bool {
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
