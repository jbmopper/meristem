package providerexport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jbmopper/meristem/internal/providercontext"
)

// IncludedFile is one exported file: repo-relative path, git mode, content
// hash, size, and the scan result (always true in an emitted bundle — the
// field exists so the reducer re-verifies instead of trusting).
type IncludedFile struct {
	Path            string `json:"path"`
	Mode            string `json:"mode"`
	Size            int64  `json:"size"`
	SHA256          string `json:"sha256"`
	RedactionPassed bool   `json:"redaction_passed"`
}

// Manifest is the durable audit record of one export. The full manifest
// (with omitted paths) stays operator-side; a workspace-embedded copy, if
// any, carries included entries only.
type Manifest struct {
	ManifestVersion   int            `json:"manifest_version"`
	GeneratorID       string         `json:"generator_id"`
	SourceRef         string         `json:"source_ref"`
	SourceCommit      string         `json:"source_commit"`
	PolicyHash        string         `json:"policy_hash"`
	RedactionPolicyID string         `json:"redaction_policy_id"`
	PathCount         int            `json:"path_count"`
	Included          []IncludedFile `json:"included"`
	Omitted           []OmittedPath  `json:"omitted,omitempty"`
	BundleDigest      string         `json:"bundle_digest"`
}

// PolicyHash hashes the structural policy: the ContextPolicy with the
// narrative Message cleared, via struct marshal (stable key order, no maps).
func PolicyHash(p providercontext.ContextPolicy) string {
	p.Message = ""
	raw, err := json.Marshal(p)
	if err != nil {
		// A struct of strings/slices/ints cannot fail to marshal; treat as
		// programmer error rather than propagating an impossible branch.
		panic(fmt.Sprintf("providerexport: marshal policy: %v", err))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ComputeBundleDigest is the canonical bundle identity: sha256 over
// `path NUL mode NUL sha256hex LF` for every included file in
// byte-lexicographic path order. Content identity plus tree shape,
// independent of filesystem, OS, framing, or timestamps.
func ComputeBundleDigest(files []IncludedFile) string {
	ordered := append([]IncludedFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	h := sha256.New()
	for _, f := range ordered {
		h.Write([]byte(f.Path))
		h.Write([]byte{0})
		h.Write([]byte(f.Mode))
		h.Write([]byte{0})
		h.Write([]byte(trimDigest(f.SHA256)))
		h.Write([]byte{'\n'})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func trimDigest(d string) string {
	const prefix = "sha256:"
	if len(d) > len(prefix) && d[:len(prefix)] == prefix {
		return d[len(prefix):]
	}
	return d
}

// BuildManifest assembles the manifest and its providercontext-facing
// payloads from a completed plan+scan pass.
func BuildManifest(policy providercontext.ContextPolicy, sourceCommit string, included []IncludedFile, omitted []OmittedPath) (Manifest, providercontext.Generated, []providercontext.ManifestEntry) {
	m := Manifest{
		ManifestVersion:   1,
		GeneratorID:       GeneratorID,
		SourceRef:         policy.RepoRef,
		SourceCommit:      sourceCommit,
		PolicyHash:        PolicyHash(policy),
		RedactionPolicyID: RedactionPolicyID,
		PathCount:         len(included),
		Included:          included,
		Omitted:           omitted,
		BundleDigest:      ComputeBundleDigest(included),
	}
	gen := providercontext.Generated{
		GeneratorID:  GeneratorID,
		SourceCommit: sourceCommit,
		BundleDigest: m.BundleDigest,
		PathCount:    m.PathCount,
	}
	entries := make([]providercontext.ManifestEntry, 0, len(included))
	for _, f := range included {
		entries = append(entries, providercontext.ManifestEntry{Path: f.Path, RedactionPassed: f.RedactionPassed})
	}
	return m, gen, entries
}
