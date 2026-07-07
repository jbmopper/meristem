package providerexport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jbmopper/meristem/internal/providercontext"
)

// Options configures one export run.
type Options struct {
	// RepoDir is the git working directory to read commit objects from. The
	// caller resolves it from Policy.RepoPath.
	RepoDir string
	// Policy is the immutable context policy driving allow/deny selection.
	Policy providercontext.ContextPolicy
	// OutDir is the workspace destination. "" means dry run: plan, scan, and
	// manifest are computed but nothing is written to disk.
	OutDir string
}

// Result is the outcome of a completed export: the durable manifest plus the
// three payloads providercontext.Reduce re-verifies.
type Result struct {
	Manifest           Manifest
	Generated          providercontext.Generated
	Entries            []providercontext.ManifestEntry
	AppliedRedactionID string
	// ManifestPath is where the operator-side manifest.json was written, or
	// "" on a dry run.
	ManifestPath string
}

// Export materializes the approved slice of a repository into OutDir per the
// deterministic-exporter design (accd39bb §2). It is read-only against the
// repo — commit objects only, never the working tree — and fails closed: any
// scan failure aborts the whole export with no partial bundle left behind.
func Export(ctx context.Context, git GitReader, opts Options) (Result, error) {
	// 1. Local policy hygiene, before touching the repo. Fail closed rather
	//    than trusting the reducer to be the first line of defense.
	if err := validatePolicy(opts.Policy); err != nil {
		return Result{}, err
	}

	// 2. Resolve the ref to a single commit; everything after reads only from
	//    this commit's tree objects.
	commit, err := git.ResolveCommit(ctx, opts.RepoDir, opts.Policy.RepoRef)
	if err != nil {
		return Result{}, err
	}

	// 3. List the tree and sort byte-lexicographically (defensive; ls-tree is
	//    already sorted).
	tree, err := git.ListTree(ctx, opts.RepoDir, commit)
	if err != nil {
		return Result{}, err
	}
	sort.Slice(tree, func(i, j int) bool { return tree[i].Path < tree[j].Path })

	// 4. Pure plan: allow/deny/mode selection before any blob is read.
	planned, omitted, err := Plan(opts.Policy, tree)
	if err != nil {
		return Result{}, err
	}

	// Prepare staging only for a real (non-dry) run. Files are written into a
	// staging dir and renamed into place at the very end, so a crash or scan
	// failure never leaves a partial bundle at the destination.
	var staging string
	if opts.OutDir != "" {
		if err := os.MkdirAll(filepath.Dir(opts.OutDir), 0o755); err != nil {
			return Result{}, fmt.Errorf("providerexport: prepare destination parent: %w", err)
		}
		staging, err = os.MkdirTemp(filepath.Dir(opts.OutDir), ".export-staging-*")
		if err != nil {
			return Result{}, fmt.Errorf("providerexport: create staging: %w", err)
		}
	}
	// Any early return past this point must not leave staging behind.
	cleanupStaging := func() {
		if staging != "" {
			_ = os.RemoveAll(staging)
		}
	}

	// 5 & 6. For each surviving path in sorted order: ReadBlob, ScanContent in
	//        memory before any write, then (real run only) write the file.
	included := make([]IncludedFile, 0, len(planned))
	for _, entry := range planned {
		blob, err := git.ReadBlob(ctx, opts.RepoDir, commit, entry.Path)
		if err != nil {
			cleanupStaging()
			return Result{}, err
		}
		passed, rule := ScanContent(entry.Path, blob)
		if !passed {
			cleanupStaging()
			return Result{}, fmt.Errorf("providerexport: export aborted: path %q tripped redaction rule %q (%s)", entry.Path, rule, RedactionPolicyID)
		}
		sum := sha256.Sum256(blob)
		included = append(included, IncludedFile{
			Path:            entry.Path,
			Mode:            entry.Mode,
			Size:            int64(len(blob)),
			SHA256:          "sha256:" + hex.EncodeToString(sum[:]),
			RedactionPassed: true,
		})
		if staging != "" {
			if err := writeStagedFile(staging, entry, blob); err != nil {
				cleanupStaging()
				return Result{}, err
			}
		}
	}

	// 7. Assemble the manifest and payloads from the pure functions.
	manifest, gen, entries := BuildManifest(opts.Policy, commit, included, omitted)
	result := Result{
		Manifest:           manifest,
		Generated:          gen,
		Entries:            entries,
		AppliedRedactionID: RedactionPolicyID,
	}

	// Dry run: nothing is written; return the computed plan + manifest.
	if opts.OutDir == "" {
		return result, nil
	}

	// Embed an included-only copy inside the workspace (privacy rule §3): the
	// omitted list stays operator-side only.
	if err := writeManifestFile(filepath.Join(staging, workspaceManifestName), manifest, false); err != nil {
		cleanupStaging()
		return Result{}, err
	}

	// Rename staging into place. os.Rename is atomic on the same filesystem,
	// so the destination appears whole or not at all.
	if err := os.Rename(staging, opts.OutDir); err != nil {
		cleanupStaging()
		return Result{}, fmt.Errorf("providerexport: publish workspace: %w", err)
	}

	// The operator-side manifest lands next to OutDir with the full omitted[].
	manifestPath := filepath.Join(filepath.Dir(opts.OutDir), operatorManifestName)
	if err := writeManifestFile(manifestPath, manifest, true); err != nil {
		return Result{}, err
	}
	result.ManifestPath = manifestPath
	return result, nil
}

const (
	operatorManifestName  = "manifest.json"
	workspaceManifestName = "manifest.json"
)

// validatePolicy runs the exporter's pre-flight hygiene: the same path and
// deny-superset checks as Reduce steps 2–3, plus the redaction-id match that
// must hold before any content is scanned (design §4).
func validatePolicy(p providercontext.ContextPolicy) error {
	if len(p.AllowedPaths) == 0 {
		return fmt.Errorf("providerexport: allowed_paths must be explicit and non-empty")
	}
	for _, entry := range append(append([]string{}, p.AllowedPaths...), p.DeniedPaths...) {
		if !providercontext.PathEntryClean(entry) {
			return fmt.Errorf("providerexport: policy path entry %q is root, absolute, or traverses", entry)
		}
	}
	for _, builtin := range providercontext.BuiltinSecretDenyList {
		if !denyListCovers(p.DeniedPaths, builtin) {
			return fmt.Errorf("providerexport: denied_paths must cover builtin secret entry %q", builtin)
		}
	}
	if p.RedactionPolicyID != RedactionPolicyID {
		return fmt.Errorf("providerexport: policy declares redaction %q but this exporter applies %q", p.RedactionPolicyID, RedactionPolicyID)
	}
	return nil
}

// writeStagedFile writes one exported blob into the staging tree, honoring
// only git's 644/755 bit so on-disk umask quirks never feed bundle identity.
func writeStagedFile(staging string, entry TreeMeta, blob []byte) error {
	dst := filepath.Join(staging, filepath.FromSlash(entry.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("providerexport: create workspace dir for %q: %w", entry.Path, err)
	}
	mode := os.FileMode(0o644)
	if entry.Mode == modeBlobExec {
		mode = 0o755
	}
	if err := os.WriteFile(dst, blob, mode); err != nil {
		return fmt.Errorf("providerexport: write %q: %w", entry.Path, err)
	}
	return nil
}

// writeManifestFile marshals the manifest deterministically. withOmitted=false
// strips the omitted list for the workspace-embedded copy.
func writeManifestFile(path string, m Manifest, withOmitted bool) error {
	if !withOmitted {
		m.Omitted = nil
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("providerexport: marshal manifest: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("providerexport: write manifest %q: %w", path, err)
	}
	return nil
}
