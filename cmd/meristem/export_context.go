package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/providercontext"
	"github.com/jbmopper/meristem/internal/providerexport"
)

// stringSlice collects a repeatable flag into an ordered slice.
type stringSlice []string

func (s *stringSlice) String() string { return fmt.Sprint([]string(*s)) }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// `meristem export-context` is the operator-side deterministic workspace
// exporter (work item accd39bb). It reads a repository at a ref, applies an
// allow/deny context policy, scans every included blob for secrets, and
// materializes the approved slice into a workspace directory with a durable
// manifest. It makes no meristem API calls and appends no events — the
// provider_context.generated append is a later slice.
func runExportContext(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("export-context", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var (
		repo     = fs.String("repo", ".", "repository working directory to read commit objects from")
		ref      = fs.String("ref", "HEAD", "git ref to resolve to a source commit")
		out      = fs.String("out", "", "workspace destination directory; empty means dry run (plan + manifest only)")
		provider = fs.String("provider", "", "target provider id (required)")
		workItem = fs.String("work-item", "", "anchor work_item UUID (required)")
		allow    stringSlice
		deny     stringSlice
	)
	fs.Var(&allow, "allow", "allowed path prefix or glob (repeatable)")
	fs.Var(&deny, "deny", "denied path prefix or glob (repeatable); the builtin secret deny list is always appended")
	if err := fs.Parse(args); err != nil {
		exportContextUsage(os.Stderr)
		return err
	}
	if fs.NArg() > 0 {
		exportContextUsage(os.Stderr)
		return fmt.Errorf("export-context: unexpected arguments %v", fs.Args())
	}
	if len(allow) == 0 {
		exportContextUsage(os.Stderr)
		return fmt.Errorf("export-context: at least one --allow is required")
	}
	if *provider == "" {
		exportContextUsage(os.Stderr)
		return fmt.Errorf("export-context: --provider is required")
	}
	workItemID, err := uuid.Parse(*workItem)
	if err != nil {
		exportContextUsage(os.Stderr)
		return fmt.Errorf("export-context: --work-item must be a UUID: %w", err)
	}

	// The builtin secret deny list is always appended so the policy is a deny
	// superset by construction (design §2 step 1 / reducer step 3).
	denied := append(append([]string{}, deny...), providercontext.BuiltinSecretDenyList...)

	policy := providercontext.ContextPolicy{
		WorkItemID:        workItemID,
		ProviderID:        *provider,
		RepoPath:          *repo,
		RepoRef:           *ref,
		AllowedPaths:      []string(allow),
		DeniedPaths:       denied,
		RedactionPolicyID: providerexport.RedactionPolicyID,
		LaunchMode:        providercontext.LaunchWorktree,
		RequiredReview:    string(domain.HumanReviewWavedThrough),
		ReducerID:         providercontext.ReducerID,
		PatienceSeconds:   3600,
	}

	result, err := providerexport.Export(ctx, providerexport.ExecGit{}, providerexport.Options{
		RepoDir: *repo,
		Policy:  policy,
		OutDir:  *out,
	})
	if err != nil {
		return err
	}

	printExportSummary(os.Stdout, result, *out)
	logger.Info("context export complete",
		slog.String("source_commit", result.Manifest.SourceCommit),
		slog.Int("included", result.Manifest.PathCount),
		slog.Int("omitted", len(result.Manifest.Omitted)),
		slog.String("bundle_digest", result.Manifest.BundleDigest),
	)
	return nil
}

func printExportSummary(w io.Writer, result providerexport.Result, outDir string) {
	m := result.Manifest
	fmt.Fprintf(w, "source ref:     %s\n", m.SourceRef)
	fmt.Fprintf(w, "source commit:  %s\n", m.SourceCommit)
	fmt.Fprintf(w, "policy hash:    %s\n", m.PolicyHash)
	fmt.Fprintf(w, "redaction:      %s\n", m.RedactionPolicyID)
	fmt.Fprintf(w, "paths included: %d\n", m.PathCount)
	fmt.Fprintf(w, "paths omitted:  %d\n", len(m.Omitted))
	fmt.Fprintf(w, "bundle digest:  %s\n", m.BundleDigest)
	if outDir == "" {
		fmt.Fprintf(w, "workspace:      (dry run — nothing written)\n")
		return
	}
	fmt.Fprintf(w, "workspace:      %s\n", outDir)
	fmt.Fprintf(w, "manifest:       %s\n", result.ManifestPath)
}

func exportContextUsage(w io.Writer) {
	fmt.Fprint(w, `usage:
  meristem export-context --work-item <uuid> --provider <id> \
      --repo <dir> --ref <ref> --allow <path> [--allow <path> ...] \
      [--deny <path> ...] [--out <dir>]

Deterministically materializes the allow/deny slice of a repository at a ref
into a workspace directory, scanning every included blob for secrets. Reads
git commit objects only, never the working tree. Makes no meristem API calls.

  --work-item  anchor work_item UUID (required)
  --provider   target provider id (required)
  --repo       repository working directory (default ".")
  --ref        git ref to resolve to a source commit (default "HEAD")
  --allow      allowed path prefix or glob (repeatable, at least one required)
  --deny       denied path prefix or glob (repeatable); the builtin secret
               deny list is always appended
  --out        workspace destination; omit for a dry run (plan + manifest only)
`)
}
