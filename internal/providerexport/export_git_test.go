package providerexport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/providercontext"
)

// gitRepo builds a throwaway git repository in t.TempDir() and commits the
// given files. Real git, no mocks (mirrors the no-mock-pools rule). Modes:
// 0755 files land as executable blobs; symlinks are created via a "@symlink:"
// content prefix naming the target.
func gitRepo(t *testing.T, files map[string]string, modes map[string]os.FileMode) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git not on PATH: %v", err)
	}
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
			"GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if target, ok := symlinkTarget(content); ok {
			if err := os.Symlink(target, full); err != nil {
				t.Fatal(err)
			}
			continue
		}
		mode := os.FileMode(0o644)
		if m, ok := modes[rel]; ok {
			mode = m
		}
		if err := os.WriteFile(full, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
	}
	run("add", "-A")
	run("commit", "-q", "-m", "fixture")
	return dir
}

func symlinkTarget(content string) (string, bool) {
	const prefix = "@symlink:"
	if len(content) > len(prefix) && content[:len(prefix)] == prefix {
		return content[len(prefix):], true
	}
	return "", false
}

// policyFor builds a valid worktree-launch policy over the given allow list,
// always appending the builtin secret deny list.
func policyFor(allow []string, deny ...string) providercontext.ContextPolicy {
	return providercontext.ContextPolicy{
		WorkItemID:        uuid.MustParse("accd39bb-eb95-493f-ade7-efc858ebe6d8"),
		ProviderID:        "cursor-cli",
		RepoPath:          ".",
		RepoRef:           "HEAD",
		AllowedPaths:      allow,
		DeniedPaths:       append(append([]string{}, deny...), providercontext.BuiltinSecretDenyList...),
		RedactionPolicyID: RedactionPolicyID,
		LaunchMode:        providercontext.LaunchWorktree,
		RequiredReview:    string(domain.HumanReviewWavedThrough),
		ReducerID:         providercontext.ReducerID,
		PatienceSeconds:   3600,
	}
}

// spyGit wraps a GitReader and records every ReadBlob path so a test can prove
// a denied blob is never read.
type spyGit struct {
	inner GitReader
	reads []string
}

func (s *spyGit) ResolveCommit(ctx context.Context, repoDir, ref string) (string, error) {
	return s.inner.ResolveCommit(ctx, repoDir, ref)
}
func (s *spyGit) ListTree(ctx context.Context, repoDir, commit string) ([]TreeMeta, error) {
	return s.inner.ListTree(ctx, repoDir, commit)
}
func (s *spyGit) ReadBlob(ctx context.Context, repoDir, commit, path string) ([]byte, error) {
	s.reads = append(s.reads, path)
	return s.inner.ReadBlob(ctx, repoDir, commit, path)
}

func TestExportDeniedBlobNeverRead(t *testing.T) {
	repo := gitRepo(t, map[string]string{
		"docs/spec.md":         "# spec\n",
		"docs/keys/prod.token": "mrs_should_never_be_read_by_export_run_x",
	}, nil)
	spy := &spyGit{inner: ExecGit{}}
	out := filepath.Join(t.TempDir(), "ws")
	res, err := Export(context.Background(), spy, Options{RepoDir: repo, Policy: policyFor([]string{"docs/"}), OutDir: out})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	for _, p := range spy.reads {
		if p == "docs/keys/prod.token" {
			t.Fatal("denied blob docs/keys/prod.token was read")
		}
	}
	if res.Manifest.PathCount != 1 {
		t.Fatalf("path_count = %d, want 1", res.Manifest.PathCount)
	}
	// It must still be recorded as omitted (allow-matched, deny-excluded).
	if !hasOmission(res.Manifest.Omitted, "docs/keys/prod.token", "denied_path") {
		t.Fatalf("omitted = %+v, want denied_path for prod.token", res.Manifest.Omitted)
	}
}

func TestExportSecretAborts(t *testing.T) {
	repo := gitRepo(t, map[string]string{
		"docs/spec.md": "# spec\n",
		"docs/leak.md": "here is a key AKIAIOSFODNN7EXAMPLE in prose\n",
	}, nil)
	out := filepath.Join(t.TempDir(), "ws")
	_, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policyFor([]string{"docs/"}), OutDir: out})
	if err == nil {
		t.Fatal("expected export to abort on secret content")
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("OutDir survived a failed export: stat err = %v", statErr)
	}
	// No manifest next to OutDir either.
	if _, statErr := os.Stat(filepath.Join(filepath.Dir(out), operatorManifestName)); !os.IsNotExist(statErr) {
		t.Fatal("manifest survived a failed export")
	}
	// And no staging left behind in the parent.
	entries, _ := os.ReadDir(filepath.Dir(out))
	if len(entries) != 0 {
		t.Fatalf("parent dir not clean after abort: %v", entries)
	}
}

func TestExportExecutableBitRoundTrip(t *testing.T) {
	repo := gitRepo(t, map[string]string{
		"scripts/run.sh": "#!/bin/sh\necho hi\n",
		"scripts/data":   "plain\n",
	}, map[string]os.FileMode{"scripts/run.sh": 0o755})
	out := filepath.Join(t.TempDir(), "ws")
	res, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policyFor([]string{"scripts/"}), OutDir: out})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	sh, err := os.Stat(filepath.Join(out, "scripts", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if sh.Mode().Perm() != 0o755 {
		t.Fatalf("run.sh perm = %o, want 0755", sh.Mode().Perm())
	}
	data, err := os.Stat(filepath.Join(out, "scripts", "data"))
	if err != nil {
		t.Fatal(err)
	}
	if data.Mode().Perm() != 0o644 {
		t.Fatalf("data perm = %o, want 0644", data.Mode().Perm())
	}
	if modeOf(res.Manifest.Included, "scripts/run.sh") != modeBlobExec {
		t.Fatalf("manifest mode for run.sh = %q, want %q", modeOf(res.Manifest.Included, "scripts/run.sh"), modeBlobExec)
	}
}

func TestExportRerunIdentical(t *testing.T) {
	files := map[string]string{
		"docs/a.md":            "alpha\n",
		"docs/b.md":            "beta\n",
		"internal/x/walker.go": "package x\n",
	}
	repo := gitRepo(t, files, nil)
	policy := policyFor([]string{"docs/", "internal/x/"})

	out1 := filepath.Join(t.TempDir(), "ws")
	res1, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policy, OutDir: out1})
	if err != nil {
		t.Fatalf("Export 1: %v", err)
	}
	out2 := filepath.Join(t.TempDir(), "ws")
	res2, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policy, OutDir: out2})
	if err != nil {
		t.Fatalf("Export 2: %v", err)
	}
	if res1.Manifest.BundleDigest != res2.Manifest.BundleDigest {
		t.Fatalf("bundle digests differ: %s vs %s", res1.Manifest.BundleDigest, res2.Manifest.BundleDigest)
	}
	m1, err := os.ReadFile(res1.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := os.ReadFile(res2.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(m1) != string(m2) {
		t.Fatalf("manifest.json not byte-identical across runs:\n--- run1 ---\n%s\n--- run2 ---\n%s", m1, m2)
	}
}

func TestExportSymlinkOmitted(t *testing.T) {
	repo := gitRepo(t, map[string]string{
		"docs/spec.md": "# spec\n",
		"docs/link.md": "@symlink:spec.md",
	}, nil)
	out := filepath.Join(t.TempDir(), "ws")
	res, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policyFor([]string{"docs/"}), OutDir: out})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if !hasOmission(res.Manifest.Omitted, "docs/link.md", "symlink") {
		t.Fatalf("omitted = %+v, want symlink for docs/link.md", res.Manifest.Omitted)
	}
	if _, statErr := os.Lstat(filepath.Join(out, "docs", "link.md")); !os.IsNotExist(statErr) {
		t.Fatal("symlink was materialized into the workspace")
	}
	if res.Manifest.PathCount != 1 {
		t.Fatalf("path_count = %d, want 1", res.Manifest.PathCount)
	}
}

func TestExportDryRunWritesNothing(t *testing.T) {
	repo := gitRepo(t, map[string]string{"docs/spec.md": "# spec\n"}, nil)
	parent := t.TempDir()
	out := filepath.Join(parent, "ws")
	res, err := Export(context.Background(), ExecGit{}, Options{RepoDir: repo, Policy: policyFor([]string{"docs/"}), OutDir: ""})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if res.Manifest.PathCount != 1 {
		t.Fatalf("dry run should still plan+manifest: path_count = %d", res.Manifest.PathCount)
	}
	if res.ManifestPath != "" {
		t.Fatalf("dry run wrote a manifest at %q", res.ManifestPath)
	}
	if res.Manifest.BundleDigest == "" {
		t.Fatal("dry run should still compute a bundle digest")
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatal("dry run created the workspace directory")
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 0 {
		t.Fatalf("dry run left files behind: %v", entries)
	}
}

func hasOmission(oms []OmittedPath, path, reason string) bool {
	for _, o := range oms {
		if o.Path == path && o.Reason == reason {
			return true
		}
	}
	return false
}

func modeOf(files []IncludedFile, path string) string {
	for _, f := range files {
		if f.Path == path {
			return f.Mode
		}
	}
	return ""
}
