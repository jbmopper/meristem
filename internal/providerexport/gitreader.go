package providerexport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// GitReader reads content out of git commit objects. Every read targets a
// resolved commit's tree, never the working tree — no checkout, no
// git archive, no smudge filters, so identical (commit, policy) inputs yield
// identical output on any machine (design §5).
type GitReader interface {
	// ResolveCommit resolves ref to a full 40-hex commit SHA.
	ResolveCommit(ctx context.Context, repoDir, ref string) (string, error)
	// ListTree lists every entry reachable from commit (recursive), with its
	// repo-relative path and git mode.
	ListTree(ctx context.Context, repoDir, commit string) ([]TreeMeta, error)
	// ReadBlob returns the raw bytes of path as it exists in commit.
	ReadBlob(ctx context.Context, repoDir, commit, path string) ([]byte, error)
}

// ExecGit is the production GitReader: it shells out to git(1) on PATH,
// mirroring cmd/meristem/git.go's pass-through stance but capturing output.
type ExecGit struct{}

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// ResolveCommit runs `git rev-parse --verify <ref>^{commit}`, dereferencing
// tags to their commit and requiring a full 40-hex SHA back.
func (ExecGit) ResolveCommit(ctx context.Context, repoDir, ref string) (string, error) {
	if strings.TrimSpace(ref) == "" {
		return "", errors.New("providerexport: empty ref")
	}
	// `--` after the revision keeps a ref that looks like a flag or a path
	// from being reinterpreted.
	out, err := runGit(ctx, repoDir, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("providerexport: resolve commit for ref %q: %w", ref, err)
	}
	commit := strings.TrimSpace(string(out))
	if !fullSHA.MatchString(commit) {
		return "", fmt.Errorf("providerexport: rev-parse returned %q, not a full 40-hex commit", commit)
	}
	return commit, nil
}

// ListTree runs `git ls-tree -r -z <commit>` and parses each NUL-terminated
// record into path + mode. The -z form disables path quoting, so non-ASCII
// and unusual paths come through verbatim for the planner to judge.
func (ExecGit) ListTree(ctx context.Context, repoDir, commit string) ([]TreeMeta, error) {
	out, err := runGit(ctx, repoDir, "ls-tree", "-r", "-z", "--end-of-options", commit)
	if err != nil {
		return nil, fmt.Errorf("providerexport: list tree of %s: %w", commit, err)
	}
	var entries []TreeMeta
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		// Record shape: "<mode> <type> <object>\t<path>".
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("providerexport: malformed ls-tree record %q", rec)
		}
		meta := rec[:tab]
		path := string(rec[tab+1:])
		fields := strings.Fields(string(meta))
		if len(fields) < 3 {
			return nil, fmt.Errorf("providerexport: malformed ls-tree meta %q", meta)
		}
		entries = append(entries, TreeMeta{Path: path, Mode: fields[0]})
	}
	return entries, nil
}

// ReadBlob runs `git cat-file blob <commit>:<path>`, reading the object out
// of the commit's tree rather than the working copy.
func (ExecGit) ReadBlob(ctx context.Context, repoDir, commit, path string) ([]byte, error) {
	out, err := runGit(ctx, repoDir, "cat-file", "blob", "--end-of-options", commit+":"+path)
	if err != nil {
		return nil, fmt.Errorf("providerexport: read blob %s:%s: %w", commit, path, err)
	}
	return out, nil
}

// runGit executes one git plumbing command in repoDir and returns stdout.
// stderr is folded into the error so callers get git's own diagnostic.
func runGit(ctx context.Context, repoDir string, args ...string) ([]byte, error) {
	bin, err := exec.LookPath("git")
	if err != nil {
		return nil, fmt.Errorf("git not on PATH: %w", err)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = repoDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}
