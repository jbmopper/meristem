package buildguard

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRebuildScriptPublishesOnlyGuardedArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("release builder is a bash script")
	}
	for _, command := range []string{"bash", "git"} {
		if _, err := exec.LookPath(command); err != nil {
			t.Skipf("%s unavailable: %v", command, err)
		}
	}

	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	remote := filepath.Join(root, "origin.git")
	runGit(t, root, "init", "--bare", "-q", remote)
	if err := os.MkdirAll(filepath.Join(repo, "scripts"), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFixture(t, filepath.Join("..", "..", "scripts", "rebuild-meristem-bin.sh"), filepath.Join(repo, "scripts", "rebuild-meristem-bin.sh"), 0o755)
	copyFixture(t, filepath.Join("..", "..", "scripts", "check-meristem-build-pin.sh"), filepath.Join(repo, "scripts", "check-meristem-build-pin.sh"), 0o755)
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(".meristem/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked-source.txt"), []byte("reviewed\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	fakeGo := filepath.Join(root, "fake-go")
	if err := os.WriteFile(fakeGo, []byte(fakeGoBuilder), 0o700); err != nil {
		t.Fatal(err)
	}

	runGit(t, repo, "init", "-q")
	runGit(t, repo, "config", "user.name", "Build Guard Test")
	runGit(t, repo, "config", "user.email", "build-guard@example.invalid")
	runGit(t, repo, "add", ".gitignore", "scripts", "tracked-source.txt")
	runGit(t, repo, "-c", "commit.gpgsign=false", "commit", "-qm", "reviewed tip")
	reviewed := strings.TrimSpace(runGit(t, repo, "rev-parse", "HEAD"))
	runGit(t, repo, "remote", "add", "origin", remote)
	runGit(t, repo, "push", "-q", "origin", "HEAD:refs/heads/v1")

	buildScript := filepath.Join(repo, "scripts", "rebuild-meristem-bin.sh")
	helper := filepath.Join(repo, "scripts", "check-meristem-build-pin.sh")
	baseEnv := append(withoutMeristemBuildEnv(os.Environ()), "GO_BIN="+fakeGo)

	// Ambient GOFLAGS can substitute unreviewed source through overlays,
	// modfiles, or tool executors while preserving the stamped commit. The
	// reviewed publisher refuses the environment rather than guessing which
	// flags are harmless.
	output, err := commandOutput(repo, append(baseEnv, "GOFLAGS=-overlay=/tmp/unreviewed-overlay.json"), buildScript)
	if err == nil || !strings.Contains(string(output), "GOFLAGS must be unset") {
		t.Fatalf("ambient GOFLAGS was not refused: %v: %s", err, output)
	}
	output, err = commandOutput(repo, baseEnv, buildScript, "--no-fetch")
	if err == nil || !strings.Contains(string(output), "--no-fetch cannot publish a reviewed artifact") {
		t.Fatalf("unfetched reviewed publication was not refused: %v: %s", err, output)
	}

	// A clean checkout at the reviewed tip publishes a matching pin and guard
	// fingerprint, and the shared launcher preflight accepts it.
	runScript(t, repo, baseEnv, buildScript)
	currentBin := filepath.Join(repo, ".meristem", "generated", "meristem-bin")
	currentPin := currentBin + ".v1-pin"
	if got, err := os.ReadFile(currentPin); err != nil || string(got) != reviewed+"\n" {
		t.Fatalf("published pin = %q, %v", got, err)
	}
	runScript(t, repo, baseEnv, helper, currentBin, currentPin)
	originalBin, err := os.ReadFile(currentBin)
	if err != nil {
		t.Fatal(err)
	}
	originalPin, err := os.ReadFile(currentPin)
	if err != nil {
		t.Fatal(err)
	}

	// A replace ref can make Git read reviewed commit A as malicious commit B
	// while rev-parse still prints A. Materialize B through the replacement so
	// the checkout appears clean only when replacements are honored; the release
	// script must read the raw graph, see the dirty tree, and refuse publication.
	if err := os.WriteFile(filepath.Join(repo, "tracked-source.txt"), []byte("unreviewed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "tracked-source.txt")
	runGit(t, repo, "-c", "commit.gpgsign=false", "commit", "-qm", "replacement payload")
	replacement := strings.TrimSpace(runGit(t, repo, "rev-parse", "HEAD"))
	runGit(t, repo, "reset", "--hard", reviewed)
	runGit(t, repo, "replace", reviewed, replacement)
	runGit(t, repo, "reset", "--hard", reviewed)
	output, err = commandOutput(repo, baseEnv, buildScript)
	if err == nil || !strings.Contains(string(output), "working tree is dirty") {
		t.Fatalf("git replacement provenance bypass was not refused: %v: %s", err, output)
	}
	assertPublishedPair(t, currentBin, currentPin, originalBin, originalPin)
	runGit(t, repo, "replace", "-d", reviewed)
	runGit(t, repo, "reset", "--hard", reviewed)

	// A working-tree change made by another process while the builder runs
	// aborts before either member of the published binary/pin pair is replaced.
	treeDriftEnv := append(baseEnv, "FAKE_GO_MUTATION=tree", "FAKE_GO_REPO_ROOT="+repo)
	output, err = commandOutput(repo, treeDriftEnv, buildScript)
	if err == nil || !strings.Contains(string(output), "working tree changed during build") {
		t.Fatalf("mid-build tree drift = %v: %s", err, output)
	}
	assertPublishedPair(t, currentBin, currentPin, originalBin, originalPin)
	if err := os.Remove(filepath.Join(repo, "mid-build-drift.txt")); err != nil {
		t.Fatal(err)
	}

	// A concurrent advance of the captured remote-tracking ref is likewise a
	// hard stop. The alternate commit has the same tree, isolating ref drift
	// from the working-tree check.
	advanced := strings.TrimSpace(runGit(t, repo, "commit-tree", reviewed+"^{tree}", "-p", reviewed, "-m", "advanced reviewed tip"))
	runGit(t, repo, "push", "-q", "origin", advanced+":refs/heads/test-advanced")
	refDriftEnv := append(baseEnv, "FAKE_GO_MUTATION=ref", "FAKE_GO_TARGET_SHA="+advanced, "FAKE_GO_REMOTE="+remote)
	output, err = commandOutput(repo, refDriftEnv, buildScript)
	if err == nil || !strings.Contains(string(output), "origin/v1 changed during build") {
		t.Fatalf("mid-build remote ref drift = %v: %s", err, output)
	}
	assertPublishedPair(t, currentBin, currentPin, originalBin, originalPin)
	runGit(t, repo, "--git-dir="+remote, "update-ref", "refs/heads/v1", reviewed)

	// HEAD is independently re-read. Using another same-tree commit makes this
	// case deterministic without also introducing a dirty checkout.
	headDriftEnv := append(baseEnv, "FAKE_GO_MUTATION=head", "FAKE_GO_TARGET_SHA="+advanced, "FAKE_GO_REPO_ROOT="+repo)
	output, err = commandOutput(repo, headDriftEnv, buildScript)
	if err == nil || !strings.Contains(string(output), "repository HEAD changed during build") {
		t.Fatalf("mid-build HEAD drift = %v: %s", err, output)
	}
	assertPublishedPair(t, currentBin, currentPin, originalBin, originalPin)
	runGit(t, repo, "update-ref", "HEAD", reviewed)

	// A transient edit-and-revert in the live checkout cannot influence an
	// authoritative build because compilation reads an immutable git archive.
	transientEnv := append(baseEnv, "FAKE_GO_MUTATION=transient", "FAKE_GO_REPO_ROOT="+repo)
	runScript(t, repo, transientEnv, buildScript)
	runScript(t, repo, baseEnv, helper, currentBin, currentPin)

	// A clean ordinary build from a commit beyond the reviewed tip refuses.
	if err := os.WriteFile(filepath.Join(repo, "next.txt"), []byte("next\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "next.txt")
	runGit(t, repo, "-c", "commit.gpgsign=false", "commit", "-qm", "unreviewed tip")
	staleOut := filepath.Join(repo, ".artifacts", "ordinary-stale")
	output, err = commandOutput(repo, append(baseEnv, "MERISTEM_BIN_OUT="+staleOut), buildScript)
	if err == nil || !strings.Contains(string(output), "is not the fetched origin/v1 tip") {
		t.Fatalf("ordinary stale build = %v: %s", err, output)
	}
	if _, statErr := os.Stat(staleOut); !os.IsNotExist(statErr) {
		t.Fatalf("ordinary stale artifact was published: %v", statErr)
	}

	// --force may publish off-tip only to an explicit alternate path, and the
	// pin remains reviewed v1 so launcher preflight rejects it.
	offTipOut := filepath.Join(repo, ".artifacts", "forced-off-tip")
	runScript(t, repo, append(baseEnv, "MERISTEM_BIN_OUT="+offTipOut), buildScript, "--force")
	if output, err := commandOutput(repo, baseEnv, helper, offTipOut, offTipOut+".v1-pin"); err == nil {
		t.Fatalf("forced off-tip artifact passed preflight: %s", output)
	}

	// Offline mode is always an explicitly alternate, non-authoritative build,
	// even if a locally rewritten origin/v1 happens to equal HEAD.
	localOnlyOut := filepath.Join(root, "forced-no-fetch")
	runGit(t, repo, "update-ref", "refs/remotes/origin/v1", strings.TrimSpace(runGit(t, repo, "rev-parse", "HEAD")))
	runScript(t, repo, append(baseEnv, "MERISTEM_BIN_OUT="+localOnlyOut), buildScript, "--force", "--no-fetch")
	if output, err := commandOutput(repo, baseEnv, helper, localOnlyOut, localOnlyOut+".v1-pin"); err == nil {
		t.Fatalf("unfetched artifact passed preflight: %s", output)
	}

	// The prior alternate output makes the tree dirty; the next forced build
	// gets an intentionally invalid linked fingerprint even if HEAD is stable.
	dirtyOut := filepath.Join(root, "forced-dirty")
	runScript(t, repo, append(baseEnv, "MERISTEM_BIN_OUT="+dirtyOut), buildScript, "--force")
	if output, err := commandOutput(repo, baseEnv, helper, dirtyOut, dirtyOut+".v1-pin"); err == nil {
		t.Fatalf("forced dirty artifact passed preflight: %s", output)
	}
}

func copyFixture(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, contents, mode); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = repo
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}

func runScript(t *testing.T, repo string, environment []string, script string, args ...string) {
	t.Helper()
	output, err := commandOutput(repo, environment, script, args...)
	if err != nil {
		t.Fatalf("%s %v: %v: %s", filepath.Base(script), args, err, output)
	}
}

func commandOutput(repo string, environment []string, script string, args ...string) ([]byte, error) {
	command := exec.Command("bash", append([]string{script}, args...)...)
	command.Dir = repo
	command.Env = environment
	return command.CombinedOutput()
}

func assertPublishedPair(t *testing.T, binaryPath, pinPath string, wantBinary, wantPin []byte) {
	t.Helper()
	gotBinary, err := os.ReadFile(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	gotPin, err := os.ReadFile(pinPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBinary) != string(wantBinary) {
		t.Fatal("published binary changed after refused rebuild")
	}
	if string(gotPin) != string(wantPin) {
		t.Fatal("published pin changed after refused rebuild")
	}
}

func withoutMeristemBuildEnv(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		if strings.HasPrefix(entry, "MERISTEM_BIN_OUT=") ||
			strings.HasPrefix(entry, "MERISTEM_V1_REMOTE=") ||
			strings.HasPrefix(entry, "MERISTEM_V1_BRANCH=") ||
			strings.HasPrefix(entry, "GO_BIN=") ||
			strings.HasPrefix(entry, "GOENV=") ||
			strings.HasPrefix(entry, "GOFLAGS=") ||
			strings.HasPrefix(entry, "GOWORK=") ||
			strings.HasPrefix(entry, "FAKE_GO_MUTATION=") ||
			strings.HasPrefix(entry, "FAKE_GO_TARGET_SHA=") ||
			strings.HasPrefix(entry, "FAKE_GO_REPO_ROOT=") ||
			strings.HasPrefix(entry, "FAKE_GO_REMOTE=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

const fakeGoBuilder = `#!/usr/bin/env bash
set -euo pipefail
[[ "${GOENV:-}" == "off" ]] || { printf 'GOENV was not disabled\n' >&2; exit 3; }
[[ "${GOWORK:-}" == "off" ]] || { printf 'GOWORK was not disabled\n' >&2; exit 3; }
[[ -z "${GOFLAGS:-}" ]] || { printf 'GOFLAGS was not cleared\n' >&2; exit 3; }
[[ "${1:-}" == "build" ]] || exit 2
shift
out=""
ldflags=""
while (($#)); do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -ldflags) ldflags="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$out" && -n "$ldflags" ]] || exit 2
main_commit="${ldflags#*-X main.version=}"
main_commit="${main_commit%% *}"
guard_commit="${ldflags##*buildguard.linkedCommit=}"
guard_commit="${guard_commit%% *}"
guard_version="$guard_commit"
source_was_reviewed=true
case "${FAKE_GO_MUTATION:-}" in
  "") ;;
  tree)
    printf 'changed during build\n' > "${FAKE_GO_REPO_ROOT:?}/mid-build-drift.txt"
    ;;
  transient)
    printf 'unreviewed\n' > "${FAKE_GO_REPO_ROOT:?}/tracked-source.txt"
    [[ "$(cat tracked-source.txt)" == "reviewed" ]] || source_was_reviewed=false
    printf 'reviewed\n' > "${FAKE_GO_REPO_ROOT:?}/tracked-source.txt"
    ;;
  ref)
    git --git-dir="${FAKE_GO_REMOTE:?}" update-ref refs/heads/v1 "${FAKE_GO_TARGET_SHA:?}"
    ;;
  head)
    git -C "${FAKE_GO_REPO_ROOT:?}" update-ref HEAD "${FAKE_GO_TARGET_SHA:?}"
    ;;
  *) exit 2 ;;
esac
if [[ ! "$guard_version" =~ ^[0-9a-f]{40}$ ]]; then
  guard_version="unknown"
fi
if ! $source_was_reviewed; then
  guard_version="unknown"
fi
cat > "$out" <<EOF
#!/usr/bin/env bash
if [[ "\${1:-}" == build-guard-status && "\$#" -eq 1 ]]; then
  printf 'meristem-build-guard-v1 %s\\n' '$guard_version'
  exit 0
fi
if [[ "\${1:-}" == version && "\${2:-}" == --commit ]]; then
  printf '%s\\n' '$guard_version'
  exit 0
fi
if [[ "\${1:-}" == version ]]; then
  printf '%s\\n' '$main_commit'
  exit 0
fi
exit 2
EOF
chmod 0755 "$out"
`
