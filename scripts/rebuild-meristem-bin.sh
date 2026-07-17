#!/usr/bin/env bash
# Build the single shared meristem artifact from a CLEAN v1 checkout.
#
# This one artifact (.meristem/generated/meristem-bin) backs BOTH the API server
# and every generated agent MCP wrapper (Claude, Codex, Cursor, Cerberus). One
# rebuild covers all of them, so a stale wrapper can no longer run divergent
# projector code against the shared database (work item a9374bdd). The live
# redeploy -- regenerate wrappers and restart the API/worker/MCP sessions -- is
# owner action tracked under work item 835e0dbf.
#
# Guards: the working tree must be clean and HEAD must be the freshly fetched
# origin/v1 tip, so the shared binary is never built from a dirty tree or the
# wrong ref. --force may build an off-target artifact only to an explicit,
# alternate output; its pin remains origin/v1, so the runtime guard blocks it.
# Running MCP client sessions notice a newly published pin dynamically.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Resolve provenance from this checkout's raw object graph. Git replace refs
# can otherwise make commit A transparently read/archive as commit B while
# rev-parse still prints A, defeating the embedded-SHA comparison. Repository
# redirection variables are also cleared so status/archive/fetch all address
# the checkout selected by this script rather than an ambient alternate index,
# object store, namespace, or worktree.
export GIT_NO_REPLACE_OBJECTS=1
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY
unset GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_COMMON_DIR GIT_NAMESPACE GIT_EXEC_PATH
unset GIT_CONFIG GIT_CONFIG_PARAMETERS
# GIT_CONFIG_KEY_n / VALUE_n entries are ignored once the count is absent.
unset GIT_CONFIG_COUNT

DEFAULT_OUT=".meristem/generated/meristem-bin"
OUT="${MERISTEM_BIN_OUT:-$DEFAULT_OUT}"
out_explicit=false
[[ -n "${MERISTEM_BIN_OUT+x}" ]] && out_explicit=true
GO_BIN="${GO_BIN:-$(command -v go || true)}"
REMOTE="${MERISTEM_V1_REMOTE:-origin}"
BRANCH="${MERISTEM_V1_BRANCH:-v1}"
force=false
skip_fetch=false

usage() {
  cat <<'USAGE'
usage:
  scripts/rebuild-meristem-bin.sh [options]

Builds .meristem/generated/meristem-bin from a clean v1 checkout. This one
artifact backs the API server AND all agent MCP wrappers.

options:
  --force       Build even if the working tree is dirty or HEAD is not the
                fetched origin/v1 tip, but only to an explicit alternate
                MERISTEM_BIN_OUT. The reviewed-v1 pin is not advanced, so that
                artifact fails closed instead of presenting itself as current.
  --no-fetch    Unsafe/offline mode. Requires --force and an explicit alternate
                MERISTEM_BIN_OUT; the artifact is deliberately non-authoritative.
  -h, --help    Show this help.

environment overrides:
  MERISTEM_BIN_OUT     output path (default .meristem/generated/meristem-bin).
                       Required and must be alternate for an unsafe --force build.
  MERISTEM_V1_REMOTE   remote name (default origin)
  MERISTEM_V1_BRANCH   branch name (default v1)
  GO_BIN               go binary (default: first `go` on PATH)
USAGE
}

while (($#)); do
  case "$1" in
    --force)    force=true; shift ;;
    --no-fetch) skip_fetch=true; shift ;;
    -h|--help)  usage; exit 0 ;;
    *)
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

log()  { printf '==> %s\n' "$*"; }
warn() { printf '!! %s\n' "$*" >&2; }
die()  { printf '!! %s\n' "$*" >&2; exit 1; }

[[ -n "$GO_BIN" ]] || die "could not find go on PATH; set GO_BIN=/absolute/path/to/go"
[[ -z "${GOFLAGS:-}" ]] \
  || die "GOFLAGS must be unset for a reviewed build; command-line overlays, modfiles, and tool executors are not allowed"
[[ "$REMOTE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] \
  || die "MERISTEM_V1_REMOTE must be a simple configured remote name"
[[ "$BRANCH" != -* ]] && git check-ref-format "refs/heads/$BRANCH" >/dev/null 2>&1 \
  || die "MERISTEM_V1_BRANCH is not a valid branch name"
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git checkout: $REPO_ROOT"

# Fetch the canonical v1 tip so the guard compares against the remote, not a
# possibly-stale local ref.
trusted_target=true
if $skip_fetch; then
  $force || die "--no-fetch cannot publish a reviewed artifact; use normal fetch, or pair it with --force and an alternate MERISTEM_BIN_OUT"
  trusted_target=false
  target_sha="$(git rev-parse "$REMOTE/$BRANCH" 2>/dev/null)" \
    || die "no local ref $REMOTE/$BRANCH; run without --no-fetch"
else
  log "fetching $REMOTE $BRANCH"
  git fetch --quiet "$REMOTE" "$BRANCH" \
    || die "git fetch $REMOTE $BRANCH failed (use --no-fetch to skip, or --force to bypass guards)"
  target_sha="$(git rev-parse FETCH_HEAD)"
fi

head_sha="$(git rev-parse HEAD)"

initial_tree_status="$(git status --porcelain)"
clean=true
[[ -z "$initial_tree_status" ]] || clean=false

on_target=true
[[ "$head_sha" == "$target_sha" ]] || on_target=false

unsafe_build=false
$clean || unsafe_build=true
$on_target || unsafe_build=true
$trusted_target || unsafe_build=true

if $force; then
  $clean     || warn "--force: building from a DIRTY working tree"
  $on_target || warn "--force: HEAD ($head_sha) is not $REMOTE/$BRANCH ($target_sha)"
else
  $clean || die "working tree is dirty; commit/stash or pass --force. The shared artifact must come from a clean checkout."
  $on_target || die "HEAD ($head_sha) is not the fetched $REMOTE/$BRANCH tip ($target_sha); check out $BRANCH or pass --force."
fi

mkdir -p "$(dirname "$OUT")"

# Resolve both paths after creating the output directory so spelling the
# shared output as an absolute path cannot evade the unsafe --force guard.
out_dir="$(cd "$(dirname "$OUT")" && pwd -P)"
out_path="$out_dir/$(basename "$OUT")"
default_dir="$(cd "$(dirname "$DEFAULT_OUT")" && pwd -P)"
default_path="$default_dir/$(basename "$DEFAULT_OUT")"
pin_path="$out_path.v1-pin"

if $force && $unsafe_build; then
  $out_explicit || die "unsafe --force build refused for the default shared artifact; set MERISTEM_BIN_OUT to an explicit alternate path"
  [[ "$out_path" != "$default_path" ]] \
    || die "unsafe --force build refused for the default shared artifact; choose an alternate MERISTEM_BIN_OUT"
  warn "unsafe build will remain pinned to reviewed $REMOTE/$BRANCH ($target_sha) and will fail the runtime consistency guard"
fi

tmp_bin="$(mktemp "$out_dir/.meristem-bin.XXXXXX")"
tmp_pin="$(mktemp "$out_dir/.meristem-v1-pin.XXXXXX")"
tmp_src=""
cleanup() {
  [[ -z "${tmp_bin:-}" ]] || rm -f "$tmp_bin"
  [[ -z "${tmp_pin:-}" ]] || rm -f "$tmp_pin"
  [[ -z "${tmp_src:-}" ]] || rm -rf "$tmp_src"
}
trap cleanup EXIT

# Capture the exact porcelain state after creating our temporary files. This
# keeps alternate outputs inside the checkout usable even when their directory
# is not ignored: the script's own untracked temporary paths are present in
# both snapshots, while any concurrent tree change still creates a mismatch.
build_tree_status="$(git status --porcelain)"

log "building $out_path from $head_sha"
guard_commit="$head_sha"
build_root="$REPO_ROOT"
build_vcs=true
if $unsafe_build; then
  # Go's vcs.modified setting may omit untracked files. Make every unsafe
  # build invalid independently of the toolchain's dirty heuristic.
  guard_commit="unreviewed-$head_sha"
else
  # Build authoritative bytes from the immutable Git object, not the live
  # checkout. A concurrent edit that is reverted before the final status check
  # can otherwise influence compilation without leaving a detectable diff.
  tmp_src="$(mktemp -d "${TMPDIR:-/tmp}/meristem-reviewed-src.XXXXXX")"
  git archive "$head_sha" | tar -x -C "$tmp_src" \
    || die "could not materialize immutable source snapshot for $head_sha"
  build_root="$tmp_src"
  build_vcs=false
fi
# Disable ambient Go workspace/config injection. In particular, GOWORK=off
# prevents a parent go.work replace from substituting unreviewed source, while
# GOENV=off and an explicit empty GOFLAGS prevent persisted/user flags from
# adding overlays, alternate modfiles, or tool executors behind this script.
(
  cd "$build_root"
  GOENV=off GOWORK=off GOFLAGS= "$GO_BIN" build \
    -buildvcs="$build_vcs" \
    -trimpath \
    -ldflags "-X main.version=$head_sha -X github.com/jbmopper/meristem/internal/buildguard.linkedCommit=$guard_commit" \
    -o "$tmp_bin" \
    ./cmd/meristem
)

built_version="$("$tmp_bin" version)"
[[ "$built_version" == "$head_sha" ]] \
  || die "built artifact reported version '$built_version', expected full HEAD $head_sha"
log "verified full compiled commit $built_version"

# Verify the independent buildguard linker value, not only main.version. An
# omitted or misspelled guard ldflag must fail before the artifact is published.
build_guard_status="$("$tmp_bin" build-guard-status)"
build_guard_protocol="meristem-build-guard-v1"
built_guard_commit="${build_guard_status#"$build_guard_protocol "}"
[[ "$build_guard_status" == "$build_guard_protocol $built_guard_commit" ]] \
  || die "built artifact did not provide the expected build-guard capability"
if ! $unsafe_build; then
  [[ "$built_guard_commit" == "$head_sha" ]] \
    || die "build guard reported '$built_guard_commit', expected full HEAD $head_sha"
  log "verified build-guard commit $built_guard_commit"
else
  [[ "$built_guard_commit" == "unknown" ]] \
    || die "unsafe artifact unexpectedly has current build-guard metadata"
  log "verified unsafe artifact is invalid to the runtime guard"
fi

# macOS Application Firewall tracks inbound-connection approvals per executable
# identity. Ad-hoc signing (`-s -`) gives the artifact a valid signature, but an
# ad-hoc identity is its content hash, which changes on every rebuild — so expect
# a re-approval prompt per rebuild anyway. The durable fix is signing with a
# stable real identity; observe the actual prompt behavior during the 835e0dbf
# redeploy. Signing is best-effort: a failure is loud but not fatal.
if [[ "$(uname -s)" == "Darwin" ]]; then
  if command -v codesign >/dev/null 2>&1; then
    if codesign -s - --force "$tmp_bin"; then
      log "ad-hoc codesigned temporary artifact"
    else
      warn "codesign failed for $out_path; macOS may re-prompt for inbound network approval on next launch"
    fi
  else
    warn "codesign not found; skipping ad-hoc signing ($OUT may re-trigger macOS firewall approval)"
  fi
fi

# Revalidate the complete reviewed snapshot as close as possible to the first
# atomic rename. A build may take long enough for HEAD, the working tree, or the
# reviewed remote tip to move underneath it. Publishing after any such drift
# could pair a binary built from one snapshot with a pin from another.
if $skip_fetch; then
  publish_target_sha="$(git rev-parse "$REMOTE/$BRANCH" 2>/dev/null)" \
    || die "local ref $REMOTE/$BRANCH disappeared during build; refusing publication"
else
  log "rechecking $REMOTE $BRANCH before publication"
  git fetch --quiet "$REMOTE" "$BRANCH" \
    || die "git fetch $REMOTE $BRANCH failed during publication check"
  publish_target_sha="$(git rev-parse FETCH_HEAD)"
fi
publish_head_sha="$(git rev-parse HEAD)"
publish_tree_status="$(git status --porcelain)"

[[ "$publish_target_sha" == "$target_sha" ]] \
  || die "$REMOTE/$BRANCH changed during build; refusing publication"
[[ "$publish_head_sha" == "$head_sha" ]] \
  || die "repository HEAD changed during build; refusing publication"
[[ "$publish_tree_status" == "$build_tree_status" ]] \
  || die "working tree changed during build; refusing publication"

# Publish the reviewed pin first and the binary second. During the tiny window
# between renames an old process/binary sees the new pin and fails closed. Both
# renames stay on the destination filesystem and are atomic.
printf '%s\n' "$target_sha" > "$tmp_pin"
chmod 0644 "$tmp_pin"
mv -f "$tmp_pin" "$pin_path"
tmp_pin=""
mv -f "$tmp_bin" "$out_path"
tmp_bin=""

log "published reviewed-v1 pin $target_sha"
log "published $out_path"

log "done. This artifact and its sibling .v1-pin back the API server AND every agent MCP wrapper."
log "Regenerate wrappers and restart live sessions under work item 835e0dbf;"
log "old sessions keep their process, but become non-authoritative and refuse work when this pin advances."
