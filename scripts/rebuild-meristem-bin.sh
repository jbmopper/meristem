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
# Guards (bypass with --force): the working tree must be clean and HEAD must be
# the freshly fetched origin/v1 tip, so the shared binary is never built from a
# dirty tree or the wrong ref. Running MCP client sessions keep their current
# process until the client session restarts.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT="${MERISTEM_BIN_OUT:-.meristem/generated/meristem-bin}"
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
                fetched origin/v1 tip. Prints a loud warning instead of aborting.
  --no-fetch    Do not run `git fetch`; compare HEAD against the existing
                origin/v1 remote-tracking ref instead.
  -h, --help    Show this help.

environment overrides:
  MERISTEM_BIN_OUT     output path (default .meristem/generated/meristem-bin)
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
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git checkout: $REPO_ROOT"

# Fetch the canonical v1 tip so the guard compares against the remote, not a
# possibly-stale local ref.
if $skip_fetch; then
  target_sha="$(git rev-parse "$REMOTE/$BRANCH" 2>/dev/null)" \
    || die "no local ref $REMOTE/$BRANCH; run without --no-fetch, or pass --force"
else
  log "fetching $REMOTE $BRANCH"
  git fetch --quiet "$REMOTE" "$BRANCH" \
    || die "git fetch $REMOTE $BRANCH failed (use --no-fetch to skip, or --force to bypass guards)"
  target_sha="$(git rev-parse FETCH_HEAD)"
fi

head_sha="$(git rev-parse HEAD)"

clean=true
[[ -z "$(git status --porcelain)" ]] || clean=false

on_target=true
[[ "$head_sha" == "$target_sha" ]] || on_target=false

if $force; then
  $clean     || warn "--force: building from a DIRTY working tree"
  $on_target || warn "--force: HEAD ($head_sha) is not $REMOTE/$BRANCH ($target_sha)"
else
  $clean || die "working tree is dirty; commit/stash or pass --force. The shared artifact must come from a clean checkout."
  $on_target || die "HEAD ($head_sha) is not the fetched $REMOTE/$BRANCH tip ($target_sha); check out $BRANCH or pass --force."
fi

mkdir -p "$(dirname "$OUT")"
log "building $OUT from $(git rev-parse --short HEAD)"
"$GO_BIN" build -o "$OUT" ./cmd/meristem
log "built $OUT"

# macOS Application Firewall tracks inbound-connection approvals per executable
# identity. Ad-hoc signing (`-s -`) gives the artifact a valid signature, but an
# ad-hoc identity is its content hash, which changes on every rebuild — so expect
# a re-approval prompt per rebuild anyway. The durable fix is signing with a
# stable real identity; observe the actual prompt behavior during the 835e0dbf
# redeploy. Signing is best-effort: a failure is loud but not fatal.
if [[ "$(uname -s)" == "Darwin" ]]; then
  if command -v codesign >/dev/null 2>&1; then
    if codesign -s - --force "$OUT"; then
      log "ad-hoc codesigned $OUT"
    else
      warn "codesign failed for $OUT; macOS may re-prompt for inbound network approval on next launch"
    fi
  else
    warn "codesign not found; skipping ad-hoc signing ($OUT may re-trigger macOS firewall approval)"
  fi
fi

log "done. This artifact backs the API server AND every agent MCP wrapper."
log "Regenerate wrappers and restart live sessions under work item 835e0dbf;"
log "running MCP client sessions keep their old process until the client restarts."
