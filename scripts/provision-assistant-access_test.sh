#!/usr/bin/env bash
# Behavioral tests for provision-assistant-access.sh --session (3818efed).
#
# No database and no live meristem binary: MERISTEM_BIN is a stub that records
# its argv and emits a fixed tokens-create result, so these tests assert the
# script's own contract - scope propagation, unique-session refusal, empty-name
# and empty-scopes failure, missing-file failure, and the wrapper token-file
# override - deterministically and offline.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
script="$repo_root/scripts/provision-assistant-access.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

stub="$tmp/stub-meristem"
cat > "$stub" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-} ${2:-}" == "tokens create" ]]; then
  printf '%s\n' "$@" > "${STUB_ARGV_OUT:?}"
  printf 'id=00000000-0000-0000-0000-000000000000\nname=stub\nroot=false\nsource=agent\nsecret=stub-secret\n'
elif [[ "${1:-} ${2:-}" == "tokens list" ]]; then
  cat "${STUB_LIST_OUT:-/dev/null}"
fi
STUB
chmod +x "$stub"

export STUB_ARGV_OUT="$tmp/argv"
export STUB_LIST_OUT="$tmp/list"
: > "$STUB_LIST_OUT"
printf 'stub-root-token\n' > "$tmp/root.token"

run() {
  MERISTEM_BIN="$stub" \
  MERISTEM_TOKEN_DIR="$tmp/tok" \
  ROOT_TOKEN_FILE="$tmp/root.token" \
  MERISTEM_AGENT_WORKTREE_BASE="$tmp/worktrees" \
    "$script" "$@"
}

failures=0
pass() { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; failures=$((failures + 1)); }

mkdir -p "$tmp/tok" "$tmp/worktrees"

# 1. --session mints with the default-deny scope set and writes the file.
if run --session s1 >/dev/null 2>&1 &&
   [[ "$(cat "$tmp/tok/s1.token")" == "stub-secret" ]] &&
   grep -qx -- '--scopes' "$STUB_ARGV_OUT" &&
   grep -qx 'feed.read,work_items.read_all,work_items.write_all,work_items.create' "$STUB_ARGV_OUT"; then
  pass "session mint passes default-deny scopes and writes token file"
else
  fail "session mint passes default-deny scopes and writes token file"
fi

# 2. --session-scopes override propagates.
if run --session s1b --session-scopes feed.read >/dev/null 2>&1 &&
   grep -qx 'feed.read' "$STUB_ARGV_OUT"; then
  pass "session scope override propagates"
else
  fail "session scope override propagates"
fi

# 3. An existing token file is refused, not silently reused.
if run --session s1 >/dev/null 2>&1; then
  fail "duplicate session name (existing file) is refused"
else
  pass "duplicate session name (existing file) is refused"
fi

# 4. An active token with a missing file is refused, never reported as success.
printf '00000000-0000-0000-0000-000000000001\ts2\troot=false\tsource=agent\tactive\tcreated=2026-07-17\n' > "$STUB_LIST_OUT"
if run --session s2 >/dev/null 2>&1; then
  fail "active token without a bearer file is refused"
else
  pass "active token without a bearer file is refused"
fi
: > "$STUB_LIST_OUT"

# 5. Empty session name fails instead of falling through to full provisioning.
if run --session= >/dev/null 2>&1; then
  fail "--session= (empty name) fails closed"
else
  pass "--session= (empty name) fails closed"
fi

# 6. Empty scopes are refused (they would select the broad legacy surface).
if run --session s3 --session-scopes= >/dev/null 2>&1; then
  fail "empty --session-scopes fails closed"
else
  pass "empty --session-scopes fails closed"
fi

# 6b. Degenerate scope strings that the server would reduce to nil scopes are
# refused too: comma-only and whitespace-only pass a naive string check but
# splitCSV drops every part, which would mint a broad legacy token.
if run --session s3b --session-scopes ',' >/dev/null 2>&1; then
  fail "comma-only --session-scopes fails closed"
else
  pass "comma-only --session-scopes fails closed"
fi
if run --session s3c --session-scopes '  ' >/dev/null 2>&1; then
  fail "whitespace-only --session-scopes fails closed"
else
  pass "whitespace-only --session-scopes fails closed"
fi
if run --session s3d --session-scopes ' ,, ' >/dev/null 2>&1; then
  fail "commas-and-spaces --session-scopes fails closed"
else
  pass "commas-and-spaces --session-scopes fails closed"
fi

# 7. --session touches no shared wrapper config.
if [[ ! -e "$tmp/tok/generated/claude-code-meristem-command.sh" ]]; then
  pass "session mode regenerates no wrappers"
else
  fail "session mode regenerates no wrappers"
fi

# 8. Full provisioning keeps the interactive override and gives unattended
# Codex tasks a distinct credential-path boundary.
if run >/dev/null 2>&1 &&
   grep -q 'MERISTEM_TOKEN_FILE:-' "$tmp/tok/generated/claude-code-meristem-command.sh" &&
   grep -q 'CODEX_MERISTEM_TOKEN_FILE:-.*MERISTEM_TOKEN_FILE:-' "$tmp/tok/generated/codex-meristem-command.sh"; then
  pass "generated wrappers separate unattended Codex and interactive token paths"
else
  fail "generated wrappers separate unattended Codex and interactive token paths"
fi

if (( failures > 0 )); then
  printf '%d test(s) failed\n' "$failures" >&2
  exit 1
fi
printf 'all tests passed\n'
