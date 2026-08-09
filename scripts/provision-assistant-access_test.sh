#!/usr/bin/env bash
# Offline behavioral tests for local-agent HTTP MCP provisioning.
# All credentials are fixed dummy fixtures under a throwaway directory; no
# database, live config, or operator token is read.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
script="$repo_root/scripts/provision-assistant-access.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

stub="$tmp/stub-meristem"
cat > "$stub" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf 'invoked\n' >> "${STUB_CALLS_OUT:?}"
if [[ "${1:-} ${2:-}" == "tokens create" ]]; then
  {
    printf 'CALL\n'
    printf '%s\n' "$@"
  } >> "${STUB_ARGV_OUT:?}"
  printf 'id=00000000-0000-0000-0000-000000000000\nname=stub\nroot=false\nsource=agent\nsecret=dummy-stub-secret\n'
elif [[ "${1:-} ${2:-}" == "tokens list" ]]; then
  cat "${STUB_LIST_OUT:-/dev/null}"
else
  exit 64
fi
STUB
chmod +x "$stub"

export STUB_ARGV_OUT="$tmp/argv"
export STUB_LIST_OUT="$tmp/list"
export STUB_CALLS_OUT="$tmp/calls"
: > "$STUB_ARGV_OUT"
: > "$STUB_LIST_OUT"
: > "$STUB_CALLS_OUT"
printf 'dummy-root-token\n' > "$tmp/root.token"

run() {
  MERISTEM_BIN="$stub" \
  MERISTEM_TOKEN_DIR="$tmp/tok" \
  ROOT_TOKEN_FILE="$tmp/root.token" \
  MERISTEM_DATABASE_URL='postgres://dummy.invalid/never-used' \
    "$script" "$@"
}

perm() {
  stat -f '%Lp' "$1" 2>/dev/null || stat -c '%a' "$1"
}

failures=0
pass() { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; failures=$((failures + 1)); }

mkdir -p "$tmp/tok"

# 1. Explicit HTTP generation is offline and succeeds without any root token.
rm -f "$tmp/root.token"
if run --generate-http >/dev/null 2>&1 && [[ ! -s "$STUB_CALLS_OUT" ]]; then
  pass "HTTP config generation is offline and does not require root"
else
  fail "HTTP config generation is offline and does not require root"
fi
printf 'dummy-root-token\n' > "$tmp/root.token"

generated="$tmp/tok/generated"

# 2. All candidates use the same active name and the required HTTP shapes.
if grep -q '^\[mcp_servers\.meristem\]$' "$generated/codex-http-mcp.toml" &&
   grep -q 'bearer_token_env_var = "MERISTEM_CODEX_TOKEN"' "$generated/codex-http-mcp.toml" &&
   grep -q '"X-Meristem-Tool-Names" = "canonical"' "$generated/codex-http-mcp.toml" &&
   grep -q '"meristem"' "$generated/claude-code-http-mcp.json" &&
   grep -q '"headersHelper"' "$generated/claude-code-http-mcp.json" &&
   grep -q '"type": "http"' "$generated/cursor-http-mcp.json" &&
   grep -Fq '${env:MERISTEM_CURSOR_TOKEN}' "$generated/cursor-http-mcp.json" &&
   ! grep -Rqs 'meristem-http' "$generated"/*.json "$generated"/*.toml; then
  pass "Codex, Claude, and Cursor replace exact active name meristem"
else
  fail "Codex, Claude, and Cursor replace exact active name meristem"
fi

# 3. Generated material is secret-free and contains no direct-database path.
if ! grep -RqsE 'dummy-(root-token|stub-secret)|postgres://' "$generated"; then
  pass "generated cutover pack contains no bearer or database URL"
else
  fail "generated cutover pack contains no bearer or database URL"
fi

# 4. Generated modes fail closed: private snippets, executable helpers.
mode_ok=true
for file in "$generated"/*.json "$generated"/*.toml "$generated/rollback/README.txt"; do
  [[ "$(perm "$file")" == 600 ]] || mode_ok=false
done
for file in "$generated"/*-launch.sh "$generated/claude-code-http-headers.sh"; do
  [[ "$(perm "$file")" == 700 ]] || mode_ok=false
done
if [[ "$(perm "$generated")" == 700 && "$(perm "$generated/rollback")" == 700 ]] && $mode_ok; then
  pass "generated snippets and helpers have private modes"
else
  fail "generated snippets and helpers have private modes"
fi

# 5. Claude headersHelper rereads the token file on every invocation.
printf 'dummy-claude-one\n' > "$tmp/claude.token"
header_one="$(CLAUDE_MERISTEM_TOKEN_FILE="$tmp/claude.token" "$generated/claude-code-http-headers.sh")"
printf 'dummy-claude-two\n' > "$tmp/claude.token"
header_two="$(CLAUDE_MERISTEM_TOKEN_FILE="$tmp/claude.token" "$generated/claude-code-http-headers.sh")"
if [[ "$header_one" == *'Bearer dummy-claude-one'* && "$header_two" == *'Bearer dummy-claude-two'* && "$header_two" == *'"X-Meristem-Tool-Names":"cursor"'* ]]; then
  pass "Claude headersHelper rereads its token on reconnect"
else
  fail "Claude headersHelper rereads its token on reconnect"
fi

# 6. Launchers inject only their client token and remove stdio/DB variables.
client_stub="$tmp/client-stub"
cat > "$client_stub" <<'CLIENT'
#!/usr/bin/env bash
set -euo pipefail
case "${CLIENT_KIND:?}" in
  codex) printf '%s|%s|%s\n' "${MERISTEM_CODEX_TOKEN:-}" "${MERISTEM_TOKEN-unset}" "${MERISTEM_DATABASE_URL-unset}" ;;
  claude) printf '%s|%s|%s\n' "${MERISTEM_CLAUDE_TOKEN:-}" "${MERISTEM_TOKEN-unset}" "${MERISTEM_DATABASE_URL-unset}" ;;
  cursor) printf '%s|%s|%s\n' "${MERISTEM_CURSOR_TOKEN:-}" "${MERISTEM_TOKEN-unset}" "${MERISTEM_DATABASE_URL-unset}" ;;
esac
CLIENT
chmod +x "$client_stub"
printf 'dummy-launch-token\n' > "$tmp/launch.token"
codex_launch="$(CLIENT_KIND=codex CODEX_BIN="$client_stub" CODEX_MERISTEM_TOKEN_FILE="$tmp/launch.token" MERISTEM_TOKEN=legacy MERISTEM_DATABASE_URL=dummy "$generated/codex-http-launch.sh")"
claude_launch="$(CLIENT_KIND=claude CLAUDE_BIN="$client_stub" CLAUDE_MERISTEM_TOKEN_FILE="$tmp/launch.token" MERISTEM_TOKEN=legacy MERISTEM_DATABASE_URL=dummy "$generated/claude-code-http-launch.sh")"
cursor_launch="$(CLIENT_KIND=cursor CURSOR_BIN="$client_stub" CURSOR_MERISTEM_TOKEN_FILE="$tmp/launch.token" MERISTEM_TOKEN=legacy MERISTEM_DATABASE_URL=dummy "$generated/cursor-http-launch.sh")"
if [[ "$codex_launch" == 'dummy-launch-token|unset|unset' &&
      "$claude_launch" == 'dummy-launch-token|unset|unset' &&
      "$cursor_launch" == 'dummy-launch-token|unset|unset' ]]; then
  pass "Codex, Claude fallback, and Cursor launchers isolate HTTP credentials"
else
  fail "Codex, Claude fallback, and Cursor launchers isolate HTTP credentials"
fi

# 6b. The original no-mode stdio path, target set, wrappers, and interactive
# token-file override remain available as an explicit compatibility surface.
: > "$STUB_ARGV_OUT"
: > "$STUB_CALLS_OUT"
if run >/dev/null 2>&1 &&
   [[ -x "$generated/codex-meristem-command.sh" ]] &&
   [[ -x "$generated/claude-code-meristem-command.sh" ]] &&
   [[ -x "$generated/claude-code-mcp-add.sh" ]] &&
   grep -q 'CODEX_MERISTEM_TOKEN_FILE:-.*MERISTEM_TOKEN_FILE:-' "$generated/codex-meristem-command.sh" &&
   grep -q 'MERISTEM_TOKEN_FILE:-' "$generated/claude-code-meristem-command.sh" &&
   [[ "$(grep -c '^CALL$' "$STUB_ARGV_OUT")" == 6 ]]; then
  pass "legacy stdio targets and generated wrappers remain available"
else
  fail "legacy stdio targets and generated wrappers remain available"
fi

mkdir -p "$tmp/bin"
cat > "$tmp/bin/claude" <<'CLAUDE'
#!/usr/bin/env bash
printf '%s\n' "$@" > "${STUB_CLAUDE_ARGS:?}"
CLAUDE
chmod +x "$tmp/bin/claude"
export STUB_CLAUDE_ARGS="$tmp/claude-args"
if PATH="$tmp/bin:$PATH" run --targets claude-code-gui --apply-claude-code >/dev/null 2>&1 &&
   grep -qx 'mcp' "$STUB_CLAUDE_ARGS" &&
   grep -qx 'add' "$STUB_CLAUDE_ARGS" &&
   grep -qx 'meristem' "$STUB_CLAUDE_ARGS"; then
  pass "legacy Claude stdio apply helper remains available"
else
  fail "legacy Claude stdio apply helper remains available"
fi

# 7. Staged minting always adds the one exact marker plus explicit scopes and
# never overwrites/reuses an existing canonical legacy token file.
printf 'dummy-legacy-token\n' > "$tmp/tok/codex.token"
: > "$STUB_ARGV_OUT"
: > "$STUB_CALLS_OUT"
if run --mint-http --targets codex >/dev/null 2>&1 &&
   [[ "$(cat "$tmp/tok/codex.token")" == 'dummy-legacy-token' ]] &&
   [[ "$(cat "$tmp/tok/codex.http-next.token")" == 'dummy-stub-secret' ]] &&
   [[ "$(perm "$tmp/tok/codex.http-next.token")" == 600 ]] &&
   grep -qx -- '--scopes' "$STUB_ARGV_OUT" &&
   grep -qx 'mcp.profile:local_agent_v1,feed.read,work_items.read_all,work_items.write_all,work_items.create' "$STUB_ARGV_OUT"; then
  pass "HTTP mint stages exact profiled scopes without touching legacy token"
else
  fail "HTTP mint stages exact profiled scopes without touching legacy token"
fi

# 8. Re-running staged mint refuses the existing staged file.
if run --mint-http --targets codex >/dev/null 2>&1; then
  fail "staged HTTP credential is never silently reused"
else
  pass "staged HTTP credential is never silently reused"
fi

# 9. Empty/degenerate business scopes and caller-supplied profile markers fail.
scope_refusal_ok=true
scope_refusal_index=0
for scopes in '' ',' '  ' ' ,, ' 'mcp.profile:local_agent_v1' 'provider.profile:owner_v1,feed.read'; do
  scope_refusal_index=$((scope_refusal_index + 1))
  if run --session-http "scope-test-$scope_refusal_index" --business-scopes "$scopes" >/dev/null 2>&1; then
    scope_refusal_ok=false
  fi
done
if $scope_refusal_ok; then
  pass "empty and caller-supplied profile scope sets fail closed"
else
  fail "empty and caller-supplied profile scope sets fail closed"
fi

# 10. The existing stdio session defaults and override contract remain unchanged.
: > "$STUB_ARGV_OUT"
if run --session stdio-session >/dev/null 2>&1 &&
   grep -qx 'feed.read,work_items.read_all,work_items.write_all,work_items.create' "$STUB_ARGV_OUT" &&
   ! grep -q 'mcp.profile:local_agent_v1' "$STUB_ARGV_OUT" &&
   [[ "$(cat "$tmp/tok/stdio-session.token")" == 'dummy-stub-secret' ]]; then
  pass "legacy stdio session defaults remain explicitly scoped"
else
  fail "legacy stdio session defaults remain explicitly scoped"
fi
: > "$STUB_ARGV_OUT"
if run --session stdio-session-override --session-scopes feed.read >/dev/null 2>&1 &&
   grep -qx 'feed.read' "$STUB_ARGV_OUT"; then
  pass "legacy stdio session scope override remains available"
else
  fail "legacy stdio session scope override remains available"
fi

# 11. HTTP session credentials receive the local marker and explicit override.
: > "$STUB_ARGV_OUT"
if run --session-http session-one --business-scopes feed.read >/dev/null 2>&1 &&
   grep -qx 'mcp.profile:local_agent_v1,feed.read' "$STUB_ARGV_OUT" &&
   [[ "$(cat "$tmp/tok/session-one.token")" == 'dummy-stub-secret' ]]; then
  pass "HTTP session credentials are uniquely profiled and explicitly scoped"
else
  fail "HTTP session credentials are uniquely profiled and explicitly scoped"
fi
if run --session-http session-one --business-scopes feed.read >/dev/null 2>&1; then
  fail "duplicate HTTP session credential name is refused"
else
  pass "duplicate HTTP session credential name is refused"
fi

# 12. HTTP generation cannot accidentally invoke the legacy live-apply path.
if run --generate-http --apply-claude-code >/dev/null 2>&1; then
  fail "HTTP generation never applies live Claude config"
else
  pass "HTTP generation never applies live Claude config"
fi

# 13. The manifest encodes the same-name/no-dual-registration rollback rule.
if grep -q '"active_server_name": "meristem"' "$generated/cutover-manifest.json" &&
   grep -q '"parallel_meristem_http_entry_forbidden": true' "$generated/cutover-manifest.json" &&
   grep -q '"stdio_rollback_entry_kept_outside_active_config": true' "$generated/cutover-manifest.json" &&
   grep -qi 'atomically replace that one active entry' "$generated/rollback/README.txt"; then
  pass "cutover pack records atomic same-name rollback invariants"
else
  fail "cutover pack records atomic same-name rollback invariants"
fi

if (( failures > 0 )); then
  printf '%d test(s) failed\n' "$failures" >&2
  exit 1
fi
printf 'all tests passed\n'
