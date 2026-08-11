#!/usr/bin/env bash
# Fail-closed stdio MCP launcher for the unattended Codex listener. This is a
# tracked runtime component; do not copy it into .meristem/generated.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
meristem_bin="$repo_root/.meristem/generated/meristem-bin"
pin_file="$meristem_bin.v1-pin"
token_file="${CODEX_MERISTEM_TOKEN_FILE:-}"
listener_codex_home="${CODEX_HOME:-}"

fail() {
  printf 'Codex listener MCP launch failed: %s\n' "$1" >&2
  exit 64
}

[[ "$listener_codex_home" == /* && -d "$listener_codex_home" && ! -L "$listener_codex_home" ]] ||
  fail "CODEX_HOME must name the dedicated listener home"
listener_codex_home_physical="$(cd "$listener_codex_home" && pwd -P)"
[[ "$listener_codex_home_physical" == "$listener_codex_home" ]] ||
  fail "CODEX_HOME must be an exact symlink-free path"
if ! listener_home_mode="$(stat -f '%Lp' "$listener_codex_home" 2>/dev/null)"; then
  listener_home_mode="$(stat -c '%a' "$listener_codex_home" 2>/dev/null || true)"
fi
[[ "$listener_home_mode" == "700" ]] || fail "CODEX_HOME must have mode 0700"
[[ "$token_file" == "$listener_codex_home/meristem-task.token" && -f "$token_file" && ! -L "$token_file" && -r "$token_file" && -s "$token_file" ]] ||
  fail "CODEX_MERISTEM_TOKEN_FILE must name an absolute, readable, non-empty file"
for binding_name in MERISTEM_MCP_EXPECT_ACTOR_ID MERISTEM_MCP_LISTENER_ACTIVATION_ID MERISTEM_MCP_LISTENER_WORK_ITEM_ID MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID; do
  binding_value="${!binding_name:-}"
  [[ "$binding_value" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ && "$binding_value" != "00000000-0000-0000-0000-000000000000" ]] ||
    fail "$binding_name is missing or malformed"
done

# Authenticate the executable before reading the bearer. The helper validates
# both the binary's embedded commit and the separately published reviewed pin.
if ! "$repo_root/scripts/check-meristem-build-pin.sh" "$meristem_bin" "$pin_file"; then
  fail "shared Meristem binary does not match the reviewed-v1 pin"
fi

# Pin the load-bearing adapter bundle to the same reviewed commit as the Go
# binary. This catches stale publication and local edits before the bearer is
# read; the fixed sibling topology prevents substituting a generated wrapper.
reviewed_commit="$(LC_ALL=C sed -n '1p' "$pin_file")"
git_bin=/usr/bin/git
if [[ ! -x "$git_bin" ]]; then
  git_bin="$(command -v git || true)"
fi
[[ "$git_bin" == /* && -x "$git_bin" ]] || fail "trusted Git executable is unavailable"
git_clean() {
  /usr/bin/env -i \
    PATH=/usr/bin:/bin LC_ALL=C \
    GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
    GIT_NO_REPLACE_OBJECTS=1 GIT_OPTIONAL_LOCKS=0 \
    "$git_bin" "$@"
}
if ! checkout_commit="$(git_clean -C "$repo_root" rev-parse --verify HEAD 2>/dev/null)"; then
  fail "listener scripts are not in a Git checkout"
fi
[[ "$checkout_commit" == "$reviewed_commit" ]] ||
  fail "listener script checkout does not match the reviewed-v1 pin"
required_paths=(
  scripts/check-meristem-build-pin.sh
  scripts/codex-listener-app-server.sh
  scripts/codex-listener-mcp-command.sh
  scripts/codex-thread-nudge.py
)
for required_path in "${required_paths[@]}"; do
  [[ -f "$repo_root/$required_path" && -x "$repo_root/$required_path" ]] ||
    fail "listener runtime component is missing or not executable"
  if ! expected_blob="$(git_clean -C "$repo_root" rev-parse "$reviewed_commit:$required_path" 2>/dev/null)"; then
    fail "listener runtime component is not tracked at the reviewed commit"
  fi
  if ! actual_blob="$(git_clean hash-object "$repo_root/$required_path" 2>/dev/null)" ||
    [[ "$actual_blob" != "$expected_blob" ]]; then
    fail "listener runtime components differ from the reviewed commit"
  fi
done

if ! token_mode="$(stat -f '%Lp' "$token_file" 2>/dev/null)"; then
  token_mode="$(stat -c '%a' "$token_file" 2>/dev/null || true)"
fi
[[ "$token_mode" == "600" ]] ||
  fail "CODEX_MERISTEM_TOKEN_FILE must have mode 0600"
if ! token_size="$(stat -f '%z' "$token_file" 2>/dev/null)"; then
  token_size="$(stat -c '%s' "$token_file" 2>/dev/null || true)"
fi
[[ "$token_size" =~ ^[0-9]+$ && "$token_size" -ge 1 && "$token_size" -le 4096 ]] ||
  fail "CODEX_MERISTEM_TOKEN_FILE has an invalid size"

token="$(tr -d '\r\n' < "$token_file")"
[[ -n "$token" ]] || fail "CODEX_MERISTEM_TOKEN_FILE contains no token"
export MERISTEM_TOKEN="$token"

# The child receives the bearer, never a path it could reinterpret through an
# interactive-token fallback. The listener is deliberately local-Postgres-only.
unset CODEX_MERISTEM_TOKEN_FILE MERISTEM_TOKEN_FILE
unset token_file token_mode token_size token listener_codex_home listener_codex_home_physical listener_home_mode binding_name binding_value
unset actual_blob checkout_commit expected_blob reviewed_commit required_path required_paths git_bin
unset -f git_clean
export MERISTEM_V1_PIN_FILE="$pin_file"
export MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable'

exec "$meristem_bin" mcp
