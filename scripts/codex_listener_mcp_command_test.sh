#!/usr/bin/env bash
# Offline regression tests for the unattended listener's credential boundary.
# Only fixed dummy credentials under a throwaway fixture are used.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
source_launcher="$repo_root/scripts/codex-listener-mcp-command.sh"
source_wrapper="$repo_root/scripts/codex-listener-app-server.sh"
source_adapter="$repo_root/scripts/codex-thread-nudge.py"

tmp="$(mktemp -d)"
tmp="$(cd "$tmp" && pwd -P)"
trap 'rm -rf "$tmp"' EXIT

fixture="$tmp/repo"
mkdir -p "$fixture/scripts" "$fixture/.meristem/generated"
cp "$source_launcher" "$source_wrapper" "$source_adapter" \
  "$repo_root/scripts/check-meristem-build-pin.sh" "$fixture/scripts/"
chmod 700 "$fixture/scripts/"*.sh

git -C "$fixture" init -q
git -C "$fixture" add scripts
GIT_AUTHOR_NAME='Listener Test' GIT_AUTHOR_EMAIL='listener@example.invalid' \
GIT_COMMITTER_NAME='Listener Test' GIT_COMMITTER_EMAIL='listener@example.invalid' \
  git -C "$fixture" -c core.hooksPath=/dev/null commit -qm 'fixture listener runtime'
reviewed_commit="$(git -C "$fixture" rev-parse HEAD)"
printf '%s\n' "$reviewed_commit" > "$fixture/.meristem/generated/meristem-bin.v1-pin"

cat > "$fixture/.meristem/generated/meristem-bin" <<STUB
#!/usr/bin/env bash
set -euo pipefail

case "\${1:-}" in
  build-guard-status)
    printf 'meristem-build-guard-v1 $reviewed_commit\n'
    ;;
  mcp)
    printf 'mcp\n' >> "\${STUB_CALLS_OUT:?}"
    printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
      "\$1" \
      "\${MERISTEM_TOKEN:-}" \
      "\${CODEX_MERISTEM_TOKEN_FILE-unset}" \
      "\${MERISTEM_TOKEN_FILE-unset}" \
      "\${MERISTEM_DATABASE_URL:-}" \
      "\${MERISTEM_V1_PIN_FILE:-}" \
      "\${MERISTEM_MCP_EXPECT_ACTOR_ID:-}" \
      "\${MERISTEM_MCP_LISTENER_ACTIVATION_ID:-}" \
      "\${MERISTEM_MCP_LISTENER_WORK_ITEM_ID:-}" \
      "\${MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID:-}"
    ;;
  *)
    exit 64
    ;;
esac
STUB
chmod 700 "$fixture/.meristem/generated/meristem-bin"

launcher="$fixture/scripts/codex-listener-mcp-command.sh"
wrapper="$fixture/scripts/codex-listener-app-server.sh"
calls="$tmp/mcp-calls"
fallback_file="$tmp/interactive-fallback.token"
printf 'dummy-interactive-fallback\n' > "$fallback_file"
printf 'dummy-default-fallback\n' > "$fixture/.meristem/codex.token"
chmod 600 "$fallback_file" "$fixture/.meristem/codex.token"

# The unattended app-server must not inherit the interactive Codex MCP table.
# It gets a dedicated config home, exact links to the primary auth/lock state,
# and the primary SQLite home needed to resume the existing desktop task.
test_home="$tmp/home"
primary_codex_home="$test_home/.codex"
listener_codex_home="$tmp/listener-codex-home"
mkdir -p "$primary_codex_home/thread-writer-locks" "$listener_codex_home"
chmod 700 "$listener_codex_home"
dedicated_file="$listener_codex_home/meristem-task.token"
printf 'dummy-listener-token\n' > "$dedicated_file"
chmod 600 "$dedicated_file"
printf '{}\n' > "$primary_codex_home/auth.json"
ln -s "$primary_codex_home/auth.json" "$listener_codex_home/auth.json"
ln -s "$primary_codex_home/thread-writer-locks" "$listener_codex_home/thread-writer-locks"
export HOME="$test_home"
export CODEX_HOME="$listener_codex_home"
export MERISTEM_LISTENER_CODEX_HOME="$listener_codex_home"
export MERISTEM_LISTENER_CODEX_SQLITE_HOME="$primary_codex_home"
export MERISTEM_MCP_EXPECT_ACTOR_ID='019fc9ec-2d6b-7861-af0e-c1a8b540d5ba'
export MERISTEM_MCP_LISTENER_ACTIVATION_ID='019fc9ec-2d6b-7861-af0e-c1a8b540d5b7'
export MERISTEM_MCP_LISTENER_WORK_ITEM_ID='019fc9ec-2d6b-7861-af0e-c1a8b540d5b9'
export MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID='019fc9ec-2d6b-7861-af0e-c1a8b540d5b8'

failures=0
pass() { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; failures=$((failures + 1)); }

# An interactive token variable, an ambient bearer, and the historical default
# file must not rescue a missing dedicated listener-token variable.
: > "$calls"
if (
  unset CODEX_MERISTEM_TOKEN_FILE
  STUB_CALLS_OUT="$calls" \
  MERISTEM_TOKEN_FILE="$fallback_file" \
  MERISTEM_TOKEN='dummy-ambient-token' \
    "$launcher" >/dev/null 2>&1
); then
  fail "missing dedicated token variable fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "missing dedicated token variable fails despite interactive fallbacks"
  else
    fail "missing dedicated token variable fails before MCP with exit 64"
  fi
fi

# The four non-secret binding IDs are an all-or-nothing contract. A missing ID
# is rejected before the marker-only bearer is read or MCP starts.
: > "$calls"
if STUB_CALLS_OUT="$calls" \
  MERISTEM_MCP_EXPECT_ACTOR_ID='' \
  CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" "$launcher" >/dev/null 2>&1; then
  fail "partial listener task binding fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "partial listener task binding fails before MCP"
  else
    fail "partial listener task binding fails before MCP with exit 64"
  fi
fi

# A private dedicated token is read, while path variables and ambient database
# configuration are removed or replaced before the MCP child starts.
: > "$calls"
expected="mcp|dummy-listener-token|unset|unset|postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable|$fixture/.meristem/generated/meristem-bin.v1-pin|$MERISTEM_MCP_EXPECT_ACTOR_ID|$MERISTEM_MCP_LISTENER_ACTIVATION_ID|$MERISTEM_MCP_LISTENER_WORK_ITEM_ID|$MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID"
if output="$(
  STUB_CALLS_OUT="$calls" \
  CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  MERISTEM_TOKEN_FILE="$fallback_file" \
  MERISTEM_TOKEN='dummy-ambient-token' \
  MERISTEM_DATABASE_URL='postgres://ambient.invalid/wrong' \
  MERISTEM_V1_PIN_FILE="$tmp/unreviewed.pin" \
    "$launcher"
)" && [[ "$output" == "$expected" && "$(cat "$calls")" == "mcp" ]]; then
  pass "mode-0600 dedicated token launches isolated local MCP"
else
  fail "mode-0600 dedicated token launches isolated local MCP"
fi

# A binary/pin mismatch must stop before MCP even when the token is otherwise
# valid. Restore the reviewed pin for the remaining cases.
: > "$calls"
printf '%s\n' 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' > \
  "$fixture/.meristem/generated/meristem-bin.v1-pin"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "mismatched reviewed pin fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "mismatched reviewed pin fails before MCP"
  else
    fail "mismatched reviewed pin fails before MCP with exit 64"
  fi
fi
printf '%s\n' "$reviewed_commit" > \
  "$fixture/.meristem/generated/meristem-bin.v1-pin"

# A locally modified load-bearing script cannot ride a correctly pinned Go
# binary. Restore the fixture copy without touching the source checkout.
cp "$fixture/scripts/codex-listener-app-server.sh" "$tmp/reviewed-wrapper"
printf '\n# local drift\n' >> "$fixture/scripts/codex-listener-app-server.sh"
: > "$calls"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "dirty listener runtime component fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "dirty listener runtime component fails before MCP"
  else
    fail "dirty listener runtime component fails before MCP with exit 64"
  fi
fi
cp "$tmp/reviewed-wrapper" "$fixture/scripts/codex-listener-app-server.sh"
chmod 700 "$fixture/scripts/codex-listener-app-server.sh"

# A replacement ref must not redirect the reviewed commit's tree lookup. Keep
# HEAD pinned to the original commit while the working wrapper matches the
# replacement commit; a replace-aware lookup would incorrectly bless it.
printf '\n# replacement-ref drift\n' >> "$fixture/scripts/codex-listener-app-server.sh"
git -C "$fixture" add scripts/codex-listener-app-server.sh
GIT_AUTHOR_NAME='Listener Test' GIT_AUTHOR_EMAIL='listener@example.invalid' \
GIT_COMMITTER_NAME='Listener Test' GIT_COMMITTER_EMAIL='listener@example.invalid' \
  git -C "$fixture" -c core.hooksPath=/dev/null commit -qm 'replacement tree'
replacement_commit="$(git -C "$fixture" rev-parse HEAD)"
git -C "$fixture" replace "$reviewed_commit" "$replacement_commit"
git -C "$fixture" update-ref HEAD "$reviewed_commit"
: > "$calls"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "Git replacement object fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "Git replacement object cannot bless dirty runtime"
  else
    fail "Git replacement object fails before MCP with exit 64"
  fi
fi
git -C "$fixture" replace -d "$reviewed_commit" >/dev/null
git -C "$fixture" read-tree "$reviewed_commit"
cp "$tmp/reviewed-wrapper" "$fixture/scripts/codex-listener-app-server.sh"
chmod 700 "$fixture/scripts/codex-listener-app-server.sh"

# Exercise the GNU-stat fallback explicitly: a failed BSD-form probe may emit
# stdout, but that output must not contaminate the successful fallback mode.
fake_stat_dir="$tmp/fake-stat-bin"
mkdir -p "$fake_stat_dir"
cat > "$fake_stat_dir/stat" <<'STAT'
#!/usr/bin/env bash
if [[ "$1" == "-f" ]]; then
  printf 'gnu-filesystem-output\n'
  exit 1
fi
if [[ "$1" == "-c" ]]; then
  if [[ "$3" == "$CODEX_HOME" ]]; then
    printf '700\n'
  else
    printf '600\n'
  fi
  exit 0
fi
exit 2
STAT
chmod 700 "$fake_stat_dir/stat"
: > "$calls"
if output="$(
  PATH="$fake_stat_dir:$PATH" \
  STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
    "$launcher"
)" && [[ "$output" == "$expected" && "$(cat "$calls")" == "mcp" ]]; then
  pass "GNU-stat fallback launches with uncontaminated mode"
else
  fail "GNU-stat fallback launches with uncontaminated mode"
fi

# A readable token with group/other permissions is still rejected.
chmod 644 "$dedicated_file"
: > "$calls"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "loose dedicated token mode fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "loose dedicated token mode fails before MCP"
  else
    fail "loose dedicated token mode fails before MCP with exit 64"
  fi
fi

# An oversized private file is not accepted as a bearer container.
chmod 600 "$dedicated_file"
dd if=/dev/zero of="$dedicated_file" bs=4097 count=1 2>/dev/null
: > "$calls"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "oversized dedicated token fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "oversized dedicated token fails before MCP"
  else
    fail "oversized dedicated token fails before MCP with exit 64"
  fi
fi
printf 'dummy-listener-token\n' > "$dedicated_file"
chmod 600 "$dedicated_file"

# The inner launcher independently rejects a swapped symlink even if the outer
# wrapper's earlier check was bypassed by a restart/race.
mv "$dedicated_file" "$tmp/real-task-token"
ln -s "$tmp/real-task-token" "$dedicated_file"
: > "$calls"
if STUB_CALLS_OUT="$calls" CODEX_MERISTEM_TOKEN_FILE="$dedicated_file" \
  "$launcher" >/dev/null 2>&1; then
  fail "symlinked dedicated token fails closed"
else
  status=$?
  if [[ "$status" -eq 64 && ! -s "$calls" ]]; then
    pass "symlinked dedicated token fails before MCP"
  else
    fail "symlinked dedicated token fails before MCP with exit 64"
  fi
fi
rm "$dedicated_file"
mv "$tmp/real-task-token" "$dedicated_file"

# The app-server wrapper's default is the tracked sibling launcher, not an
# independently generated compatibility command.
fake_codex="$tmp/fake-codex"
cat > "$fake_codex" <<'CODEX'
#!/usr/bin/env bash
printf 'called\n' >> "${CODEX_CALLS_OUT:?}"
printf '%s\n' "$@"
CODEX
chmod 700 "$fake_codex"
codex_calls="$tmp/codex-calls"
expected_command="mcp_servers.meristem_listener={command=\"$launcher\",enabled_tools=[\"work_items.append_event\",\"work_items.get\",\"work_items.get_assignment\"],env={CODEX_MERISTEM_TOKEN_FILE=\"$dedicated_file\",MERISTEM_MCP_EXPECT_ACTOR_ID=\"$MERISTEM_MCP_EXPECT_ACTOR_ID\",MERISTEM_MCP_LISTENER_ACTIVATION_ID=\"$MERISTEM_MCP_LISTENER_ACTIVATION_ID\",MERISTEM_MCP_LISTENER_WORK_ITEM_ID=\"$MERISTEM_MCP_LISTENER_WORK_ITEM_ID\",MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID=\"$MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID\"}}"
if wrapper_output="$(
  CODEX_CALLS_OUT="$codex_calls" \
  CODEX_BIN="$fake_codex" \
  MERISTEM_LISTENER_MCP_COMMAND="$tmp/stale-command" \
    "$wrapper" app-server --stdio
)" && grep -Fqx -- "$expected_command" <<< "$wrapper_output" &&
  [[ "$(cat "$codex_calls")" == "called" ]]; then
  pass "app-server uses the tracked sibling and ignores stale overrides"
else
  fail "app-server uses the tracked sibling and ignores stale overrides"
fi

# The read-only updated-app probe starts the same isolated app-server with both
# MCP entries inert. It requires no Meristem bearer or activation binding.
mv "$dedicated_file" "$tmp/task-token-away"
: > "$codex_calls"
expected_probe_command='mcp_servers.meristem_listener={command="/usr/bin/false",enabled=false}'
if probe_output="$(
  unset MERISTEM_MCP_EXPECT_ACTOR_ID MERISTEM_MCP_LISTENER_ACTIVATION_ID
  unset MERISTEM_MCP_LISTENER_WORK_ITEM_ID MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID
  CODEX_CALLS_OUT="$codex_calls" CODEX_BIN="$fake_codex" MERISTEM_LISTENER_PROBE=1 \
    "$wrapper" app-server --stdio
)" && grep -Fqx -- "$expected_probe_command" <<< "$probe_output" &&
  [[ "$(cat "$codex_calls")" == "called" ]]; then
  pass "read-only probe uses inert MCP configuration without a bearer"
else
  fail "read-only probe uses inert MCP configuration without a bearer"
fi
mv "$tmp/task-token-away" "$dedicated_file"

# With no explicit CODEX_BIN, command lookup must find the installed binary on
# PATH and preserve the same exact app-server argv.
path_bin="$tmp/codex-path-bin"
mkdir -p "$path_bin"
cp "$fake_codex" "$path_bin/codex"
chmod 700 "$path_bin/codex"
: > "$codex_calls"
if path_output="$(
  unset CODEX_BIN
  PATH="$path_bin:/usr/bin:/bin" \
  CODEX_CALLS_OUT="$codex_calls" \
    "$wrapper" app-server --stdio
)" && grep -Fqx -- "$expected_command" <<< "$path_output" &&
  [[ "$(cat "$codex_calls")" == "called" ]]; then
  pass "app-server resolves Codex from the deployment PATH"
else
  fail "app-server resolves Codex from the deployment PATH"
fi

# No other Codex mode or argument vector may inherit this credentialed MCP
# configuration outside the decline-only app-server adapter.
for invalid_argv in 'app-server' 'exec' 'app-server --stdio extra'; do
  : > "$codex_calls"
  read -r -a argv <<< "$invalid_argv"
  if CODEX_CALLS_OUT="$codex_calls" \
    CODEX_BIN="$fake_codex" \
      "$wrapper" "${argv[@]}" >/dev/null 2>&1; then
    fail "wrapper rejects argv: $invalid_argv"
  else
    status=$?
    if [[ "$status" -eq 64 && ! -s "$codex_calls" ]]; then
      pass "wrapper rejects argv: $invalid_argv"
    else
      fail "wrapper rejects argv before Codex: $invalid_argv"
    fi
  fi
done

if [[ "$failures" -ne 0 ]]; then
  printf '%s listener MCP regression test(s) failed\n' "$failures" >&2
  exit 1
fi

printf 'all listener MCP credential regressions passed\n'
