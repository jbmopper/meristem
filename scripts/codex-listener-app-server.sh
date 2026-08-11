#!/usr/bin/env bash
# Launch a dedicated Codex listener app-server with Meristem's local stdio
# compatibility transport. Interactive Codex remains configured for HTTP.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CODEX_BIN="${CODEX_BIN:-$(command -v codex || true)}"
MERISTEM_COMMAND="$SCRIPT_DIR/codex-listener-mcp-command.sh"
LISTENER_CODEX_HOME="${MERISTEM_LISTENER_CODEX_HOME:-}"
LISTENER_CODEX_SQLITE_HOME="${MERISTEM_LISTENER_CODEX_SQLITE_HOME:-}"
HOME_DIR="${HOME:-}"
EXPECTED_ACTOR_ID="${MERISTEM_MCP_EXPECT_ACTOR_ID:-}"
ACTIVATION_ID="${MERISTEM_MCP_LISTENER_ACTIVATION_ID:-}"
WORK_ITEM_ID="${MERISTEM_MCP_LISTENER_WORK_ITEM_ID:-}"
ASSIGNMENT_EVENT_ID="${MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID:-}"
PROBE_MODE="${MERISTEM_LISTENER_PROBE:-}"

[[ "$#" -eq 2 && "$1" == "app-server" && "$2" == "--stdio" ]] || {
  echo "Codex listener wrapper only permits: app-server --stdio" >&2
  exit 64
}
[[ -n "$CODEX_BIN" && -x "$CODEX_BIN" ]] || {
  echo "missing Codex binary" >&2
  exit 64
}
[[ "$MERISTEM_COMMAND" == /* && -x "$MERISTEM_COMMAND" ]] || {
  echo "missing absolute executable Meristem listener MCP command" >&2
  exit 64
}
[[ "$HOME_DIR" == /* && -d "$HOME_DIR" ]] || {
  echo "missing absolute HOME for Codex listener isolation" >&2
  exit 64
}
PRIMARY_CODEX_HOME="$HOME_DIR/.codex"
[[ "$LISTENER_CODEX_HOME" == /* && -d "$LISTENER_CODEX_HOME" && ! -L "$LISTENER_CODEX_HOME" ]] || {
  echo "missing absolute dedicated listener CODEX_HOME" >&2
  exit 64
}
LISTENER_CODEX_HOME_PHYSICAL="$(cd "$LISTENER_CODEX_HOME" && pwd -P)"
[[ "$LISTENER_CODEX_HOME_PHYSICAL" == "$LISTENER_CODEX_HOME" ]] || {
  echo "dedicated listener CODEX_HOME must be an exact symlink-free path" >&2
  exit 64
}
if ! LISTENER_HOME_MODE="$(stat -f '%Lp' "$LISTENER_CODEX_HOME" 2>/dev/null)"; then
  LISTENER_HOME_MODE="$(stat -c '%a' "$LISTENER_CODEX_HOME" 2>/dev/null || true)"
fi
[[ "$LISTENER_HOME_MODE" == "700" ]] || {
  echo "dedicated listener CODEX_HOME must have mode 0700 (got ${LISTENER_HOME_MODE:-unknown})" >&2
  exit 64
}
[[ "$LISTENER_CODEX_SQLITE_HOME" == "$PRIMARY_CODEX_HOME" && -d "$LISTENER_CODEX_SQLITE_HOME" ]] || {
  echo "listener CODEX_SQLITE_HOME must be the primary Codex state directory" >&2
  exit 64
}
[[ "$LISTENER_CODEX_HOME" != "$PRIMARY_CODEX_HOME" && ! -e "$LISTENER_CODEX_HOME/config.toml" && ! -L "$LISTENER_CODEX_HOME/config.toml" ]] || {
  echo "listener CODEX_HOME must be isolated and contain no config.toml" >&2
  exit 64
}
[[ -z "$PROBE_MODE" || "$PROBE_MODE" == "1" ]] || {
  echo "invalid listener probe mode" >&2
  exit 64
}
TOKEN_FILE="$LISTENER_CODEX_HOME/meristem-task.token"
if [[ "$PROBE_MODE" != "1" ]]; then
  [[ -f "$TOKEN_FILE" && ! -L "$TOKEN_FILE" && -r "$TOKEN_FILE" && -s "$TOKEN_FILE" ]] || {
    echo "missing fixed non-empty listener task token file" >&2
    exit 64
  }
  for value in "$EXPECTED_ACTOR_ID" "$ACTIVATION_ID" "$WORK_ITEM_ID" "$ASSIGNMENT_EVENT_ID"; do
    [[ "$value" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ && "$value" != "00000000-0000-0000-0000-000000000000" ]] || {
      echo "missing or malformed listener task binding id" >&2
      exit 64
    }
  done
fi
[[ -L "$LISTENER_CODEX_HOME/auth.json" && "$(readlink "$LISTENER_CODEX_HOME/auth.json")" == "$PRIMARY_CODEX_HOME/auth.json" && -r "$LISTENER_CODEX_HOME/auth.json" ]] || {
  echo "listener CODEX_HOME must link the primary auth.json exactly" >&2
  exit 64
}
[[ -L "$LISTENER_CODEX_HOME/thread-writer-locks" && "$(readlink "$LISTENER_CODEX_HOME/thread-writer-locks")" == "$PRIMARY_CODEX_HOME/thread-writer-locks" && -d "$LISTENER_CODEX_HOME/thread-writer-locks" ]] || {
  echo "listener CODEX_HOME must link primary thread-writer-locks exactly" >&2
  exit 64
}
case "$MERISTEM_COMMAND:$TOKEN_FILE:$LISTENER_CODEX_HOME:$LISTENER_CODEX_SQLITE_HOME:$EXPECTED_ACTOR_ID:$ACTIVATION_ID:$WORK_ITEM_ID:$ASSIGNMENT_EVENT_ID" in
  *\"*|*\\*|*$'\n'*|*$'\r'*)
    echo "unsupported character in listener MCP path" >&2
    exit 64
    ;;
esac
if [[ "$PROBE_MODE" != "1" ]]; then
  if ! TOKEN_MODE="$(stat -f '%Lp' "$TOKEN_FILE" 2>/dev/null)"; then
    TOKEN_MODE="$(stat -c '%a' "$TOKEN_FILE" 2>/dev/null || true)"
  fi
  [[ "$TOKEN_MODE" == "600" ]] || {
    echo "Codex listener token file must have mode 0600 (got ${TOKEN_MODE:-unknown})" >&2
    exit 64
  }
  if ! TOKEN_SIZE="$(stat -f '%z' "$TOKEN_FILE" 2>/dev/null)"; then
    TOKEN_SIZE="$(stat -c '%s' "$TOKEN_FILE" 2>/dev/null || true)"
  fi
  [[ "$TOKEN_SIZE" =~ ^[0-9]+$ && "$TOKEN_SIZE" -ge 1 && "$TOKEN_SIZE" -le 4096 ]] || {
    echo "Codex listener task token file has an invalid size" >&2
    exit 64
  }
fi

unset CODEX_MERISTEM_TOKEN_FILE MERISTEM_LISTENER_MCP_COMMAND MERISTEM_TOKEN MERISTEM_TOKEN_FILE MERISTEM_DATABASE_URL
unset MERISTEM_MCP_EXPECT_ACTOR_ID MERISTEM_MCP_LISTENER_ACTIVATION_ID
unset MERISTEM_MCP_LISTENER_WORK_ITEM_ID MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID
unset MERISTEM_LISTENER_CODEX_HOME MERISTEM_LISTENER_CODEX_SQLITE_HOME
unset MERISTEM_LISTENER_PROBE
unset TOKEN_MODE TOKEN_SIZE LISTENER_CODEX_HOME_PHYSICAL
export CODEX_HOME="$LISTENER_CODEX_HOME"
export CODEX_SQLITE_HOME="$LISTENER_CODEX_SQLITE_HOME"
if [[ "$PROBE_MODE" == "1" ]]; then
  exec "$CODEX_BIN" \
    --config 'features.apps=false' \
    --config 'mcp_servers.meristem_listener={command="/usr/bin/false",enabled=false}' \
    app-server --stdio
fi
exec "$CODEX_BIN" \
  --config 'features.apps=false' \
  --config "mcp_servers.meristem_listener={command=\"$MERISTEM_COMMAND\",enabled_tools=[\"work_items.append_event\",\"work_items.get\",\"work_items.get_assignment\"],env={CODEX_MERISTEM_TOKEN_FILE=\"$TOKEN_FILE\",MERISTEM_MCP_EXPECT_ACTOR_ID=\"$EXPECTED_ACTOR_ID\",MERISTEM_MCP_LISTENER_ACTIVATION_ID=\"$ACTIVATION_ID\",MERISTEM_MCP_LISTENER_WORK_ITEM_ID=\"$WORK_ITEM_ID\",MERISTEM_MCP_LISTENER_ASSIGNMENT_EVENT_ID=\"$ASSIGNMENT_EVENT_ID\"}}" \
  app-server --stdio
