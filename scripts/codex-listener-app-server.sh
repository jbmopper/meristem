#!/usr/bin/env bash
# Launch a dedicated Codex listener app-server with Meristem's local stdio
# compatibility transport. Interactive Codex remains configured for HTTP.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CODEX_BIN="${CODEX_BIN:-$(command -v codex || true)}"
MERISTEM_COMMAND="${MERISTEM_LISTENER_MCP_COMMAND:-$REPO_ROOT/.meristem/generated/codex-meristem-command.sh}"
TOKEN_FILE="${CODEX_MERISTEM_TOKEN_FILE:-}"

[[ -n "$CODEX_BIN" && -x "$CODEX_BIN" ]] || {
  echo "missing Codex binary" >&2
  exit 64
}
[[ "$MERISTEM_COMMAND" == /* && -x "$MERISTEM_COMMAND" ]] || {
  echo "missing absolute executable Meristem listener MCP command" >&2
  exit 64
}
[[ "$TOKEN_FILE" == /* && -s "$TOKEN_FILE" ]] || {
  echo "missing absolute non-empty Codex listener token file" >&2
  exit 64
}
case "$MERISTEM_COMMAND:$TOKEN_FILE" in
  *\"*|*\\*|*$'\n'*|*$'\r'*)
    echo "unsupported character in listener MCP path" >&2
    exit 64
    ;;
esac
if ! TOKEN_MODE="$(stat -f '%Lp' "$TOKEN_FILE" 2>/dev/null)"; then
  TOKEN_MODE="$(stat -c '%a' "$TOKEN_FILE" 2>/dev/null || true)"
fi
[[ "$TOKEN_MODE" == "600" ]] || {
  echo "Codex listener token file must have mode 0600 (got ${TOKEN_MODE:-unknown})" >&2
  exit 64
}

exec "$CODEX_BIN" \
  --config 'mcp_servers.meristem.enabled=false' \
  --config "mcp_servers.meristem_listener={command=\"$MERISTEM_COMMAND\",env={CODEX_MERISTEM_TOKEN_FILE=\"$TOKEN_FILE\"}}" \
  "$@"
