#!/usr/bin/env bash
# Launch a dedicated Codex listener app-server with Meristem's local stdio
# compatibility transport. Interactive Codex remains configured for HTTP.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CODEX_BIN="${CODEX_BIN:-$(command -v codex || true)}"
MERISTEM_COMMAND="${MERISTEM_LISTENER_MCP_COMMAND:-$REPO_ROOT/.meristem/generated/codex-meristem-command.sh}"

[[ -n "$CODEX_BIN" && -x "$CODEX_BIN" ]] || {
  echo "missing Codex binary" >&2
  exit 64
}
[[ "$MERISTEM_COMMAND" == /* && -x "$MERISTEM_COMMAND" ]] || {
  echo "missing absolute executable Meristem listener MCP command" >&2
  exit 64
}
case "$MERISTEM_COMMAND" in
  *[\"\\$'\n'$'\r']*)
    echo "unsupported character in Meristem listener MCP command path" >&2
    exit 64
    ;;
esac

exec "$CODEX_BIN" \
  --config 'mcp_servers.meristem.enabled=false' \
  --config "mcp_servers.meristem_listener={command=\"$MERISTEM_COMMAND\"}" \
  "$@"
