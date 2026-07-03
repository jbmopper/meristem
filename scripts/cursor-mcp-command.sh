#!/usr/bin/env bash
# Cursor MCP launch wrapper for meristem.
#
# Cursor spawns user-level MCP servers with the active workspace as cwd.
# Do not use relative paths like `go run ./cmd/meristem mcp` in mcp.json;
# they resolve against whatever project window triggered the spawn.
#
# Point ~/.cursor/mcp.json at this script:
#   "meristem": {
#     "command": "/Users/juliusmopper/Dev/meristem/scripts/cursor-mcp-command.sh"
#   }

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOKEN_FILE="${MERISTEM_TOKEN_FILE:-$REPO_ROOT/.meristem/cursor-mcp.token}"
GO_BIN="${GO_BIN:-$(command -v go || true)}"

export MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
export MERISTEM_MCP_TOOL_NAMES="${MERISTEM_MCP_TOOL_NAMES:-cursor}"

[[ -n "$GO_BIN" ]] || {
  echo "cursor-mcp-command: go not found on PATH; set GO_BIN=/absolute/path/to/go" >&2
  exit 1
}
[[ -s "$TOKEN_FILE" ]] || {
  echo "cursor-mcp-command: missing token file $TOKEN_FILE" >&2
  echo "Mint one with: MERISTEM_TOKEN=\$(cat .meristem/root.token) go run ./cmd/meristem tokens create --name cursor-mcp --source agent" >&2
  exit 1
}

export MERISTEM_TOKEN
MERISTEM_TOKEN="$(cat "$TOKEN_FILE")"

cd "$REPO_ROOT"
exec "$GO_BIN" run ./cmd/meristem mcp
