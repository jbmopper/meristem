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
PRIMARY_REPO="$REPO_ROOT"
DEFAULT_WORKTREE_BASE="$(cd "$PRIMARY_REPO/.." && pwd)"
WORKSPACE_ROOT="${MERISTEM_AGENT_WORKTREE:-${MERISTEM_CURSOR_WORKTREE:-$DEFAULT_WORKTREE_BASE/meristem-cursor-mcp}}"
TOKEN_FILE="${MERISTEM_TOKEN_FILE:-$PRIMARY_REPO/.meristem/cursor-mcp.token}"
# One shared build artifact backs the API server AND every agent wrapper, so a
# stale worktree can no longer run divergent projector code against the shared
# database (work item a9374bdd). Rebuild it from a clean v1 checkout with
# scripts/rebuild-meristem-bin.sh.
MERISTEM_BIN="${MERISTEM_BIN:-$PRIMARY_REPO/.meristem/generated/meristem-bin}"
export MERISTEM_V1_PIN_FILE="${MERISTEM_V1_PIN_FILE:-$MERISTEM_BIN.v1-pin}"

[[ "$MERISTEM_BIN" == /* && "$MERISTEM_V1_PIN_FILE" == /* ]] || {
  echo "cursor-mcp-command: shared binary and reviewed pin paths must be absolute" >&2
  exit 64
}

export MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
export MERISTEM_MCP_TOOL_NAMES="${MERISTEM_MCP_TOOL_NAMES:-cursor}"

# Validate the public build metadata before reading the bearer-token file. The
# running binary repeats this check dynamically, but this preflight prevents a
# known-stale wrapper from touching secret material at all.
if ! "$PRIMARY_REPO/scripts/check-meristem-build-pin.sh" "$MERISTEM_BIN" "$MERISTEM_V1_PIN_FILE"; then
  echo "cursor-mcp-command: shared meristem build does not match the reviewed-v1 pin; refusing to read credentials" >&2
  exit 64
fi

[[ -e "$WORKSPACE_ROOT/.git" ]] || {
  echo "cursor-mcp-command: missing meristem worktree $WORKSPACE_ROOT" >&2
  echo "Create it with: $PRIMARY_REPO/scripts/prepare-agent-worktree.sh --target cursor-mcp" >&2
  exit 64
}
[[ -s "$TOKEN_FILE" ]] || {
  echo "cursor-mcp-command: missing token file $TOKEN_FILE" >&2
  echo "Mint one with the root-token provisioning flow." >&2
  exit 1
}

export MERISTEM_TOKEN
MERISTEM_TOKEN="$(cat "$TOKEN_FILE")"

cd "$WORKSPACE_ROOT"
exec "$MERISTEM_BIN" mcp
