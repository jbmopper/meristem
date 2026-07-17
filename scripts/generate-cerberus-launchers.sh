#!/usr/bin/env bash
# Generate secret-free Cerberus MCP launcher scripts.
#
# The generated scripts read their own token file at runtime and never fall
# back to a generic Codex or legacy Aegis token.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
DEFAULT_WORKTREE_BASE="$(cd "$REPO_ROOT/.." && pwd)"
AGENT_WORKTREE_BASE="${MERISTEM_AGENT_WORKTREE_BASE:-$DEFAULT_WORKTREE_BASE}"
# One shared build artifact backs the API server AND every agent wrapper, so a
# stale worktree can no longer run divergent projector code against the shared
# database (work item a9374bdd). The generated launcher execs this artifact and
# fails closed if it is missing rather than falling back to a per-worktree
# `go run`. Rebuild it from a clean v1 checkout with
# scripts/rebuild-meristem-bin.sh.
MERISTEM_BIN="${MERISTEM_BIN:-$REPO_ROOT/.meristem/generated/meristem-bin}"
[[ "$MERISTEM_BIN" == /* ]] || {
  echo "generate-cerberus-launchers: MERISTEM_BIN must be an absolute path" >&2
  exit 64
}

ROOT_ID="${CERBERUS_ROOT_ID:-98853a93-2de4-42fb-9438-a1a54caf9589}"
ROOT_SHORT="${CERBERUS_ROOT_SHORT:-98853a93}"
TOKEN_DIR="${MERISTEM_TOKEN_DIR:-.meristem}"
GENERATED_DIR="${CERBERUS_GENERATED_DIR:-.meristem/generated/cerberus-$ROOT_SHORT}"

mkdir -p "$GENERATED_DIR"
chmod 700 "$TOKEN_DIR"
chmod 700 "$GENERATED_DIR"

write_launcher() {
  local head="$1"
  local token_name="$2"
  local script="$GENERATED_DIR/$head-meristem-command.sh"
  local token_file="$TOKEN_DIR/$token_name.token"
  local workspace_root="$AGENT_WORKTREE_BASE/meristem-cerberus-$head-$ROOT_SHORT"

  cat > "$script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$workspace_root"
token_file="\$primary_repo/$token_file"
# Exec the single shared build artifact that also backs the API server, so this
# wrapper stays on one code version (work item a9374bdd). Rebuild it from a clean
# v1 checkout with \$primary_repo/scripts/rebuild-meristem-bin.sh.
meristem_bin="$MERISTEM_BIN"
export MERISTEM_V1_PIN_FILE="\${MERISTEM_V1_PIN_FILE:-\$meristem_bin.v1-pin}"
[[ "\$MERISTEM_V1_PIN_FILE" == /* ]] || {
  echo "reviewed-v1 pin path must be absolute" >&2
  exit 64
}
if ! "\$primary_repo/scripts/check-meristem-build-pin.sh" "\$meristem_bin" "\$MERISTEM_V1_PIN_FILE"; then
  echo "shared meristem build does not match the reviewed-v1 pin; refusing to read credentials" >&2
  exit 64
fi
if [[ ! -e "\$workspace_root/.git" ]]; then
  echo "missing Cerberus $head worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target cerberus-$head-$ROOT_SHORT" >&2
  exit 64
fi
if [[ ! -s "\$token_file" ]]; then
  echo "missing or empty Cerberus token file for $head: \$token_file" >&2
  exit 64
fi
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
export CERBERUS_ROOT_ID="$ROOT_ID"
export CERBERUS_HEAD="$head"
export MERISTEM_TOKEN="\$(tr -d '\\n' < "\$token_file")"
exec "\$meristem_bin" mcp
EOF
  chmod 700 "$script"
  printf '%s\n' "$script"
}

write_launcher coordinator "aegis-cerberus-coordinator-$ROOT_SHORT"
write_launcher grower "cerberus-grower-$ROOT_SHORT"
write_launcher healer "cerberus-healer-$ROOT_SHORT"
