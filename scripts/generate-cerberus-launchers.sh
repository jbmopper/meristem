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
MERISTEM_BIN="${MERISTEM_BIN:-$REPO_ROOT/.meristem/generated/meristem-bin}"
if [[ ! -x "$MERISTEM_BIN" ]]; then
  MERISTEM_BIN="${GO_BIN:-go} run ./cmd/meristem"
fi

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
exec $MERISTEM_BIN mcp
EOF
  chmod 700 "$script"
  printf '%s\n' "$script"
}

write_launcher coordinator "aegis-cerberus-coordinator-$ROOT_SHORT"
write_launcher grower "cerberus-grower-$ROOT_SHORT"
write_launcher healer "cerberus-healer-$ROOT_SHORT"
