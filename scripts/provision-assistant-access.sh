#!/usr/bin/env bash
# Provision per-assistant meristem access with secret-safe local MCP config.
#
# This script automates what is safe to automate today:
#
# - Mint one source=agent token per local assistant target.
# - Store each bearer secret in .meristem/<target>.token with mode 0600.
# - Generate MCP config snippets that read token files at runtime instead of
#   embedding bearer secrets in shared JSON config.
# - Optionally run `claude mcp add` when the Claude Code CLI is installed.
#
# It deliberately does NOT create remote Claude.ai/Cowork/ChatGPT connectors.
# Those need a public HTTPS remote MCP endpoint and a separate auth design; a
# localhost stdio process is not reachable from those cloud products.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MERISTEM_BIN="${MERISTEM_BIN:-go run ./cmd/meristem}"
GO_BIN="${GO_BIN:-$(command -v go || true)}"
MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
DEFAULT_WORKTREE_BASE="$(cd "$REPO_ROOT/.." && pwd)"
AGENT_WORKTREE_BASE="${MERISTEM_AGENT_WORKTREE_BASE:-$DEFAULT_WORKTREE_BASE}"
TOKEN_DIR="${MERISTEM_TOKEN_DIR:-.meristem}"
ROOT_TOKEN_FILE="${ROOT_TOKEN_FILE:-$TOKEN_DIR/root.token}"
GENERATED_DIR="$TOKEN_DIR/generated"
case "$GENERATED_DIR" in
  /*) GENERATED_DIR_ABS="$GENERATED_DIR" ;;
  *)  GENERATED_DIR_ABS="$REPO_ROOT/$GENERATED_DIR" ;;
esac

DEFAULT_TARGETS=(
  codex
  codex-cli
  claude-code
  claude-code-cli
  claude-code-gui
  claude-desktop
)

targets=("${DEFAULT_TARGETS[@]}")
apply_claude_code=false
print_remote=false

usage() {
  cat <<'USAGE'
usage:
  scripts/provision-assistant-access.sh [options]

options:
  --targets a,b,c       Provision only these target names.
  --apply-claude-code   Run `claude mcp add ...` if the Claude CLI is installed.
  --print-remote        Print remote-only targets that are intentionally not minted.
  -h, --help            Show this help.

defaults:
  codex,codex-cli,claude-code,claude-code-cli,
  claude-code-gui,claude-desktop

security:
  Secrets are written only to .meristem/*.token with mode 0600.
  Generated JSON/config snippets read token files at runtime and do not
  contain bearer tokens.

worktrees:
  Generated local MCP wrappers cd into per-agent worktrees. Prepare them with
  scripts/prepare-agent-worktree.sh --target codex
  scripts/prepare-agent-worktree.sh --target claude-code-gui
USAGE
}

while (($#)); do
  case "$1" in
    --targets)
      IFS=',' read -r -a targets <<< "${2:?--targets requires a comma-separated value}"
      shift 2
      ;;
    --targets=*)
      IFS=',' read -r -a targets <<< "${1#--targets=}"
      shift
      ;;
    --apply-claude-code)
      apply_claude_code=true
      shift
      ;;
    --print-remote)
      print_remote=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

export MERISTEM_DATABASE_URL

log()  { printf '==> %s\n' "$*"; }
warn() { printf '!! %s\n' "$*" >&2; }
die()  { printf '!! %s\n' "$*" >&2; exit 1; }

sanitize_target() {
  local target="$1"
  case "$target" in
    cursor* )
      die "unsupported target $target: Cursor-specific assistant bootstrapping is no longer supported"
      ;;
  esac
  case "$target" in
    ''|*[^A-Za-z0-9_.-]*)
      die "invalid target name $target; use only letters, numbers, dot, underscore, and dash"
      ;;
  esac
}

token_file_for() {
  printf '%s/%s.token' "$TOKEN_DIR" "$1"
}

worktree_for() {
  printf '%s/meristem-%s' "$AGENT_WORKTREE_BASE" "$1"
}

active_token_exists() {
  local name="$1"
  $MERISTEM_BIN tokens list 2>/dev/null |
    awk -F'\t' -v name="$name" '$2 == name && $5 == "active" { found = 1 } END { exit(found ? 0 : 1) }'
}

write_secret_from_capture() {
  local capture_file="$1"
  local dest_file="$2"
  local secret
  secret="$(awk -F= '$1 == "secret" { print $2 }' "$capture_file")"
  if [[ -z "$secret" ]]; then
    warn "could not parse token secret from command output:"
    sed 's/secret=.*/secret=<redacted>/' "$capture_file" >&2
    die "aborting so no half-provisioned secret is lost"
  fi
  umask 077
  printf '%s\n' "$secret" > "$dest_file"
  chmod 600 "$dest_file"
}

mint_target() {
  local target="$1"
  sanitize_target "$target"

  local file
  file="$(token_file_for "$target")"
  if [[ -s "$file" ]]; then
    chmod 600 "$file"
    log "$target: token file exists at $file"
    return
  fi

  if active_token_exists "$target"; then
    warn "$target: active token exists, but $file is missing or empty."
    warn "$target: cannot recover bearer secret from meristem; revoke/rotate manually if you need this target."
    return
  fi

  log "$target: minting source=agent token"
  local tmp
  tmp="$(mktemp)"
  MERISTEM_TOKEN="$(cat "$ROOT_TOKEN_FILE")" \
    $MERISTEM_BIN tokens create --name "$target" --source agent > "$tmp"
  write_secret_from_capture "$tmp" "$file"
  rm -f "$tmp"
  log "$target: wrote $file"
}

write_generated_configs() {
  mkdir -p "$GENERATED_DIR"
  chmod 700 "$TOKEN_DIR"
  chmod 700 "$GENERATED_DIR"

  local claude_code_workspace codex_workspace
  claude_code_workspace="$(worktree_for claude-code-gui)"
  codex_workspace="$(worktree_for codex)"

  cat > "$GENERATED_DIR/claude-code-meristem-command.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$claude_code_workspace"
if [[ ! -e "\$workspace_root/.git" ]]; then
  echo "missing Claude Code meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target claude-code-gui" >&2
  exit 64
fi
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
export MERISTEM_TOKEN="\$(cat "\$primary_repo/.meristem/claude-code-gui.token")"
exec "$GO_BIN" run ./cmd/meristem mcp
EOF
  chmod 700 "$GENERATED_DIR/claude-code-meristem-command.sh"

  cat > "$GENERATED_DIR/codex-meristem-command.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$codex_workspace"
if [[ ! -e "\$workspace_root/.git" ]]; then
  echo "missing Codex meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target codex" >&2
  exit 64
fi
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
export MERISTEM_TOKEN="\$(cat "\$primary_repo/.meristem/codex.token")"
exec "$GO_BIN" run ./cmd/meristem mcp
EOF
  chmod 700 "$GENERATED_DIR/codex-meristem-command.sh"

  cat > "$GENERATED_DIR/claude-code-mcp-add.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
claude mcp add meristem --scope user -- "$GENERATED_DIR_ABS/claude-code-meristem-command.sh"
EOF
  chmod 700 "$GENERATED_DIR/claude-code-mcp-add.sh"

  log "generated secret-free MCP snippets in $GENERATED_DIR"
}

apply_claude_code_config() {
  local file
  file="$(token_file_for claude-code-gui)"
  [[ -s "$file" ]] || die "cannot apply Claude Code config: $file is missing"
  if ! command -v claude >/dev/null 2>&1; then
    warn "claude CLI not found on PATH; run this later:"
    warn "  $GENERATED_DIR/claude-code-mcp-add.sh"
    return
  fi
  "$GENERATED_DIR/claude-code-mcp-add.sh"
  log "registered meristem MCP with Claude Code user scope"
}

[[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required to mint assistant tokens"
[[ -n "$GO_BIN" ]] || die "could not find go on PATH; set GO_BIN=/absolute/path/to/go"

mkdir -p "$TOKEN_DIR"
chmod 700 "$TOKEN_DIR"

log "provisioning assistant tokens"
for target in "${targets[@]}"; do
  [[ -n "$target" ]] || continue
  mint_target "$target"
done

write_generated_configs

if $apply_claude_code; then
  apply_claude_code_config
fi

if $print_remote; then
  cat <<'EOF'

Remote-only targets not provisioned by this script:
  chatgpt-remote
  claude-web
  cowork
  claude-mobile

These need a public HTTPS remote MCP endpoint and explicit remote auth.
Do not point them at 127.0.0.1 or a local stdio MCP command.
EOF
fi

cat <<EOF

done.

Useful files:
  $GENERATED_DIR/codex-meristem-command.sh
  $GENERATED_DIR/claude-code-meristem-command.sh
  $GENERATED_DIR/claude-code-mcp-add.sh

Prepare required worktrees:
  scripts/prepare-agent-worktree.sh --target codex
  scripts/prepare-agent-worktree.sh --target claude-code-gui

Verify Claude Code after applying:
  /mcp

Verify HTTP with a target token:
  curl -fsS http://127.0.0.1:8080/v1/feed?limit=5 \\
    -H "Authorization: Bearer \$(cat .meristem/claude-code-gui.token)"
EOF
