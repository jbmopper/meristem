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
session_target=""
session_requested=false
# Default-deny session credential scopes: what a working agent session needs,
# nothing more. Empty scopes would select the broad legacy MCP surface
# (access.legacyUnscoped), so --session refuses an empty override (3818efed).
session_scopes="feed.read,work_items.read_all,work_items.write_all,work_items.create"

usage() {
  cat <<'USAGE'
usage:
  scripts/provision-assistant-access.sh [options]

options:
  --targets a,b,c       Provision only these target names.
  --session NAME        Mint one per-session credential (source=agent) named
                        NAME, write .meristem/NAME.token, and print the
                        MERISTEM_TOKEN_FILE export for this session. Touches
                        no shared wrapper config. NAME must be unique: an
                        existing token file or active token of that name is
                        refused, because two live sessions must never share
                        one actor token (3818efed). Name the credential under
                        the agent persona's lineage (claude-fork, codex-0716)
                        so principal rollup stays a naming query. Wrappers
                        sample MERISTEM_TOKEN_FILE once at MCP process start;
                        restart the MCP process to adopt a new credential
                        (live reauthentication is 7313e2ab).
  --session-scopes CSV  Scope list for --session credentials. Default:
                        feed.read,work_items.read_all,work_items.write_all,
                        work_items.create. Must be non-empty: empty scopes
                        would grant the broad legacy surface.
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
    --session)
      session_target="${2:?--session requires a name}"
      session_requested=true
      shift 2
      ;;
    --session=*)
      session_target="${1#--session=}"
      session_requested=true
      shift
      ;;
    --session-scopes)
      session_scopes="${2:?--session-scopes requires a comma-separated scope list}"
      shift 2
      ;;
    --session-scopes=*)
      session_scopes="${1#--session-scopes=}"
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
# Exec the single shared build artifact that also backs the API server, instead
# of \`go run\` from this worktree. This keeps every wrapper on one code version,
# so a stale worktree can no longer run divergent projector code against the
# shared database (work item a9374bdd). Rebuild it from a clean v1 checkout with
# \$primary_repo/scripts/rebuild-meristem-bin.sh.
meristem_bin="\$primary_repo/.meristem/generated/meristem-bin"
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
  echo "missing Claude Code meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target claude-code-gui" >&2
  exit 64
fi
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
# MERISTEM_TOKEN_FILE lets a session point this wrapper at its own per-session
# credential (minted with provision-assistant-access.sh --session) without
# editing shared config; the per-app token remains the default (3818efed).
export MERISTEM_TOKEN="\$(cat "\${MERISTEM_TOKEN_FILE:-\$primary_repo/.meristem/claude-code-gui.token}")"
# Claude (like Cursor) rejects dot-namespaced MCP tool names; advertise the
# underscore aliases. Dispatch still accepts canonical names.
export MERISTEM_MCP_TOOL_NAMES="\${MERISTEM_MCP_TOOL_NAMES:-cursor}"
exec "\$meristem_bin" mcp
EOF
  chmod 700 "$GENERATED_DIR/claude-code-meristem-command.sh"

  cat > "$GENERATED_DIR/codex-meristem-command.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$codex_workspace"
# Exec the single shared build artifact that also backs the API server, instead
# of \`go run\` from this worktree. This keeps every wrapper on one code version,
# so a stale worktree can no longer run divergent projector code against the
# shared database (work item a9374bdd). Rebuild it from a clean v1 checkout with
# \$primary_repo/scripts/rebuild-meristem-bin.sh.
meristem_bin="\$primary_repo/.meristem/generated/meristem-bin"
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
  echo "missing Codex meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target codex" >&2
  exit 64
fi
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
# CODEX_MERISTEM_TOKEN_FILE is the unattended app-server boundary: the stem
# listener may pass a dedicated task credential without forwarding the
# bridge's MERISTEM_TOKEN_FILE. Interactive sessions retain the existing
# MERISTEM_TOKEN_FILE override and per-app fallback (3818efed).
token_file="\${CODEX_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-\$primary_repo/.meristem/codex.token}}"
export MERISTEM_TOKEN="\$(cat "\$token_file")"
exec "\$meristem_bin" mcp
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

if $session_requested; then
  # Per-session credential: one mint, no shared wrapper regeneration, no
  # claude-mcp registration. The session exports MERISTEM_TOKEN_FILE so the
  # existing generated wrappers pick up its credential; every other session
  # keeps its own. This deliberately does NOT reuse mint_target: session
  # identities must be unique (never silently reuse an existing token or
  # file) and must carry explicit default-deny scopes (3818efed round 2).
  [[ -n "$session_target" ]] || die "--session requires a non-empty name"
  # String-level non-emptiness is not enough: the server reduces the CSV by
  # splitting on commas and dropping empty/whitespace parts, so ',' or ' '
  # would reach tokens create as nil scopes and mint a silent legacyUnscoped
  # broad token. Require at least one scope that survives that reduction.
  session_scopes_effective=false
  IFS=',' read -r -a session_scope_parts <<< "$session_scopes"
  for session_scope_part in "${session_scope_parts[@]}"; do
    if [[ -n "${session_scope_part//[[:space:]]/}" ]]; then
      session_scopes_effective=true
      break
    fi
  done
  $session_scopes_effective || die "--session-scopes must contain at least one non-empty scope: empty scopes select the broad legacy surface"
  $apply_claude_code && die "--session cannot be combined with --apply-claude-code"
  sanitize_target "$session_target"

  session_file="$(token_file_for "$session_target")"
  [[ -e "$session_file" ]] && die "$session_file already exists; per-session names must be unique - pick a new name (e.g. append a date or counter)"
  if active_token_exists "$session_target"; then
    die "an active token named $session_target already exists; two live sessions must never share an actor token - pick a unique name"
  fi

  log "provisioning per-session credential $session_target (scopes: $session_scopes)"
  session_tmp="$(mktemp)"
  MERISTEM_TOKEN="$(cat "$ROOT_TOKEN_FILE")" \
    $MERISTEM_BIN tokens create --name "$session_target" --source agent --scopes "$session_scopes" > "$session_tmp"
  write_secret_from_capture "$session_tmp" "$session_file"
  rm -f "$session_tmp"
  [[ -s "$session_file" ]] || die "session credential file $session_file was not written"

  case "$session_file" in
    /*) session_file_abs="$session_file" ;;
    *)  session_file_abs="$REPO_ROOT/$session_file" ;;
  esac
  cat <<EOF

done. Point this session's MCP wrapper at its own credential with:

  export MERISTEM_TOKEN_FILE=$session_file_abs

Wrappers read this once at MCP process start: restart the MCP process (not
just the shell) to adopt it. The shared per-app tokens and generated wrappers
are unchanged.
EOF
  exit 0
fi

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
