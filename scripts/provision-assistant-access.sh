#!/usr/bin/env bash
# Generate and provision secret-safe local-agent Streamable HTTP MCP access.
#
# The no-mode invocation preserves the existing stdio development/rollback
# provisioning surface. The explicit --generate-http path is deliberately
# offline: it writes only secret-free client snippets and launch/helpers under
# .meristem/generated/. HTTP credential minting, applying generated snippets,
# and restarting clients are separate owner cutover actions.

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
case "$TOKEN_DIR" in
  /*) TOKEN_DIR_ABS="$TOKEN_DIR" ;;
  *)  TOKEN_DIR_ABS="$REPO_ROOT/$TOKEN_DIR" ;;
esac
case "$GENERATED_DIR" in
  /*) GENERATED_DIR_ABS="$GENERATED_DIR" ;;
  *)  GENERATED_DIR_ABS="$REPO_ROOT/$GENERATED_DIR" ;;
esac

LOCAL_PROFILE_SCOPE="mcp.profile:local_agent_v1"
DEFAULT_BUSINESS_SCOPES="feed.read,work_items.read_all,work_items.write_all,work_items.create"
DEFAULT_STDIO_TARGETS=(
  codex
  codex-cli
  claude-code
  claude-code-cli
  claude-code-gui
  claude-desktop
)
DEFAULT_HTTP_TARGETS=(codex claude-code cursor)

mode="stdio"
mode_selected=false
targets_explicit=false
targets=("${DEFAULT_STDIO_TARGETS[@]}")
business_scopes="$DEFAULT_BUSINESS_SCOPES"
credential_suffix="http-v1"
session_target=""
session_scopes="$DEFAULT_BUSINESS_SCOPES"
apply_claude_code=false
print_remote=false

usage() {
  cat <<'USAGE'
usage:
  scripts/provision-assistant-access.sh [options]

modes (mutually exclusive):
  (no mode)             Preserve the existing stdio provisioning path: mint
                         configured assistant tokens and regenerate the
                         development/rollback stdio wrappers.
  --generate-http        Generate secret-free HTTP MCP entries and launch/helpers.
                         It does not read root/client tokens,
                         contact Postgres, or apply live client configuration.
  --mint-http            Mint staged HTTP-profile credentials. Requires the root
                         token and writes .meristem/<target>.http-next.token.
                         Existing active token files are never reused or replaced.
  --session NAME         Mint one unique explicitly scoped stdio session credential at
                         .meristem/NAME.token using the existing stdio session
                         contract. No shared config is regenerated.
  --session-http NAME    Mint one unique local-agent HTTP-profile session
                         credential. No shared config is regenerated.

options:
  --targets a,b,c        Credential targets for stdio/mint modes. HTTP targets
                         are codex, claude-code, cursor. --generate-http always
                         emits the complete three-client cutover pack.
  --business-scopes CSV  Explicit non-profile scopes. Default:
                         feed.read,work_items.read_all,work_items.write_all,
                         work_items.create. At least one effective scope is required.
  --credential-suffix S  Suffix for staged token names (default: http-v1).
  --session-scopes CSV   Existing alias for stdio --session scopes.
  --apply-claude-code    Existing stdio path: apply its generated command
                         wrapper with `claude mcp add`.
  --print-remote         Describe cloud-only targets not handled here.
  -h, --help             Show this help.

security and cutover:
  Every HTTP-mode credential receives exactly mcp.profile:local_agent_v1 plus
  the explicit business scopes. Existing token files may be legacy-unscoped
  and are never accepted as proof of HTTP-profile authority. The stdio path is
  retained only for tested development/rollback compatibility.

  Generated candidates all replace the existing active name `meristem`; no
  `meristem-http` entry is generated. Before cutover, stop/disconnect that
  client, preserve its stdio entry outside active config, atomically replace
  the same-named entry, then restart/reconnect and smoke. Applying config,
  swapping staged token files, revoking old credentials, and rollback are
  separate owner-approved operations.
USAGE
}

select_mode() {
  local selected="$1"
  if $mode_selected && [[ "$mode" != "$selected" ]]; then
    printf '!! modes are mutually exclusive (%s and %s)\n' "$mode" "$selected" >&2
    exit 2
  fi
  mode="$selected"
  mode_selected=true
}

while (($#)); do
  case "$1" in
    --generate-http)
      select_mode generate-http
      shift
      ;;
    --mint-http)
      select_mode mint-http
      shift
      ;;
    --session)
      select_mode session
      session_target="${2:?--session requires a name}"
      shift 2
      ;;
    --session=*)
      select_mode session
      session_target="${1#--session=}"
      shift
      ;;
    --session-http)
      select_mode session-http
      session_target="${2:?--session-http requires a name}"
      shift 2
      ;;
    --session-http=*)
      select_mode session-http
      session_target="${1#--session-http=}"
      shift
      ;;
    --targets)
      IFS=',' read -r -a targets <<< "${2:?--targets requires a comma-separated value}"
      targets_explicit=true
      shift 2
      ;;
    --targets=*)
      IFS=',' read -r -a targets <<< "${1#--targets=}"
      targets_explicit=true
      shift
      ;;
    --business-scopes)
      business_scopes="${2:?$1 requires a comma-separated scope list}"
      shift 2
      ;;
    --business-scopes=*)
      business_scopes="${1#*=}"
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
    --credential-suffix)
      credential_suffix="${2:?--credential-suffix requires a value}"
      shift 2
      ;;
    --credential-suffix=*)
      credential_suffix="${1#--credential-suffix=}"
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

if ! $targets_explicit; then
  case "$mode" in
    generate-http|mint-http) targets=("${DEFAULT_HTTP_TARGETS[@]}") ;;
    *) targets=("${DEFAULT_STDIO_TARGETS[@]}") ;;
  esac
fi

log()  { printf '==> %s\n' "$*"; }
warn() { printf '!! %s\n' "$*" >&2; }
die()  { printf '!! %s\n' "$*" >&2; exit 1; }

sanitize_name() {
  local name="$1"
  case "$name" in
    ''|*[^A-Za-z0-9_.-]*)
      die "invalid name $name; use only letters, numbers, dot, underscore, and dash"
      ;;
  esac
}

validate_target() {
  local target="$1"
  case "$target" in
    codex|claude-code|cursor) ;;
    *) die "unsupported HTTP MCP target $target; use codex, claude-code, or cursor" ;;
  esac
}

sanitize_stdio_target() {
  local target="$1"
  case "$target" in
    cursor*)
      die "unsupported stdio target $target: use explicit --generate-http/--mint-http for Cursor"
      ;;
  esac
  sanitize_name "$target"
}

token_file_for() {
  printf '%s/%s.token' "$TOKEN_DIR" "$1"
}

worktree_for() {
  printf '%s/meristem-%s' "$AGENT_WORKTREE_BASE" "$1"
}

normalize_business_scopes() {
  local raw="$1"
  local part trimmed
  local -a effective=()
  IFS=',' read -r -a parts <<< "$raw"
  for part in "${parts[@]}"; do
    trimmed="${part#"${part%%[![:space:]]*}"}"
    trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
    [[ -n "$trimmed" ]] || continue
    case "$trimmed" in
      mcp.profile:*|provider.profile:*)
        die "--business-scopes accepts authority scopes only; profile marker $trimmed is managed by this script"
        ;;
    esac
    effective+=("$trimmed")
  done
  ((${#effective[@]} > 0)) || die "--business-scopes must contain at least one non-profile scope"
  local joined=""
  for part in "${effective[@]}"; do
    if [[ -n "$joined" ]]; then
      joined+=","
    fi
    joined+="$part"
  done
  printf '%s' "$joined"
}

require_effective_scopes() {
  local raw="$1"
  local part
  IFS=',' read -r -a scope_parts <<< "$raw"
  for part in "${scope_parts[@]}"; do
    if [[ -n "${part//[[:space:]]/}" ]]; then
      return 0
    fi
  done
  return 1
}

effective_scopes() {
  local normalized
  normalized="$(normalize_business_scopes "$business_scopes")" || return 1
  printf '%s,%s' "$LOCAL_PROFILE_SCOPE" "$normalized"
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

mint_stdio_target() {
  local target="$1"
  sanitize_stdio_target "$target"

  local file
  file="$(token_file_for "$target")"
  if [[ -s "$file" ]]; then
    chmod 600 "$file"
    log "$target: token file exists at $file"
    return
  fi
  if active_token_exists "$target"; then
    warn "$target: active token exists, but $file is missing or empty"
    warn "$target: cannot recover the bearer; revoke/rotate manually if needed"
    return
  fi

  local capture
  capture="$(mktemp)"
  if ! MERISTEM_TOKEN="$(tr -d '\r\n' < "$ROOT_TOKEN_FILE")" \
    $MERISTEM_BIN tokens create --name "$target" --source agent > "$capture"; then
    rm -f "$capture"
    return 1
  fi
  write_secret_from_capture "$capture" "$file"
  rm -f "$capture"
  log "$target: wrote $file"
}

write_stdio_configs() {
  mkdir -p "$GENERATED_DIR"
  chmod 700 "$TOKEN_DIR" "$GENERATED_DIR"

  local claude_code_workspace codex_workspace
  claude_code_workspace="$(worktree_for claude-code-gui)"
  codex_workspace="$(worktree_for codex)"

  cat > "$GENERATED_DIR/claude-code-meristem-command.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$claude_code_workspace"
meristem_bin="\$primary_repo/.meristem/generated/meristem-bin"
export MERISTEM_V1_PIN_FILE="\${MERISTEM_V1_PIN_FILE:-\$meristem_bin.v1-pin}"
[[ "\$MERISTEM_V1_PIN_FILE" == /* ]] || { echo "reviewed-v1 pin path must be absolute" >&2; exit 64; }
if ! "\$primary_repo/scripts/check-meristem-build-pin.sh" "\$meristem_bin" "\$MERISTEM_V1_PIN_FILE"; then
  echo "shared meristem build does not match the reviewed-v1 pin; refusing to read credentials" >&2
  exit 64
fi
[[ -e "\$workspace_root/.git" ]] || {
  echo "missing Claude Code meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target claude-code-gui" >&2
  exit 64
}
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
export MERISTEM_TOKEN="\$(tr -d '\\r\\n' < "\${MERISTEM_TOKEN_FILE:-\$primary_repo/.meristem/claude-code-gui.token}")"
export MERISTEM_MCP_TOOL_NAMES="\${MERISTEM_MCP_TOOL_NAMES:-cursor}"
exec "\$meristem_bin" mcp
EOF
  chmod 700 "$GENERATED_DIR/claude-code-meristem-command.sh"

  cat > "$GENERATED_DIR/codex-meristem-command.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
primary_repo="$REPO_ROOT"
workspace_root="$codex_workspace"
meristem_bin="\$primary_repo/.meristem/generated/meristem-bin"
export MERISTEM_V1_PIN_FILE="\${MERISTEM_V1_PIN_FILE:-\$meristem_bin.v1-pin}"
[[ "\$MERISTEM_V1_PIN_FILE" == /* ]] || { echo "reviewed-v1 pin path must be absolute" >&2; exit 64; }
if ! "\$primary_repo/scripts/check-meristem-build-pin.sh" "\$meristem_bin" "\$MERISTEM_V1_PIN_FILE"; then
  echo "shared meristem build does not match the reviewed-v1 pin; refusing to read credentials" >&2
  exit 64
fi
[[ -e "\$workspace_root/.git" ]] || {
  echo "missing Codex meristem worktree: \$workspace_root" >&2
  echo "create it with: \$primary_repo/scripts/prepare-agent-worktree.sh --target codex" >&2
  exit 64
}
cd "\$workspace_root"
export MERISTEM_DATABASE_URL="$MERISTEM_DATABASE_URL"
token_file="\${CODEX_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-\$primary_repo/.meristem/codex.token}}"
export MERISTEM_TOKEN="\$(tr -d '\\r\\n' < "\$token_file")"
exec "\$meristem_bin" mcp
EOF
  chmod 700 "$GENERATED_DIR/codex-meristem-command.sh"

  cat > "$GENERATED_DIR/claude-code-mcp-add.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
claude mcp add meristem --scope user -- "$GENERATED_DIR_ABS/claude-code-meristem-command.sh"
EOF
  chmod 700 "$GENERATED_DIR/claude-code-mcp-add.sh"
  log "generated development/rollback stdio wrappers in $GENERATED_DIR"
}

apply_claude_code_config() {
  local file
  file="$(token_file_for claude-code-gui)"
  [[ -s "$file" ]] || die "cannot apply Claude Code config: $file is missing"
  if ! command -v claude >/dev/null 2>&1; then
    warn "claude CLI not found; run $GENERATED_DIR/claude-code-mcp-add.sh later"
    return
  fi
  "$GENERATED_DIR/claude-code-mcp-add.sh"
  log "registered the development/rollback stdio meristem entry with Claude Code"
}

mint_one() {
  local credential_name="$1"
  local dest_file="$2"
  local scopes="$3"

  sanitize_name "$credential_name"
  [[ ! -e "$dest_file" ]] || die "$dest_file already exists; staged/profiled credentials are never silently reused"
  if active_token_exists "$credential_name"; then
    die "an active token named $credential_name already exists; choose a different --credential-suffix"
  fi

  local capture
  capture="$(mktemp)"
  if ! MERISTEM_TOKEN="$(tr -d '\r\n' < "$ROOT_TOKEN_FILE")" \
    $MERISTEM_BIN tokens create --name "$credential_name" --source agent --scopes "$scopes" > "$capture"; then
    rm -f "$capture"
    return 1
  fi
  write_secret_from_capture "$capture" "$dest_file"
  rm -f "$capture"
}

prepare_generated_dir() {
  umask 077
  mkdir -p "$GENERATED_DIR/rollback"
  chmod 700 "$GENERATED_DIR" "$GENERATED_DIR/rollback"
}

write_http_configs() {
  prepare_generated_dir

  local codex_token="$TOKEN_DIR_ABS/codex.token"
  local claude_token="$TOKEN_DIR_ABS/claude-code.token"
  local cursor_token="$TOKEN_DIR_ABS/cursor.token"

  cat > "$GENERATED_DIR/codex-http-mcp.toml" <<'EOF'
[mcp_servers.meristem]
url = "http://127.0.0.1:8080/mcp"
bearer_token_env_var = "MERISTEM_CODEX_TOKEN"
http_headers = { "X-Meristem-Tool-Names" = "canonical" }
EOF

  cat > "$GENERATED_DIR/codex-http-launch.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
token_file="\${CODEX_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-$codex_token}}"
[[ -s "\$token_file" ]] || { echo "missing Codex Meristem token file: \$token_file" >&2; exit 64; }
export MERISTEM_CODEX_TOKEN="\$(tr -d '\\r\\n' < "\$token_file")"
[[ -n "\$MERISTEM_CODEX_TOKEN" ]] || { echo "empty Codex Meristem token file" >&2; exit 64; }
unset MERISTEM_TOKEN MERISTEM_TOKEN_FILE MERISTEM_DATABASE_URL
exec "\${CODEX_BIN:-codex}" "\$@"
EOF
  chmod 700 "$GENERATED_DIR/codex-http-launch.sh"

  cat > "$GENERATED_DIR/claude-code-http-headers.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
token_file="\${CLAUDE_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-$claude_token}}"
[[ -s "\$token_file" ]] || { echo "missing Claude Meristem token file: \$token_file" >&2; exit 64; }
token="\$(tr -d '\\r\\n' < "\$token_file")"
[[ "\$token" =~ ^[A-Za-z0-9._~-]+\$ ]] || { echo "invalid Claude Meristem token file" >&2; exit 64; }
printf '{"Authorization":"Bearer %s","X-Meristem-Tool-Names":"cursor"}\\n' "\$token"
EOF
  chmod 700 "$GENERATED_DIR/claude-code-http-headers.sh"

  cat > "$GENERATED_DIR/claude-code-http-mcp.json" <<EOF
{
  "mcpServers": {
    "meristem": {
      "type": "http",
      "url": "http://127.0.0.1:8080/mcp",
      "headersHelper": "$GENERATED_DIR_ABS/claude-code-http-headers.sh"
    }
  }
}
EOF

  cat > "$GENERATED_DIR/claude-code-http-env-mcp.json" <<'EOF'
{
  "mcpServers": {
    "meristem": {
      "type": "http",
      "url": "http://127.0.0.1:8080/mcp",
      "headers": {
        "Authorization": "Bearer ${MERISTEM_CLAUDE_TOKEN}",
        "X-Meristem-Tool-Names": "cursor"
      }
    }
  }
}
EOF

  cat > "$GENERATED_DIR/claude-code-http-launch.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
token_file="\${CLAUDE_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-$claude_token}}"
[[ -s "\$token_file" ]] || { echo "missing Claude Meristem token file: \$token_file" >&2; exit 64; }
export MERISTEM_CLAUDE_TOKEN="\$(tr -d '\\r\\n' < "\$token_file")"
[[ -n "\$MERISTEM_CLAUDE_TOKEN" ]] || { echo "empty Claude Meristem token file" >&2; exit 64; }
unset MERISTEM_TOKEN MERISTEM_TOKEN_FILE MERISTEM_DATABASE_URL
exec "\${CLAUDE_BIN:-claude}" "\$@"
EOF
  chmod 700 "$GENERATED_DIR/claude-code-http-launch.sh"

  cat > "$GENERATED_DIR/cursor-http-mcp.json" <<'EOF'
{
  "mcpServers": {
    "meristem": {
      "type": "http",
      "url": "http://127.0.0.1:8080/mcp",
      "headers": {
        "Authorization": "Bearer ${env:MERISTEM_CURSOR_TOKEN}",
        "X-Meristem-Tool-Names": "cursor"
      }
    }
  }
}
EOF

  cat > "$GENERATED_DIR/cursor-http-launch.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
token_file="\${CURSOR_MERISTEM_TOKEN_FILE:-\${MERISTEM_TOKEN_FILE:-$cursor_token}}"
[[ -s "\$token_file" ]] || { echo "missing Cursor Meristem token file: \$token_file" >&2; exit 64; }
export MERISTEM_CURSOR_TOKEN="\$(tr -d '\\r\\n' < "\$token_file")"
[[ -n "\$MERISTEM_CURSOR_TOKEN" ]] || { echo "empty Cursor Meristem token file" >&2; exit 64; }
unset MERISTEM_TOKEN MERISTEM_TOKEN_FILE MERISTEM_DATABASE_URL
exec "\${CURSOR_BIN:-cursor}" "\$@"
EOF
  chmod 700 "$GENERATED_DIR/cursor-http-launch.sh"

  cat > "$GENERATED_DIR/cutover-manifest.json" <<'EOF'
{
  "schema": "meristem.local_http_mcp_cutover.v1",
  "active_server_name": "meristem",
  "candidate_entries": {
    "codex": "codex-http-mcp.toml",
    "claude-code": "claude-code-http-mcp.json",
    "cursor": "cursor-http-mcp.json"
  },
  "invariants": {
    "client_stopped_or_disconnected_before_replace": true,
    "same_named_entry_replaced_atomically": true,
    "parallel_meristem_http_entry_forbidden": true,
    "stdio_rollback_entry_kept_outside_active_config": true,
    "prior_token_revoked_only_after_http_smoke": true
  },
  "rollback_directory": "rollback"
}
EOF

  cat > "$GENERATED_DIR/rollback/README.txt" <<'EOF'
This directory is inert rollback storage, not active MCP configuration.

Before changing a client, stop/disconnect it and copy its exact existing
same-named `meristem` stdio entry here. Atomically replace that one active entry
with the generated HTTP candidate; never add a second `meristem-http` entry.
Keep the prior credential active until the HTTP attribution/idempotency smoke
passes. On failure, stop/disconnect, atomically restore the saved stdio entry
and prior token file, restart/reconnect, and smoke the restored client.
EOF

  chmod 600 \
    "$GENERATED_DIR/codex-http-mcp.toml" \
    "$GENERATED_DIR/claude-code-http-mcp.json" \
    "$GENERATED_DIR/claude-code-http-env-mcp.json" \
    "$GENERATED_DIR/cursor-http-mcp.json" \
    "$GENERATED_DIR/cutover-manifest.json" \
    "$GENERATED_DIR/rollback/README.txt"
  log "generated secret-free HTTP MCP candidates in $GENERATED_DIR"
}

print_remote_targets() {
  $print_remote || return 0
  cat <<'EOF'

Remote-only targets not provisioned by this script:
  chatgpt-remote
  claude-web
  cowork
  claude-mobile

These require a public HTTPS MCP endpoint and the provider/OAuth profiles.
Do not point them at 127.0.0.1 or use the local-agent profile marker.
EOF
}

case "$mode" in
  stdio)
    [[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required to mint assistant tokens"
    [[ -n "$GO_BIN" ]] || die "could not find go on PATH; set GO_BIN=/absolute/path/to/go"
    mkdir -p "$TOKEN_DIR"
    chmod 700 "$TOKEN_DIR"
    export MERISTEM_DATABASE_URL
    log "provisioning development/rollback stdio assistant tokens"
    for target in "${targets[@]}"; do
      [[ -n "$target" ]] || continue
      mint_stdio_target "$target"
    done
    write_stdio_configs
    if $apply_claude_code; then
      apply_claude_code_config
    fi
    print_remote_targets
    cat <<EOF

done. Development/rollback stdio files:
  $GENERATED_DIR/codex-meristem-command.sh
  $GENERATED_DIR/claude-code-meristem-command.sh
  $GENERATED_DIR/claude-code-mcp-add.sh

For release HTTP candidates, run --generate-http. Do not register both stdio
and HTTP entries for the same client against the shared database.
EOF
    ;;

  generate-http)
    $apply_claude_code && die "--apply-claude-code belongs to the no-mode stdio compatibility path"
    for target in "${targets[@]}"; do
      [[ -n "$target" ]] || continue
      validate_target "$target"
    done
    write_http_configs
    print_remote_targets
    cat <<EOF

done. No token or live client configuration was read or changed.

Generated candidates (all replace the exact active name meristem):
  $GENERATED_DIR/codex-http-mcp.toml
  $GENERATED_DIR/claude-code-http-mcp.json
  $GENERATED_DIR/cursor-http-mcp.json

Launch/helper files:
  $GENERATED_DIR/codex-http-launch.sh
  $GENERATED_DIR/claude-code-http-headers.sh
  $GENERATED_DIR/claude-code-http-launch.sh
  $GENERATED_DIR/cursor-http-launch.sh

Next owner actions, only after the reviewed server is running on loopback:
  1. stage explicit HTTP credentials with --mint-http;
  2. stop/disconnect one client and save its stdio entry under rollback/;
  3. atomically replace that same meristem entry and staged token file;
  4. restart/reconnect, smoke attribution/idempotency, then revoke the old token.
EOF
    ;;

  mint-http)
    $apply_claude_code && die "--apply-claude-code cannot be combined with --mint-http"
    [[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required for --mint-http"
    sanitize_name "$credential_suffix"
    scopes="$(effective_scopes)"
    mkdir -p "$TOKEN_DIR"
    chmod 700 "$TOKEN_DIR"
    export MERISTEM_DATABASE_URL
    for target in "${targets[@]}"; do
      [[ -n "$target" ]] || continue
      validate_target "$target"
      staged_file="$TOKEN_DIR/$target.http-next.token"
      credential_name="$target-$credential_suffix"
      log "$target: minting staged source=agent HTTP-profile credential $credential_name"
      mint_one "$credential_name" "$staged_file" "$scopes"
      log "$target: wrote staged $staged_file (mode 0600); active $TOKEN_DIR/$target.token is untouched"
    done
    print_remote_targets
    ;;

  session)
    [[ -n "$session_target" ]] || die "--session requires a non-empty name"
    [[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required for --session"
    $apply_claude_code && die "--session cannot be combined with --apply-claude-code"
    require_effective_scopes "$session_scopes" || die "--session-scopes must contain at least one non-empty scope"
    sanitize_name "$session_target"
    mkdir -p "$TOKEN_DIR"
    chmod 700 "$TOKEN_DIR"
    export MERISTEM_DATABASE_URL
    session_file="$TOKEN_DIR/$session_target.token"
    log "minting unique explicitly scoped stdio session credential $session_target"
    mint_one "$session_target" "$session_file" "$session_scopes"
    case "$session_file" in
      /*) session_file_abs="$session_file" ;;
      *)  session_file_abs="$REPO_ROOT/$session_file" ;;
    esac
    cat <<EOF

done. Point this session's stdio MCP wrapper at its own credential with:

  export MERISTEM_TOKEN_FILE=$session_file_abs

Restart the stdio MCP process to adopt it. Shared wrappers are unchanged.
EOF
    ;;

  session-http)
    [[ -n "$session_target" ]] || die "--session-http requires a non-empty name"
    [[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required for --session-http"
    $apply_claude_code && die "--session-http cannot be combined with --apply-claude-code"
    sanitize_name "$session_target"
    scopes="$(effective_scopes)"
    mkdir -p "$TOKEN_DIR"
    chmod 700 "$TOKEN_DIR"
    export MERISTEM_DATABASE_URL
    session_file="$TOKEN_DIR/$session_target.token"
    log "minting unique HTTP-profile session credential $session_target"
    mint_one "$session_target" "$session_file" "$scopes"
    case "$session_file" in
      /*) session_file_abs="$session_file" ;;
      *)  session_file_abs="$REPO_ROOT/$session_file" ;;
    esac
    cat <<EOF

done. Launch exactly one client with the matching file override:

  CODEX_MERISTEM_TOKEN_FILE=$session_file_abs
  CLAUDE_MERISTEM_TOKEN_FILE=$session_file_abs
  CURSOR_MERISTEM_TOKEN_FILE=$session_file_abs

Use only the variable for the selected client. Codex/Cursor require a full
restart; Claude's headersHelper rereads its file on reconnect.
EOF
    ;;

  *)
    die "internal error: unsupported mode $mode"
    ;;
esac
