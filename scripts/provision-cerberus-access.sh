#!/usr/bin/env bash
# Provision scoped Cerberus MCP token files for the first pilot.
#
# This script uses the local root token only to mint the planned source=agent
# tokens: Aegis's scoped coordinator identity plus the grower/healer worker
# identities. It does not print bearer secrets.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MERISTEM_BIN="${MERISTEM_BIN:-go run ./cmd/meristem}"
MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
TOKEN_DIR="${MERISTEM_TOKEN_DIR:-.meristem}"
ROOT_TOKEN_FILE="${ROOT_TOKEN_FILE:-$TOKEN_DIR/root.token}"
ROOT_ID="${CERBERUS_ROOT_ID:-98853a93-2de4-42fb-9438-a1a54caf9589}"
ROOT_SHORT="${CERBERUS_ROOT_SHORT:-98853a93}"

SCOPES="work_items.tree:$ROOT_ID,work_items.read,work_items.write,feed.read_assigned"

export MERISTEM_DATABASE_URL

log() { printf '==> %s\n' "$*"; }
die() { printf '!! %s\n' "$*" >&2; exit 1; }

[[ -s "$ROOT_TOKEN_FILE" ]] || die "$ROOT_TOKEN_FILE is required"

mkdir -p "$TOKEN_DIR"
chmod 700 "$TOKEN_DIR"

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
    sed 's/secret=.*/secret=<redacted>/' "$capture_file" >&2
    die "could not parse token secret; aborting"
  fi
  umask 077
  printf '%s\n' "$secret" > "$dest_file"
  chmod 600 "$dest_file"
}

mint_one() {
  local name="$1"
  local file="$TOKEN_DIR/$name.token"

  if [[ -s "$file" ]]; then
    chmod 600 "$file"
    log "$name: token file exists"
    return
  fi

  if active_token_exists "$name"; then
    die "$name: active token row exists but $file is missing; revoke/rotate manually because the bearer secret cannot be recovered"
  fi

  log "$name: minting scoped source=agent token"
  local tmp
  tmp="$(mktemp)"
  MERISTEM_TOKEN="$(tr -d '\n' < "$ROOT_TOKEN_FILE")" \
    $MERISTEM_BIN tokens create --name "$name" --source agent --scopes "$SCOPES" > "$tmp"
  write_secret_from_capture "$tmp" "$file"
  rm -f "$tmp"
  log "$name: wrote $file"
}

mint_one "aegis-cerberus-coordinator-$ROOT_SHORT"
mint_one "cerberus-grower-$ROOT_SHORT"
mint_one "cerberus-healer-$ROOT_SHORT"

bash scripts/generate-cerberus-launchers.sh

log "done"
