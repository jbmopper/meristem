#!/usr/bin/env bash
# meristem bootstrap — one-shot stand-up.
#
# What it does (idempotent at every step):
#   1. Validates deterministic resource-safety controls. If this fails,
#      the system must not be restarted.
#   2. Brings up the Postgres container if it is not already healthy.
#   3. Runs `meristem migrate` to apply any pending migrations.
#   4. Mints a root token if none exists, writing the secret to
#      .meristem/root.token (mode 0600). If a root already exists, the
#      token file is left alone.
#   5. Mints a system-source token (`seed`) if none exists, writing the
#      secret to .meristem/seed.token (mode 0600). The seed token is
#      what `meristem seed v1` needs to attribute its writes to a
#      `system` actor instead of root or a human.
#   6. Runs `meristem seed v1` so the v0 acceptance criterion "each v1
#      substrate item exists as a `work_item`" is live. Per the seed
#      slice, that command is itself idempotent (one transaction per
#      item, deterministic event ids), so reruns are no-ops.
#   7. Prints a short summary of what other commands you might want to
#      run next.
#
# Re-running the script is safe: each step short-circuits when its
# postcondition already holds.
#
# Usage:
#   scripts/bootstrap.sh          # use the host `go run ./cmd/meristem`
#   MERISTEM_BIN=./meristem ...     # use a prebuilt binary instead
#
# Designed to need only Docker, Go, and a POSIX shell. It deliberately
# does not run inside Docker itself; the host-side bootstrap is the
# fastest path for an integrator who already has Go installed. For a
# pure-Docker stand-up, see `docker compose --profile app up -d` plus
# the manual `docker compose run --rm meristem tokens create --root`
# step in README.md.

set -euo pipefail

# --- configuration ---------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MERISTEM_BIN="${MERISTEM_BIN:-go run ./cmd/meristem}"
MERISTEM_DATABASE_URL="${MERISTEM_DATABASE_URL:-postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable}"
MERISTEM_HTTP_ADDR="${MERISTEM_HTTP_ADDR:-:8080}"
TOKEN_DIR="${MERISTEM_BOOTSTRAP_DIR:-.meristem}"
ROOT_TOKEN_FILE="${TOKEN_DIR}/root.token"
SEED_TOKEN_FILE="${TOKEN_DIR}/seed.token"
SEED_TOKEN_NAME="${MERISTEM_SEED_TOKEN_NAME:-seed}"

export MERISTEM_DATABASE_URL MERISTEM_HTTP_ADDR

# --- helpers ---------------------------------------------------------------

log()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m==>\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m==>\033[0m %s\n' "$*" >&2; exit 1; }

# Wait until the Postgres container reports `healthy` (per the compose
# healthcheck). 30 s is generous for a cold start on first pull.
wait_for_postgres() {
    local deadline=$(( $(date +%s) + 30 ))
    while true; do
        local status
        status="$(docker inspect --format '{{.State.Health.Status}}' meristem-postgres 2>/dev/null || echo missing)"
        case "$status" in
            healthy)  return 0 ;;
            missing)  die "meristem-postgres container does not exist; check docker compose output above" ;;
        esac
        if [[ $(date +%s) -ge $deadline ]]; then
            die "Postgres did not become healthy within 30s (last status: $status)"
        fi
        sleep 1
    done
}

# Look at `meristem tokens list` and return 0 iff a root token is present
# in any state (active or revoked). We treat "an active root once existed"
# as the bootstrap postcondition because re-rotating root from a script
# would silently throw away whatever secret a previous bootstrap stored.
root_token_exists() {
    $MERISTEM_BIN tokens list 2>/dev/null | awk -F'\t' '{ for (i=1; i<=NF; i++) if ($i == "root=true") print "yes" }' | grep -q yes
}

# Look at `meristem tokens list` and return 0 iff a token with the
# bootstrap-managed seed name exists in any state. We key on name, not
# source, because the script owns the lifecycle of this specific token;
# operators are free to mint additional system-source tokens with other
# names without confusing the script.
seed_token_exists() {
    $MERISTEM_BIN tokens list 2>/dev/null | awk -F'\t' -v name="$SEED_TOKEN_NAME" '$2 == name { print "yes" }' | grep -q yes
}

# Parse `secret=mrs_...` out of a `tokens create` capture file and
# write it to the destination with mode 0600. The token files are the
# only secrets bootstrap.sh persists; we keep them in $TOKEN_DIR which
# is itself mode 0700.
write_secret_from_capture() {
    local capture_file="$1"
    local dest_file="$2"
    local secret
    secret="$(awk -F= '$1 == "secret" { print $2 }' "$capture_file")"
    if [[ -z "$secret" ]]; then
        warn "could not parse a secret from tokens create output:"
        cat "$capture_file" >&2
        die "bootstrap aborted; mint the missing token manually and re-run"
    fi
    umask 077
    printf '%s\n' "$secret" > "$dest_file"
}

# --- step 1: resource-safety gate ------------------------------------------

log "validating deterministic resource-safety controls"
$MERISTEM_BIN safety check >/dev/null

# --- step 2: postgres ------------------------------------------------------

log "ensuring postgres is up"
docker compose up -d postgres
wait_for_postgres
log "postgres is healthy on 127.0.0.1:5432"

# --- step 3: migrations ----------------------------------------------------

log "applying migrations"
$MERISTEM_BIN migrate

# --- step 4: root token ----------------------------------------------------

mkdir -p "$TOKEN_DIR"
chmod 700 "$TOKEN_DIR"

if root_token_exists; then
    log "root token already provisioned (skipping mint)"
    if [[ ! -s "$ROOT_TOKEN_FILE" ]]; then
        warn "$ROOT_TOKEN_FILE is missing or empty; if you have lost the root secret, rotate with:"
        warn "  $MERISTEM_BIN tokens create --root --replace"
        warn "and store the printed secret somewhere safe."
    fi
else
    log "minting root token"
    # `tokens create --root` prints `secret=mrs_...` on stdout among
    # other key=value pairs.
    tmp_root="$(mktemp)"
    trap 'rm -f "$tmp_root" "${tmp_seed:-}"' EXIT
    $MERISTEM_BIN tokens create --root --name root > "$tmp_root"
    write_secret_from_capture "$tmp_root" "$ROOT_TOKEN_FILE"
    log "root secret written to $ROOT_TOKEN_FILE (mode 0600); guard or move it."
fi

# --- step 5: seed token (system source) ------------------------------------
# `meristem seed v1` requires a system-source token (not root); see
# docs/v0.md "Events" — "The seed command uses a dedicated `system`
# token, not root."

if seed_token_exists; then
    log "seed token '$SEED_TOKEN_NAME' already provisioned (skipping mint)"
    if [[ ! -s "$SEED_TOKEN_FILE" ]]; then
        warn "$SEED_TOKEN_FILE is missing or empty; if you have lost the seed secret, rotate by"
        warn "revoking the existing token and minting a new one:"
        warn "  $MERISTEM_BIN tokens revoke --id <id of $SEED_TOKEN_NAME>"
        warn "  MERISTEM_TOKEN=\$(cat $ROOT_TOKEN_FILE) \\"
        warn "    $MERISTEM_BIN tokens create --name $SEED_TOKEN_NAME --source system"
    fi
else
    log "minting seed token (source=system, name=$SEED_TOKEN_NAME)"
    if [[ ! -s "$ROOT_TOKEN_FILE" ]]; then
        die "cannot mint seed token: $ROOT_TOKEN_FILE is missing. Either restore it or rotate root."
    fi
    tmp_seed="$(mktemp)"
    trap 'rm -f "${tmp_root:-}" "$tmp_seed"' EXIT
    MERISTEM_TOKEN="$(cat "$ROOT_TOKEN_FILE")" \
        $MERISTEM_BIN tokens create --name "$SEED_TOKEN_NAME" --source system > "$tmp_seed"
    write_secret_from_capture "$tmp_seed" "$SEED_TOKEN_FILE"
    log "seed secret written to $SEED_TOKEN_FILE (mode 0600)."
fi

# --- step 6: seed v1 substrate backlog -------------------------------------
# Closes v0 acceptance test #7 ("each v1 substrate item exists as a
# work_item"). The command is itself idempotent — one transaction per
# item with a deterministic event id — so reruns produce no new events.

log "seeding v1 substrate backlog (meristem seed v1)"
MERISTEM_TOKEN="$(cat "$SEED_TOKEN_FILE")" $MERISTEM_BIN seed v1

# --- step 7: next steps ----------------------------------------------------

cat <<EOF

bootstrap complete.

Next steps:

  # Start the API on the host:
  MERISTEM_DATABASE_URL='$MERISTEM_DATABASE_URL' $MERISTEM_BIN api

  # In another supervised shell/process, start the deterministic worker:
  MERISTEM_DATABASE_URL='$MERISTEM_DATABASE_URL' \\
    MERISTEM_TOKEN=\$(cat $SEED_TOKEN_FILE) \\
    $MERISTEM_BIN worker

  # In another shell, mint a per-service token (one per integrator):
  MERISTEM_TOKEN=\$(cat $ROOT_TOKEN_FILE) \\
    $MERISTEM_BIN tokens create --name jay --source agent

  # Then post a real signal:
  MERISTEM_TOKEN=mrs_... examples/curl-signal.sh

  # Confirm the v1 backlog is live:
  curl -sS -H "Authorization: Bearer \$(cat $ROOT_TOKEN_FILE)" \\
    'http://localhost:8080/v1/work-items?state=captured' | head -40

EOF
