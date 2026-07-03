#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker_context="${MERISTEM_DOCKER_CONTEXT:-colima}"
container="${MERISTEM_POSTGRES_CONTAINER:-meristem-postgres}"
postgres_user="${MERISTEM_POSTGRES_USER:-meristem}"
postgres_db="${MERISTEM_POSTGRES_DB:-meristem}"
backup_dir="${MERISTEM_BACKUP_DIR:-$repo_root/.meristem/backups}"

docker_cmd=(docker)
if [[ -n "$docker_context" ]]; then
  docker_cmd+=(--context "$docker_context")
fi

usage() {
  cat <<'EOF'
usage:
  scripts/snapshot-db.sh create [output.dump]
  scripts/snapshot-db.sh list <archive.dump>

Creates or inspects Postgres custom-format meristem DB snapshots using the
running meristem-postgres container as the pg_dump/pg_restore tool host.

Environment:
  MERISTEM_DOCKER_CONTEXT       Docker context to use (default: colima)
  MERISTEM_POSTGRES_CONTAINER   Postgres container name (default: meristem-postgres)
  MERISTEM_POSTGRES_USER        Postgres role (default: meristem)
  MERISTEM_POSTGRES_DB          Database name (default: meristem)
  MERISTEM_BACKUP_DIR           Snapshot directory (default: .meristem/backups)
EOF
}

require_container() {
  if ! "${docker_cmd[@]}" inspect "$container" >/dev/null 2>&1; then
    echo "snapshot-db: container $container not found in context $docker_context" >&2
    exit 1
  fi
}

create_snapshot() {
  local target="${1:-}"
  local ts remote
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  if [[ -z "$target" ]]; then
    mkdir -p "$backup_dir"
    chmod 700 "$backup_dir"
    target="$backup_dir/meristem-$ts.dump"
  fi

  remote="/tmp/meristem-snapshot-$ts.dump"
  "${docker_cmd[@]}" exec "$container" pg_dump \
    -U "$postgres_user" \
    -d "$postgres_db" \
    -Fc \
    --no-owner \
    --no-acl \
    -f "$remote"
  "${docker_cmd[@]}" cp "$container:$remote" "$target"
  "${docker_cmd[@]}" exec "$container" rm -f "$remote" >/dev/null 2>&1 || true
  chmod 600 "$target"
  printf '%s\n' "$target"
}

list_archive() {
  local archive="${1:-}"
  if [[ -z "$archive" ]]; then
    usage >&2
    exit 2
  fi
  if [[ ! -f "$archive" ]]; then
    echo "snapshot-db: archive not found: $archive" >&2
    exit 1
  fi

  local remote
  remote="/tmp/meristem-list-$$.dump"
  "${docker_cmd[@]}" cp "$archive" "$container:$remote"
  "${docker_cmd[@]}" exec "$container" pg_restore -l "$remote"
  "${docker_cmd[@]}" exec "$container" rm -f "$remote" >/dev/null 2>&1 || true
}

cmd="${1:-}"
case "$cmd" in
  create)
    require_container
    shift
    create_snapshot "$@"
    ;;
  list)
    require_container
    shift
    list_archive "$@"
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
