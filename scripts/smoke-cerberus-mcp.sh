#!/usr/bin/env bash
# Smoke generated Cerberus MCP wrappers with initialize + tools/list.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

ROOT_SHORT="${CERBERUS_ROOT_SHORT:-98853a93}"
GENERATED_DIR="${CERBERUS_GENERATED_DIR:-.meristem/generated/cerberus-$ROOT_SHORT}"

required_tools=(
  '"feed.read"'
  '"work_items.list"'
  '"work_items.get"'
  '"work_items.spawn_child"'
  '"work_items.append_event"'
  '"work_items.update_metadata"'
  '"work_items.transition"'
)

hidden_tools=(
  '"inbox.capture"'
  '"deterministic_errors.list"'
  '"deterministic_errors.get"'
  '"work_items.create"'
)

smoke_one() {
  local head="$1"
  local script="$GENERATED_DIR/$head-meristem-command.sh"
  [[ -x "$script" ]] || {
    echo "missing wrapper for $head: $script" >&2
    return 1
  }

  local output
  output="$(
    {
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}'
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
    } | "$script"
  )"

  grep -q '"protocolVersion":"2025-06-18"' <<< "$output" || {
    echo "$head: initialize response missing expected protocol version" >&2
    echo "$output" >&2
    return 1
  }

  for tool in "${required_tools[@]}"; do
    grep -q "$tool" <<< "$output" || {
      echo "$head: missing required tool $tool" >&2
      echo "$output" >&2
      return 1
    }
  done

  for tool in "${hidden_tools[@]}"; do
    if grep -q "$tool" <<< "$output"; then
      echo "$head: unexpectedly advertised hidden tool $tool" >&2
      echo "$output" >&2
      return 1
    fi
  done

  printf '%s: ok\n' "$head"
}

smoke_one coordinator
smoke_one grower
smoke_one healer
