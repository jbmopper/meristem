#!/usr/bin/env bash
# Event-driven Meristem -> Codex task nudge bridge.
#
# This local adapter tails the canonical REST SSE feed with a dedicated
# read-only identity. It persists routing metadata only, then asks Codex's
# supported app-server lifecycle to nudge one existing task. It never launches
# a free-standing reviewer and never copies Meristem event bodies into a prompt.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

API="${MERISTEM_API:-http://127.0.0.1:8080}"
TOKEN_FILE="${MERISTEM_TOKEN_FILE:-}"
THREAD_ID="${CODEX_THREAD_ID:-}"
WAKE_ACTORS="${MERISTEM_WAKE_ACTOR_TOKEN_IDS:-}"
CODEX_BIN="${CODEX_BIN:-$(command -v codex 2>/dev/null || true)}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 2>/dev/null || true)}"
DURABILITY_PYTHON_BIN="${MERISTEM_DURABILITY_PYTHON_BIN:-/usr/bin/python3}"
NUDGE_HELPER="${CODEX_NUDGE_HELPER:-$REPO_ROOT/scripts/codex-thread-nudge.py}"
CURL_BIN="${CURL_BIN:-$(command -v curl 2>/dev/null || true)}"
JQ_BIN="${JQ_BIN:-$(command -v jq 2>/dev/null || true)}"
MKFIFO_BIN="${MKFIFO_BIN:-$(command -v mkfifo 2>/dev/null || true)}"

STATE_DIR="${MERISTEM_WAKE_STATE_DIR:-$REPO_ROOT/.meristem/codex-stem-bridge}"
CURSOR_FILE="${MERISTEM_WAKE_CURSOR_FILE:-$STATE_DIR/cursor}"
QUEUE_FILE="${MERISTEM_WAKE_QUEUE_FILE:-$STATE_DIR/pending.tsv}"
DELIVERY_FILE="${MERISTEM_WAKE_DELIVERY_FILE:-$STATE_DIR/delivery.tsv}"
MARKER_FILE="${MERISTEM_WAKE_MARKER_FILE:-$STATE_DIR/delivery.json}"
SEEN_FILE="${MERISTEM_WAKE_SEEN_FILE:-$STATE_DIR/seen-event-ids}"
INITIALIZED_FILE="${MERISTEM_WAKE_INITIALIZED_FILE:-$STATE_DIR/initialized}"
LANE_BLOCKED_FILE="${MERISTEM_WAKE_LANE_BLOCKED_FILE:-$STATE_DIR/lane-blocked}"
QUARANTINE_DIR="${MERISTEM_WAKE_QUARANTINE_DIR:-$STATE_DIR/quarantine}"
BRIDGE_LOCK_DIR="${MERISTEM_BRIDGE_LOCK_DIR:-$STATE_DIR/bridge.lock}"
BRIDGE_PID_FILE="$BRIDGE_LOCK_DIR/pid"
WAKE_LOCK_DIR="${MERISTEM_WAKE_LOCK_DIR:-$STATE_DIR/wake.lock}"
WAKE_PID_FILE="$WAKE_LOCK_DIR/pid"
QUEUE_LOCK_DIR="${MERISTEM_QUEUE_LOCK_DIR:-$STATE_DIR/queue.lock}"
QUEUE_PID_FILE="$QUEUE_LOCK_DIR/pid"
LOG_FILE="${MERISTEM_WAKE_LOG_FILE:-$REPO_ROOT/.meristem/logs/codex-stem-bridge.log}"
STREAM_FIFO="${MERISTEM_WAKE_STREAM_FIFO:-$STATE_DIR/stream.fifo}"

COALESCE_SECONDS="${MERISTEM_WAKE_COALESCE_SECONDS:-8}"
RECONNECT_SECONDS="${MERISTEM_WAKE_RECONNECT_SECONDS:-3}"
MAX_WAKE_ATTEMPTS="${MERISTEM_WAKE_MAX_ATTEMPTS:-3}"
WAKE_RETRY_SECONDS="${MERISTEM_WAKE_RETRY_SECONDS:-20}"
WAKE_DEFER_SECONDS="${MERISTEM_WAKE_DEFER_SECONDS:-60}"
REQUEST_TIMEOUT="${MERISTEM_WAKE_REQUEST_TIMEOUT:-30}"
COMPLETION_TIMEOUT="${MERISTEM_WAKE_COMPLETION_TIMEOUT:-1800}"
DRY_RUN="${MERISTEM_WAKE_DRY_RUN:-0}"
MAINTENANCE_ONLY="${MERISTEM_WAKE_MAINTENANCE_ONLY:-0}"
PROCESS_PID="$$"
WAKE_WORKER_PID=""
ACTIVE_NUDGE_PID=""
ACTIVE_STREAM_PID=""
ACTIVE_WAKE_DELAY_PID=""
ACTIVE_LOOP_DELAY_PID=""
STREAM_LAUNCH_SIGNAL=0
NUDGE_LAUNCH_SIGNAL=0
LANE_BLOCK_NOTIFIED=0
LANE_BLOCKED_EXIT=78
LANE_STATE_INVALID_EXIT=79
CURSOR_STATE_INVALID_EXIT=80
FILTER_IDENTITY_RETRYABLE=1
FILTER_IDENTITY_DISPROVED=2
FEED_FILTER_QUERY="scope=assigned&exclude_actor=self"
FEED_FILTER_IDENTITY="assigned-exclude-self-v1"

lane_block_status() {
  local first="" second="" extra="" mode="" size=""

  # Existence is the gate. Never follow a link or echo file contents: this
  # status path is safe to call from a local supervisor without credentials.
  [[ -L "$LANE_BLOCKED_FILE" ]] && return "$LANE_STATE_INVALID_EXIT"
  [[ -e "$LANE_BLOCKED_FILE" ]] || return 0
  [[ -f "$LANE_BLOCKED_FILE" ]] || return "$LANE_STATE_INVALID_EXIT"
  mode="$(stat -f '%Lp' "$LANE_BLOCKED_FILE" 2>/dev/null || true)"
  size="$(stat -f '%z' "$LANE_BLOCKED_FILE" 2>/dev/null || true)"
  [[ "$mode" == "600" ]] || return "$LANE_STATE_INVALID_EXIT"
  [[ "$size" =~ ^[0-9]+$ ]] || return "$LANE_STATE_INVALID_EXIT"
  [[ ${#size} -le 3 ]] || return "$LANE_STATE_INVALID_EXIT"
  (( 10#$size > 0 && 10#$size <= 256 )) || return "$LANE_STATE_INVALID_EXIT"

  {
    IFS= read -r first || return "$LANE_STATE_INVALID_EXIT"
    IFS= read -r second || return "$LANE_STATE_INVALID_EXIT"
    if IFS= read -r extra; then
      return "$LANE_STATE_INVALID_EXIT"
    fi
  } <"$LANE_BLOCKED_FILE"
  [[ "$first" == "version=1" ]] || return "$LANE_STATE_INVALID_EXIT"
  [[ "$second" == "reason=configuration" || "$second" == "reason=ambiguous" ||
     "$second" =~ ^reason=protocol-[0-9]+$ ]] || return "$LANE_STATE_INVALID_EXIT"
  return "$LANE_BLOCKED_EXIT"
}

# A side-effect-free supervisor probe. It intentionally runs before credential,
# binary, URL, directory, lock, queue, and log initialization.
if [[ "$#" -gt 0 ]]; then
  if [[ "$#" == "1" && "$1" == "--health" ]]; then
    lane_block_status
    exit $?
  fi
  printf 'meristem-codex-sse-bridge: unsupported argument\n' >&2
  exit 64
fi

lane_block_status
initial_lane_status=$?
if [[ "$initial_lane_status" != "0" ]]; then
  exit "$initial_lane_status"
fi

die() {
  printf 'meristem-codex-sse-bridge: %s\n' "$*" >&2
  exit 64
}

[[ -n "$TOKEN_FILE" ]] || die "MERISTEM_TOKEN_FILE is required"
[[ -f "$TOKEN_FILE" ]] || die "token file does not exist: $TOKEN_FILE"
[[ -n "$THREAD_ID" ]] || die "CODEX_THREAD_ID is required"
[[ -n "$WAKE_ACTORS" ]] || die "MERISTEM_WAKE_ACTOR_TOKEN_IDS is required"
[[ -x "$CODEX_BIN" ]] || die "codex executable not found"
[[ -x "$PYTHON_BIN" ]] || die "python executable not found"
[[ -x "$DURABILITY_PYTHON_BIN" ]] || die "durability python executable not found"
[[ -f "$NUDGE_HELPER" ]] || die "Codex nudge helper not found"
[[ -x "$CURL_BIN" ]] || die "curl executable not found"
[[ -x "$JQ_BIN" ]] || die "jq executable not found"
[[ -x "$MKFIFO_BIN" ]] || die "mkfifo executable not found"
API_PORT=""
if [[ "$API" =~ ^http://127\.0\.0\.1:([0-9]{1,5})$ ]]; then
  API_PORT="${BASH_REMATCH[1]}"
elif [[ "$API" =~ ^http://\[::1\]:([0-9]{1,5})$ ]]; then
  API_PORT="${BASH_REMATCH[1]}"
else
  die "MERISTEM_API must be an exact loopback HTTP origin with a numeric port"
fi
(( 10#$API_PORT >= 1 && 10#$API_PORT <= 65535 )) || die "MERISTEM_API port is out of range"
case "$DRY_RUN" in 0|1) ;; *) die "MERISTEM_WAKE_DRY_RUN must be 0 or 1" ;; esac
case "$MAINTENANCE_ONLY" in 0|1) ;; *) die "MERISTEM_WAKE_MAINTENANCE_ONLY must be 0 or 1" ;; esac

# Refuse a credential file readable by group/other. There is no fallback to an
# operator/root credential.
TOKEN_MODE="$(stat -f '%Lp' "$TOKEN_FILE" 2>/dev/null || true)"
[[ "$TOKEN_MODE" == "600" ]] || die "token file must have mode 0600 (got ${TOKEN_MODE:-unknown})"

fsync_file_and_parent() {
  "$DURABILITY_PYTHON_BIN" -c '
import os, sys
path = os.path.abspath(sys.argv[1])
fd = os.open(path, os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
fd = os.open(os.path.dirname(path), os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
' "$1" >/dev/null 2>&1
}

fsync_parent() {
  "$DURABILITY_PYTHON_BIN" -c '
import os, sys
fd = os.open(os.path.dirname(os.path.abspath(sys.argv[1])), os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
' "$1" >/dev/null 2>&1
}

durable_replace() {
  "$DURABILITY_PYTHON_BIN" -c '
import os, sys
source = os.path.abspath(sys.argv[1])
target = os.path.abspath(sys.argv[2])
fd = os.open(source, os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
os.replace(source, target)
fd = os.open(os.path.dirname(target), os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
' "$1" "$2" >/dev/null 2>&1
}

umask 077
mkdir -p "$STATE_DIR" "$QUARANTINE_DIR" "$(dirname "$LOG_FILE")"
chmod 700 "$STATE_DIR" "$QUARANTINE_DIR"
touch "$QUEUE_FILE" "$SEEN_FILE" "$LOG_FILE"
chmod 600 "$QUEUE_FILE" "$SEEN_FILE" "$LOG_FILE"
fsync_file_and_parent "$QUEUE_FILE" || die "could not durably initialize queue"
fsync_file_and_parent "$SEEN_FILE" || die "could not durably initialize seen set"

log_meta() {
  # Callers pass allowlisted routing/lifecycle metadata only.
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >>"$LOG_FILE"
}

lock_owner_alive() {
  local pid_file="$1" owner
  owner="$(tr -d '\r\n' <"$pid_file" 2>/dev/null || true)"
  [[ "$owner" =~ ^[0-9]+$ ]] && kill -0 "$owner" 2>/dev/null
}

acquire_singleton_lock() {
  local lock_dir="$1" pid_file="$2" label="$3"
  if [[ -d "$lock_dir" ]]; then
    if lock_owner_alive "$pid_file"; then
      die "$label is already active"
    fi
    rm -f "$pid_file"
    rmdir "$lock_dir" 2>/dev/null || die "stale $label lock has unexpected contents"
  fi
  mkdir "$lock_dir" || die "could not acquire $label lock"
  printf '%s\n' "$$" >"$pid_file"
  chmod 600 "$pid_file"
}

acquire_singleton_lock "$BRIDGE_LOCK_DIR" "$BRIDGE_PID_FILE" "bridge"

stop_active_stream() {
  local pid="$ACTIVE_STREAM_PID" attempt=0
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && (( attempt < 100 )); do
      sleep 0.05
      attempt=$((attempt + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    wait "$pid" 2>/dev/null || true
  fi
  ACTIVE_STREAM_PID=""
  exec 8<&- 2>/dev/null || true
  exec 9>&- 2>/dev/null || true
  if [[ -p "$STREAM_FIFO" ]]; then
    rm -f "$STREAM_FIFO"
  fi
}

stop_loop_delay() {
  local pid="$ACTIVE_LOOP_DELAY_PID"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    wait "$pid" 2>/dev/null || true
  fi
  ACTIVE_LOOP_DELAY_PID=""
}

loop_delay() {
  sleep "$1" &
  ACTIVE_LOOP_DELAY_PID=$!
  wait "$ACTIVE_LOOP_DELAY_PID" 2>/dev/null || true
  ACTIVE_LOOP_DELAY_PID=""
}

reap_finished_wake_worker() {
  local pid="$WAKE_WORKER_PID"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  # The worker removes its PID file only from its EXIT trap. Once that file is
  # gone, waiting is safe and prevents a stale cached PID from ever being used.
  if [[ ! -f "$WAKE_PID_FILE" ]]; then
    wait "$pid" 2>/dev/null || true
    WAKE_WORKER_PID=""
  fi
}

settle_wake_worker_after_lane_block() {
  local pid="$WAKE_WORKER_PID" owner="" attempt=0
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  while (( attempt < 200 )); do
    owner="$(tr -d '\r\n' <"$WAKE_PID_FILE" 2>/dev/null || true)"
    if [[ "$owner" != "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  reap_finished_wake_worker
}

cleanup_bridge() {
  local owner="" pid="$WAKE_WORKER_PID" attempt=0
  stop_active_stream
  stop_loop_delay
  if [[ -f "$WAKE_PID_FILE" ]]; then
    owner="$(tr -d '\r\n' <"$WAKE_PID_FILE" 2>/dev/null || true)"
  fi
  # Signal only a child that is still named by the wake lock. A cached PID
  # whose lock has disappeared is reaped below but is never signaled.
  if [[ "$pid" =~ ^[0-9]+$ && "$owner" == "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    # This outer budget exceeds the worker's helper-cleanup budget, which in
    # turn exceeds the helper's app-server process-group cleanup budget.
    while kill -0 "$pid" 2>/dev/null && (( attempt < 300 )); do
      sleep 0.05
      attempt=$((attempt + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    wait "$pid" 2>/dev/null || true
  fi
  WAKE_WORKER_PID=""
  owner="$(tr -d '\r\n' <"$BRIDGE_PID_FILE" 2>/dev/null || true)"
  if [[ "$owner" == "$$" ]]; then
    rm -f "$BRIDGE_PID_FILE"
    rmdir "$BRIDGE_LOCK_DIR" 2>/dev/null || true
  fi
}
trap cleanup_bridge EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -d "$WAKE_LOCK_DIR" ]]; then
  if lock_owner_alive "$WAKE_PID_FILE"; then
    die "wake worker is still active"
  fi
  rm -f "$WAKE_PID_FILE"
  rmdir "$WAKE_LOCK_DIR" 2>/dev/null || die "stale wake lock has unexpected contents"
fi

acquire_queue_lock() {
  local attempt=0 owner
  while (( attempt < 200 )); do
    if mkdir "$QUEUE_LOCK_DIR" 2>/dev/null; then
      printf '%s\n' "$PROCESS_PID" >"$QUEUE_PID_FILE"
      chmod 600 "$QUEUE_PID_FILE"
      return 0
    fi
    owner="$(tr -d '\r\n' <"$QUEUE_PID_FILE" 2>/dev/null || true)"
    if [[ "$owner" =~ ^[0-9]+$ ]] && ! kill -0 "$owner" 2>/dev/null; then
      rm -f "$QUEUE_PID_FILE"
      rmdir "$QUEUE_LOCK_DIR" 2>/dev/null || true
    fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  log_meta "queue_lock_timeout"
  return 1
}

release_queue_lock() {
  local owner
  owner="$(tr -d '\r\n' <"$QUEUE_PID_FILE" 2>/dev/null || true)"
  if [[ "$owner" == "$PROCESS_PID" ]]; then
    rm -f "$QUEUE_PID_FILE"
    rmdir "$QUEUE_LOCK_DIR" 2>/dev/null || true
  fi
}

write_cursor() {
  local cursor="$1" tmp
  cursor_valid "$cursor" || return 1
  tmp="$CURSOR_FILE.tmp.$$.$PROCESS_PID"
  printf '%s\n' "$cursor" >"$tmp" || return 1
  chmod 600 "$tmp"
  durable_replace "$tmp" "$CURSOR_FILE"
}

cursor_valid() {
  local cursor="$1"
  [[ -n "$cursor" && ${#cursor} -le 4096 && "$cursor" != *[!A-Za-z0-9_-]* ]]
}

cursor_file_valid() {
  local first="" extra="" mode="" size=""

  [[ -L "$CURSOR_FILE" ]] && return 1
  [[ -f "$CURSOR_FILE" ]] || return 1
  mode="$(stat -f '%Lp' "$CURSOR_FILE" 2>/dev/null || true)"
  size="$(stat -f '%z' "$CURSOR_FILE" 2>/dev/null || true)"
  [[ "$mode" == "600" ]] || return 1
  [[ "$size" =~ ^[0-9]+$ ]] || return 1
  [[ ${#size} -le 4 ]] || return 1
  (( 10#$size > 1 && 10#$size <= 4097 )) || return 1
  {
    IFS= read -r first || return 1
    if IFS= read -r extra; then
      return 1
    fi
  } <"$CURSOR_FILE"
  cursor_valid "$first"
}

cursor_identity_marker_valid() {
  local first="" second="" extra="" mode="" size=""

  [[ -L "$INITIALIZED_FILE" ]] && return 1
  [[ -f "$INITIALIZED_FILE" ]] || return 1
  mode="$(stat -f '%Lp' "$INITIALIZED_FILE" 2>/dev/null || true)"
  size="$(stat -f '%z' "$INITIALIZED_FILE" 2>/dev/null || true)"
  [[ "$mode" == "600" ]] || return 1
  [[ "$size" =~ ^[0-9]+$ ]] || return 1
  [[ ${#size} -le 3 ]] || return 1
  (( 10#$size > 0 && 10#$size <= 128 )) || return 1
  {
    IFS= read -r first || return 1
    IFS= read -r second || return 1
    if IFS= read -r extra; then
      return 1
    fi
  } <"$INITIALIZED_FILE"
  [[ "$first" == "version=2" && "$second" == "filter=$FEED_FILTER_IDENTITY" ]]
}

mark_cursor_initialized() {
  local tmp
  if [[ -e "$INITIALIZED_FILE" || -L "$INITIALIZED_FILE" ]]; then
    cursor_identity_marker_valid
    return $?
  fi
  tmp="$INITIALIZED_FILE.tmp.$$.$PROCESS_PID"
  printf 'version=2\nfilter=%s\n' "$FEED_FILTER_IDENTITY" >"$tmp" || return 1
  chmod 600 "$tmp"
  durable_replace "$tmp" "$INITIALIZED_FILE"
}

verify_server_filter_identity() {
  local token="$1" cursor="$2" probe_url probe_response="" probe_body="" probe_status="" error_code="" curl_rc=0

  probe_url="${API%/}/v1/feed?wait=0s&limit=1&cursor=$cursor"
  probe_response="$(printf 'header = "Authorization: Bearer %s"\n' "$token" | \
    "$CURL_BIN" -q --noproxy '*' --proto '=http' --proto-redir '=http' --max-redirs 0 \
      --config - --silent --show-error --output - --write-out $'\n%{http_code}' \
      "$probe_url" 2>/dev/null)" || curl_rc=$?
  if [[ "$curl_rc" != "0" || "$probe_response" != *$'\n'* ]]; then
    unset probe_response probe_body token
    return "$FILTER_IDENTITY_RETRYABLE"
  fi
  probe_status="${probe_response##*$'\n'}"
  probe_body="${probe_response%$'\n'*}"
  if [[ ! "$probe_status" =~ ^[0-9][0-9][0-9]$ ]]; then
    unset probe_response probe_body token
    return "$FILTER_IDENTITY_RETRYABLE"
  fi
  error_code="$(printf '%s' "$probe_body" | "$JQ_BIN" -r '.error.code // ""' 2>/dev/null || true)"
  unset probe_response probe_body token
  if [[ "$probe_status" == "400" && "$error_code" == "cursor_filter_mismatch" ]]; then
    return 0
  fi
  if [[ "$probe_status" =~ ^5[0-9][0-9]$ || "$probe_status" == "408" ||
        "$probe_status" == "425" || "$probe_status" == "429" ]]; then
    return "$FILTER_IDENTITY_RETRYABLE"
  fi
  return "$FILTER_IDENTITY_DISPROVED"
}

block_lane() {
  local reason="$1" tmp lane_rc
  tmp="$LANE_BLOCKED_FILE.tmp.$$.$PROCESS_PID"
  acquire_queue_lock || return 1
  lane_block_status
  lane_rc=$?
  if [[ "$lane_rc" == "$LANE_BLOCKED_EXIT" ]]; then
    release_queue_lock
    return 0
  fi
  if [[ "$lane_rc" != "0" ]]; then
    release_queue_lock
    return 1
  fi
  if ! printf 'version=1\nreason=%s\n' "$reason" >"$tmp"; then
    release_queue_lock
    return 1
  fi
  chmod 600 "$tmp"
  if ! durable_replace "$tmp" "$LANE_BLOCKED_FILE"; then
    rm -f "$tmp"
    release_queue_lock
    return 1
  fi
  release_queue_lock
  log_meta "wake_lane_blocked reason=$reason"
}

advance_cursor_if_lane_open() {
  local cursor="$1" lane_rc
  acquire_queue_lock || return 1
  lane_block_status
  lane_rc=$?
  if [[ "$lane_rc" != "0" ]]; then
    release_queue_lock
    return "$lane_rc"
  fi
  if ! write_cursor "$cursor"; then
    release_queue_lock
    return 1
  fi
  release_queue_lock
}

actor_selected() {
  local actor="$1" candidate old_ifs
  old_ifs="$IFS"
  IFS=','
  for candidate in $WAKE_ACTORS; do
    candidate="$(printf '%s' "$candidate" | tr -d '[:space:]')"
    if [[ -n "$candidate" && "$actor" == "$candidate" ]]; then
      IFS="$old_ifs"
      return 0
    fi
  done
  IFS="$old_ifs"
  return 1
}

kind_selected() {
  case "$1" in
    work_item.created|work_item.transitioned|work_item.event_appended|work_item.metadata_updated|work_item.relation_added)
      return 0
      ;;
    *) return 1 ;;
  esac
}

event_known_locked() {
  local event_id="$1"
  awk -F '\t' -v wanted="$event_id" '$1 == wanted { found=1; exit } END { exit !found }' \
    "$QUEUE_FILE" 2>/dev/null && return 0
  if [[ -f "$DELIVERY_FILE" ]]; then
    awk -F '\t' -v wanted="$event_id" '$1 == wanted { found=1; exit } END { exit !found }' \
      "$DELIVERY_FILE" 2>/dev/null && return 0
  fi
  awk -v wanted="$event_id" '$0 == wanted { found=1; exit } END { exit !found }' \
    "$SEEN_FILE" 2>/dev/null
}

record_delivery_seen_locked() {
  local combined="$SEEN_FILE.combined.$$.$PROCESS_PID" bounded="$SEEN_FILE.bounded.$$.$PROCESS_PID"
  [[ -f "$DELIVERY_FILE" ]] || return 1
  {
    cat "$SEEN_FILE"
    awk -F '\t' 'NF { print $1 }' "$DELIVERY_FILE"
  } >"$combined" || return 1
  awk 'NF && !seen[$0]++' "$combined" | tail -n 2048 >"$bounded" || {
    rm -f "$combined" "$bounded"
    return 1
  }
  chmod 600 "$bounded"
  durable_replace "$bounded" "$SEEN_FILE" || {
    rm -f "$combined" "$bounded"
    return 1
  }
  rm -f "$combined"
}

retire_legacy_artifacts() {
  local result batch target result_count=0 batch_count=0
  # Old codex-result files contain raw app-server/exec transcripts and may
  # include configuration material. They are generated scratch, never evidence.
  for result in "$STATE_DIR"/codex-result.*; do
    [[ -f "$result" ]] || continue
    rm -f "$result"
    result_count=$((result_count + 1))
  done

  acquire_queue_lock || die "could not lock legacy recovery"
  for batch in "$QUEUE_FILE".batch.*; do
    [[ -f "$batch" ]] || continue
    # These batches were already admitted by the retired free-form wake path.
    # Preserve metadata for audit but never replay them.
    while IFS=$'\t' read -r event_id _; do
      [[ -n "$event_id" ]] && printf '%s\n' "$event_id" >>"$SEEN_FILE"
    done <"$batch"
    target="$QUARANTINE_DIR/legacy-$(basename "$batch")"
    [[ -e "$target" ]] && target="$target.$$"
    mv "$batch" "$target"
    chmod 600 "$target"
    batch_count=$((batch_count + 1))
  done
  release_queue_lock
  (( result_count > 0 )) && log_meta "legacy_raw_results_removed count=$result_count"
  (( batch_count > 0 )) && log_meta "legacy_batches_quarantined count=$batch_count"
}

rotate_queue_to_delivery() {
  [[ ! -e "$DELIVERY_FILE" ]] || return 0
  acquire_queue_lock || return 1
  if [[ ! -e "$DELIVERY_FILE" && -s "$QUEUE_FILE" ]]; then
    mv "$QUEUE_FILE" "$DELIVERY_FILE" || {
      release_queue_lock
      return 1
    }
    fsync_parent "$DELIVERY_FILE" || {
      release_queue_lock
      return 1
    }
    : >"$QUEUE_FILE"
    chmod 600 "$QUEUE_FILE" "$DELIVERY_FILE"
    fsync_file_and_parent "$QUEUE_FILE" || {
      release_queue_lock
      return 1
    }
  fi
  release_queue_lock
}

retain_unadmitted_delivery() {
  # Keeping the exact delivery file in place is crash-idempotent. Combining it
  # back into pending would require a cross-file transaction and can duplicate
  # a batch if the host dies between replace and unlink.
  [[ ! -e "$MARKER_FILE" ]] || return 1
  [[ -s "$DELIVERY_FILE" ]]
}

delivery_all_seen_locked() {
  local event_id
  [[ -s "$DELIVERY_FILE" ]] || return 1
  while IFS=$'\t' read -r event_id _; do
    [[ -n "$event_id" ]] || continue
    awk -v wanted="$event_id" '$0 == wanted { found=1; exit } END { exit !found }' \
      "$SEEN_FILE" 2>/dev/null || return 1
  done <"$DELIVERY_FILE"
}

delivery_any_seen_locked() {
  local event_id
  [[ -s "$DELIVERY_FILE" ]] || return 1
  while IFS=$'\t' read -r event_id _; do
    [[ -n "$event_id" ]] || continue
    if awk -v wanted="$event_id" '$0 == wanted { found=1; exit } END { exit !found }' \
      "$SEEN_FILE" 2>/dev/null; then
      return 0
    fi
  done <"$DELIVERY_FILE"
  return 1
}

recover_seen_delivery() {
  local had_marker=0 all_seen=0
  acquire_queue_lock || return 1
  if ! delivery_any_seen_locked; then
    release_queue_lock
    return 2
  fi
  delivery_all_seen_locked && all_seen=1
  [[ -e "$MARKER_FILE" ]] && had_marker=1
  if [[ "$had_marker" == "1" || "$all_seen" == "0" ]]; then
    release_queue_lock
    quarantine_delivery "recovered-seen"
    return $?
  fi
  rm -f "$DELIVERY_FILE"
  fsync_parent "$DELIVERY_FILE" || {
    release_queue_lock
    return 1
  }
  release_queue_lock
  log_meta "seen_delivery_recovered"
}

finish_delivery() {
  acquire_queue_lock || return 1
  if ! record_delivery_seen_locked; then
    release_queue_lock
    return 1
  fi
  rm -f "$DELIVERY_FILE" "$MARKER_FILE"
  fsync_parent "$DELIVERY_FILE" || {
    release_queue_lock
    return 1
  }
  release_queue_lock
}

quarantine_delivery() {
  local reason="$1" stem
  stem="$QUARANTINE_DIR/$(date -u '+%Y%m%dT%H%M%SZ').$$.$reason"
  acquire_queue_lock || return 1
  if ! record_delivery_seen_locked; then
    release_queue_lock
    return 1
  fi
  mv "$DELIVERY_FILE" "$stem.tsv" || {
    release_queue_lock
    return 1
  }
  [[ -f "$MARKER_FILE" ]] && mv "$MARKER_FILE" "$stem.json"
  chmod 600 "$stem.tsv"
  [[ -f "$stem.json" ]] && chmod 600 "$stem.json"
  fsync_parent "$stem.tsv" || {
    release_queue_lock
    return 1
  }
  fsync_parent "$DELIVERY_FILE" || {
    release_queue_lock
    return 1
  }
  release_queue_lock
  log_meta "delivery_quarantined reason=$reason"
}

wake_worker() {
  local count attempt rc retry_delay owner reason recovery_rc worker_identity=""

  # Bash 3.2 has no BASHPID. The parent records this background process's real
  # pid immediately after spawning it; wait for that identity before taking
  # any shared-state lock.
  for _ in $(seq 1 100); do
    worker_identity="$(tr -d '\r\n' <"$WAKE_PID_FILE" 2>/dev/null || true)"
    [[ "$worker_identity" =~ ^[0-9]+$ ]] && break
    sleep 0.01
  done
  [[ "$worker_identity" =~ ^[0-9]+$ ]] || return 1
  PROCESS_PID="$worker_identity"

  cleanup_wake_lock() {
    local stop_attempt=0
    if [[ "$ACTIVE_WAKE_DELAY_PID" =~ ^[0-9]+$ ]] && kill -0 "$ACTIVE_WAKE_DELAY_PID" 2>/dev/null; then
      kill -TERM "$ACTIVE_WAKE_DELAY_PID" 2>/dev/null || true
    fi
    if [[ "$ACTIVE_WAKE_DELAY_PID" =~ ^[0-9]+$ ]]; then
      wait "$ACTIVE_WAKE_DELAY_PID" 2>/dev/null || true
    fi
    ACTIVE_WAKE_DELAY_PID=""
    if [[ "$ACTIVE_NUDGE_PID" =~ ^[0-9]+$ ]] && kill -0 "$ACTIVE_NUDGE_PID" 2>/dev/null; then
      kill -TERM "$ACTIVE_NUDGE_PID" 2>/dev/null || true
      while kill -0 "$ACTIVE_NUDGE_PID" 2>/dev/null && (( stop_attempt < 200 )); do
        sleep 0.05
        stop_attempt=$((stop_attempt + 1))
      done
      if kill -0 "$ACTIVE_NUDGE_PID" 2>/dev/null; then
        kill -KILL "$ACTIVE_NUDGE_PID" 2>/dev/null || true
      fi
      wait "$ACTIVE_NUDGE_PID" 2>/dev/null || true
    fi
    ACTIVE_NUDGE_PID=""
    owner="$(tr -d '\r\n' <"$WAKE_PID_FILE" 2>/dev/null || true)"
    if [[ "$owner" == "$PROCESS_PID" ]]; then
      rm -f "$WAKE_PID_FILE"
      rmdir "$WAKE_LOCK_DIR" 2>/dev/null || true
    fi
  }
  trap cleanup_wake_lock EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  worker_delay() {
    sleep "$1" &
    ACTIVE_WAKE_DELAY_PID=$!
    wait "$ACTIVE_WAKE_DELAY_PID" 2>/dev/null || true
    ACTIVE_WAKE_DELAY_PID=""
  }

  while [[ -s "$DELIVERY_FILE" || -s "$QUEUE_FILE" ]]; do
    if [[ ! -s "$DELIVERY_FILE" ]]; then
      worker_delay "$COALESCE_SECONDS"
      rotate_queue_to_delivery || return 1
    fi
    [[ -s "$DELIVERY_FILE" ]] || continue
    recover_seen_delivery
    recovery_rc=$?
    case "$recovery_rc" in
      0) continue ;;
      2) ;;
      *) return 1 ;;
    esac
    count="$(wc -l <"$DELIVERY_FILE" | tr -d '[:space:]')"

    if [[ "$DRY_RUN" == "1" ]]; then
      if [[ -e "$MARKER_FILE" ]]; then
        quarantine_delivery "dry-run-marked" || true
        return 1
      fi
      if finish_delivery; then
        log_meta "wake_dry_run events=$count"
        continue
      fi
      return 1
    fi

    rc=75
    attempt=1
    while (( attempt <= MAX_WAKE_ATTEMPTS )); do
      NUDGE_LAUNCH_SIGNAL=0
      trap 'NUDGE_LAUNCH_SIGNAL=130' INT
      trap 'NUDGE_LAUNCH_SIGNAL=143' TERM
      "$PYTHON_BIN" "$NUDGE_HELPER" deliver \
        --codex-bin "$CODEX_BIN" \
        --thread-id "$THREAD_ID" \
        --repo-root "$REPO_ROOT" \
        --batch-file "$DELIVERY_FILE" \
        --marker-file "$MARKER_FILE" \
        --request-timeout "$REQUEST_TIMEOUT" \
        --completion-timeout "$COMPLETION_TIMEOUT" \
        --idle-only \
        >/dev/null 2>/dev/null &
      ACTIVE_NUDGE_PID=$!
      trap 'exit 130' INT
      trap 'exit 143' TERM
      if [[ "$NUDGE_LAUNCH_SIGNAL" != "0" ]]; then
        exit "$NUDGE_LAUNCH_SIGNAL"
      fi
      wait "$ACTIVE_NUDGE_PID"
      rc=$?
      ACTIVE_NUDGE_PID=""
      if [[ "$rc" == "0" ]]; then
        if "$JQ_BIN" -e '.state == "completed"' "$MARKER_FILE" >/dev/null 2>&1; then
          break
        fi
        rc=76
        break
      fi
      # Only a pre-dispatch transient with no marker is safe to retry.
      if [[ "$rc" != "75" || -e "$MARKER_FILE" ]]; then
        break
      fi
      log_meta "wake_pre_dispatch_retry events=$count attempt=$attempt"
      retry_delay=$((WAKE_RETRY_SECONDS * attempt))
      (( retry_delay > 300 )) && retry_delay=300
      worker_delay "$retry_delay"
      attempt=$((attempt + 1))
    done

    if [[ "$rc" == "0" ]]; then
      if finish_delivery; then
        log_meta "wake_completed events=$count attempt=$attempt"
        continue
      fi
      log_meta "wake_completion_receipt_failed events=$count"
      return 1
    fi

    if [[ "$rc" == "75" && ! -e "$MARKER_FILE" ]]; then
      retain_unadmitted_delivery || return 1
      log_meta "wake_deferred_pre_dispatch events=$count attempts=$MAX_WAKE_ATTEMPTS"
      worker_delay "$WAKE_DEFER_SECONDS"
      continue
    fi

    case "$rc" in
      64) reason="configuration" ;;
      76) reason="ambiguous" ;;
      77) reason="terminal-failure" ;;
      *) reason="protocol-$rc" ;;
    esac
    # Once admission may have happened, no subsequent batch may reuse this
    # dedicated lane until a human explicitly reconciles/clears the ambiguity.
    # Persist the block before moving evidence so a crash cannot reopen it.
    if [[ -e "$MARKER_FILE" && "$rc" != "77" ]]; then
      block_lane "$reason" || return 1
    fi
    quarantine_delivery "$reason" || true
    return 1
  done
}

ensure_wake_worker() {
  local lane_rc
  reap_finished_wake_worker
  lane_block_status
  lane_rc=$?
  if [[ "$lane_rc" != "0" ]]; then
    if [[ "$LANE_BLOCK_NOTIFIED" == "0" ]]; then
      if [[ "$lane_rc" == "$LANE_BLOCKED_EXIT" ]]; then
        log_meta "wake_lane_remains_blocked"
      else
        log_meta "wake_lane_state_invalid"
      fi
      LANE_BLOCK_NOTIFIED=1
    fi
    return "$lane_rc"
  fi
  LANE_BLOCK_NOTIFIED=0
  [[ -s "$DELIVERY_FILE" || -s "$QUEUE_FILE" ]] || return 0
  if mkdir "$WAKE_LOCK_DIR" 2>/dev/null; then
    wake_worker &
    WAKE_WORKER_PID=$!
    printf '%s\n' "$WAKE_WORKER_PID" >"$WAKE_PID_FILE"
    chmod 600 "$WAKE_PID_FILE"
  fi
}

queue_wake() {
  local event_id="$1" actor="$2" kind="$3" subject_id="$4" cursor="$5" known=0 lane_rc
  acquire_queue_lock || return 1
  lane_block_status
  lane_rc=$?
  if [[ "$lane_rc" != "0" ]]; then
    release_queue_lock
    return "$lane_rc"
  fi
  if event_known_locked "$event_id"; then
    known=1
  else
    printf '%s\t%s\t%s\t%s\n' "$event_id" "$actor" "$kind" "$subject_id" >>"$QUEUE_FILE" || {
      release_queue_lock
      return 1
    }
    fsync_file_and_parent "$QUEUE_FILE" || {
      release_queue_lock
      return 1
    }
  fi
  if ! write_cursor "$cursor"; then
    release_queue_lock
    log_meta "cursor_write_failed event_id=$event_id"
    return 1
  fi
  release_queue_lock
  if [[ "$known" == "0" ]]; then
    log_meta "event_queued event_id=$event_id actor=$actor kind=$kind subject_id=$subject_id"
  fi
}

consume_stream() {
  local token cursor url bootstrap_url bootstrap_body curl_rc=0 parser_rc=0 stream_pid="" lane_rc=0 probe_rc=0
  local -a curl_args

  lane_block_status
  lane_rc=$?
  [[ "$lane_rc" == "0" ]] || return "$lane_rc"

  token="$(tr -d '\r\n' <"$TOKEN_FILE")"
  if [[ -z "$token" ]]; then
    die "token file is empty"
  fi

  cursor=""
  if [[ -e "$CURSOR_FILE" || -L "$CURSOR_FILE" ]]; then
    if ! cursor_file_valid || ! cursor_identity_marker_valid; then
      unset token
      log_meta "cursor_identity_invalid_local_state"
      return "$CURSOR_STATE_INVALID_EXIT"
    fi
    cursor="$(cat "$CURSOR_FILE" 2>/dev/null || true)"
    if ! cursor_valid "$cursor"; then
      unset token
      log_meta "cursor_identity_invalid_local_state"
      return "$CURSOR_STATE_INVALID_EXIT"
    fi
  elif [[ -e "$INITIALIZED_FILE" || -L "$INITIALIZED_FILE" ]]; then
    unset token
    log_meta "cursor_missing_after_initialization"
    return "$CURSOR_STATE_INVALID_EXIT"
  fi
  if [[ -z "$cursor" ]]; then
    bootstrap_url="${API%/}/v1/feed?wait=0s&limit=1&${FEED_FILTER_QUERY}"
    if ! bootstrap_body="$(printf 'header = "Authorization: Bearer %s"\n' "$token" | \
      "$CURL_BIN" -q --noproxy '*' --proto '=http' --proto-redir '=http' --max-redirs 0 \
        --config - --silent --show-error --fail "$bootstrap_url" 2>/dev/null)"; then
      unset token
      log_meta "cursor_bootstrap_failed"
      return 1
    fi
    cursor="$(printf '%s' "$bootstrap_body" | "$JQ_BIN" -r '.next_cursor // ""' 2>/dev/null || true)"
    unset bootstrap_body
    if ! cursor_valid "$cursor"; then
      unset token
      log_meta "cursor_bootstrap_failed"
      return 1
    fi
    verify_server_filter_identity "$token" "$cursor"
    probe_rc=$?
    case "$probe_rc" in
      0) ;;
      "$FILTER_IDENTITY_RETRYABLE")
        unset token
        log_meta "feed_filter_identity_probe_retryable identity=$FEED_FILTER_IDENTITY"
        return 1
        ;;
      *)
        unset token
        log_meta "feed_filter_identity_unverified identity=$FEED_FILTER_IDENTITY"
        return "$CURSOR_STATE_INVALID_EXIT"
        ;;
    esac
    advance_cursor_if_lane_open "$cursor"
    lane_rc=$?
    if [[ "$lane_rc" != "0" ]]; then
      unset token
      return "$lane_rc"
    fi
    if ! mark_cursor_initialized; then
      unset token
      log_meta "cursor_bootstrap_failed"
      return 1
    fi
    log_meta "cursor_bootstrapped"
  else
    verify_server_filter_identity "$token" "$cursor"
    probe_rc=$?
    case "$probe_rc" in
      0) ;;
      "$FILTER_IDENTITY_RETRYABLE")
        unset token
        log_meta "feed_filter_identity_probe_retryable identity=$FEED_FILTER_IDENTITY"
        return 1
        ;;
      *)
        unset token
        log_meta "feed_filter_identity_unverified identity=$FEED_FILTER_IDENTITY"
        return "$CURSOR_STATE_INVALID_EXIT"
        ;;
    esac
  fi

  url="${API%/}/v1/feed/stream?${FEED_FILTER_QUERY}"
  curl_args=(-q --noproxy '*' --proto '=http' --proto-redir '=http' --max-redirs 0 --config - --no-buffer --silent --show-error --fail --header "Last-Event-ID: $cursor" "$url")
  if [[ -e "$STREAM_FIFO" ]]; then
    if [[ ! -p "$STREAM_FIFO" ]]; then
      unset token
      log_meta "stream_fifo_invalid"
      return 1
    fi
    rm -f "$STREAM_FIFO"
  fi
  "$MKFIFO_BIN" "$STREAM_FIFO" || {
    unset token
    log_meta "stream_fifo_create_failed"
    return 1
  }
  chmod 600 "$STREAM_FIFO"
  # Open a temporary read/write descriptor first so neither the dedicated
  # reader nor curl's writer can deadlock the parent while opening the FIFO.
  exec 9<>"$STREAM_FIFO" || {
    unset token
    rm -f "$STREAM_FIFO"
    log_meta "stream_fifo_open_failed"
    return 1
  }
  exec 8<"$STREAM_FIFO" || {
    unset token
    exec 9>&- 2>/dev/null || true
    rm -f "$STREAM_FIFO"
    log_meta "stream_fifo_open_failed"
    return 1
  }

  # Curl is an owned background child. The main shell parses from a FIFO with
  # the read builtin, so TERM is handled immediately even during a quiet,
  # long-lived SSE connection. The bearer crosses only an anonymous pipe.
  STREAM_LAUNCH_SIGNAL=0
  trap 'STREAM_LAUNCH_SIGNAL=130' INT
  trap 'STREAM_LAUNCH_SIGNAL=143' TERM
  printf 'header = "Authorization: Bearer %s"\n' "$token" |
    "$CURL_BIN" "${curl_args[@]}" >"$STREAM_FIFO" 2>/dev/null &
  ACTIVE_STREAM_PID=$!
  stream_pid="$ACTIVE_STREAM_PID"
  exec 9>&-
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [[ "$STREAM_LAUNCH_SIGNAL" != "0" ]]; then
    exit "$STREAM_LAUNCH_SIGNAL"
  fi
  unset token

  local line frame_cursor="" frame_event_id="" frame_actor="" frame_source="" frame_kind="" frame_subject="" fields="" frame_has_id=0 frame_has_data=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    case "$line" in
      id:\ *)
        if [[ "$frame_has_id" == "1" || "$frame_has_data" == "1" ]]; then
          log_meta "stream_frame_invalid_structure"
          parser_rc=1
          break
        fi
        frame_has_id=1
        frame_cursor="${line#id: }"
        ;;
      data:\ *)
        if [[ "$frame_has_id" != "1" || "$frame_has_data" == "1" ]]; then
          log_meta "stream_frame_invalid_structure"
          parser_rc=1
          break
        fi
        if ! fields="$(printf '%s' "${line#data: }" | "$JQ_BIN" -er '
          def nonempty_string: type == "string" and length > 0;
          if ((.event_id | nonempty_string) and
              (.actor_token_id | nonempty_string) and
              (.source | nonempty_string) and
              (.kind | nonempty_string) and
              (.subject_id | nonempty_string))
          then [.event_id, .actor_token_id, .source, .kind, .subject_id] | @tsv
          else error("invalid routing metadata")
          end' 2>/dev/null)"; then
          log_meta "stream_frame_invalid_metadata"
          parser_rc=1
          break
        fi
        frame_has_data=1
        IFS=$'\t' read -r frame_event_id frame_actor frame_source frame_kind frame_subject <<<"$fields"
        ;;
      :*)
        ensure_wake_worker
        lane_rc=$?
        if [[ "$lane_rc" != "0" ]]; then
          parser_rc="$lane_rc"
          break
        fi
        ;;
      '')
        if [[ "$frame_has_id" == "1" || "$frame_has_data" == "1" ]]; then
          if [[ "$frame_has_id" != "1" || "$frame_has_data" != "1" || -z "$frame_cursor" ]]; then
            log_meta "stream_frame_invalid_structure"
            parser_rc=1
            break
          fi
          if [[ -z "$frame_event_id" || -z "$frame_actor" || -z "$frame_source" || -z "$frame_kind" || -z "$frame_subject" ]]; then
            log_meta "stream_frame_invalid_metadata"
            parser_rc=1
            break
          fi
          if [[ "$frame_source" == "agent" ]] && actor_selected "$frame_actor" && kind_selected "$frame_kind"; then
            queue_wake "$frame_event_id" "$frame_actor" "$frame_kind" "$frame_subject" "$frame_cursor"
            lane_rc=$?
            if [[ "$lane_rc" != "0" ]]; then
              parser_rc="$lane_rc"
              break
            fi
            # Hand control back to the bridge loop so it can start the wake
            # worker as a directly owned, waitable child.
            parser_rc=10
            break
          else
            advance_cursor_if_lane_open "$frame_cursor"
            lane_rc=$?
            if [[ "$lane_rc" != "0" ]]; then
              parser_rc="$lane_rc"
              break
            fi
          fi
        fi
        frame_cursor=""
        frame_event_id=""
        frame_actor=""
        frame_source=""
        frame_kind=""
        frame_subject=""
        frame_has_id=0
        frame_has_data=0
        ;;
    esac
  done <&8

  if [[ "$parser_rc" == "0" && ( "$frame_has_id" == "1" || "$frame_has_data" == "1" ) ]]; then
    log_meta "stream_frame_truncated"
    parser_rc=1
  fi

  if [[ "$parser_rc" != "0" ]] && kill -0 "$stream_pid" 2>/dev/null; then
    kill -TERM "$stream_pid" 2>/dev/null || true
  fi
  wait "$stream_pid" 2>/dev/null
  curl_rc=$?
  ACTIVE_STREAM_PID=""
  exec 8<&-
  rm -f "$STREAM_FIFO"
  log_meta "stream_disconnected curl_rc=$curl_rc parser_rc=$parser_rc"
  if [[ "$parser_rc" == "$LANE_BLOCKED_EXIT" || "$parser_rc" == "$LANE_STATE_INVALID_EXIT" ||
        "$parser_rc" == "$CURSOR_STATE_INVALID_EXIT" ]]; then
    return "$parser_rc"
  fi
  return 1
}

retire_legacy_artifacts
if [[ -e "$MARKER_FILE" && ! -s "$DELIVERY_FILE" ]]; then
  orphan="$QUARANTINE_DIR/$(date -u '+%Y%m%dT%H%M%SZ').$$.orphan-marker.json"
  mv "$MARKER_FILE" "$orphan"
  chmod 600 "$orphan"
  log_meta "orphan_marker_quarantined"
fi
if [[ "$MAINTENANCE_ONLY" == "1" ]]; then
  log_meta "maintenance_completed"
  exit 0
fi
log_meta "bridge_started api=${API%/} feed_identity=$FEED_FILTER_IDENTITY actor_count=$(printf '%s' "$WAKE_ACTORS" | awk -F, '{print NF}') dry_run=$DRY_RUN"
bridge_rc=0
ensure_wake_worker || bridge_rc=$?
if [[ "$bridge_rc" == "$LANE_BLOCKED_EXIT" || "$bridge_rc" == "$LANE_STATE_INVALID_EXIT" ||
      "$bridge_rc" == "$CURSOR_STATE_INVALID_EXIT" ]]; then
  settle_wake_worker_after_lane_block
  exit "$bridge_rc"
fi
while true; do
  bridge_rc=0
  consume_stream 2>/dev/null || bridge_rc=$?
  if [[ "$bridge_rc" == "$LANE_BLOCKED_EXIT" || "$bridge_rc" == "$LANE_STATE_INVALID_EXIT" ||
        "$bridge_rc" == "$CURSOR_STATE_INVALID_EXIT" ]]; then
    settle_wake_worker_after_lane_block
    exit "$bridge_rc"
  fi
  bridge_rc=0
  ensure_wake_worker || bridge_rc=$?
  if [[ "$bridge_rc" == "$LANE_BLOCKED_EXIT" || "$bridge_rc" == "$LANE_STATE_INVALID_EXIT" ||
        "$bridge_rc" == "$CURSOR_STATE_INVALID_EXIT" ]]; then
    settle_wake_worker_after_lane_block
    exit "$bridge_rc"
  fi
  loop_delay "$RECONNECT_SECONDS"
done
