#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/meristem-codex-bridge-test.XXXXXX")"
BRIDGE_PID=""
STOP_FORCED=0

stop_bridge() {
  local attempt=0
  STOP_FORCED=0
  if [[ -n "$BRIDGE_PID" ]] && kill -0 "$BRIDGE_PID" 2>/dev/null; then
    kill -TERM "$BRIDGE_PID" 2>/dev/null || true
    while kill -0 "$BRIDGE_PID" 2>/dev/null && (( attempt < 400 )); do
      sleep 0.05
      attempt=$((attempt + 1))
    done
    if kill -0 "$BRIDGE_PID" 2>/dev/null; then
      STOP_FORCED=1
      kill -KILL "$BRIDGE_PID" 2>/dev/null || true
    fi
    wait "$BRIDGE_PID" 2>/dev/null || true
  fi
  BRIDGE_PID=""
}

cleanup() {
  stop_bridge
  if [[ "${KEEP_TMP:-0}" == "1" ]]; then
    printf 'bridge test state retained at %s\n' "$TMP_ROOT" >&2
  else
    rm -rf "$TMP_ROOT"
  fi
}
trap cleanup EXIT
trap 'printf "bridge test failed at line %s\n" "$LINENO" >&2' ERR

TOKEN_FILE="$TMP_ROOT/listener.token"
FAKE_CURL="$TMP_ROOT/fake-curl"
FAKE_CODEX="$TMP_ROOT/fake-codex"
BLOCKING_CODEX="$TMP_ROOT/blocking-codex"
BLOCKING_CODEX_PID="$TMP_ROOT/blocking-codex.pid"
FAKE_PYTHON="$TMP_ROOT/fake-python"
SIGNAL_STARTED="$TMP_ROOT/helper-started"
SIGNAL_TERM="$TMP_ROOT/helper-terminated"
SIGNAL_HELPER_PID="$TMP_ROOT/helper.pid"
SIGNAL_CHILD_PID="$TMP_ROOT/helper-child.pid"
LAUNCH_CHILD_PID="$TMP_ROOT/launch-child.pid"
NUDGE_LAUNCH_HELPER_PID="$TMP_ROOT/nudge-launch-helper.pid"
mkdir -p "$TMP_ROOT/tmp"

printf '%s\n' 'test-token-never-forwarded' >"$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

printf '%s\n' \
  '#!/bin/sh' \
  '[ "$1" = "-q" ] || exit 97' \
  'saw_noproxy=0' \
  'previous=""' \
  'last=""' \
  'for arg in "$@"; do' \
  '  [ "$previous" = "--noproxy" ] && [ "$arg" = "*" ] && saw_noproxy=1' \
  '  previous="$arg"' \
  '  last="$arg"' \
  'done' \
  '[ "$saw_noproxy" = "1" ] || exit 96' \
  'case "$last" in' \
  '  *"/v1/feed?"*) printf '\''%s\n'\'' '\''{"next_cursor":"cursor-0"}'\'' ;;' \
  '  *"/v1/feed/stream")' \
  '    count=0' \
  '    [ -f "$FAKE_CURL_COUNT_FILE" ] && count=$(cat "$FAKE_CURL_COUNT_FILE")' \
  '    count=$((count + 1))' \
  '    printf '\''%s\n'\'' "$count" >"$FAKE_CURL_COUNT_FILE"' \
  '    case "$FAKE_CURL_SCENARIO" in' \
  '      blocking)' \
  '        exec sleep 300' \
  '        ;;' \
  '      launch_signal)' \
  '        printf '\''%s\n'\'' "$$" >"$LAUNCH_CHILD_PID"' \
  '        kill -TERM "$(cat "$FAKE_BRIDGE_PID_FILE")"' \
  '        exec sleep 300' \
  '        ;;' \
  '      malformed)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {not-json'\'' '\'''\''' \
  '        ;;' \
  '      missing_event)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      missing_actor)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"event_id":"event-1","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      missing_source)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      missing_kind)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      missing_id)' \
  '        printf '\''%s\n'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      multiple_data)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      truncated)' \
  '        printf '\''%s\n'\'' '\''id: cursor-bad'\'' '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\''' \
  '        ;;' \
  '      selected_then_blocking)' \
  '        if [ "$count" -gt 1 ]; then exec sleep 300; fi' \
  '        printf '\''%s\n'\'' "id: cursor-$count" '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '      incrementing)' \
  '        printf '\''%s\n'\'' "id: cursor-$count" "data: {\"event_id\":\"event-$count\",\"actor_token_id\":\"claude-test\",\"source\":\"agent\",\"kind\":\"work_item.event_appended\",\"subject_id\":\"item-$count\"}" '\'''\''' \
  '        ;;' \
  '      *)' \
  '        cursor="cursor-$count"' \
  '        printf '\''%s\n'\'' "id: $cursor" '\''data: {"event_id":"event-1","actor_token_id":"claude-test","source":"agent","kind":"work_item.event_appended","subject_id":"item-1"}'\'' '\'''\''' \
  '        ;;' \
  '    esac' \
  '    ;;' \
  '  *) exit 22 ;;' \
  'esac' >"$FAKE_CURL"
chmod 700 "$FAKE_CURL"

# A tiny app-server fixture exercises the real nudge helper through successful
# initialize/resume/start/completion, without opening a real Codex task.
printf '%s\n' \
  '#!/usr/bin/env python3' \
  'import json, sys' \
  'def send(value):' \
  '    print(json.dumps(value, separators=(",", ":")), flush=True)' \
  'for raw in sys.stdin:' \
  '    message = json.loads(raw)' \
  '    method = message.get("method")' \
  '    if method == "initialize":' \
  '        send({"id": message["id"], "result": {}})' \
  '    elif method == "initialized":' \
  '        continue' \
  '    elif method == "thread/resume":' \
  '        thread_id = message["params"]["threadId"]' \
  '        send({"id": message["id"], "result": {"thread": {"id": thread_id, "status": {"type": "idle"}, "turns": []}}})' \
  '    elif method == "turn/start":' \
  '        thread_id = message["params"]["threadId"]' \
  '        send({"id": message["id"], "result": {"turn": {"id": "fixture-turn"}}})' \
  '        send({"method": "turn/completed", "params": {"threadId": thread_id, "turn": {"id": "fixture-turn", "status": "completed", "items": []}}})' >"$FAKE_CODEX"
chmod 700 "$FAKE_CODEX"

# A blocking app-server fixture lets the shutdown test prove that terminating
# the bridge also reaches the real helper's separately-sessioned child.
printf '%s\n' \
  '#!/usr/bin/env python3' \
  'import json, os, sys, time' \
  "open(\"$BLOCKING_CODEX_PID\", \"w\").write(str(os.getpid()))" \
  'def send(value):' \
  '    print(json.dumps(value, separators=(",", ":")), flush=True)' \
  'for raw in sys.stdin:' \
  '    message = json.loads(raw)' \
  '    method = message.get("method")' \
  '    if method == "initialize":' \
  '        send({"id": message["id"], "result": {}})' \
  '    elif method == "initialized":' \
  '        continue' \
  '    elif method == "thread/resume":' \
  '        thread_id = message["params"]["threadId"]' \
  '        send({"id": message["id"], "result": {"thread": {"id": thread_id, "status": {"type": "idle"}, "turns": []}}})' \
  '    elif method == "turn/start":' \
  '        send({"id": message["id"], "result": {"turn": {"id": "blocking-turn"}}})' \
  '        while True: time.sleep(60)' >"$BLOCKING_CODEX"
chmod 700 "$BLOCKING_CODEX"

# This fixture stands in for Python only in failure/termination shell tests.
# It never receives a credential; it sees the metadata-only delivery arguments.
printf '%s\n' \
  '#!/bin/sh' \
  'marker=""' \
  'while [ "$#" -gt 0 ]; do' \
  '  if [ "$1" = "--marker-file" ]; then marker="$2"; shift 2; else shift; fi' \
  'done' \
  'calls=0' \
  '[ -f "$FAKE_HELPER_CALLS" ] && calls=$(cat "$FAKE_HELPER_CALLS")' \
  'calls=$((calls + 1))' \
  'printf '\''%s\n'\'' "$calls" >"$FAKE_HELPER_CALLS"' \
  'case "$FAKE_HELPER_SCENARIO" in' \
  '  transient) exit 75 ;;' \
  '  ambiguous)' \
  '    printf '\''%s\n'\'' '\''{"state":"dispatching"}'\'' >"$marker"' \
  '    chmod 600 "$marker"' \
  '    exit 76' \
  '    ;;' \
  '  signal)' \
  '    child=""' \
  '    trap '\''touch "$SIGNAL_TERM"; [ -n "$child" ] && kill -TERM "$child" 2>/dev/null; [ -n "$child" ] && wait "$child" 2>/dev/null; exit 143'\'' TERM INT' \
  '    printf '\''%s\n'\'' "$$" >"$SIGNAL_HELPER_PID"' \
  '    sleep 300 &' \
  '    child=$!' \
  '    printf '\''%s\n'\'' "$child" >"$SIGNAL_CHILD_PID"' \
  '    touch "$SIGNAL_STARTED"' \
  '    wait "$child"' \
  '    ;;' \
  '  launch_signal)' \
  '    printf '\''%s\n'\'' "$$" >"$NUDGE_LAUNCH_HELPER_PID"' \
  '    kill -TERM "$(cat "$FAKE_WAKE_PID_FILE")"' \
  '    exec sleep 300' \
  '    ;;' \
  '  *) exit 64 ;;' \
  'esac' >"$FAKE_PYTHON"
chmod 700 "$FAKE_PYTHON"

wait_for_log() {
  local wanted="$1"
  for _ in $(seq 1 200); do
    if [[ -f "$LOG_FILE" ]] && grep -Fq "$wanted" "$LOG_FILE"; then
      return 0
    fi
    sleep 0.05
  done
  printf 'timed out waiting for log marker: %s\n' "$wanted" >&2
  [[ -f "$LOG_FILE" ]] && sed -n '1,120p' "$LOG_FILE" >&2
  return 1
}

wait_for_file() {
  local wanted="$1"
  for _ in $(seq 1 200); do
    [[ -e "$wanted" ]] && return 0
    sleep 0.05
  done
  return 1
}

start_bridge() {
  local name="$1" curl_scenario="$2" dry_run="$3" python_bin="$4" helper_scenario="${5:-none}" codex_bin="${6:-$FAKE_CODEX}"
  STATE_DIR="$TMP_ROOT/$name-state"
  LOG_FILE="$TMP_ROOT/$name.log"
  COUNT_FILE="$TMP_ROOT/$name-curl-count"
  HELPER_CALLS="$TMP_ROOT/$name-helper-calls"
  env \
    TMPDIR="$TMP_ROOT/tmp" \
    MERISTEM_API="http://127.0.0.1:8080" \
    MERISTEM_TOKEN_FILE="$TOKEN_FILE" \
    CODEX_THREAD_ID="dedicated-listener-thread" \
    MERISTEM_WAKE_ACTOR_TOKEN_IDS="claude-test" \
    CODEX_BIN="$codex_bin" \
    PYTHON_BIN="$python_bin" \
    CURL_BIN="$FAKE_CURL" \
    JQ_BIN="$(command -v jq)" \
    MERISTEM_WAKE_STATE_DIR="$STATE_DIR" \
    MERISTEM_WAKE_LOG_FILE="$LOG_FILE" \
    MERISTEM_WAKE_COALESCE_SECONDS=0 \
    MERISTEM_WAKE_RECONNECT_SECONDS=0.1 \
    MERISTEM_WAKE_MAX_ATTEMPTS=1 \
    MERISTEM_WAKE_RETRY_SECONDS=0 \
    MERISTEM_WAKE_DEFER_SECONDS=0.2 \
    MERISTEM_WAKE_REQUEST_TIMEOUT=3 \
    MERISTEM_WAKE_COMPLETION_TIMEOUT=3 \
    MERISTEM_WAKE_DRY_RUN="$dry_run" \
    FAKE_CURL_SCENARIO="$curl_scenario" \
    FAKE_CURL_COUNT_FILE="$COUNT_FILE" \
    FAKE_BRIDGE_PID_FILE="$STATE_DIR/bridge.lock/pid" \
    FAKE_HELPER_SCENARIO="$helper_scenario" \
    FAKE_HELPER_CALLS="$HELPER_CALLS" \
    FAKE_WAKE_PID_FILE="$STATE_DIR/wake.lock/pid" \
    SIGNAL_STARTED="$SIGNAL_STARTED" \
    SIGNAL_TERM="$SIGNAL_TERM" \
    SIGNAL_HELPER_PID="$SIGNAL_HELPER_PID" \
    SIGNAL_CHILD_PID="$SIGNAL_CHILD_PID" \
    LAUNCH_CHILD_PID="$LAUNCH_CHILD_PID" \
    NUDGE_LAUNCH_HELPER_PID="$NUDGE_LAUNCH_HELPER_PID" \
    /bin/bash "$REPO_ROOT/scripts/meristem-codex-sse-bridge.sh" &
  BRIDGE_PID=$!
}

# URL userinfo cannot smuggle the bearer off loopback.
if env \
  MERISTEM_API="http://127.0.0.1:8080@evil.example" \
  MERISTEM_TOKEN_FILE="$TOKEN_FILE" \
  CODEX_THREAD_ID="dedicated-listener-thread" \
  MERISTEM_WAKE_ACTOR_TOKEN_IDS="claude-test" \
  CODEX_BIN="$FAKE_CODEX" \
  PYTHON_BIN="/usr/bin/python3" \
  CURL_BIN="$FAKE_CURL" \
  JQ_BIN="$(command -v jq)" \
  MERISTEM_WAKE_STATE_DIR="$TMP_ROOT/invalid-origin-state" \
  MERISTEM_WAKE_LOG_FILE="$TMP_ROOT/invalid-origin.log" \
  /bin/bash "$REPO_ROOT/scripts/meristem-codex-sse-bridge.sh" >/dev/null 2>&1; then
  printf 'invalid loopback origin was accepted\n' >&2
  exit 1
fi

# Duplicate event IDs across reconnects produce one wake and advance safely.
start_bridge dedupe selected 1 /usr/bin/python3
wait_for_log 'wake_dry_run events=1'
for _ in $(seq 1 100); do
  [[ -f "$STATE_DIR/cursor" ]] && [[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-2" ]] && break
  sleep 0.05
done

[[ "$(grep -Fc 'event_queued event_id=event-1' "$LOG_FILE")" == "1" ]]
grep -Fxq 'event-1' "$STATE_DIR/seen-event-ids"
[[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-2" ]]
[[ ! -e "$STATE_DIR/delivery.tsv" ]]
[[ ! -e "$STATE_DIR/delivery.json" ]]
[[ ! -s "$STATE_DIR/pending.tsv" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# A malformed frame aborts the stream and leaves the prior cursor untouched,
# so reconnect can replay it after the producer is healthy.
start_bridge malformed malformed 1 /usr/bin/python3
wait_for_log 'stream_frame_invalid_metadata'
[[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-0" ]]
[[ ! -s "$STATE_DIR/pending.tsv" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# Every required routing field is validated before TSV rendering, so Bash 3.2
# whitespace collapsing cannot shift a missing middle field into another slot.
for missing in missing_event missing_actor missing_source missing_kind; do
  start_bridge "$missing" "$missing" 1 /usr/bin/python3
  wait_for_log 'stream_frame_invalid_metadata'
  [[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-0" ]]
  [[ ! -s "$STATE_DIR/pending.tsv" ]]
  stop_bridge
  [[ "$STOP_FORCED" == "0" ]]
done

for structure in missing_id multiple_data; do
  start_bridge "$structure" "$structure" 1 /usr/bin/python3
  wait_for_log 'stream_frame_invalid_structure'
  [[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-0" ]]
  [[ ! -s "$STATE_DIR/pending.tsv" ]]
  stop_bridge
  [[ "$STOP_FORCED" == "0" ]]
done

start_bridge truncated truncated 1 /usr/bin/python3
wait_for_log 'stream_frame_truncated'
[[ "$(tr -d '\r\n' <"$STATE_DIR/cursor")" == "cursor-0" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# A quiet long-poll is an owned background curl child; TERM reaches cleanup
# immediately instead of waiting for the remote stream to disconnect.
start_bridge blocking blocking 1 /usr/bin/python3
wait_for_file "$COUNT_FILE"
stop_bridge
[[ "$STOP_FORCED" == "0" ]]
[[ ! -d "$STATE_DIR/bridge.lock" ]]
[[ ! -p "$STATE_DIR/stream.fifo" ]]

# TERM in the narrow background-launch window is latched until $! is recorded,
# then normal EXIT cleanup owns and reaps the stream child.
start_bridge launch-race launch_signal 1 /usr/bin/python3
wait_for_file "$LAUNCH_CHILD_PID"
launch_pid="$(tr -d '\r\n' <"$LAUNCH_CHILD_PID")"
for _ in $(seq 1 200); do
  ! kill -0 "$BRIDGE_PID" 2>/dev/null && break
  sleep 0.05
done
! kill -0 "$BRIDGE_PID" 2>/dev/null
wait "$BRIDGE_PID" 2>/dev/null || true
BRIDGE_PID=""
! kill -0 "$launch_pid" 2>/dev/null
[[ ! -d "$STATE_DIR/bridge.lock" ]]
[[ ! -p "$STATE_DIR/stream.fifo" ]]

# Once initialization is durable, loss of the cursor fails closed instead of
# re-bootstrapping at a newer feed tip.
mkdir -p "$TMP_ROOT/missing-cursor-state"
printf '%s\n' v1 >"$TMP_ROOT/missing-cursor-state/initialized"
chmod 600 "$TMP_ROOT/missing-cursor-state/initialized"
start_bridge missing-cursor blocking 1 /usr/bin/python3
wait_for_log 'cursor_missing_after_initialization'
[[ ! -e "$COUNT_FILE" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# A crash after the seen receipt is durable but before delivery unlink is
# recovered without a second wake or reliance on PID state.
mkdir -p "$TMP_ROOT/recovery-state"
printf '%s\t%s\t%s\t%s\n' event-1 claude-test work_item.event_appended item-1 >"$TMP_ROOT/recovery-state/delivery.tsv"
printf '%s\n' event-1 >"$TMP_ROOT/recovery-state/seen-event-ids"
: >"$TMP_ROOT/recovery-state/pending.tsv"
chmod 600 "$TMP_ROOT/recovery-state/delivery.tsv" "$TMP_ROOT/recovery-state/seen-event-ids" "$TMP_ROOT/recovery-state/pending.tsv"
start_bridge recovery blocking 1 /usr/bin/python3
wait_for_log 'seen_delivery_recovered'
[[ ! -e "$STATE_DIR/delivery.tsv" ]]
! grep -Fq 'wake_dry_run' "$LOG_FILE"
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# Exercise the real Python helper and app-server protocol end to end.
start_bridge success selected 0 /usr/bin/python3
wait_for_log 'wake_completed events=1'
grep -Fxq 'event-1' "$STATE_DIR/seen-event-ids"
[[ ! -e "$STATE_DIR/delivery.tsv" ]]
[[ ! -e "$STATE_DIR/delivery.json" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# A pre-dispatch transient keeps the exact batch and retries on its own bounded
# timer even when the parent has returned to a quiet SSE long-poll.
start_bridge transient selected_then_blocking 0 "$FAKE_PYTHON" transient
wait_for_log 'wake_deferred_pre_dispatch events=1 attempts=1'
for _ in $(seq 1 100); do
  [[ "$(grep -Fc 'wake_deferred_pre_dispatch events=1 attempts=1' "$LOG_FILE")" -ge 2 ]] && break
  sleep 0.05
done
[[ "$(grep -Fc 'wake_deferred_pre_dispatch events=1 attempts=1' "$LOG_FILE")" -ge 2 ]]
[[ ! -s "$STATE_DIR/pending.tsv" ]]
[[ -s "$STATE_DIR/delivery.tsv" ]]
[[ ! -e "$STATE_DIR/delivery.json" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# TERM during helper launch is latched until its PID is owned, then cleanup
# reaches that helper before the worker lock is released.
start_bridge helper-launch selected_then_blocking 0 "$FAKE_PYTHON" launch_signal
wait_for_file "$NUDGE_LAUNCH_HELPER_PID"
nudge_launch_pid="$(tr -d '\r\n' <"$NUDGE_LAUNCH_HELPER_PID")"
for _ in $(seq 1 200); do
  ! kill -0 "$nudge_launch_pid" 2>/dev/null && [[ ! -e "$STATE_DIR/wake.lock/pid" ]] && break
  sleep 0.05
done
! kill -0 "$nudge_launch_pid" 2>/dev/null
[[ ! -e "$STATE_DIR/wake.lock/pid" ]]
kill -0 "$BRIDGE_PID" 2>/dev/null
[[ -s "$STATE_DIR/delivery.tsv" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# Any marker plus an uncertain outcome is quarantined and never replayed.
start_bridge ambiguous incrementing 0 "$FAKE_PYTHON" ambiguous
wait_for_log 'delivery_quarantined reason=ambiguous'
find "$STATE_DIR/quarantine" -name '*.ambiguous.tsv' -print -quit | grep -q .
find "$STATE_DIR/quarantine" -name '*.ambiguous.json' -print -quit | grep -q .
[[ -e "$STATE_DIR/lane-blocked" ]]
for _ in $(seq 1 100); do
  [[ -s "$STATE_DIR/pending.tsv" ]] && break
  sleep 0.05
done
[[ -s "$STATE_DIR/pending.tsv" ]]
[[ "$(tr -d '\r\n' <"$HELPER_CALLS")" == "1" ]]
stop_bridge
[[ "$STOP_FORCED" == "0" ]]

# Stopping the bridge propagates through worker -> helper -> helper child and
# waits for the chain before releasing its locks.
start_bridge signal selected 0 "$FAKE_PYTHON" signal
wait_for_file "$SIGNAL_STARTED"
helper_pid="$(tr -d '\r\n' <"$SIGNAL_HELPER_PID")"
child_pid="$(tr -d '\r\n' <"$SIGNAL_CHILD_PID")"
stop_bridge
[[ "$STOP_FORCED" == "0" ]]
[[ -e "$SIGNAL_TERM" ]]
! kill -0 "$helper_pid" 2>/dev/null
! kill -0 "$child_pid" 2>/dev/null
[[ ! -d "$STATE_DIR/bridge.lock" ]]
[[ ! -d "$STATE_DIR/wake.lock" ]]

# Repeat shutdown through the real helper and verify its separately-sessioned
# app-server process is also gone before the bridge reports stopped.
start_bridge actual-signal selected 0 /usr/bin/python3 none "$BLOCKING_CODEX"
wait_for_file "$BLOCKING_CODEX_PID"
blocking_pid="$(tr -d '\r\n' <"$BLOCKING_CODEX_PID")"
for _ in $(seq 1 100); do
  [[ -f "$STATE_DIR/delivery.json" ]] && grep -Fq '"state":"accepted"' "$STATE_DIR/delivery.json" && break
  sleep 0.05
done
grep -Fq '"state":"accepted"' "$STATE_DIR/delivery.json"
stop_bridge
[[ "$STOP_FORCED" == "0" ]]
! kill -0 "$blocking_pid" 2>/dev/null
[[ ! -d "$STATE_DIR/bridge.lock" ]]
[[ ! -d "$STATE_DIR/wake.lock" ]]

# No raw transcripts or bearer-bearing curl config copies are ever persisted.
! find "$TMP_ROOT" -name 'codex-result.*' -print -quit | grep -q .
! find "$TMP_ROOT/tmp" -name 'meristem-codex-sse.*' -print -quit | grep -q .

printf 'meristem_codex_sse_bridge_test: ok\n'
