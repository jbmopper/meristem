#!/usr/bin/env bash
# Background meristem feed watcher: HTTP long-poll, cursor persistence, agent wake
# only when new events arrive. See .cursor/meristem-feed-watcher.md for handoff.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

API="${MERISTEM_API:-http://127.0.0.1:8080}"
CURSOR_FILE="${MERISTEM_FEED_CURSOR_FILE:-.cursor/meristem-feed-cursor}"
NOTE_FILE="${MERISTEM_FEED_NOTE_FILE:-.cursor/meristem-feed-note}"
PENDING_LOG="${MERISTEM_FEED_PENDING_LOG:-.meristem/feed-watch-pending.jsonl}"
WAIT="${MERISTEM_FEED_WAIT:-5s}"
LIMIT="${MERISTEM_FEED_LIMIT:-50}"

resolve_token() {
  if [ -n "${MERISTEM_TOKEN:-}" ]; then
    printf '%s' "$MERISTEM_TOKEN"
    return 0
  fi
  local dir="$REPO_ROOT"
  while [ "$dir" != "/" ]; do
    for f in "$dir/.meristem/root.token"; do
      if [ -f "$f" ]; then
        tr -d '[:space:]' < "$f"
        return 0
      fi
    done
    dir="$(dirname "$dir")"
  done
  return 1
}

TOKEN="$(resolve_token || true)"
if [ -z "$TOKEN" ]; then
  echo "meristem-feed-watch: no token (set MERISTEM_TOKEN or .meristem/*.token)" >&2
  exit 1
fi

PID_FILE="${MERISTEM_FEED_PID_FILE:-.meristem/feed-watch.pid}"
LOG_FILE="${MERISTEM_FEED_LOG_FILE:-.meristem/feed-watch.log}"

mkdir -p "$(dirname "$CURSOR_FILE")" "$(dirname "$NOTE_FILE")" "$(dirname "$PENDING_LOG")" "$(dirname "$PID_FILE")"

printf '%s\n' "$$" >"$PID_FILE"
trap 'rm -f "$PID_FILE"' EXIT INT TERM

process_response() {
  local resp_file="$1"
  python3 - "$CURSOR_FILE" "$NOTE_FILE" "$PENDING_LOG" "$resp_file" <<'PY'
import json, sys, pathlib
from datetime import datetime, timezone

cursor_file, note_file, pending_log, resp_file = sys.argv[1:5]
raw = pathlib.Path(resp_file).read_text(encoding="utf-8")
try:
    r = json.loads(raw)
except json.JSONDecodeError:
    sys.exit(2)

next_cursor = r.get("next_cursor") or ""
if next_cursor:
    pathlib.Path(cursor_file).write_text(next_cursor + "\n")

items = r.get("items") or []
has_more = bool(r.get("has_more"))
if not items:
    sys.exit(1 if has_more else 0)

def compact(item):
    kind = item.get("kind") or "?"
    sid = (item.get("subject_id") or "")[:8]
    occurred = (item.get("occurred_at") or "")[11:19]  # HH:MM:SS
    payload = item.get("payload") or {}
    inner_kind = payload.get("inner_kind") or ""
    if not inner_kind and isinstance(payload.get("inner"), dict):
        inner_kind = payload["inner"].get("inner_kind") or ""
    detail = ""
    if kind == "work_item.transitioned":
        detail = f"{payload.get('from')}->{payload.get('to')}"
    elif kind == "work_item.event_appended" and isinstance(payload.get("inner"), dict):
        inner = payload["inner"]
        detail = inner.get("summary") or inner.get("raw") or ""
        if isinstance(detail, str) and len(detail) > 120:
            detail = detail[:117] + "..."
    elif kind == "work_item.created":
        detail = (payload.get("title") or "")[:80]
    lines = [f"{occurred} {kind} {sid} {inner_kind} {detail}".strip()]
    return {
        "event_id": item.get("event_id"),
        "line": lines[0],
        "kind": kind,
        "subject_id": item.get("subject_id"),
        "inner_kind": inner_kind,
    }

compacts = [compact(i) for i in items]
record = {
    "at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "count": len(items),
    "cursor": next_cursor,
    "event_ids": [c["event_id"] for c in compacts],
    "lines": [c["line"] for c in compacts],
    "items": items,
}
with open(pending_log, "a", encoding="utf-8") as f:
    f.write(json.dumps(record, separators=(",", ":")) + "\n")

note = [
    f"# Meristem feed — {record['at']} ({len(items)} new)",
    f"cursor: {next_cursor}",
    "",
    *record["lines"],
    "",
    "Full payload: .meristem/feed-watch-pending.jsonl (last line)",
    "Handoff: .cursor/meristem-feed-watcher.md",
]
pathlib.Path(note_file).write_text("\n".join(note) + "\n", encoding="utf-8")

print("AGENT_LOOP_WAKE_FEED", json.dumps({
    "prompt": "/meristem-poll-feed digest — read ONLY .cursor/meristem-feed-note; summarize NEW lines briefly; no feed_read snapshot; no unchanged-state restatement.",
    "count": len(items),
    "note_file": note_file,
    "pending_log": pending_log,
    "event_ids": record["event_ids"],
}))
sys.exit(1 if has_more else 0)  # 1 => drain another page immediately
PY
}

fetch_page() {
  local cursor="$1"
  local url="$API/v1/feed?wait=$WAIT&limit=$LIMIT"
  if [ -n "$cursor" ]; then
    local enc
    enc="$(python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$cursor")"
    url="$url&cursor=$enc"
  fi
  local tmp
  tmp="$(mktemp)"
  if curl -sf -H "Authorization: Bearer $TOKEN" "$url" -o "$tmp"; then
    process_response "$tmp"
    local rc=$?
    rm -f "$tmp"
    return "$rc"
  fi
  rm -f "$tmp"
  return 2
}

echo "FEED_WATCH_STARTED pid=$$ wait=$WAIT api=$API" | tee -a "$LOG_FILE"

while true; do
  CURSOR=""
  if [ -f "$CURSOR_FILE" ]; then
    CURSOR="$(tr -d '[:space:]' < "$CURSOR_FILE")"
  fi
  while true; do
    fetch_page "$CURSOR"
    rc=$?
    if [ "$rc" -eq 2 ]; then
      echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) feed curl failed" >>"$LOG_FILE"
      sleep 5
      break
    fi
    if [ -f "$CURSOR_FILE" ]; then
      CURSOR="$(tr -d '[:space:]' < "$CURSOR_FILE")"
    fi
    # rc 0: page drained; rc 1: has_more — fetch again immediately (no wait in URL still ok)
    [ "$rc" -eq 0 ] && break
  done
done
