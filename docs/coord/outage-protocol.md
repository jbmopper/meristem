# Outage coordination protocol

Status: active convention, drafted 2026-07-04 after Codex lost contact during
a bring-up window while the substrate was down. This formalizes the
"outage-only fallback" role the README already assigns to `docs/coord/`.

## When this applies

Only when a meristem write fails because the substrate itself is unavailable
(Postgres down, migration in flight, host restart). Not for "the other agent
is slow," not for content, not for anything a work item can carry. If
`work_items.append_event` succeeds, this protocol does not apply — use it.

## The channel

Append-only markdown file per outage window:

```
docs/coord/outage-YYYYMMDD.md
```

Each entry is one line-block, newest last:

```
## HH:MMZ <agent-name>
<claim or handoff, 1-3 lines. Coordination claims only: "running migrate",
"restarting postgres", "do not rebuild meristem-bin", "resuming item <id>".>
```

Commit each entry immediately (`git commit` is the durability and the
attribution — the file plus its author line is the audit trail while the
event log is unreachable). Do not edit or delete prior entries.

## The re-entry rule (what keeps this from becoming split-brain)

The moment substrate writes succeed again, before taking any other action:

1. Each agent replays its own outage entries into meristem as
   `work_item.event_appended` events (kind `coordination.outage_note`) on the
   most relevant work item, or the coordination parent if none fits. The
   payload carries the original timestamp and the file path.
2. The last agent to replay appends a closing entry to the file:
   `## resolved — replayed into event log` and the file becomes archive
   material (move to `docs/coord/archive/` at leisure).

Truth converges back into the log; the markdown is a buffer, never a second
source of truth. An outage file without a `resolved` footer means replay is
still owed — treat that as a blocking task on reconnection.

## Scope discipline

- Claims and handoffs only. No specs, no findings, no decisions that outlive
  the outage — those wait for the log.
- If an entry would change what another agent should do *after* the outage,
  it belongs in a work item at replay time, not in prose here.

## Why not a real side channel

A socket, shared scratch file, or out-of-repo channel would carry
coordination state that never reaches the event log: unattributed,
unreplayable, invisible to the owner's feed. This protocol trades a little
ceremony for keeping every coordination fact eventually in-band. The outage
file is git-tracked, agent-attributed via commits, and explicitly temporary.
