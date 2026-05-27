# Minimum Viable Agent-to-Agent Coordination

This document scopes the smallest extension that lets two agents
coordinate through `meristem` instead of using the owner as a relay.
It is intentionally narrower than "communication features." The goal is
durable, attributed, asynchronous coordination on top of the existing
event log, `work_item`s, and feed.

This document is advisory. If it conflicts with `docs/spec.md`, that
file wins. If it proves useful, the pieces here should be promoted into
the spec and into tracked `work_item`s in the running system.

## Thesis

Agent-to-agent coordination in `meristem` is **not** a separate chat
subsystem. It is one more shape of "state needing resolution" on top of
the existing substrate:

- A proposal is an attributed event on a subject.
- A reply is an attributed event on the same subject.
- A missing reply is a bounded-patience breach.
- A disagreement is a resolver problem.
- A tie-break is a decision recorded in the same event log.

Push is only the wake-up path. The event log and its projections remain
the source of truth.

## What Already Exists

The current substrate already provides the durable half of this story:

- `work_item`s are durable subjects with lifecycle and attribution.
- `work_item.event_appended` can carry arbitrary coordination facts.
- `/v1/feed` exposes a human-meaningful fold over the event log.
- `meristem feed --watch` gives a crude watch loop.
- `patience.breached` proves the worker can observe stale non-terminal
  state.

That is enough for coarse, asynchronous collaboration today.

It is **not** enough for robust coordination yet, because the missing
piece is reliable wake-up and resume: if an agent goes away and comes
back, it cannot currently resume the feed from a durable cursor.

## Goals

This slice should make the following true for two agents working in
parallel:

1. Agent A can leave a durable proposal, question, claim, or handoff on
   a `work_item`.
2. Agent B can go away, come back later, and resume from a durable feed
   cursor without missing intervening events.
3. Wake-up latency is seconds-to-minutes, not "whenever someone happens
   to poll manually."
4. A needed reply can be modeled as blocked work with a timeout, not as
   an open socket or a held MCP tool call.
5. Any eventual decision is recorded in the same audit trail as the
   work it affects.

## Non-Goals

This slice does **not** attempt to build:

- a chat room
- real-time presence
- distributed locks or file leases
- group-thread UX
- a new "agent" object or `agent_kind` enum
- inline binary attachments in messages
- server-initiated push over MCP stdio

Those may become future work, but they are not required for minimum
viable coordination.

## Slice 1: Reliable Wake-Up And Resume

This is the foundation. Without it, every higher-level coordination
feature is best-effort.

### Feed Contract

Add resumable cursor + long-poll support to `/v1/feed`.

Recommended shape:

- `GET /v1/feed?cursor=<opaque>&limit=<n>&wait=<duration>`

Contract:

- `cursor` is **opaque** to clients. Clients persist and replay it; they
  do not inspect or construct it.
- Results are returned **oldest-first** so tailing reads naturally from
  top to bottom.
- Responses include a `next_cursor` representing "resume after the last
  item returned here."
- Delivery semantics are **at-least-once**. Consumers must dedupe by
  `event_id`.
- `wait` is bounded long-poll, not a subscription. If there are no newer
  items than `cursor`, the server may hold the request open up to a
  configured cap, then return an empty page plus the unchanged cursor.
- No `LISTEN/NOTIFY`, websocket dependency, or other Postgres-specific
  push primitive. The server remains portable; long-poll is implemented
  with ordinary reads and bounded waiting.

Rationale:

- `event_id` is deterministic, not chronological, so it should not be
  exposed as the resume primitive.
- An opaque cursor lets the server encode `(occurred_at, id)` or evolve
  the scheme later without breaking clients.
- Long-poll closes the wake-up gap for both humans and agents without
  changing the substrate's trust model.

### Watcher Contract

`meristem feed --watch` should consume the cursor contract above.

Minimum behavior:

- Persist the last accepted cursor locally.
- Resume from that cursor on restart.
- Deduplicate by `event_id`.
- Support filtering by simple client-side predicates such as mentions,
  subject id, or work_item id.

This is enough to make "another agent replied while I was gone" a normal
resume case rather than a manual archaeology task.

### Push Constraint

Push remains a thin layer above the feed cursor.

- Notifications carry only enough information to wake a consumer:
  effectively "item changed; resume from cursor X."
- The consumer re-reads truth from `/v1/feed`.
- MCP over stdio cannot receive unsolicited server push, so any desktop
  or editor push path must be a sidecar or local watcher process, not a
  new push transport on the `meristem` server itself.

## Slice 2: Coordination On A Subject

Once wake-up and resume are reliable, coordination can stay inside the
existing `work_item` model.

### Subject Model

Coordination happens on a `work_item`:

- sometimes the work item being discussed
- sometimes a dedicated coordination work item (for broad phase planning
  or A/B thread management)

No separate thread object is required for the minimum slice.

### Minimum Event Conventions

For the first pass, coordination facts can ride inside
`work_item.event_appended` using stable inner kinds. The smallest useful
set is:

- `coord.claimed`
- `coord.proposed`
- `coord.replied`
- `coord.handoff`
- `coord.decision_recorded`

Suggested payload fields:

```json
{
  "text": "free-form note",
  "author": "agent-A",
  "mentions": ["agent-B"],
  "reply_to_event_id": "uuid?",
  "decision": "accept|reject|defer?",
  "touched_paths": ["internal/feed", "cmd/meristem"],
  "next_step": "optional short string"
}
```

Notes:

- `reply_to_event_id` is optional in the first pass but should be
  supported so the flat log can express minimal thread structure.
- `touched_paths` is convention, not enforcement.
- These shapes are coordination facts, not schema-level agent taxonomy.

### When Conventions Stop Being Enough

If a coordination state changes a parent `work_item`'s lifecycle, it
should stop being "just a note" and become structural.

Examples:

- a proposal that blocks execution until answered
- a disagreement that needs a tie-break
- a human decision required before work can continue

Those cases want real resolver semantics, likely as explicit event kinds
or child `work_item`s:

- `decision.requested`
- `decision.recorded`

The parent item then moves to `blocked` while awaiting resolution. The
executor is released; nothing holds a process or MCP response open while
waiting.

## Shared Resolution Model

The useful unification is that coordination is not a special case. These
are the same shape:

- `patience.breached` on ordinary work
- unanswered proposal past timeout
- approval expiry
- stale queue item

All are "a state needing resolution exceeded its budget; route it to the
named resolver."

This suggests one convergence engine with multiple trigger types, not a
dedicated communication subsystem.

## Policy Guardrails

Three constraints should hold from the start:

1. **Push is advisory.** Truth always comes from the log and feed.
2. **Approval safety does not weaken.** Approval flows may share the same
   engine shape, but they remain default-deny. No auto-approve path
   lands under this slice.
3. **Deciders stay conservative at first.** The initial useful set is:
   `human`, `token_id:<id>`, and eventually `policy:<name>`. More exotic
   resolvers can wait until there is real use.

## Suggested Sequencing

This slice should land in this order:

1. Feed cursor + long-poll on `/v1/feed`.
2. `meristem feed --watch` consuming that cursor contract.
3. Mention-filtered wake-up using the watcher.
4. Stable coordination event conventions on `work_item.event_appended`.
5. Structural resolver events for proposals/decisions that affect
   lifecycle.
6. CLI sugar such as `meristem reply` or `meristem await`, only once the
   underlying model has been proven useful and the current ergonomics
   hurt in practice.

The important sequencing point is that reply/await UX is **not** the
primitive. Durable blocked state and resumable feed delivery are the
primitive; CLI commands are sugar on top.

## Success Criteria

This slice is successful when:

1. Two agents can coordinate on a shared `work_item` without the user
   relaying messages.
2. Restarting a watcher does not lose its place in the feed.
3. A reply that arrives during a disconnect is still observed after
   resume.
4. A coordination timeout becomes visible to the convergence engine
   rather than silently stalling.
5. Every proposal, reply, and decision remains attributable and
   replayable from the event log.

## Work Item Mapping

As of 2026-05-27, this section is a historical map, not the durable backlog.
Use live meristem `work_item`s and the feed for current coordination state. The
short references originally recorded here (`e1625848`, `d56a0bc3`) are not
full MCP-addressable ids and should not be used as live handoff targets from
this file.

Live related anchors visible through MCP during the 2026-05-27 migration:

- `5f10552c-2435-4128-8a93-3765fb31be3e` — shipped MCP `feed.read`
  watcher parity for cursor/wait/`next_cursor`.
- `e54cda1b-2841-4adc-a4d4-16bc73c5c4a6` — analysis item that selected
  MCP feed watcher parity as the next slice.
- `95888b61-97ff-41eb-9b0f-bc515ea2394d` — migration slice that moved
  active markdown coordination/checklist state into meristem.

If this document proves useful, the next natural backlog item after
the feed-resume work is:

- **Minimum viable agent-to-agent coordination over meristem**
  - stable coordination event conventions
  - watcher-based wake-up
  - structural decision flow only where lifecycle depends on it
