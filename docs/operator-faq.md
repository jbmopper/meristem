# Operator FAQ

Plain-language answers to the questions a new operator (or a new agent
contributing to meristem) is going to hit in the first hour. Not
comprehensive; the canonical sources are `docs/spec.md` and `docs/v0.md`.
This doc exists so you can be productive without reading either one
first.

If you're an agent reading this and you're about to add a new question,
add it as a new `## Question?` section, keep the answer plain, and link
to the spec for the full version where it makes sense.

---

## Is there a UI? How do I use it?

No web UI in v0. The surfaces are:

- **CLI**: `meristem tokens|seed|rebuild|migrate|api|mcp|healthcheck`.
- **HTTP REST**: the `/v1/...` endpoints (see `internal/api/server.go`
  for the full list).
- **MCP over stdio**: for Cursor, Claude Code, or any agent that speaks
  MCP. Each agent gets its own token row.
- **Go SDK**: a tiny client at `pkg/meristem` for `POST /v1/signals`.

Day-to-day the "UI" is `curl | jq`, an iPhone Shortcut you'd build, your
editor's MCP integration, or a shell function. A web UI is a future
work_item, deliberately not in v0 — the long-term operator surface
should be voice/prose-first, not yet-another-dashboard.

## What happens to events when they're in the system?

Three things, in order:

1. They're appended to the `events` table. Immutable. Append-only is
   enforced by row + statement triggers; nothing in the system can
   `UPDATE`, `DELETE`, or `TRUNCATE` them.
2. **In the same transaction**, every projection writer registered for
   that event kind runs and writes the derived rows in `work_items`,
   `messages`, `idempotency_keys`, etc. By the time the HTTP handler
   returns, the state you'd query is consistent with the event you just
   wrote.
3. They sit there as your audit log. You can query them directly
   (`SELECT * FROM events WHERE subject_id = ?`), replay them via
   `meristem rebuild`, or use them to investigate "how did this thing
   get here?"

What events do **not** do: trigger side effects. There's no event bus,
no subscribers, no listeners. The handler that wrote the event also
wrote the projection in the same transaction. Future side-effect-doing
things (the worker, future connectors) will **poll projections on a
timer** — they won't subscribe to events. That's a deliberate
simplicity choice.

## How is idempotency defined here?

Three distinct layers; they answer different questions.

- **HTTP `Idempotency-Key`** answers "did this exact POST already
  happen?" Client sends a UUID in the header on every POST. Middleware
  caches the response keyed on `(token_id, method+path, key,
  body_hash)` for 24 hours.

  - Same key + same body → cached response with header
    `Idempotency-Replayed: true`.
  - Same key + different body → `422 idempotency_key_conflict`.
  - Concurrent racing duplicates serialize on a Postgres advisory lock,
    so you can't slip a duplicate through under a race.

- **`dedupe_key`** in the signal payload answers "is this the same
  *logical* work as something we've seen?" Collapses
  semantically-equivalent signals into one work_item even when each call
  has a fresh `Idempotency-Key`. Use case: same flaky test failing on
  three different CI runs → one work_item with three signals attached,
  not three duplicates.

- **Deterministic event ids** answer "did this exact state change
  already get recorded?" Every event id is
  `uuid(sha256(subject_kind:subject_id:kind:canonical_payload))[:16]`.
  Replays just hit a PK conflict and become no-ops. That's why
  `meristem seed v1` re-running 5 times still produces 18 work_items
  (not 90), and why `meristem rebuild` can fold the whole log into a
  sandbox without ghost rows.

## Should the DAG reject cycles? Isn't a fail-retry a kind of cycle?

The DAG and the state machine are different things, which is what makes
this confusing.

- The work_item DAG models **decomposition** — "B is a sub-task of A."
  A cycle there would mean "B is a sub-task of A *and* A is a sub-task
  of B," which is meaningless. Reject.
- Fail-retry is about **state on a single work_item**, not relations
  between work_items. Item X goes `running → failed`; a human or the
  worker says "try again" and transitions it `failed → planned`. Same
  row, same id, no graph change. The lifecycle explicitly allows
  re-entering active states from `failed` and `blocked` for exactly
  this reason.
- Put another way: the parent/child tree is a **plan**; state is
  **progress**. Plans don't loop; progress does.

## What is "state", and what is a "projection"?

- **State** is what's observable right now. A row in `work_items` says
  `state='running'`, a row in `messages` exists, a row in
  `idempotency_keys` is cached. State answers "what is true at this
  moment?"
- **Projections** are the *code* that derives state from events. They
  live in `internal/projections/`. Each projector subscribes to one or
  more event kinds; when a matching event is appended, the projector
  runs in the same transaction and writes the derived row.

So every "state" table is a projection of `events`. The event log is
the source of truth; state tables are caches the projectors maintain.
`meristem rebuild` proves this by dropping the projections into a
sandbox schema, replaying every event, and content-hashing the rebuilt
tables against the live ones — they have to match.

Why this matters in practice:

- **"Is the state correct?"** → run `meristem rebuild`. Mismatches mean
  a projector has a bug; the events themselves are still authoritative.
- **"Can I add a new view?"** → write a new projector, drop the table,
  replay. The data is already in events. No migration that has to
  backfill data from scattered sources.
- **"Did the system lie about what happened?"** → no, because
  attribution is on every event and projectors are pure functions of
  events. The audit answers "who did this, when, with what authority"
  without you having to instrument anything.
