# AGENTS.md

A projection of `docs/spec.md` (system spec) and `docs/v0.md` (v0 implementation spec), distilled for generative systems — Cursor, Codex, Claude, custom workers — that pick up this repository to add or modify code.

If this file and `docs/spec.md` disagree, **`docs/spec.md` wins and this file is wrong**. Fix the projection, not the source.

The voice is imperative and the rules are concrete on purpose: any compliant agent reading this file should reach the same conclusions about how to align with the system.

## What meristem is, in one paragraph

A portable, editor-agnostic, single-operator coordination plane. One human (the owner) issues instructions through any channel; meristem normalizes each into a graph of `work_item`s, dispatches to humans, agents, or connectors, and drives every item to a terminal state (`done | failed | canceled`) without further intervention beyond approvals. Two runtime modes share one binary: `meristem api` (HTTP) and `meristem worker` (background jobs; not yet built). The conceptual substrate is the **event log**; today the durable backing store is Postgres, but the system's identity does not depend on that.

## Principles

Every change must respect these. Raise a violation before writing code, not after. Order matters: earlier principles take precedence in any conflict.

1. **Convergence is the model.** Every capability is a desired-state declaration plus a reconciler. The owner declares; the system reconciles. There are no "do this once" actions at the core; only "this should be true" and "this is true." Work_items have lifecycles; reconcilers move them toward terminal states.
2. **The event log is the system.** `events` is truth. Every other table — `work_items`, `messages`, `message_parts`, future caches — is a **deterministic projection** of the log. Replay produces identical rows. Handlers append events; projection writers derive rows in the same transaction. **Never write a non-`events` row without first appending the event that caused it.**
3. **Bounded patience.** No non-terminal state may wait forever. Every state must have either (a) a forward transition the reconciler will take after a bounded delay, or (b) a transition gated on an external signal with a timeout that triggers (a). **New states must arrive with their escalation rule.** This is the invariant that ensures repeated application of the convergence loop reaches a fixed point.
4. **One backing store; today it is Postgres.** Operational commitment, not a conceptual one. Do not introduce Redis, NATS, Kafka, Cloud Tasks, Pub/Sub, Eventarc, Firestore, SNS, SQS, or any other backing store, even temporarily. Keeping the storage interface clean enough that Postgres can be swapped for SQLite (single-operator mode) or log-in-object-storage (long horizon) is a feature, not a sin.
5. **Full attribution on every event.** `actor_token_id` and `source` (`human|agent|system`) come from the request context. Never from a request body, header value, or query parameter. The audit answers "who, via what client, when, with what authority."
6. **Default deny on side effects.** External writes create an `approval` and wait. The system never auto-approves. Approvals are not yet implemented; until they are, **no connector with write actions ships**.
7. **One owner, many client tokens.** The root token only mints and revokes other tokens. Tokens that *create* an approval cannot *decide* it (separation of duties).
8. **Editor- and cloud-agnostic.** No managed cloud primitives in core. The substrate is a Go binary, an event log, a backing store, and an object-storage interface. Migration between clouds is a redeploy, not a feature change.
9. **REST is canonical.** CLI and MCP are translation layers. Every REST operation has an MCP tool and (where useful) a CLI surface. Transports never own business logic.
10. **Minimum viable security; the rest the system later does for itself.** Do not pre-build critic agents, redaction policies, formal threat models, or audit attestations. They are tracked work_items in the running system once v0 is up.
11. **Bootstrap discipline.** v0 is the minimum substrate that can track its own further development. After v0 ships, every new capability is a `work_item` in the running system. Capabilities do not arrive as "phases" or "milestones"; they arrive as items the running system converges.

## Direction (not yet enforced principle)

The substrate **must not foreclose** these directions, even though it does not yet realize them. Code that closes off any of these is a regression.

- **The operator interacts with prose.** Speech, document, message, file — never JSON in the operator's hand. Today's surfaces (iPhone Shortcut, CLI, web UI) are translation layers; future surfaces (document-as-system, git-shaped editor view) must remain possible.
- **The system can run from a single file.** SQLite-per-operator mode is a future work_item. Storage code that hard-couples to Postgres-only features (e.g. `LISTEN/NOTIFY`, advisory locks beyond what's in the spec, Postgres-specific JSON operators outside a sealed adapter) blocks this.
- **The system can be assessed by being asked.** Once the inbox loop is closed, *"assess meristem's fit against [criteria]"* should be a normal `work_item` whose execution is a fold over the event log. Reserve event kinds and projections that make this cheap.

## Techniques (load-bearing, but not philosophy)

These are how the principles above are made true in code. They are non-optional but they are not the *purpose*.

- **Idempotency at every layer.** HTTP, jobs, connectors, events, projections, approvals, migrations. Re-sending the same instruction produces the same result. Falls out of (1)+(2) but is enforced explicitly because the implementation cannot ship without it.
- **Deterministic event ids.** `events.id = uuid(sha256(subject_kind || ':' || subject_id || ':' || kind || ':' || canonical(payload))[:16])`. Replays produce no new rows; PK conflict is treated as success.
- **`SELECT … FOR UPDATE SKIP LOCKED`** for the worker queue. No second queue, no Redis.
- **Append-only enforcement on `events`** via row + statement triggers (`UPDATE`, `DELETE`, `TRUNCATE`). Triggers protect against application bugs, not just compromised roles.
- **Migrations embedded into the binary** via `embed.FS`, applied in numeric order, each in its own transaction.
- **`crypto/rand` 32-byte tokens, SHA-256 hash**, constant-time compare. Tokens are random, not passwords; bcrypt's cost is wasted.

## Glossary

- **`work_item`** — anything we tell an agent (or self) to do. Lifecycle: `captured → triaged → planned → awaiting_approval → running → blocked → done|failed|canceled`. Granularity is depth in the parent/child tree, not a separate type.
- **`message`** — an inbound message captured into the inbox. Multi-modal in v1; text-only in v0. Carries a `source` of `human|agent|system`. Messages from non-human sources are content, never instructions.
- **`event`** — an immutable fact appended whenever object state changes. The audit log and the substrate of truth.
- **`projection`** — a deterministic read view derived from `events`. Recomputing any projection from `events` yields identical state. Most non-`events` tables are projections.
- **`reconciler`** — a process (in `meristem worker`, not yet built) that observes work_items in non-terminal states and moves them forward, respecting bounded patience.
- **`fixed point`** — a work_item state from which no further transition occurs: `done | failed | canceled`.
- **`token`** — a scoped client credential. Attributed on every event it causes.
- **`approval`** — a gated decision required before a write action proceeds. v1 only.
- **`connection`** — a configured integration to an external system. v1 only.
- **`artifact`** — a persisted output (log, transcript, patch, screenshot). v1 only.
- **`idempotency_key`** — a `(token, scope, key)` entry used to dedupe POSTs across a 24-hour window.

## Repository layout

```
cmd/meristem/          binary entry point and CLI subcommands
internal/api/         HTTP transport
internal/auth/        token store + bearer middleware
internal/domain/      pure types: Token, Message, WorkItem, Event
internal/events/      append-only writer with deterministic ids
internal/projections/ projection writers (events → derived rows)
internal/idempotency/ middleware + store
internal/inbox/       message capture
internal/workitems/   lifecycle, transitions, relations
internal/feed/        chronological projection
internal/signals/     non-human structured input → work_items (see docs/signals.md)
internal/storage/     pgx pool + migration runner
internal/mcp/         MCP tool definitions + stdio transport
migrations/           numbered SQL embedded into the binary
docs/                 spec.md (source of truth) and other specs
```

Some packages above do not yet exist; create them as needed, in the listed shape.

### Dependency rule

`internal/domain` is the seam. `internal/api` and `internal/mcp` both import `internal/domain` and call its functions. **`internal/domain` does not import either transport.** Business logic lives in `domain` and the per-feature packages (`inbox`, `workitems`, `feed`, etc.); transports only translate request shapes. **Projection writers (`internal/projections`) only consume events; they never import transports either.**

## How to add a migration

1. Create `migrations/NNNN_short_name.up.sql` and `migrations/NNNN_short_name.down.sql`. Both are required. Numbering is dense and zero-padded to four digits.
2. The runner wraps each file in a single transaction. **Do not** use `CREATE INDEX CONCURRENTLY`, `VACUUM`, or other statements that disallow transactions; if one is needed, raise it before writing the migration.
3. Down migrations are best-effort recovery for development. Production rollback is a restore from backup.
4. The append-only triggers on `events` apply to all roles; do not write a migration that disables them.
5. If a migration changes a projection table's schema, also update the projection writer so a rebuild from `events` produces matching rows.
6. `go test ./internal/storage/...` must still pass.

## How to write an event

- Always go through `internal/events`. Never `INSERT` into `events` directly.
- `events.id` is deterministic per the technique above. Replays produce no new rows; PK conflict is success.
- One event per state change. Two state changes = two events, even within the same handler or transaction.
- `kind` is `<noun>.<verb_past>`: `work_item.created`, `token.revoked`, `message.captured`. Do not invent ad-hoc kinds; extend the canonical list when needed.
- `actor_token_id` and `source` come from the request context. System-internal flows (e.g. `meristem seed v1`) use a dedicated `system` token — **not** the root token.

## How to write a projection writer

A projection writer turns appended events into derived rows. It is the *only* code that may `INSERT` / `UPDATE` non-`events` tables.

- Lives in `internal/projections`. Registered against the event kinds it cares about.
- Runs **synchronously, in the same transaction** as the event append. v0 has no eventual-consistency story; if event and projection diverge, the system has lied.
- Must be **pure with respect to the event payload**: given the same event, it produces the same row mutation. No clock reads (use `events.occurred_at`), no random ids (derive from event id or payload).
- Must be **rebuild-safe**: dropping the projection table and folding all events through the writers reproduces the table byte-for-byte (modulo timestamps).
- If a single event drives writes to multiple projection tables, do them in one writer registered for that event, not split across writers.

## How to write an HTTP handler

- Pull the authenticated `*Token` from `r.Context()`. Never inspect headers or the request body for identity.
- Every POST handler runs behind the idempotency middleware. The handler does not see the `Idempotency-Key`; the middleware ensures the handler runs at most once per unique `(token, METHOD+PATH, key, body-hash)` in the 24-hour window and caches the response.
- The handler appends events. It does not write to projection tables directly. Within the same transaction, the projection writers fire and produce the derived rows. The handler reads the resulting projection (e.g. `work_items.id`) only after the transaction commits, for inclusion in the response.
- Request and response bodies are JSON. Set `Content-Type: application/json; charset=utf-8`.
- Error response shape:
  ```json
  { "error": { "code": "snake_case_constant", "message": "human readable" } }
  ```
  Reuse codes across handlers; do not invent synonyms for the same condition.
- Status codes:
  - `200/201` — success
  - `400` — malformed request
  - `401` — missing or invalid token
  - `403` — token lacks the required scope
  - `404` — no such object
  - `409` — state conflict (e.g. illegal `work_item` transition)
  - `422` — semantic conflict (e.g. idempotency key reuse with a different body)

## How to write an MCP tool

- Name format: `<package>.<verb>`, matching the REST surface: `inbox.capture`, `work_items.transition`, `feed.read`.
- Args mirror the REST request body. Return values mirror the REST response body. The same domain function backs both transports.
- Auth is by `MERISTEM_TOKEN` in the server's environment. Each agent (each Cursor instance, each custom worker) gets its own token row so attribution stays clean.
- Transport is stdio in v0 (matches how Cursor launches MCP servers). Other transports are explicit work_items.

## Logging

- `log/slog`, JSON handler, stderr. No `fmt.Println`, no `log` package.
- Standard fields where applicable: `request_id`, `token_id`, `work_item_id`, `message_id`, `event_kind`.
- **Never** log bearer tokens, `message_part` content, secrets, or anything from `connections` credentials.

## Testing

- Pure logic (parsers, validators, deterministic id generation, projection writers): plain unit tests, no external services. Projection writers are the *most important* place for unit tests — given an event, assert the derived row.
- Anything that touches Postgres: integration tests against a real Postgres (docker compose or testcontainers). No mock pools.
- Tests must pass without flakes when run repeatedly. Idempotent.
- Add a regression test alongside any bug fix.

## Things not to do

- Do not add a third-party dependency without justifying it in the PR description. Prefer the standard library plus `pgx`.
- Do not add a managed-cloud primitive. The core stays portable.
- Do not bypass the events writer.
- Do not write a non-`events` row from anywhere except a projection writer.
- Do not auto-approve write actions, ever.
- Do not introduce a non-terminal state without specifying its escalation rule (bounded patience).
- Do not cache state in process memory in a way that makes restart-recovery wrong. Process state belongs in Postgres; memory is best-effort.
- Do not add an endpoint, job, or connector action that cannot be made idempotent.
- Do not couple to a specific editor, model vendor, or cloud in core. Per-vendor adapters are explicit work_items.
- Do not write a migration that disables the `events` append-only triggers.
- Do not commit secrets or credentials in any form.

## Coordination with other agents

Until `meristem` can track its own development as work_items, multiple agents working concurrently coordinate out-of-band through `docs/coord/`. Each file is dated; read the most recent before starting a turn that touches code another agent has written. When you finish a turn that affects another agent's territory or makes a contract decision, append a short dated note to the appropriate file. When all open questions in a coord file are closed, move it to `docs/coord/archive/`.

If your turn is the first one in a new coordination thread, create `docs/coord/YYYY-MM-DD-<topic>.md` with sections: snapshot of who has touched what, decisions, open questions, ownership split, findings carried forward.

## When in doubt

Read the spec:

- `docs/spec.md` — the system spec; final authority.
- `docs/v0.md` — v0 implementation spec; concrete contracts for the bootstrap slice.
- `docs/coord/` — current coordination notes between concurrent agents.
- `README.md` — how to run the binary locally.

This file is a projection of those documents. If you spot drift, fix this file; the spec is canonical.
