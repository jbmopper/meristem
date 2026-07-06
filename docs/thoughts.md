# Thoughts: Convergence And Storage Substrate

> Status: historical design note. The convergence framing from this note has
> been promoted into [`spec.md`](spec.md) and [`AGENTS.md`](../AGENTS.md). Treat
> those files as canonical; use this file only for background rationale.

Good question to ask now, before more code locks the answers in. Let me actually interrogate both, not just defend them.

## Idempotency: what is it really for?

Idempotency-at-every-layer is solving a specific problem: **the world is unreliable, so we want the same instruction, repeated, to produce the same outcome.** It's the "safety net under retries" abstraction. It does nothing on its own to make the system *do the right thing* — it only makes it safe to *try again*.

That's a tactic, not a philosophy. The philosophy underneath is one of several:

### Option A — Idempotency-as-foundation (current)
"The system is a sequence of imperative actions; we make each action safe to retry." HTTP-style. Every POST has a key, every job has dedupe, every event has a deterministic id.
- **Pros:** Familiar, composes with REST, no new mental model for contributors.
- **Cons:** The user/operator still thinks in *actions* ("did my POST go through?"). You're papering over the network's unreliability rather than removing the user's exposure to it. Idempotency keys are a *receipt-passing* protocol — an operator-level concept leaking into the substrate.

### Option B — Convergence-as-foundation (declarative + reconciler)
"The operator declares what the world should look like; the system reconciles continuously." Kubernetes / Terraform / Nix. There are no actions in the API surface, only desired-state edits. Reconcilers loop until reality matches intent.
- **Pros:** Directly matches the stated goal ("drive every work item to a terminal state … without further intervention"). The user *cannot* care whether a request "succeeded" because there are no requests, only state. Retries are invisible. Crash recovery is the same code path as steady-state operation. This is the most direct expression of "self-building and convergent."
- **Cons:** Hard to express user-facing actions like "send this message right now" in pure declarative form; you usually end up with a hybrid where some things are declarative and some are imperative-with-idempotency. The mental model shift is real.

### Option C — Event-sourced with content-addressed identity
"The event log is truth. Every other table is a projection. Identity is derived from content, not assigned." Datomic, EventStore, Git's object model. Idempotency becomes a *consequence of the data model*: if the event id is `hash(content)`, you cannot duplicate; if the projection is deterministic, replays are free.
- **Pros:** No middleware needed — the schema *is* the safety. Time travel comes free. Audit comes free. Already half-built: `events.id` in `AGENTS.md` is already specified as a content-derived hash.
- **Cons:** Querying current state requires a projection layer; "what is the state of work_item X right now?" is a fold over the log, cached in a table. Schema migrations on projections are easy; schema migrations on the event log are not.

### Option D — Local-first / CRDT
"State is everywhere, edits commute, sync is eventual." Linear, Figma, Replicache, Automerge.
- **Pros:** Massively humane interaction (no spinners, instant edits, offline-capable, no "did it go through"). For a *single*-operator system this is almost suspiciously well-fitted.
- **Cons:** The data model gets harder (CRDT semantics). Multi-agent coordination with humans-in-the-loop has approval and ordering requirements that fight CRDT freedom. Probably overkill for the substrate, possibly correct for the operator's UI.

### Option E — At-most-once with explicit reconciliation
"Don't retry. If something fails, surface it; the operator (or a reconciler agent) decides." The honest version of "no, we don't have a duplicate-detection problem because we never repeat."
- **Pros:** No silent automation; every weird state gets human attention.
- **Cons:** Operator toil at exactly the moment we're trying to eliminate it. Probably the wrong answer for this system.

### My read
Idempotency isn't *wrong*; it's *too low in the stack* to be the headline principle. For a system whose stated identity is "self-building and convergent," **convergence is the foundation; idempotency is a derived property**. The right top-line should be something like:

> *"Every observable state is a deterministic projection of declared intent and an append-only log. The operator declares; the system reconciles."*

That commitment makes idempotency unavoidable rather than optional, and — more importantly — it changes how every new feature gets designed. You stop asking "what's the POST endpoint and how do we dedupe it" and start asking "what state did the operator declare, and what loop drives reality toward it."

The current code is *compatible* with this reframe; nothing in the schema fights it. But `AGENTS.md` would be saying something more honest.

---

## Postgres: what is "Postgres is the system" really for?

It's solving three problems with one answer:

1. **Where does durable state live?** Postgres.
2. **Where does the queue live?** Postgres (`SELECT … FOR UPDATE SKIP LOCKED`).
3. **Where does the audit log live?** Postgres (`events`).

The principle's real content is *"do not introduce a second backing store."* That's an operational commitment (one thing to back up, one thing to fail, one transactional boundary), not a data-model commitment. Conflating them obscures the actual choices.

The data-model question — *what is the substrate, conceptually?* — has separate answers:

### Option 1 — Postgres-as-system (current)
Postgres is both the storage layer and the conceptual substrate. Tables are first-class. The truth is "what's in the rows right now."
- **Pros:** Mature, transactional, queryable, single ops story, runs anywhere.
- **Cons:** Conflates "live state" with "history," making time-travel and projection-rebuilding feel like exotic operations rather than the default mode of existence. Forecloses local-first patterns. Hard to think about the system as "the operator's repo, with a server attached" — which is the most humane framing for a single-operator tool.

### Option 2 — Append-only log as substrate, Postgres as projection
The conceptual system is the event log. Postgres holds it (as the `events` table) *and* holds the cached projections (`work_items`, etc.). Every projection is rebuildable from the log. You could swap Postgres for SQLite-with-a-log-file or "log-in-object-storage + Postgres cache" without changing the user-visible system.
- **Pros:** The data model maps cleanly onto the convergence principle above. Projections become first-class — a `feed` is just a different fold over the same log. Time travel, branching, replay become natural. The events table you already specify *is exactly this*; the principle just hasn't been pulled up to the top.
- **Cons:** Discipline cost: every contributor has to internalize "events are truth, tables are caches." Projection migrations need a versioned-rebuild story.

### Option 3 — SQLite-per-operator
For a system explicitly framed as "single operator," Postgres might be over-specified. SQLite gives you the same transactional guarantees, zero ops, ships inside the binary, and the entire database is one file the operator can email to themselves.
- **Pros:** Drastically more humane: no docker compose, no DSN, no port. The operator's machine *is* the meristem. Backup = file copy. Move to a new machine = copy a file.
- **Cons:** Concurrent writers are a tighter constraint, multi-host deployment becomes "you don't," and full-text search and JSONB operations are weaker than Postgres. The constraint that pushes you off SQLite isn't the operator — it's the agents writing concurrently.

### Option 4 — Git as substrate
Work items are files in a directory. Events are commits. Branches are alternate plans. Merges are how parallel agents converge. The repo is sync'able with any git host or with `rsync`. Postgres becomes optional — a query index over a git tree.
- **Pros:** Wildly portable. Editor-native (the operator can `vim` a work item). Branching plans is *free*. Auditability is *free*. The operator's existing tooling (diff, blame, log, bisect) all work on the system itself. Maximally humane for someone who lives in a terminal.
- **Cons:** Concurrent-writer semantics are coarse (commits, not row locks). Querying-by-attribute requires a sidecar index. Some operations (a 50ms HTTP write) become structurally awkward. Real-time collaboration is hard.

### Option 5 — Document-as-system
The operator interacts with a single living document — extended `spec.md` plus current state, like a Notion page or an org-mode buffer. The system parses and reconciles. There's still a database underneath, but the *user-facing substrate* is prose.
- **Pros:** This is probably the most humane single-operator interface that exists. It matches how the operator already thinks (you're already writing `docs/spec.md`). It naturally accommodates "instructions in any form."
- **Cons:** Parser/reconciler is non-trivial. Multi-agent writes to a single doc is a CRDT problem (back to Option D). But this could live *on top of* options 2/3/4.

### My read
"Postgres is the system" is doing two unrelated jobs and should be split:

- **Operational commitment (keep):** *"There is one durable backing store. Today it's Postgres. Don't add a second."*
- **Conceptual substrate (change):** *"The event log is the system. Everything else — including every Postgres table that isn't `events` — is a projection that can be rebuilt from the log."*

This costs you nothing in the current codebase (you already have an append-only `events` table with deterministic ids and triggers protecting it) and buys you optionality on every other dimension: SQLite swap-in for true single-operator mode, object-storage offload of cold history, time-travel as a normal feature, branching plans, and — eventually — a git or document-shaped frontend without re-architecting.

---

## Connecting back to the goal

You said: **self-building, convergent, humane.**

| Goal | What serves it |
|---|---|
| Self-building | The system must be able to enqueue work for itself, observe outcomes, and adjust. That's a reconciler pattern, not a request/response pattern. |
| Convergent | Same — the system must be expressed as desired-state + loop, not action + retry. |
| Humane | The operator must not be exposed to the substrate. No idempotency keys in their head, no "did it go through," no JSON payloads, ideally no ports. The interface should be prose, files, or speech — whatever they were going to use anyway. |

The two principles you're stress-testing are pointed at the wrong layer for those goals:

- "Idempotency at every layer" → reframe as **"the system is convergent; idempotency is a consequence."**
- "Postgres is the system" → split into **"one backing store" (operational)** and **"the event log is the system" (conceptual)**.

Concretely, if you wanted to write that into `AGENTS.md` today, the top of the principle list might become:

1. **Convergence is the model.** Every capability is expressed as a desired-state declaration plus a reconciler. The system has no "do this once" actions at its core; only "this should be true" and "this is true."
2. **The event log is truth.** Every other table, file, view, or cache is a deterministic projection of the log. Replay produces identical state.
3. **One backing store; today it is Postgres.** Operational commitment, not conceptual one. Keeping Postgres swappable for SQLite (single-operator mode) or log-in-object-storage (very long horizon) is a feature, not a sin.
4. **The operator interacts with prose.** Structured data is what the system maintains internally. The interface is a document, a message, a file — never a JSON payload.

Then idempotency becomes a *technique*, listed lower down with the other techniques (deterministic ids, content-addressing, FOR UPDATE SKIP LOCKED) — important, but not philosophy.

---

**Where I'd push back on myself:** the convergent/declarative model has a real cost when humans want imperative control ("send this *now*"). And the local-first/document substrate has a real cost when you have multiple agents writing concurrently with ordering requirements. Neither problem is fatal, but they're the places where the humane framing fights the engineering.

Want to pick one of these threads to go deeper on? The most consequential decision is probably **Option 2 (event log as substrate)** vs. status quo, because it's both the cheapest to adopt today (the schema already supports it) and the one that most opens up the humane-interface options later.  
