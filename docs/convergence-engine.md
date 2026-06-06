# Convergence Engine — Scaffold and Handoff

Status: **deterministic core and Slice A persistence are implemented; default
worker wiring is quarantined.** This document describes what exists on this
branch and the directions for the subsequent agent that finishes wiring the
engine into the running system.

It is a projection of `docs/spec.md` → "Convergence Patterns" and AGENTS.md
principle #12 ("Convergence has a deterministic reduction"). Where this file
and the spec disagree, the spec wins.

## What this scaffold is

The convergence engine is the deterministic *reducer* half of every
convergence pattern. The probabilistic subsystem proposes and judges (samples,
fans out across models, grades a patch); the engine reduces the signals it is
handed into one of three verdicts — `accept | reject | escalate` — and that
verdict, recorded as an event, is the only thing that advances a `work_item`.

The reducer core is pure and unit-testable. Slice A adds the persistence
boundary: verdict events are projected into `convergence_verdicts`, while the
default worker emitter remains disabled until pattern declaration and bounded
worker integration land.

### Landed in this branch

- `internal/convergence/` — reducer core plus verdict persistence boundary:
  - `Signal` / `SignalSource` — the evidence a reducer folds over (boolean
    `Pass`, scalar `Score`, audit-safe `Raw`, attribution).
  - `Reducer` interface — `Identity()`, `Version()`, `Reduce([]Signal)`. Pure
    and replayable.
  - Canonical reducers: `MajorityVote`, `Unanimous`, `Threshold` (mean ≥
    accept), `AllPassChecklist` (consumes `suggested_convergence_checks`).
  - `Run(reducer, signals, attempt)` → `Reduction`, with a content-addressed
    `InputsDigest` (SHA-256 over `events.CanonicalJSON(signals)` — the same
    canonicalizer the deterministic event id uses).
  - `Reduction.EventPayload()` — the canonical wire shape for the verdict
    event, so the persistence slice does not re-invent it.
  - `Registry` + `DefaultRegistry()` — identity → reducer for replay.
  - `Budget` / `Escalation` / `Budget.Next(...)` — the bounded-patience half:
    maps `(verdict, attempt, budget)` → `OutcomeAccept | OutcomeRetry |
    OutcomeEscalate`. A `Budget` is invalid without an escalation rule (spec
    rule #3 enforced at construction).
  - `AppendVerdict(...)` / `VerdictEventSpec(...)` — the sanctioned append path
    for `convergence.verdict_recorded` events.
  - `RegisterProjectors(...)` — projects verdict events into
    `convergence_verdicts` keyed by `event_id`.
- `migrations/0010_convergence_verdicts.*.sql` — the projection table, with
  indexed `work_item_id` and `attempt` columns and `signals` stored as JSONB.
- `internal/app/projectors.go` and `cmd/meristem/rebuild.go` — registry and
  rebuild wiring for the new projection.
- `internal/domain` — convergence taxonomy:
  - `Verdict` type + `VerdictAccept | VerdictReject | VerdictEscalate`.
  - `EventConvergenceVerdictRecorded = "convergence.verdict_recorded"`, added
    to `AllEventKinds`.
  - `SubjectConvergence = "convergence"`.
- `internal/feed` — `convergence.verdict_recorded` classified as feed-visible
  (it is narrative, not audit noise). The drift guard passes.

### Deliberately NOT in this branch

- No default emitter in default builds. The append helper exists, but worker
  code must not call it until pattern declaration and bounded worker
  integration land.
- No model calls. Producing signals is the probabilistic subsystem's job.
- No default worker wiring. The engine reduces; the worker gathers signals and
  acts only after persistence and pattern declaration land.
- There is an experimental worker driver behind the
  `convergence_worker_experiment` build tag. It is intentionally excluded from
  default builds because it drives lifecycle transitions before pattern
  declaration and bounded worker integration are ready.
- Parameterized reducers (`Threshold`, `AllPassChecklist`) are **not** in
  `DefaultRegistry()`: their behavior depends on per-work_item configuration,
  so registering a zero-value instance would make replay silently use the
  wrong threshold. Callers construct the configured instance and `Run` it
  directly, or register it under an identity that encodes its parameters.

## Runtime boundary: open vs. closed

Convergence is intentionally open on the probabilistic edge and closed on the
deterministic substrate.

- Open at runtime: subprocesses may adaptively emit signals, spawn child
  `work_item`s, vary samples, choose prompts, and fan out across model or tool
  calls. Those actions become durable only by appending events.
- Closed at runtime: event kinds, migrations, projectors, reducers, and reducer
  versions are fixed code. A new reducer or projector is never minted by a
  subprocess at runtime; it is a normal code/migration `work_item` that lands,
  registers, and becomes replayable.

This line keeps replay sound: the log is folded by a fixed, versioned set of
pure functions. A runtime-generated reducer or projector would make durable
truth depend on process state outside the event log.

## Initial pattern catalog

This catalog names the actor shapes the execution substrate should serve. It is
not a new schema object and it is not an `agent_kind` enum. Specialization lives
in the work_item prompt/body, child edges, emitted signals, and token scopes.

### Node vocabulary

A `work_item` is the node. A parent/child relation is the durable graph edge.
The node's definition of done may include successor generation: before the node
claims convergence, it must have appended the events that create the next
processes it owes (for example validator children, diverger samples, or a
fan-in reducer request). The successor edge is durable only when recorded as
`work_item.created` plus `work_item.relation_added` events; prose intent in a
prompt is not enough.

The reducer sees events and signals, not private in-process state. A node may
adaptively choose prompts, models, samples, or tools, but it cannot create a new
reducer/projector at runtime. If a new reducer or projector is needed, that is a
normal code+migration work_item that lands first and becomes replayable.

### Actor topology

- **Actor/coordinator.** Owns the local plan for one subtree: spawn children,
  assign scoped tokens, gather feed updates, and request reduction. It should
  not directly mark a parent done from model judgment.
- **Grower/driver.** Produces or revises a candidate. In the resume bicameral
  shape, this is the driver chamber. Its done condition is "candidate and
  audit-safe evidence emitted," not "parent accepted."
- **Diverger.** Fans out alternatives: different prompts, models, samples,
  rubrics, or repair strategies. Its done condition includes child creation or
  signal emission for each declared branch.
- **Critic/converger.** Evaluates a candidate and emits structured signals
  (`pass`, `score`, bounded `raw`, source metadata). It can be probabilistic,
  but it does not own lifecycle truth.
- **Reducer/reconciler.** Deterministic system-side actor: gathers the declared
  signals, runs a fixed reducer, appends `convergence.verdict_recorded`, and
  only then applies budget/escalation lifecycle transitions.

### Pattern shapes

- **Unitary DAG.** A single actor runs sequential nodes such as precheck →
  plan → generate → validate → reduce. Use when deterministic checks are strong
  and fan-out would only add cost.
- **Bicameral driver/converger loop.** A grower/driver produces a candidate;
  a critic/converger emits a harsh score or pass/fail signal; a deterministic
  `Threshold`, `Unanimous`, or `AllPassChecklist` reducer accepts, retries, or
  escalates. The loop is self-limiting through max attempts or wall-clock
  budget, never through an unbounded "try again."
- **Fan-out/fan-in.** A coordinator spawns one child per declared attribute,
  rubric, model, or sample. Each child emits a signal. Fan-in runs only when
  the required signal set is present or patience expires, then reduces by vote,
  threshold, checklist, schema match, or run-to-green.
- **Diverger/grower pool.** A diverger creates candidate-generating children,
  not validators. A later fan-in selects or validates candidates with a fixed
  reducer. This is useful when the hard part is search, not judgment.

### Token templates for subactor issuance

These templates are the near-term target for `9779f82e` issuance. They should
use only known, fail-closed scopes until new scope names land as their own
work_item.

- `same_tree_coordinator`: `work_items.tree:<root>`, `work_items.read`,
  `work_items.write`, `feed.read_assigned`. Can spawn children and append
  progress/signals inside the assigned subtree.
- `same_tree_grower`: `work_items.tree:<root>`, `work_items.read`,
  `work_items.write`, `feed.read_assigned`. Can create candidate child work and
  append candidate evidence; cannot decide approvals.
- `same_tree_critic`: `work_items.tree:<root>`, `work_items.read`,
  `work_items.write`, `feed.read_assigned`. Can append bounded review signals;
  cannot transition the parent to terminal based on free-form judgment.
- `same_tree_reader`: `work_items.tree:<root>`, `work_items.read`,
  `feed.read_assigned`. Useful for read-only auditors and context gatherers.
- `system_reducer`: not a normal subactor token. The worker/reconciler uses a
  system-attributed token to append verdicts and lifecycle transitions through
  fixed reducer code.

## Directions for the subsequent agent

Coordinate through live meristem (MCP/feed/work_items), not this doc, once the
API is reachable. The slices below are ordered; each ships with its own tests
and respects the AGENTS.md "Things not to do" list.

### Slice A — persist the verdict (events + projection) — implemented

1. Add a migration `migrations/0010_convergence_verdicts.up.sql` /`.down.sql`
   (both required; one transaction each; no `CREATE INDEX CONCURRENTLY`).
   Create a `convergence_verdicts` projection table keyed on `event_id` (one
   row per verdict event), with indexed `work_item_id` and `attempt` columns
   rather than a `(work_item_id, attempt)` primary key. Columns mirror
   `Reduction.EventPayload()`: `work_item_id`, `reducer_identity`,
   `reducer_version`, `attempt`, `inputs_digest`, `disposition`, `reason`,
   `signals jsonb`, plus `event_id`, attribution, and `occurred_at`.
2. Write the projector in `internal/projections` (or a feature package
   consistent with the existing layout), registered for
   `EventConvergenceVerdictRecorded`. It must be pure w.r.t. the payload and
   rebuild-safe — folding the log reproduces the table byte-for-byte. Add a
   unit test that asserts the derived row given an event (the most important
   place for projection tests, per AGENTS.md).
3. Use `convergence.AppendVerdict` to append the verdict event via
   `internal/events` (never `INSERT` directly): subject kind
   `SubjectConvergence`, subject id is the judged `work_item_id`, and
   `attempt` stays in the payload. The event id is therefore derived from
   `(work_item_id, attempt, payload)`: replaying the same reduction is a no-op
   at the events writer, while a genuine retry gets a new attempt and row.
   Payload = `Reduction.EventPayload()`.
4. Update the projection rebuild path (`cmd/meristem rebuild`) and confirm the
   v0 acceptance test #6 (truncate-and-rebuild) still passes with the new
   table.

Backfill/rebuild story: the migration is schema-only; it does not insert
projection rows directly. Existing dev databases that already contain
experimental `convergence.verdict_recorded` events are repaired by folding the
event log through the newly registered projector (the rebuild/replay path), or
by resetting the dev database. This preserves the rule that non-`events` rows
are produced by projectors, not ad hoc migration DML.

### Slice B — worker integration (bounded patience)

1. In `internal/worker`, after the existing `patience.breached` scan, add a
   convergence pass for `work_item`s in `running` that declare a convergence
   pattern. The worker is the deterministic reducer's caller: it gathers
   signals (from child `validate` work_items, recorded `work_item.event_appended`
   grader outputs, or external-signal projections), calls
   `convergence.Run(...)`, appends the verdict event, then applies
   `Budget.Next(...)`:
   - `OutcomeAccept` → transition the work_item toward `done`.
   - `OutcomeRetry` → leave `running`; the next scan re-attempts (attempt+1).
   - `OutcomeEscalate` → apply the `Escalation` rule: `EscalateFail` →
     `failed`; `EscalateHandToHuman` → `blocked` + `human_review_status`
     blocked; `EscalateRequestApproval` → the approval system (v1; until
     approvals ship, treat as `blocked` with a reason, never auto-approve).
2. Every transition is its own event (one state change = one event). The
   verdict event and the transition event are distinct.
3. Where the work_item declares no pattern, do nothing — convergence is opt-in
   per item until a default policy is a separate, deliberate work_item.

### Slice C — declare the pattern on a work_item

Decide how a `work_item` names its reducer + budget. Two options; pick and
record the decision as a work_item event:

- Extend `work_item` metadata (a `convergence_pattern` field beside
  `suggested_convergence_checks`) via a new `work_item.metadata_updated`
  payload shape and migration, **or**
- Keep it out of the schema and derive the reducer from
  `suggested_convergence_checks` (checklist → `AllPassChecklist`) as the v1
  default, deferring multi-model/threshold patterns to explicit declaration.

The second is the smaller step and keeps the schema unchanged; prefer it
unless multi-model is needed immediately.

### Slice D — surface (REST + MCP), optional

Only after A–C: expose read access to `convergence_verdicts` (a
`work_items.get` detail enrichment, or a `convergence.list` tool) so an
operator and other agents can see why an item converged or escalated. REST is
canonical; the MCP tool mirrors it.

## Invariants the next agent must not break

- The reducer is pure and replayable. Never let a model's free-form judgment
  drive the lifecycle directly — the reducer must be specified and logged
  (spec rule #1, AGENTS.md principle #12).
- The verdict is appended as an event before its projection row exists, and the
  projector writes the row in the same transaction.
- Every new non-terminal situation ships with its escalation rule; nothing
  waits forever (bounded patience).
- `Signal.Raw` and event payloads are audit-safe: no secrets, no raw private
  message content.
- Adding an event kind = extend the domain constant + `AllEventKinds` + a feed
  classification + a registered projector when the event has a derived read
  view.
