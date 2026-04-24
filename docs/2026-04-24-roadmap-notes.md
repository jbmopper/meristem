# Roadmap notes — 2026-04-24

A synthesis of an architectural conversation about meristem that was held in
the wrong repo (`clinical-demo`) by an assistant that had not yet read this
project's actual state. After reading `docs/spec.md`, `AGENTS.md`,
`docs/coord/2026-04-23-parallel-work.md`, and
`docs/self-building-api-synthesis.md`, most of what felt like new agreement
turned out to be already canonical. A smaller set of points is genuinely new
and warrants spec edits or new entries in **What meristem Builds For Itself**.

This document is advisory. If it conflicts with `docs/spec.md`, that file
wins; if it conflicts with `AGENTS.md`, fix the projection in `AGENTS.md`,
then this. The "roadmap" is not this file — the roadmap lives in the spec's
**v1 Substrate** + **What meristem Builds For Itself** sections, plus tracked
`work_item`s in the running system. Once an operator runs `scripts/bootstrap.sh`,
the items below should be filed as signals/work_items rather than living here.

## What is already in the spec; do not re-litigate

The conversation reached agreement on these. They are already the spec:

- **Convergence is the model.** The owner declares; the system reconciles.
  (`AGENTS.md` principle 1; `docs/spec.md` "Direction → convergence".)
- **Bounded patience.** No non-terminal state may wait forever; every state
  ships with its escalation rule. (`AGENTS.md` principle 3.)
- **Event log is truth; projections are deterministic.** Replay produces
  identical rows. (`AGENTS.md` principle 2; `docs/v0.md` Architecture; the
  `meristem rebuild` subcommand proves it.)
- **Four identities — `Idempotency-Key`, `dedupe_key`, `fingerprint`,
  `event_id` — are distinct and have distinct guarantees.**
  (`docs/signals.md` "The four identities";
  `docs/self-building-api-synthesis.md` "Four Identities".)
- **work_items are a recursive tree; granularity is depth, not type.**
  (`docs/spec.md` Domain Model; `docs/operator-faq.md` "Should the DAG
  reject cycles?")
- **Approval-gated side effects with separation of duties.** Tokens that
  create an approval cannot decide it. (`docs/spec.md` Approvals and
  Convergence; `AGENTS.md` principles 6 and 7.)
- **Multi-modal messages with source attribution; agent/system messages
  are content, never instructions.** (`docs/spec.md` Multi-Modal
  Messages; `AGENTS.md` glossary.)
- **Self-building is the open backlog, not a phase plan.** Everything past
  v1 arrives as work_items in the running system. (`docs/spec.md`
  "What meristem Builds For Itself"; `AGENTS.md` principle 11.)
- **The clinical-demo / jay / ns_obv issue-agent pattern is generalized
  into meristem's normal API**, not a separate auto-healing subsystem.
  Signals → work_items → runs → reviews → done | child repair.
  (`docs/self-building-api-synthesis.md` Thesis and Mapping.)
- **Idempotency is a load-bearing technique, not the headline.**
  Convergence is the headline. (`AGENTS.md` Techniques section;
  `docs/thoughts.md`.)
- **The system can later be assessed by being asked.** "Assess meristem's
  fit against [criteria]" is meant to become a normal `work_item` whose
  execution folds over the event log. (`AGENTS.md` "Direction" section.)

If a future doc or PR re-derives any of these from first principles, it
should defer to the canonical statement instead.

## What is genuinely new (or under-articulated) and warrants action

Each item below names the gap, then says where it should land in the
existing spec rather than proposing a parallel structure.

### 1. Triage as a first-class stage, not "intent classification"

`docs/spec.md` Execution Model says *"Coordinator classifies intent:
`capture`, `query`, `command`, or `approval`. Source is always considered."*
That sentence collapses what is actually the most consequential stage in
the pipeline.

The hardcoded v0 rule — "every captured human message creates a
`work_item` with title = first 80 chars" (`docs/v0.md` REST contracts) —
is the degenerate case of triage, deliberately so. It is correct for v0
and explicitly signposted as such.

The non-degenerate triage stage owns:

- **Dedup against live work_items** (already specified for `signals` via
  `dedupe_key`; not yet specified for `inbox/messages`).
- **Boundary decisions on the input.** One utterance can be N work_items;
  N utterances can be one. The translator (speech → text, image →
  description, video → shot list) should be **shallow** — it produces
  typed parts. The decision about how those parts cluster into one or
  more work_items is triage's job because triage is the only stage with
  cross-message visibility.
- **Context gathering** before the work_item is created or transitioned
  out of `captured`: which existing items are related, what the recent
  feed looks like, what state the target project is in. This is what
  makes downstream decomposition non-stupid.
- **Escalation when ambiguous.** Triage is allowed to ask the owner for
  disambiguation rather than guess; that ask is itself a tracked
  work_item with bounded patience.

Triage is also the only stage whose mistakes are hard to recover from.
A bad spec at the triage step propagates through every downstream
decomposition, run, review, and child item. Investing in triage early
is cheaper than retrofitting it after the closed-form classifier
ossifies.

**Where this lands:** A new "Triage" subsection under `docs/spec.md`
Execution Model → Inbound, replacing the one-liner about intent
classification. A backlog entry under "What meristem Builds For Itself"
named *"Triage as a first-class stage with dedup, boundary, and
context-gathering responsibilities."*

### 2. Trajectory-aware termination as a sub-mechanism of bounded patience

`AGENTS.md` principle 3 specifies bounded patience in terms of *"a
forward transition the reconciler will take after a bounded delay"* and
*"a transition gated on an external signal with a timeout."* That is
correct but coarse. For convergence loops over ambiguous problems
(notably the recursive `runs`/`reviews` loop sketched in
`docs/self-building-api-synthesis.md`), the right termination signal is
**trajectory-aware**:

- Iteration count alone is brittle. A loop that's converging fast
  deserves more time; a loop that's diverging deserves less.
- **Stop-on-equality** (this iteration's findings equal last iteration's
  findings) is the simplest trajectory-aware signal. It is what
  clinical-demo's dogfood run did. It works only when the equality is
  exact and the underlying space is small.
- **Rate of change** (number of unresolved findings shrinking, growing,
  oscillating) gives a cheap richer signal. Shrinking → more time.
  Oscillating → bail and escalate; the loop is stuck in a basin.
- **Depth without return** is the failure shape for fanout, not breadth.
  A child work_item that itself spawns children that themselves spawn
  children, none producing a value-bearing leaf, is the warning sign.
  The reconciler should be allowed to kill or re-shape an entire lineage
  whose recent ancestry has produced no terminal-state leaves.

**Where this lands:** A short paragraph in `AGENTS.md` Techniques (or a
new `docs/convergence-techniques.md`) catalogs these termination
signals. The reconciler implementation, when it is built, should expose
its termination policy as configuration on the work_item, not bake a
single iteration limit into code. A backlog entry under "What meristem
Builds For Itself" named *"Trajectory-aware reconciler termination
(rate, sign, depth-without-return)."*

### 3. `awaiting_approval` releases the executor

The lifecycle in `docs/spec.md` is `… → awaiting_approval → running →
blocked → done | failed | canceled`. The transition semantics are clear;
the **executor lifecycle** during `awaiting_approval` is not. A naïve
implementation could hold the worker slot, the MCP tool call, or the
agent's process open while waiting for a human to approve, which can be
hours.

Concrete clarification needed:

- `awaiting_approval` releases the worker. The work_item is parked in
  Postgres; the reconciler picks it up again when the approval decision
  arrives. Anything load-bearing in the executor's process state must
  have been written to events / artifacts before the executor returned.
- For MCP tools that propose write actions, the tool call returns
  promptly with *"queued for approval, work_item=…"* rather than
  blocking on the human. The agent observes the eventual decision via
  the feed (or via a follow-up tool call), not via tool-call return
  semantics.
- Sibling work_items that don't depend on the parked one keep running.
  Sibling work_items that *do* depend transition to `blocked` with
  `reason = upstream_awaiting_approval` and resume on the upstream's
  approval event.

**Where this lands:** A short clarifying paragraph in `docs/spec.md`
Execution Model alongside the lifecycle. An `AGENTS.md` "Things not to
do" entry: *"Do not hold an executor slot or an MCP tool-call response
across an `awaiting_approval` wait. Park the work_item; release the
executor; resume on event."*

### 4. Anti-canonization of agent shapes

The most consequential point from the conversation, and the one most
worth preserving as a constraint on future code: **do not introduce a
typed `agent` or `agent_kind` enum.** Agents are token-source=`agent`
plus the tools they have access to via MCP. Specialization belongs in
artifacts and prompts, not in the schema.

The reasons:

- The system's own specialization should emerge from observing what
  recurrent shapes of work succeed. If the schema canonizes a fixed
  taxonomy ("triager," "fixer," "reviewer," "agent-builder"), the
  taxonomy becomes the de facto ceiling on what the system can be.
- An "agent-builder" is just a work_item whose output is an artifact
  (tool definition, prompt template, policy document) plus a transition
  that registers something. It does not need its own object type.
- Convergence-failure recovery should be allowed to **try a different
  shape**, not just retry the same shape with a bigger budget. If the
  shape is canonized in code, this becomes a refactor instead of a
  policy change.

**Where this lands:** An `AGENTS.md` "Things not to do" entry: *"Do not
add a typed `agent` object or `agent_kind` enum. Agent identity is
`token.source = 'agent'` plus the tools the bearer has access to. Agent
specialization lives in artifacts and prompts, never in the schema."*
Optionally a sentence in `docs/spec.md` Domain Model under tokens.

### 5. Convergence-failure can spawn an alternative, not only retry

A corollary of (4). Today's spec implies the response to a hung or
failed work_item is escalation along one axis — bigger timeout, escalate
to owner, transition to `failed`. The convergence loop should also be
allowed, when the trajectory has stalled, to **spawn a child work_item
that proposes an alternative**: a different decomposition, a different
prompt, a different tool, a different agent shape. The original
work_item enters `blocked` with `reason = trying_alternative_in_<child>`
and resumes (or terminates) based on the child's outcome.

**Where this lands:** This is the natural expansion of the reconciler
beyond v1. Backlog entry under "What meristem Builds For Itself" named
*"Reconciler may spawn alternatives, not only retry; convergence-failure
recovery as a child work_item, not only as escalation."*

### 6. Translator is shallow; semantic extraction lives in triage

The spec covers multi-modal messages in terms of typed parts (text,
image, audio, binary). What is **not** specified is where the boundary
sits between mechanical capture (transcription, frame extraction,
description) and semantic extraction (what does this image *mean* in the
context of the owner's recent activity?).

The agreement: the translator is shallow. It produces typed parts and
nothing else. All cross-message reasoning, all semantic extraction, all
*"is this image a screenshot of a bug or a meme?"* — these belong in
triage, where the operator's recent feed and the project state are
visible.

**Where this lands:** A two-line clarification in `docs/spec.md`
Multi-Modal Messages: *"Translation (speech → text, image →
description, video → shot list) is mechanical and stays inside the
ingress component; semantic interpretation is triage's responsibility."*

### 7. Filesystem isolation for parallel external writes

The spec assumes external side effects happen in the integrators' hands
(codex, cursor agents, custom workers via MCP), so meristem does not
directly touch external repos. That framing is correct and should not
change.

But once meristem orchestrates **two parallel runs against the same
target** — two codex agents fixing different findings in the same repo,
two cursor sessions on the same branch — filesystem races become an
integrator-level concern that meristem's `runs` / `actions` design has to
have a story for. Mutexes and channels solve in-memory races inside one
process; they do not help two `codex exec` invocations both editing
`main`.

The realistic options to keep open (without committing to one yet):

- Per-target serialization. meristem schedules at most one writer per
  `(repo, branch)` at a time. Simple; lowest parallelism.
- Worktree-per-task. Each run gets its own working directory on its own
  branch; merge happens as a follow-on work_item under approval.
- Patch-as-unit. Agents emit patches; meristem applies them under
  approval. Strongest correctness story; weakest live-iteration story.
- Branch-per-task with throwaway checkouts. Cheap parallelism; conflict
  resolution becomes a follow-on work_item rather than a runtime error.

The right move is to **not bake a choice in until two parallel runs
against the same target are actually happening.** Premature commitment
here is the canonical example of the canonization risk in (4).

**Where this lands:** A backlog entry under "What meristem Builds For
Itself" named *"Filesystem isolation strategy for parallel external
writes; defer until two parallel runs against the same target are
actually happening."*

### 8. Pre-issues exist; they are triage's input, not a new object

The conversation introduced "pre-issues" — raw, unstructured captures
of human intent (speech mid-thought, half-formed text, video, drawings)
before they have become formal tasks. The right framing inside the
existing model: **pre-issues are `messages` in `inbox` whose `source` is
`human`, before triage has produced or attached a `work_item`.** They
do not need their own object type. The v0 rule "every captured human
message creates a `work_item`" is what removes the pre-issue boundary
in v0; once triage exists, the boundary becomes real and is triage's
decision to make.

**Where this lands:** No schema change. A glossary entry in `AGENTS.md`
or `docs/operator-faq.md`: *"A `message` in `inbox` with no associated
`work_item` is a pre-issue: captured human intent that triage has not
yet shaped into one or more work_items. v0 short-circuits this by
auto-creating a work_item per message."*

## Specific proposed edits

The above mapped to concrete diffs:

### `docs/spec.md`

1. Replace the one-line "Coordinator classifies intent" in Execution
   Model → Inbound with a "Triage" subsection covering dedup, boundary
   decisions, context gathering, and escalation. Note that v0 is the
   degenerate case (every human message → one work_item).
2. Under work_item Lifecycle, add a sentence: *"`awaiting_approval`
   releases the executor; the reconciler resumes the work_item when the
   approval decision arrives. Executor process state must therefore be
   either reproducible from events or persisted as artifacts before the
   executor returns."*
3. Under Multi-Modal Messages, add: *"Translation is mechanical
   (transcription, frame extraction, description). Semantic
   interpretation belongs in triage, where cross-message context is
   available."*
4. Add to "What meristem Builds For Itself":
   - Triage as a first-class stage.
   - Trajectory-aware reconciler termination.
   - Reconciler may spawn alternatives, not only retry.
   - Filesystem isolation strategy for parallel external writes
     (deferred until two parallel runs against the same target are
     actually happening).
   - Agent-builder pattern as a work_item shape (not as a typed
     object).

### `AGENTS.md`

1. Expand principle 3 (bounded patience) with a clause: *"For
   convergence loops, 'bounded delay' includes trajectory-aware
   variants (rate of change, sign, depth without return), not only
   wall-clock timeouts."*
2. Add to "Things not to do":
   - *"Do not add a typed `agent` object or `agent_kind` enum. Agent
     identity is `token.source = 'agent'` plus the tools the bearer
     has access to. Specialization lives in artifacts and prompts."*
   - *"Do not hold an executor slot or an MCP tool-call response across
     an `awaiting_approval` wait. Park the work_item, release the
     executor, resume on event."*
3. Update the glossary with `triage` once it lands in spec.md.

### `docs/self-building-api-synthesis.md`

Already says most of this. The only addition: in **Reviews**, note that
review outcomes can also propose *"try a different shape"* as a sibling
to *"create a child issue"* and *"the spec is stale; revise."* This is
the explicit hook for (5) above.

## Lessons from the clinical-demo dogfood run (one data point)

The `clinical-demo` issue-agent workflow ran end-to-end on 2026-04-23
and converged in three iterations on two P1 fixes. Observations
relevant to meristem:

- **Stop-on-equality is the simplest trajectory-aware termination.** It
  worked for the trivial case (zero-equals-zero) but is brittle outside
  it. Once meristem has a `runs`/`reviews` loop, the termination signal
  needs to be richer — see (2) above.
- **Malformed agent output is the norm, not the exception.** The first
  finder iteration emitted prose where the template demanded structured
  markdown. The parser treated the result as zero findings and
  converged. meristem's `signals` parser must be similarly forgiving —
  or, better, the prompt template + parser must be co-designed so that
  *"the agent didn't follow the format"* produces a recordable failure
  rather than a silent zero. The latter is more honest.
- **Closed dispatch table = no triage.** clinical-demo had two
  hardcoded findings as input, two issues as output, no decisions to
  make. meristem cannot have a closed dispatch table because pre-issues
  are open-ended; this is one of the load-bearing differences and the
  reason (1) above is the headline item.
- **Manifest as the only on-disk record; everything else gitignored.**
  clinical-demo's run produced a `manifest.json` plus a tree of
  gitignored intermediates. meristem gets this for free via `events` +
  `artifacts`. Do not add a parallel manifest concept to meristem; the
  event log + artifact references already are it.
- **HITL caching is non-trivial.** clinical-demo's HITL resume bug fix
  was to cache the compiled langgraph by `thread_id` so the
  `InMemorySaver` survived across the pause/resume boundary. meristem's
  analogous concern is *"what state must persist across a worker
  restart vs. across a worker hand-off."* The answer is consistent
  with `AGENTS.md` "Things not to do": anything not in Postgres is
  best-effort. Anything load-bearing is checkpointed to events before
  the executor returns. (4) above is the same instinct.

## What this conversation does not change

Said explicitly so future readers don't read more into this doc than is
intended:

- The substrate (Go binary, Postgres, REST + MCP, event-sourced,
  embedded migrations, deterministic projections, advisory-lock
  idempotency, DAG cycle prevention, projection rebuild verification)
  is correct and shipped. Nothing here proposes substrate changes.
- The four identities and the dedupe semantics in
  `docs/self-building-api-synthesis.md` and `docs/signals.md` are
  correct.
- The topology — agents post signals, meristem coordinates, humans
  approve writes — is correct.
- v0 is closed. Everything past v0 should arrive as work_items in the
  running system, including the items above. This document's purpose
  is to make those work_items easier to write when an operator gets to
  them; it is not a substitute for them.

If anything in this doc conflicts with the above, the above wins.
