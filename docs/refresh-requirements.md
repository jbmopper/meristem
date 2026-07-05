# Refresh requirements: disciplined spin-up

Status: draft for owner review. Drafted 2026-07-03 from the self-review
session (work item `81ca857f`) and the requirements discussion that followed.

This document specifies the mechanisms the next phase must add so that the
system can grow its own seed: self-defining convergence, self-identifying
research questions, self-generating next steps — maximizing one-touch
effectiveness for the owner and the system's capacity to self-build and
self-heal.

Each requirement ends with its convergence checks. Per R9, these checks are
the exit criteria; there is no separate milestone gate. The requirements
practice the discipline they specify: they are seeded as `work_item`s with
these checks attached, and the running system converges them.

## Lexicon

The metaphoric register is botanical, matching the project name. These terms
are load-bearing shorthand, defined once here:

- **xylem** — the budgeted consumable flow: event writes, wall-clock time,
  delegation (agent-stack) depth, spawn count, chatter rate. Drawn upward
  under meter. The deterministic layer owns xylem: it measures, budgets, and
  cuts off.
- **phloem** — the rich directed flow: context briefs, findings, plans,
  decisions, escalation summaries. The generative layer traffics in phloem;
  projections (R6) are how phloem is loaded and routed.
- **tropism** — a named convergence pattern: the declared direction of growth
  toward a fixed point, with deterministic reducer semantics. Initial set:
  `run-to-green`, `checklist-all`, `judge-vote`, `external-signal`,
  `human-ack`.
- **cultivar** — a named, versioned bundle: worker profile + tropism + xylem
  budget + phloem projection. A work item executed by an agent runs under
  exactly one cultivar.
- **rootstock** — the small predefined set of cultivars that self-definition
  grafts onto; the recursion base case (see R1). Rootstock cultivars ship in
  the seed and cannot be self-modified — changing rootstock is an
  owner-approved migration.

The deterministic/generative split of `docs/spec.md` is the vascular system:
xylem is what the deterministic subsystem meters, phloem is what the
probabilistic subsystem exchanges. Neither flow crosses into the other's
authority.

## R1 — Self-defining convergence

A work item arriving without convergence checks is a *state*, not an error.
The system responds by spawning a child work item: "define convergence for
X." That child runs under the `convergence-scribe` rootstock cultivar
(tropism `checklist-all`, falling back to `human-ack` when the checklist
cannot be satisfied). Its output is a proposed check list written to the
parent's metadata; a deterministic reducer validates the proposal before it
lands:

- every check is executable (names a command, query, or signal the worker
  can run) **or** explicitly human-gated (`human-ack:` prefix);
- the list is non-empty and each entry is non-blank (existing normalization);
- the proposal event records reducer identity and inputs (spec: Convergence
  Patterns).

The parent cannot leave `triaged` until its checks exist. The owner never
writes checks unless they choose to; one-touch capture stays one-touch.

Base case: the definition child itself carries predefined checks from its
rootstock cultivar and never spawns its own definition task. Rootstock is
what makes self-definition terminate. Expect iteration here; the reducer and
the rootstock checklist are versioned data (R2, R5), not code.

Convergence checks:
- Integration test: capturing an item with no checks produces a
  `convergence-scribe` child; parent blocked from leaving `triaged` until a
  valid proposal lands; invalid proposals (blank, non-executable,
  un-gated) are rejected by the reducer with a structured refusal.
- Integration test: the definition child never recurses (no
  grandchild definition task).

## R2 — Tropisms and cultivars

A versioned registry, stored as data (events + projection, per R5/R6):

- **Tropisms** pair a probabilistic proposer role with a pure, replayable
  reducer (spec: Convergence Patterns). The reducer set is closed and
  versioned; adding reducer *semantics* is a code change with tests. The
  tropism *instances* (parameterizations) are open data.
- **Cultivars** bundle worker profile + tropism + xylem budget + phloem
  projection under a name and version. R1's scribe selects and parameterizes
  from this registry; it never invents free-form reducers.

Open set of cultivars, closed set of reducer semantics: flexibility and
robustness held in one place.

Convergence checks:
- Registry projection exists and is rebuildable from events (replay test).
- Every seeded tropism has a reducer unit test over recorded signals,
  asserting verdict + reducer-identity event.
- A work item created with an unknown cultivar or tropism name is refused
  with a structured error naming the registry tool to consult.

## R3 — Reconciler as metronome (noticing/acting split)

The deterministic layer owns **noticing**; agents own **acting**.

`meristem worker` runs deterministic ticks (SELECT … FOR UPDATE SKIP LOCKED):
scan non-terminal items, compare age-in-state against the item's patience
budget (from its cultivar or explicit override), and act only mechanically:

- transition on timeout exactly as the item's declared escalation rule
  states;
- spawn the escalation item the rule names;
- append a dispatch entry to the dispatch feed (R6) saying "this item needs
  attention," with the cultivar that should handle it.

The reconciler never authors content, plans, or judgments. Launchers — agent
wrappers, or the owner's own session — consume the dispatch feed and wake
workers with phloem loaded from the item's projection. Owner control lives
in: policy profiles (R4), `human_review_status` gates, and the escalation
inbox. The metronome and tripwire are deterministic; the hands are agents.

Convergence checks:
- Integration test: an item exceeding its patience budget is transitioned /
  escalated per its declared rule within one tick, with full attribution;
  `patience.breached` records the resolved budget source and escalation rule.
- Integration test: the reconciler appends only event kinds from its
  mechanical allowlist (no free-form payloads).
- Soak: reconciler runs against a seeded backlog for a bounded interval;
  no non-terminal item lacks either a forward transition or a pending
  dispatch entry (principle 3 made observable).

## R4 — Bring-up as a policy profile

"Mellowing" during bring-up is a declared fact, not lore. Policy profiles
are named, fingerprinted (like the safety policy), reported in `/readyz`,
and carry explicit exit criteria:

- `bring-up`: relaxed patience budgets (long but finite), escalation routes
  to the owner feed instead of `failed`, generous xylem.
- `steady`: spec-normal budgets.

Profile switch is an owner action recorded as an event. Bring-up's exit
criteria are themselves convergence checks on named substrate items (R9).

Convergence checks:
- `/readyz` reports the active profile fingerprint; profile switch appends
  an attributed event.
- No profile admits an infinite patience value (extends the existing
  non-zero validation to non-infinite).

## R5 — Worker profiles and cultivars as data

Profiles and cultivars are rows projected from events, updatable via work
items — never hardcoded. Self-extension is allowed and gated: a worker that
discovers a possible new worker files a scoped subtask proposing the
cultivar; the proposal flows through the existing subactor-grant reducer
plus a `human_review_status` gate before any token or profile is minted.
Separation of duties as in the token layer: the proposer cannot approve.

Rootstock cultivars are excluded from self-modification (owner-approved
migration only).

Convergence checks:
- Integration test: cultivar create/update round-trips through events and
  replays identically.
- Integration test: a worker-proposed cultivar cannot become active without
  passing the grant reducer and review gate; the denial path leaves an
  attributed trail.

## R6 — Projections and feeds as data

Named projections are defined by filter expressions over event kinds,
subjects, and trees — added as data, not code. Feeds are scoped views over
projections; `feed.read` gains a projection parameter. The dispatch feed
(R3) is one such projection.

Event kinds adopt a taxonomy separating xylem-metered chatter from phloem:
`progress.*` (heartbeats, tool logs — budgeted, cheap to drop from briefs),
`decision.*` (verdicts, plans, findings), lifecycle (transitions,
relations). Context assembly for a worker is a *projection*, not a
per-launch prompt hack — the Cerberus context-composer idea landed in the
deterministic layer where any model can benefit from it.

Convergence checks:
- Integration test: define a projection via API, read it via `feed.read`,
  rebuild it from events, byte-identical.
- The June-2026 archive is replayed through the taxonomy classifier and the
  report is committed; if the sample contradicts the expected split, the
  artifact records the mismatch rather than weakening the classifier.

## R7 — Xylem budgets in the substrate

Safety-policy entries, enforced deterministically, referenced by cultivars:
max children per item, max events per item per hour (by taxonomy class),
max delegation depth, max concurrent running items per token. Exhaustion is
not an error state: it is a `blocked` transition with an escalation per the
item's rule.

Convergence checks:
- Unit tests per budget: exceeding it blocks with a structured, attributed
  event; never a silent drop.
- Integration test: a spawn chain deeper than the budget is refused at the
  grant layer.

## R8 — Seed, bootstrap, and the corpus

`meristem seed` is a CLI command with an explicit division of labor:

- **Stage 0 — human, once per host.** `scripts/bootstrap.sh`: safety check,
  Postgres up, migrate, mint root. Root-token custody is human-only,
  permanently (principle 7). Agents never see root.
- **Stage 1 — human, one command.** `meristem seed` under a
  `system`-source token. Idempotent (discriminator-aware). Plants: the
  substrate backlog (these R-items), rootstock cultivars, named
  projections including the dispatch feed. Policy profiles are code-owned
  and reported through `/readyz`; switching to `bring-up` remains an explicit
  non-root human action so agents are governed by the profile rather than
  authoring it. Client-token minting remains a root/human act; until the
  approval-table substrate exists, any token-request handoff is represented
  as ordinary work_item/review state, not an auto-approved side effect.
- **Stage 2 — agents.** Everything else. The first minted worker connects,
  reads the dispatch feed, and the system grows itself. Human touchpoints
  from here: token grants, explicit profile switches, escalations, future
  approvals, and panic revoke.

Corpus: raw dumps stay private (they are the owner's legible planning
diary — inbox captures verbatim, tooling topology, working hours). The
publishable corpus is produced by a deterministic exporter with an
event-kind allowlist and a scrub pass over free-text fields. The exporter is
a fold over the log — the first instance of "assessed by being asked."
Replay test: restore an archive dump → replay → projections identical.

Convergence checks:
- `seed` twice on a fresh database: second run appends zero fresh events.
- Fresh bootstrap + seed on a clean host reaches "first worker dispatched"
  with root custody, system-token seed attribution, an explicit human
  `bring-up` switch, and a recorded token-grant/review step for the first
  worker.
- Exporter output on the 2026-06-06 archive contains no token names, no
  inbox `message.captured` bodies, and no non-allowlisted kinds (test
  asserts the allowlist).
- Replay test green on all archived dumps.

## R9 — Dogma conformance and role-scoped briefings

AGENTS.md remains the single dogmatic projection of the spec — but every
bullet in its Techniques section must map to a test or deterministic check
in the repo, so the dogma cannot drift from reality (the deterministic-id
bug was dogma-enshrined; never again).

Briefings are sized to the role: each cultivar carries a briefing
*projection* of AGENTS.md — a leaf worker gets a short imperative checklist
(its tools, its tropism, its budget, its refusal semantics), not the full
catechism. Small models comply with dogma they can hold in context;
resistance was a context problem, not a character problem.

The v0-as-milestone concept is retired. Waypoints are these R-items and
their convergence checks, tracked in the running system (principle 11
applied to the waypoints themselves).

Convergence checks:
- A conformance map (doc or test manifest) linking every Techniques bullet
  to its test; CI fails on unmapped bullets.
- Each rootstock cultivar has a briefing projection ≤ 40 lines, generated
  from AGENTS.md source sections, regenerated in CI when the source
  changes.

## Open questions (tracked as research work items, per R1's spirit)

- Reducer semantics for `judge-vote` with heterogeneous model panels: how
  are judge identities recorded so verdicts stay replayable?
- Whether the taxonomy (R6) needs a `question.*` class for
  self-identified research questions, so "the system asks" is a feed the
  owner can subscribe to.
- Rootstock revision cadence: what evidence should trigger an
  owner-approved rootstock migration?
