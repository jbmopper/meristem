# Architecture charter: owner intent as desired state, with recurring drift review

> **Status: DRAFT for owner review.** Tracker work item `bddf0f3a` (state:
> `captured`). This charter names an architectural stance the system already
> half-embodies and proposes the one piece it does not yet have — a recurring
> drift review. Nothing here is settled until the owner acks it. Where this
> document and [`docs/spec.md`](spec.md) disagree, the spec wins; this is a
> reading of the spec, not a replacement for it.

## Thesis

The owner declares **intent as desired state**, in prose, one touch at a time.
The system's whole job is two things: **converge** toward that desired state
without further intervention beyond approvals, and **report drift** honestly
when the world has moved away from it. Convergence is the engine; drift review
is the conscience. Everything below is an elaboration of that one sentence,
grounded in mechanisms that already exist in this repo.

This is not new philosophy. It is the [Core Principles](spec.md) restated as an
operating charter: "Direction → convergence," "the event log is the system,"
"bounded patience," "default deny on side effects." The contribution of this
document is to name **drift** as a first-class concern with the same seriousness
the codebase already gives convergence, and to propose a cadence for catching it.

---

## 1. Desired state — where owner intent lives, concretely

Owner intent is not a mood or a backlog note. It is recorded, event-sourced
state that other machinery keys on. It lives in five concrete places:

- **Work items + `suggested_convergence_checks`.** A captured instruction
  becomes a `work_item`; its convergence checks are the machine-legible
  statement of "what would make this done." Each check carries a class prefix
  (`convergence-scribe` proposes them, a pure reducer validates them): `cmd:`,
  `event:`, `query:` are machine-verifiable; `human-ack:` is owner-gated.
  Unprefixed prose is refused with `unclassified_check` — intent that cannot be
  checked is not accepted as a check. A parent cannot leave `triaged` until
  valid checks land ([`docs/scribe-spec.md`](scribe-spec.md), R1).
- **`human_review_status` gates.** Every work item carries `blocked |
  waved_through | approved`. `waved_through` (the default) says "ordinary work,
  no gate"; `blocked` says "a human must clear this before it counts as
  converged"; `approved` records explicit owner clearance. This field is the
  owner's declared level of trust in a given branch of the graph.
- **Approvals.** For side effects (external writes), intent is expressed as a
  default-deny gate: the system never auto-approves, and the token that
  *creates* an approval cannot *decide* it ([`docs/spec.md`](spec.md),
  "Approvals and Convergence"). Approval is desired state that only the owner
  can supply.
- **The registry — cultivars, tropisms, projections.** How work should be done
  is itself declared data: a `cultivar` bundles worker profile + tropism +
  xylem budget + phloem projection; a `tropism` names a convergence pattern
  bound to a pure reducer; a `projection` is a stored filter over the log. These
  are an open set of names over a **closed** set of reducer semantics
  ([`docs/registry-spec.md`](registry-spec.md)). **Rootstock** entries are the
  immutable base case.
- **Policy profiles.** The owner's operating posture — `bring-up` vs `steady` —
  is a recorded `policy_profile.switched` event with a fingerprint, not lore in
  someone's head ([`docs/refresh-requirements.md`](refresh-requirements.md) R4).
  It declares how patient and how generous the substrate should be right now.

**Machine-checkable vs. human-ack.** The dividing line is the check prefix.
`cmd:` / `event:` / `query:` checks are evidence the running system can gather
and a reducer can fold — "done" is a fact the log can prove. `human-ack:` checks,
`human_review_status=blocked`, and approvals are the places where intent
*requires the owner's own hand* and no amount of automation may substitute for
it. `human_review_status=blocked` is deliberately THE fixed point of the
escalation loop: a blocked item still records `patience.breached` for visibility
but is never recursively escalated, so a backlog of owner-gated questions waits
quietly instead of generating an escalation storm ([`docs/owner-deep-dive.md`](owner-deep-dive.md),
Step 4).

---

## 2. Convergence — the contract between intent and implementation

Convergence is how a `work_item` reaches a terminal state (`done | failed |
canceled`), and the charter's claim is narrow and strict: **done means the
checks pass, not that someone said so.**

- **The worker/reconciler loop.** `meristem worker` runs deterministic ticks.
  Each `ScanOnce` runs four passes in order — **scribe** (spawn a check-proposer
  for checkless items), **dispatch** (queue items that have checks for a
  launcher), **convergence** (record a verdict for running items with checks),
  **breach** (emit `patience.breached` for over-budget epochs). The reconciler
  *notices*; agents *act*. It never authors content, plans, or judgments
  ([`docs/refresh-requirements.md`](refresh-requirements.md) R3;
  [`docs/owner-deep-dive.md`](owner-deep-dive.md) Step 4).
- **Deterministic evidence.** The probabilistic side proposes and judges
  (samples, fans out, grades a patch); the deterministic reducer reduces the
  signals it is handed into one of three verdicts — `accept | reject | escalate`
  — and *that verdict, recorded as `convergence.verdict_recorded`, is the only
  thing that advances the item* ([`docs/convergence-engine.md`](convergence-engine.md)).
  The reducer is pure and replayable (`AllPassChecklist`, `MajorityVote`,
  `Unanimous`, `Threshold`), and its evidence is content-addressed by an
  `InputsDigest` (SHA-256 over canonical reducer config + signals). Stale
  identical inputs do not burn a fresh attempt.
- **Checks as the contract.** Because checks are executable and verdicts are
  logged, "is this slice done?" is a question the *running system* answers, not
  a human judgment call. This is why R9 retired the notion of a milestone gate:
  the waypoints are the R-items and their convergence checks, tracked in the
  system itself. A model's free-form "looks good to me" may never drive the
  lifecycle directly — that would make the verdict unspecified and
  unreplayable, which the invariants explicitly forbid.
- **Bounded patience closes the loop.** No non-terminal state waits forever
  (Principle 3). Every state has a forward transition after a bounded delay or a
  timeout that fires one. `Budget.Next(verdict, attempt, budget)` maps to
  `accept → done`, `retry`, or `escalate`; a budget without an escalation rule
  is invalid at construction. This is the property that lets the system be
  trusted to "run itself out" while the owner is unreachable.

Convergence, then, is the mechanism by which implementation is pulled toward
declared intent and — crucially — by which the system can *prove* it got there.

---

## 3. Drift — a taxonomy, with real examples from this repo

Convergence assumes intent and implementation are being actively reconciled.
**Drift is the class of gap that convergence does not automatically close** —
because the desired state lives in one place and the reality it describes lives
in another, and nothing yet compares them. Four kinds have already bitten this
project:

1. **Spec-seed drift** — the seeded backlog diverging from the specs that
   authored it. This one already has a detector: `backlog.readiness` computes
   `spec_seed_drift`, reporting `missing_refresh_item:Rn` when a *partial*
   refresh backlog is present (some of R1–R9 seeded, siblings missing)
   ([`docs/backlog-readiness.md`](backlog-readiness.md)). The detector was
   itself tuned against a real failure mode — commit `bd36fbd` scoped it to
   partial backlogs so a clean bring-up carrying zero refresh items raises no
   false positives. Spec-seed drift is where "what the specs say should exist as
   work" and "what was actually seeded" fall out of step.

2. **Docs-vs-runtime drift** — operator docs describing a reality the runtime no
   longer has. [`docs/operations.md`](operations.md) still documents bring-up as
   a manual sequence of foreground `go run ./cmd/meristem api` / `worker`
   commands. Once a launchd/supervisor autostart brings the stack up on boot,
   that manual runbook describes a world that no longer exists — an operator
   following it double-starts or fights the supervisor. The doc is not wrong
   about history; it is stale relative to runtime, and nothing forces the two to
   agree.

3. **Policy drift** — live configuration diverging from the reviewed profile.
   `Policy.Fingerprint()` is a short stable hex id over the canonical policy,
   surfaced in `/readyz`, logs, and `meristem safety check`, precisely so a
   runbook can answer "which policy build is this binary running?"
   ([`docs/owner-deep-dive.md`](owner-deep-dive.md) Step 3). Commits `7a714ce`
   (govern pool size and worker cadence from the profile) and `18df568` (tighten
   profile-switch authority) are the seam where live behavior is meant to follow
   the reviewed profile. Policy drift is when the fingerprint reported at
   `/readyz` is not the fingerprint of the profile the owner last reviewed.

4. **Artifact drift** — the running binary predating the reviewed source. The
   owner deep dive names this "binary drift" as the original sin that motivated
   per-agent worktrees: someone rebuilds the shared `.meristem/generated/meristem-bin`
   from whatever branch is checked out, "so the running binary silently diverges
   from the ref everyone thinks they are on" — "the audit log says one thing,
   the binary does another." The repo hardened the *build* side (commit `d4b1868`
   unifies every wrapper and the API on one clean-`v1` artifact; `74a0b05`
   documents the codesign/firewall reality honestly), but [`operations.md`](operations.md)
   is explicit that **running sessions are not hot-swapped** — a rebuild only
   changes what the *next* launch execs. So repo-side rebuild (work item
   `a9374bdd`) and live redeploy (work item `835e0dbf`) are deliberately
   separate, and the window between them *is* artifact drift. The `0013`
   migration (`DEFAULT now()` on `state_entered_at` "for old-binary compat",
   commit `9a2ea08`) is an accommodation for exactly this window.

A fifth kind, **projection drift**, is the one the system already fully closes:
`meristem rebuild` folds the log through the projectors into a sandbox schema and
diffs against live, and CI gates on it (commit `6b9e96b` repaired a live instance
from events). It is listed here as the model for the others — a mechanical
comparison of a derived artifact against its source of truth, run on a cadence.

The through-line: every drift is a **comparison the system does not currently run
automatically** between two things that should agree. Convergence closes the gap
between intent and *implementation within a work item*; drift review closes the
gap between declared state and *the wider reality* — backlog, docs, live policy,
running binary.

---

## 4. Recurring drift review — a proposed cadence

The proposal is a standing, seeded **drift-review work item** that runs on a
fixed interval and treats each drift class as a comparison with a named left and
right side. It practices the discipline it enforces: it is a work item with its
own convergence checks, and the running system converges it.

**What gets compared against what, each cycle:**

| Drift class | Left (declared) | Right (reality) | How compared |
|---|---|---|---|
| Spec-seed | R-item specs' seeded items | `work_items` projection | `backlog.readiness` `spec_seed_drift` (already automated) |
| Docs-vs-runtime | `operations.md` bring-up steps, `/readyz` shape | actual autostart units, live `/readyz` | manual check this cycle (candidate for automation — see open questions) |
| Policy | reviewed profile fingerprint | `/readyz` `safety_policy` fingerprint | fingerprint equality |
| Artifact | `origin/v1` tip / reviewed ref | build ref of live API + `.meristem/generated/meristem-bin` | `meristem version` vs `git rev-parse origin/v1` |
| Projection | event log | live projection tables | `meristem rebuild` (already gated in CI) |

**How findings become work items.** A drift finding is captured the same way any
instruction is: `inbox.capture` (or a direct `work_items.create`) files a
`work_item` per finding, the scribe attaches convergence checks, and the item
converges like anything else. A finding whose remedy is mechanical (re-seed a
missing R-item, run `rebuild`) gets `cmd:`/`event:` checks and converges without
the owner. A finding whose remedy is a *decision* (redeploy now? accept this
policy change?) gets a `human-ack:` check and lands on the `owner-attention`
feed as `blocked`.

**Who acks.** The review's own terminal convergence check is `human-ack: owner
reviewed this cycle's drift report` — the owner reads the `owner-attention` feed,
records a decision event, and the review item converges. Suggested interval:
**weekly**, plus a mandatory run **immediately after any redeploy** (redeploy is
the moment artifact and policy drift are most likely to open or close). The
interval itself is an owner decision (§6).

Nothing about this cadence is a second source of truth: the review reads
projections and the log, files ordinary work items, and its findings live in the
same graph as everything else.

---

## 5. Decision rights

The charter's authority model is the spec's, made explicit for drift. It rests
on one provenance rule and a clean split.

**What the owner alone decides** (no agent, and — by separation of duties — not
even the root token, may substitute):

- **Scheduling and executing live deploys.** Repo-side rebuild is agent-safe
  (work item `a9374bdd`); the live redeploy that restarts the API/worker/MCP
  sessions is owner action (`835e0dbf`).
- **Pinning commits / trunk hygiene.** Fast-forwarding `footgun` to `v1` is
  `--ff-only`, and the terminal `human-ack:` on the whole refresh is the owner
  confirming the docs landed ([`docs/owner-deep-dive.md`](owner-deep-dive.md)
  Step 7).
- **Waving through vs. blocking review.** Setting `human_review_status`
  (`approved` vs `blocked`) and deciding approvals. The approver token must
  differ from the proposer; tokens that create approvals cannot decide them.
- **Switching policy profiles.** `policy_profile.switch` is an operator scope;
  root is explicitly **forbidden** (`root_token_forbidden`).
- **Rootstock migration, token minting, and panic revoke.** Rootstock cultivars
  refuse redefinition; root custody is human-only and permanent (Principle 7);
  `revoke-all` is root's one broad power.

**What agents may do autonomously:** propose convergence checks (scribe), spawn
children and append signals/evidence, run reducers and record verdicts via a
`system`-attributed token, request escalations, propose new cultivars through the
R5 grant + review gate (**but never approve their own proposal**), and rebuild
the shared binary repo-side. In drift terms: agents may *detect* drift and *file*
findings freely; they may *remedy* mechanical drift under `cmd:`/`event:` checks;
they may **not** redeploy, switch profiles, wave through review, or migrate
rootstock.

**The provenance rule: only genuine owner decisions may carry owner authority.**
`actor_token_id` and `source` come from the authenticated request context on
every event — never from a request body or header — so the audit answers "who,
via what client, with what authority." Owner authority attaches to `source=human`
owner-decision tokens and to explicit decision events; it cannot be forged by an
agent claiming it in a payload, and it cannot be back-doored through root, whose
authority is deliberately narrowed to minting and revoking tokens. A drift
finding that says "the owner approved this" is only true if a genuine
owner-decision event backs it. This is separation of duties made structural, and
it is what keeps the drift-review loop itself trustworthy: the review can *ask*
for an ack, but only the owner can *supply* one.

---

## 6. Open questions for the owner

- **Cadence.** Is weekly-plus-post-deploy the right interval, or should the
  review run on a different trigger (every seed, every profile switch)?
- **Docs-vs-runtime automation.** Should docs-vs-runtime drift get its own
  detector (e.g. a check that `operations.md`'s bring-up matches the live
  autostart units and `/readyz` shape), or stay a manual line item? It is the
  one class with no mechanical comparator today.
- **The reviewed baseline.** Is `origin/v1` tip the canonical "reviewed source"
  ref for artifact drift, and is the last owner-acked profile fingerprint the
  canonical "reviewed policy"? Where should those baselines be recorded so the
  comparison has an unambiguous left-hand side?
- **A `question.*` feed.** The refresh doc already asks whether the taxonomy
  needs a `question.*` class so "the system asks" is a subscribable feed. Drift
  findings are a natural first tenant — should drift review publish there?
- **Rootstock revision cadence.** Carried over from
  [`docs/refresh-requirements.md`](refresh-requirements.md): what evidence should
  trigger an owner-approved rootstock migration? Drift review is a plausible
  source of that evidence.
