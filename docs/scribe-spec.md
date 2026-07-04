# Convergence scribe: R1 implementation spec

Status: implementable spec, drafted 2026-07-04 by the Claude spec session for
work item `d7d8f598` (R1, parent `c6ba707b`). Implements
`docs/refresh-requirements.md` R1 against the codebase as of v1 tip `9c479c6`.
Suggested implementation owner per the divvy plan: Codex.

R1 in one sentence: a work item arriving without convergence checks is a
*state*, not an error — the system spawns a scribe child to propose checks, a
deterministic reducer validates them onto the parent, and the parent cannot
advance past `triaged` until that happens.

## Mechanics overview

Five pieces, four of which reuse existing machinery:

1. **Noticing** — a scribe pass in the metronome worker (`ScanOnce`) finds
   checkless items and mechanically spawns their scribe child. Reuses the
   worker's noticing/acting split and its idempotent per-row transactions.
2. **Gating** — the work-items service refuses forward transitions out of
   `captured`/`triaged` while checks are empty. Pure service-level guard.
3. **Proposing** — a scribe agent (any model, launched under the
   `convergence-scribe` rootstock cultivar) appends one first-class event:
   `convergence.checks_proposed`.
4. **Reducing** — a pure validator accepts or refuses the proposal. On
   accept, the same transaction writes the checks to the parent via the
   existing `work_item.metadata_updated` path with reducer identity in the
   payload. Reuses the convergence verdict machinery (budget, attempts,
   stale-input skip, escalation on exhaustion).
5. **Terminating** — the scribe child's own checks are born with it
   (rootstock base case), so the spawn rule can never recurse.

## 1. Noticing: the scribe pass

Added to `internal/worker` alongside the breach and convergence passes.

Candidate query: work items in state `captured` or `triaged` with
`suggested_convergence_checks = '[]'` and no existing scribe child. The
"no existing scribe child" test is deterministic because the child id is
derived, not random:

```
scribe_child_id = uuid5(ns, parent_id || "|convergence-scribe|v1")
```

One scribe child per parent, ever — the deterministic id plus the events
writer's replay dedupe makes the spawn idempotent across concurrent workers,
the same way escalation ids work today.

The spawn is mechanical authorship (templated title/body from the parent
snapshot), which is the same authority the escalations service already
exercises when it creates human-attention items. The worker appends, in one
transaction, on fresh spawn only:

- `work_item.created` (subject: scribe child) with:
  - title: `Define convergence for: <parent title>` (truncated to fit title norms)
  - body: templated — parent id, title, body excerpt, and the proposal
    contract (§3) so a small model needs no other context
  - state: `triaged` (it is born ready for pickup)
  - `suggested_convergence_checks`: `["query:parent_checks_defined"]` (§5)
  - `human_review_status`: `waved_through`
- `work_item.relation_added` (parent → scribe child)
- the scribe child's cultivar recorded in the created payload as
  `"cultivar": "convergence-scribe@v1"` — launch metadata, not schema
  identity, per the Cerberus reducer-contract rule

Exclusions from the candidate set:

- items whose `human_review_status` is `blocked` (they are the owner's; same
  fixed-point rule as the escalation guard in `9c479c6`)
- scribe children themselves (they are born with checks, so they never match
  the empty-checks predicate — the exclusion is structural, not a filter)

Until R4 lands, the scribe pass ships behind the same operational caution as
the rest of the metronome: do not run against the live backlog before
grooming or bring-up budgets.

## 2. Gating: checks required to advance

Service-level guard in `workitems.Transition` (not in `domain.CanTransition`,
which stays a pure state-shape function):

> A transition from `captured` or `triaged` to `planned`, `awaiting_approval`,
> or `running` is refused when the item's `suggested_convergence_checks` is
> empty. Error: `convergence_checks_required` (structured, names the scribe
> mechanism). Transitions to `blocked`, `canceled`, `failed`, and
> `captured→triaged` remain legal — triage itself must not require checks.

Escalation human items and scribe children are unaffected (both are created
with checks). Existing checkless items in the live DB are unaffected until
someone tries to advance them, at which point the refusal names the fix.

## 3. Proposing: the event contract

New first-class kind (field-minimal per docs/cerberus-reducer-event-contracts.md):

```
kind: convergence.checks_proposed
subject: work_item (the PARENT, not the scribe child)
payload:
  proposal_of:  <scribe child work_item id>   -- structural: reducer keys on it
  checks:       ["...", ...]                  -- structural: the proposal
  classified:   [{check, class: machine|human}, ...]  -- structural: reducer validates
  rationale:    "<free text>"                 -- narrative
  cultivar:     "convergence-scribe@v1"       -- launch metadata
```

Appended by the scribe agent through the normal MCP
`work_items.append_event`? **No** — through a dedicated append path so the
kind is first-class rather than wrapped in `work_item.event_appended`:
implementation adds `convergence.propose_checks` as an MCP tool + REST route
(`POST /v1/work-items/{id}/convergence-proposal`), mutating, idempotency-key
required, gated by `work_items.write` + tree scope like the other work-item
writes. The event id inherits the discriminator mechanics automatically, so
a scribe retrying an identical proposal under a new key creates a new
proposal event (distinct action), while a replayed request collapses.

Feed classification: `convergence.checks_proposed` joins `IncludedKinds`;
the partition test forces the decision. Anchor for scoped feeds: subject is
the parent work item, so `feedItemAnchors` needs no new case.

## 4. Reducing: validation onto the parent

Pure function in `internal/convergence` (new reducer alongside checklist):

```
ValidateChecksProposal(proposal) -> accept | refuse(reason)
```

Rules (v1, deliberately light for bring-up):

- `checks` non-empty; every entry non-blank after trim (reuses the
  normalization in workitems)
- every entry carries an explicit classification prefix, and the
  classification is consistent (per refresh-requirements R1: executable *or
  explicitly human-gated* — there is no default):
  - `machine` entries must match the machine grammar: a recognized prefix
    (`cmd:`, `event:`, `query:`) — these are verifiable by a worker without
    human judgment
  - `human` entries must be prefixed `human-ack:`
  - **unprefixed prose is refused** with `unclassified_check` naming the
    offending entry. The reducer governs *proposals* only — every proposal
    is new text authored by the scribe, so there is no legacy-corpus
    pressure here; hand-written checks on existing items are outside the
    reducer's jurisdiction and remain untouched
- at least one entry total; no duplicate entries
- `proposal_of` names a live scribe child of the subject parent

On the worker's next convergence pass over the scribe child (or evaluated
inline by the propose endpoint — implementation may choose; the reducer is
pure either way), the verdict is recorded through the existing
`convergence.verdict_recorded` machinery with reducer identity
`checks_proposal.v1`, inputs digest over the proposal payload, and the
existing budget/attempt semantics:

- **accept** → same transaction: `work_item.metadata_updated` on the parent
  (from: `[]`, to: proposed checks; system source, worker actor, reducer
  identity in payload) + scribe child transitions to `done`
  (`query:parent_checks_defined` is now literally true and the checklist
  pass can verify it)
- **refuse** → verdict records the structured reason; scribe child stays
  `running`/`triaged`; the scribe may re-propose (new proposal event, new
  digest → fresh attempt). Stale identical re-proposals are skipped by the
  existing stale-inputs rule
- **budget exhausted** → existing escalation path: hand to human. The
  human-ack fallback of the rootstock is exactly the escalation item; no new
  mechanism

If the parent's checks are already non-empty when a proposal arrives
(owner filled them in manually, or a racing proposal won), the reducer
refuses with `checks_already_defined` and the scribe child is transitioned
`done` with that reason — convergence by another road is still convergence.

## 5. Terminating: the rootstock base case

The scribe child is born with `["query:parent_checks_defined"]` — non-empty,
so it never matches the scribe pass predicate. No scribe-for-a-scribe is
possible *structurally*, not by filter.

`query:` checks resolve against a small **builtin query set registered in
code** (`internal/convergence`), each a pure predicate over projections,
evaluated by the checklist pass the same way `checklist.item:<name>` signals
are keyed today — the query name is the item name. The set ships with exactly
one member in this slice:

- `parent_checks_defined` — true iff the item's parent (via
  `work_item_relations`) has non-empty `suggested_convergence_checks`

Proposals naming a `query:` check outside the registered set are refused
with `unknown_query_check` (same shape as the registry's `unknown_reducer`
refusal in docs/registry-spec.md). This keeps the grammar honest: `query:`
means "the worker can evaluate this without judgment," which is only true
for queries the worker actually knows. With this, the existing checklist
pass can verify and close the child even if the accept-transaction's
transition raced.

The `convergence-scribe@v1` cultivar is **hardcoded as the rootstock
constant for this slice** (title template, child checks, proposal contract,
reducer id, budget). It migrates to R2's registry as seed data when the
registry exists; the constant is the interim representation, and R2 should
treat this spec's values as the first registry fixture.

## Xylem accounting

- one scribe child per parent ever (deterministic id)
- proposal attempts bounded by the existing convergence budget
  (`defaultConvergenceBudget`); exhaustion escalates to the owner
- the scribe pass emits at most: 1 created + 1 relation per parent lifetime,
  plus 1 proposal event per attempt — no per-tick chatter

## Convergence checks (from work item d7d8f598, restated implementably)

1. Integration test: create a checkless item → `worker --once` spawns exactly
   one scribe child (repeat runs: zero new); parent transition to `planned`
   refused with `convergence_checks_required`; valid proposal → parent has
   checks, child `done`, parent may advance.
2. Integration test: proposals that are empty, blank-entry, unprefixed
   (`unclassified_check`), naming an unregistered query
   (`unknown_query_check`), or duplicate-entry are refused with structured
   reasons recorded in the verdict event; refusal does not mutate the parent.
3. Integration test: scribe children and human-review-blocked items never
   receive scribe children (run two full scan cycles; count children).
4. Replay test: event log through a full propose/refuse/re-propose/accept
   cycle rebuilds identical projections.

## Deferred, explicitly

- Tropism/cultivar registry (R2): this spec hardcodes the one rootstock.
- Dispatch feed entries for scribe children (R3 remainder): until then,
  launchers find them as `triaged` items in the tree.
- Machine-grammar execution (`cmd:`/`query:` runners): validation only in
  this slice; execution is the checklist pass's existing concern and R2's
  future one.
- `question.*` taxonomy for scribe-raised research questions (open question
  in refresh doc).
