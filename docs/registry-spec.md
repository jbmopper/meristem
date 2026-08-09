# Tropism and cultivar registry: R2 implementation spec

Status: implemented spec, drafted 2026-07-04 by the Claude spec session for
work item `52bbc0ef` (R2, parent `c6ba707b`). Companion to
[`docs/scribe-spec.md`](scribe-spec.md); the scribe's `convergence-scribe`
rootstock is this registry's first seed fixture.

R2 in one sentence: an open set of named, versioned cultivars over a closed
set of reducer semantics, stored as events and projected — so the scribe
selects convergence behavior from data instead of inventing it, and adding a
new worker flavor is a write, not a deploy.

## Data model

Two new subject kinds, both event-sourced and projected. Field-minimal
payloads per docs/cerberus-reducer-event-contracts.md: structural fields only
where a reducer or projection must key on them.

### `tropism.defined`

```
kind: tropism.defined
subject_kind: tropism
subject_id: uuid5(ns, "tropism|" + name)     -- one subject per name; versions are events on it
payload:
  name:        "checklist-all"               -- structural
  version:     2                              -- structural, monotonic per name
  reducer:     {identity: "all_pass_checklist", version: 1}  -- structural: must name a registered reducer implementation
  params:      { ... }                        -- structural: reducer-specific config (e.g. budget defaults)
  description: "<free text>"                  -- narrative
```

A tropism *instance* parameterizes a reducer *implementation*. The reducer
implementations are code — the closed set, versioned by `(identity, version)`,
each a pure function with unit tests (`internal/convergence`). v1 registered
reducers:

- `all_pass_checklist@1` — existing all-pass checklist (already shipped)
- `checks_proposal@1` — the scribe validation reducer (scribe spec §4)
- `human_ack@1` — trivially: verdict follows an owner decision event

Reserved, not implemented in this slice: `run_to_green@1`, `judge_vote@1`
(blocked on the heterogeneous-panel open question), `external_signal@1`.
Defining a tropism whose reducer `(identity, version)` is not registered is
refused at append time with `unknown_reducer` — the registry cannot point at
semantics that do not exist.

### `cultivar.defined`

```
kind: cultivar.defined
subject_kind: cultivar
subject_id: uuid5(ns, "cultivar|" + name)
payload:
  name:       "convergence-scribe"           -- structural
  version:    1                               -- structural, monotonic per name
  rootstock:  true                            -- structural: immutability class (see below)
  tropism:    {name: "checklist-all", version: 2}   -- structural ref, validated at append
  profile:                                    -- structural: worker launch shape
    briefing:   "briefings/convergence-scribe.md"   -- R9 artifact path
    scopes_template: ["work_items.tree:{root}", "work_items.read", "work_items.write", "feed.read_assigned"]
    dispatch_capability: "convergence.propose_checks" -- semantic listener demand; distinct from cultivar
  xylem:                                      -- structural: budget envelope
    max_attempts: 3
    max_wall_seconds: 1800
    max_depth: 1
    max_children_per_item: 0                  -- optional; 0/absent uses safety policy fallback
    max_concurrent_running_items_per_token: 0 -- optional; 0/absent uses safety policy fallback
    max_events_per_item_per_hour_by_class:    -- optional; omitted/0 class entries use fallback
      progress: 0
      decision: 0
  phloem:     "projection:work-item-brief"    -- string ref until R6 lands; resolved ref after
  description: "<free text>"                  -- narrative
```

### Projections

Migration adds `tropisms` and `cultivars` tables projecting the latest
version per name (full history stays in `events`; the projection is the
"current registry"). Projectors follow the existing registry pattern in
`internal/projections`. Replay rebuilds both tables identically — this is
R2's first convergence check and it falls out of doing projection the normal
way.

## Write path and authority

New REST/MCP surface, mutating, idempotency-key required:

- `registry.define_tropism` / `POST /v1/registry/tropisms`
- `registry.define_cultivar` / `POST /v1/registry/cultivars`
- `registry.list` / `registry.get` (read; visible to any work-items-capable token)

Authority for this slice: writes require a new scope `registry.write`, held
by operator-authorized client tokens and (per R8) the seed token. The root
token does not participate here; it remains limited to minting and revoking
tokens. **R5's self-extension flow is explicitly out of scope here** — when it
lands, worker-proposed cultivars arrive through the grant reducer + review gate
and end in the same `cultivar.defined` append, so the event contract needs no
change.

Versioning rule: a `*.defined` append for an existing name must carry
`version = current + 1`, else refused with `version_conflict`. Nothing is
ever mutated or deleted; deactivation is a future `cultivar.retired` event
kind, reserved now, not implemented.

**Rootstock immutability:** entries with `rootstock: true` refuse redefinition
in this slice. Changing rootstock is an owner-approved migration represented by
a future explicit migration/approval path, not by broadening the root token's
authority. This enforces the R1/refresh-doc rule at the only write seam without
violating the token model.

## Consumption seams

Anywhere a cultivar name is accepted, it is validated against the projection
and refusal is structured:

1. **Scribe pass** (scribe spec §1): resolves `convergence-scribe` from the
   registry projection at scan start and records the current `<name>@<version>`
   string as launch metadata. Missing rootstock refuses with
   `unknown_cultivar` naming `registry.list` before any child is spawned.
2. **Dispatch entries** (R3 remainder, `b6526f08`): the reconciler names the
   exact handling cultivar and its profile's semantic `dispatch_capability`
   as separate dispatch fields; unknown name is impossible by construction
   because the rule data references the registry. Historical profiles without
   the field receive a deterministic exact-version compatibility mapping.
3. **Work item metadata / launch wrappers**: `"cultivar": "name@version"`
   strings in event payloads stay launch metadata (never schema identity, per
   the Cerberus contract rule); tools that *act* on them validate first.

Refusal shape everywhere: `unknown_cultivar: no cultivar named X; consult
registry.list` — satisfying R2's third convergence check verbatim.

## R5 self-extension activation

Worker-proposed cultivars do not call `POST /v1/registry/cultivars` directly.
They call `POST /v1/work-items/{id}/cultivar-activations` or the MCP tool
`registry.activate_cultivar`, where `{id}` is the proposal work item.

The activation service:

- resolves the proposed `profile.scopes_template`, replacing `{root}` with the
  proposal work item id;
- evaluates the existing subactor-grant reducer as `same_tree_worker` against
  the proposing token, requested scopes, tree relation, delegation budget, and
  the proposal work item's `human_review_status`;
- requires an explicit `human_review_status=approved` event whose
  `actor_token_id` is not the proposing token;
- appends `cultivar_activation.requested` and exactly one of
  `cultivar_activation.granted`, `cultivar_activation.denied`, or
  `cultivar_activation.escalated`;
- appends `cultivar.defined` only on the granted path, in the same transaction;
- never mints a token as part of activation.

Rootstock remains the recursion base case: worker-proposed rootstock activation
is denied before `cultivar.defined`. Changing rootstock remains an
owner-approved migration path, not self-extension.

## Seed fixtures (R8 tie-in)

`meristem seed` plants, idempotently:

- tropisms: `checklist-all@1` (reducer `all_pass_checklist@1`, current default
  budget), `checks-proposal@1` (reducer `checks_proposal@1`),
  `human-ack@1` (reducer `human_ack@1`)
- cultivars (all `rootstock: true`):
  - `convergence-scribe@1` — initial seeded version for the scribe rootstock
  - `human-attention@1` — the escalation-item shape the metronome already
    creates (checks `["human_response_recorded"]`, tropism `human-ack@1`)
  - `checklist-worker@1` — the generic leaf worker running declared checks

Seeding an already-seeded registry appends zero fresh events (deterministic
ids + discriminator-free payloads that legitimately never repeat).

## Convergence checks (from work item 52bbc0ef, restated implementably)

1. Replay test: define tropisms and cultivars across multiple versions,
   rebuild projections from events, byte-identical rows.
2. Reducer unit test per seeded tropism: feed recorded signals, assert the
   verdict and the reducer-identity field in `convergence.verdict_recorded`.
3. Integration test: scribe pass and dispatch writer refuse an unknown
   cultivar name with `unknown_cultivar` naming `registry.list`; registry
   write with unregistered reducer `(identity, version)` refused with `unknown_reducer`;
   any redefinition of a rootstock cultivar refused (no actor may redefine
   rootstock in this slice, per the immutability rule).
4. Seed idempotency: second `meristem seed` run appends zero fresh registry
   events.

## Deferred, explicitly

- R6 phloem resolution — `phloem` stays an opaque string ref until named
  projections exist.
- `judge_vote.v1` reducer semantics — blocked on the heterogeneous-panel
  open question in the refresh doc.
- `cultivar.retired` — kind reserved, not implemented.
