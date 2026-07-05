# Named projections and feeds as data: R6 implementation spec

Status: implemented spec, drafted 2026-07-04 by the Claude spec session for
work item `d7a857a8` (R6, parent `c6ba707b`). Companion to
[`docs/registry-spec.md`](registry-spec.md) (resolves its opaque `phloem`
string refs) and to the R3 remainder `b6526f08` (provides the dispatch feed
its reconciler writes into). Base: v1 tip `1a400ee`. Suggested implementation
owner per the divvy plan: Codex.

R6 in one sentence: a named feed view is a stored filter expression over the
event log — added by a write, evaluated by the existing feed machinery,
scoped by the existing access reducers — so partitioning work and keeping
context lean never again requires a deploy.

## Kind taxonomy (prerequisite, code-level)

A total classification of event kinds into classes, as a code map in
`internal/feed` with a partition test in the style of
`IncludedKinds`/`ExcludedKinds` (every kind classified, or the test names
the unclassified newcomer):

- `lifecycle` — work_item.created/transitioned/relation_added/metadata_updated,
  escalation.requested, patience.breached, signal.received
- `decision` — convergence.verdict_recorded, convergence.checks_proposed,
  subactor_grant.*, message.captured
- `progress` — none at the outer level today; `work_item.event_appended` is
  classified by **inner_kind prefix**: `coordination.*` → `decision`,
  everything else (`agent.*`, `worker.*`, unprefixed) → `progress`
- `admin` — token.*, idempotency.recorded, deterministic_error.* (never in
  agent-facing views; governed by their own scopes)

Classes are selectable in projection definitions (`class:progress`) and are
the unit R7's per-class event budgets meter. The June-2026 archive replay
check (below) validates the inner_kind heuristic against the Cerberus
experience before anything depends on it.

## Data model

One new subject kind, event-sourced and projected, mirroring the registry
pattern:

```
kind: projection.defined
subject_kind: projection
subject_id: uuid5(ns, "projection|" + name)
payload:
  name:      "dispatch"                        -- structural
  version:   1                                  -- structural, monotonic per name
  type:      "feed"                             -- structural: only "feed" in this slice ("brief" reserved)
  rootstock: true                               -- structural: same immutability rule as cultivars
  filter:                                       -- structural: the expression
    kinds:        ["escalation.requested"]      -- exact kinds, and/or:
    kind_classes: ["decision"]                  -- taxonomy classes
  description: "<free text>"                    -- narrative
```

Filter semantics: `kinds ∪ kind_classes` selects. **Projections select
content; they never grant or narrow authority.** There is no scope field:
every projection read passes through the caller's existing access reduction
(`FilterFeedItems`/`feedItemAnchors` for `feed.read_assigned` tokens,
unfiltered for `feed.read`/root/legacy), identical to today's feed. A
projection therefore cannot show a token anything its scopes would hide,
and cannot hide tree content from a token that could see it on the default
feed — visibility has exactly one owner, the access reducer. (Per-tree or
per-subject view narrowing, if a consumer ever needs it beyond what token
scoping provides, arrives later as read-time *parameters*, not stored
authority.) No payload-level predicates in this slice — selection stays
cheap, indexable, and obviously deterministic. Projection rows land in a
`projections` table via the normal projector pattern; replay rebuilds
identically.

A definition whose `kinds` contains an unknown kind, or whose class names
are not in the taxonomy, is refused at append with `unknown_kind` /
`unknown_kind_class`. `admin`-class kinds are refused in any projection
definition (`kind_not_projectable`) — their read paths stay where their
scopes live.

## Read path

`GET /v1/feed?projection=<name>` and the `feed.read` MCP tool gain an
optional `projection` argument:

- absent → today's behavior, byte-for-byte (the default view is effectively
  a projection named `activity` seeded to match current `IncludedKinds`;
  the implementation may literally define it that way to collapse code paths)
- present → resolve name against the projection table (`unknown_projection:
  no projection named X; consult projections.list` on miss), apply the
  filter, then apply the caller's access reduction exactly as today — a
  `feed.read_assigned` token reads any projection and sees its in-tree
  subset; `feed.read` sees the whole view
- cursor semantics unchanged and cursors are **per-projection** (the cursor
  token embeds the projection name+version; a cursor from one projection is
  refused on another with `cursor_projection_mismatch` — this closes the
  time-cutoff failure mode from the 2026-07-03 coordination miss by making
  the right thing the only thing)

Long-poll/SSE paths reuse the same resolved filter; no new transport.

## Write path and authority

- `projections.define` / `POST /v1/registry/projections` — mutating,
  idempotency-key required, scope `registry.write` (same authority family
  as tropisms/cultivars: all three are "named things agents select from";
  one scope keeps the owner's mental model small)
- `projections.list` / `projections.get` — read, any work-items-capable token
- versioning and rootstock immutability rules identical to
  `cultivar.defined` (version_conflict, rootstock redefinition refused, no
  root-authority widening)

## Seed fixtures (R8 tie-in)

- `activity@1` — the current default feed: `kinds` = today's
  `IncludedKinds` (rootstock). Backs the no-argument read path for every
  token class, since access reduction is unchanged
- `owner-attention@1` — `kinds: [escalation.requested, patience.breached]`
  (rootstock) — the owner's nudge feed
- `dispatch@1` — **not seeded in this slice.** It references
  `dispatch.requested`, which does not exist yet, and seeding it would
  contradict this spec's own `unknown_kind` validation. The R3 remainder
  (`b6526f08`) ships the kind and this fixture together
- `work-item-brief@1` — **reserved name only** (`type: "brief"` refused at
  append in this slice); R2's phloem ref stays a string that now has a
  registry to eventually resolve against

## Convergence checks (from work item d7a857a8, restated implementably)

1. Integration test: define a projection via API, read it via
   `feed.read?projection=`, rebuild the projections table from events —
   byte-identical; cursor from one projection refused on another.
2. Integration test: a `feed.read_assigned` token reading any projection —
   including `activity@1` via the no-argument default path — sees exactly
   the same in-tree subset it sees today; a `feed.read` token sees the
   whole view. Projections grant nothing and hide nothing that token
   scopes do not.
3. Taxonomy partition test: every kind in `domain.AllEventKinds` classified
   exactly once; `admin` kinds refused in definitions.
4. Archive replay: June-2026 dump classified by the taxonomy; the committed
   validation artifact records the observed progress/decision split and keeps
   `coordination.*` classified as `decision` even though the original `>90%`
   progress expectation was disproven by the sample.

## Deferred, explicitly

- `type: "brief"` composition (item snapshot + last-N decision events +
  open children) — the phloem document R2 references; needs its own spec
  once a consumer exists.
- Payload-level filter predicates — not until a concrete projection needs
  one; selection stays kind/class/scope-shaped.
- Per-class xylem budgets — R7's job; the taxonomy classes here are its
  metering unit.
- `projection.retired` — reserved, mirroring `cultivar.retired`.
