# Review dispatch: closing the cross-agent review loop

Status: implementable spec, drafted 2026-07-06 by Claude at owner request.
Converts the review convention that built this system (implementation by one
agent, independent review by another) from a habit into a worker pass, using
only existing machinery: deterministic children, the registry, and the
dispatch feed. The disciplined descendant of the bicameral-net idea.

One sentence: when an implementation item closes, the system spawns a
review child under a `reviewer` rootstock cultivar and dispatches it — so
one agent's landing automatically becomes the other agent's work.

## 1. The `reviewer@1` rootstock cultivar

Seeded like the other rootstocks (registry fixtures + briefing projection):

- Tropism: `checklist-all@1`.
- Xylem: 2 attempts, 3600s wall, depth 1.
- Briefing (`docs/briefings/reviewer.md`, ≤40 lines, generated via
  internal/dogma like the others). Imperative core:
  - Run the full suite at the exact commit under review; never accept on a
    stale or dirty tree.
  - Verify against the item's convergence checks and the spec it cites,
    not against taste.
  - Findings become child work items with machine-grammar checks
    (`cmd:`/`event:`/`query:`/`human-ack:`), severity-labeled; verdicts are
    `accepted` / `accepted_with_finding` / `blocking_finding` events.
  - **Never claim a review of your own work**: if the implementation
    commit's attribution matches your token, stand down and leave the
    dispatch for another claimant.
  - Fix-forward only when the author is absent AND main is red.

## 2. Noticing: the review pass

A fourth-ish worker pass, structurally identical to the scribe pass:

- Candidates: items that transitioned to `done` whose closing evidence
  marks implementation (see §3), with no existing review child.
- Deterministic child id: `uuid5(ns, parent_id || "|reviewer|v1")` — one
  review child per parent, ever; idempotent across concurrent workers.
- The child is born `triaged`, cultivar `reviewer@1`, checks:
  `["event: review verdict recorded on the parent (accepted or findings filed as child items)"]`,
  body templated with parent snapshot + closing commit refs.
- The existing dispatch pass then routes it: `dispatch.requested` naming
  `reviewer@1`. No new dispatch machinery.
- Exclusions mirror the scribe pass: review children themselves (born with
  checks, never implementation-marked), human-review-blocked items.

## 3. What marks an item as implementation

Structural, not regex: closers that want review append the existing
`coordination.implementation_ready` inner kind or close with a payload
field `"commits": [...]` on their summary event. The review pass treats
either as the marker. Docs-only or trivial closers simply omit it — review
is requested by evidence, not imposed on everything. (The briefings tell
implementers: substrate-touching closes carry commits; the conformance map
links this rule.)

## 4. Separation of duties at claim time (phase 2, optional)

Phase 1 relies on the briefing rule plus full attribution (a self-review
claim is auditable and visible). Phase 2, if violations ever occur: the
claim gate refuses transitions of a review child to `running` by the token
that authored the parent's implementation events — one query at the
transition seam, same shape as the concurrent-claims budget.

## 5. What this buys

- A lands work → system spawns+dispatches review → B claims, reviews,
  files findings → findings dispatch back as A-claimable work. Neither
  agent's goal mode needs to invent anything: the loop feeds itself from
  real landings.
- Review coverage becomes observable (readiness surface shows unreviewed
  landings as pending dispatches) instead of depending on whoever happened
  to be watching the feed.

## Convergence checks (for the implementation item)

1. `event:` integration test: closing an item with an implementation marker
   spawns exactly one reviewer child (idempotent on re-scan) and the
   dispatch pass routes it naming reviewer@1.
2. `event:` docs-only close without the marker spawns nothing.
3. `query:` reviewer@1 cultivar + briefing seeded; briefing ≤40 lines,
   generated and drift-guarded via internal/dogma.
4. `cmd:` go test ./... green at the exact commit.
5. `human-ack:` owner confirms the loop on the first real dispatched review.
