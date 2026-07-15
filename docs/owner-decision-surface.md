# Owner decision surface

Status: design, drafted 2026-07-15 by the Claude design session for work item
`c893c4c8` ("Owner decision surface: bounded queue, recommendations with
defaults, self-expiring escalations"). Companion to
[`escalations.md`](escalations.md) (the machinery this bounds and expires),
[`backlog-readiness.md`](backlog-readiness.md) (the fold this queue derives
from), [`safety.md`](safety.md) (where the cap lives), and
[`projections-spec.md`](projections-spec.md) (the projection-as-data
conventions the surface follows). Provenance depends on work item `c2830e82`
("Owner-ack provenance"); §3 states the seam. Base: v1 tip `e0ce32c`. No code
changes accompany this document.

The one-sentence thesis: **owner-blocking decisions should arrive as a bounded
queue of requests, each carrying a recommendation and a default, and each
escalation should expire on a deadline instead of rotting in `blocked`
forever.**

---

## The problem this solves

The owner is a single human coordinating many agents. Today an owner-blocking
decision is expressed the only way the substrate offers: the item lands in
`blocked` or `awaiting_approval` and waits. `internal/escalations.Request`
makes this durable — it appends `escalation.requested`, spawns a
`human-attention` child (`state = captured`, `human_review_status = blocked`,
`suggested_convergence_checks = ["human_response_recorded"]`), and moves the
origin to `blocked` — but it stops there. There is no recommendation, no
default, and no deadline.

Three consequences follow, all observed:

1. **Decisions accumulate without a ceiling.** `blocked` is *the* fixed point
   of the escalation loop (owner deep dive, step 4): a blocked-on-owner item
   still records its `patience.breached` but is deliberately exempt from
   further escalation (`PatienceEscalationsSkippedAwaitingHuman`), so it waits
   quietly, forever, being not-your-problem-yet. That is exactly right for
   *not nagging* and exactly wrong for *being found*: eighteen-plus blocked
   items is a real observed state.
2. **Each decision is discovered by reading the whole backlog.** The only
   surface that gathers them is `backlog.readiness`, whose `blockers` group is
   an unbounded scan (`limit = 0`) over the entire `work_items` projection.
   Finding "what needs me" means reading everything and filtering by eye.
3. **Nothing self-resolves.** `approvals.Expire` is the one place the substrate
   already lets a decision time out — and it does the safe thing, reverting the
   item to `blocked` rather than approving it. But escalations have no
   equivalent: an escalation that no longer matters is indistinguishable from
   one that urgently does, and both wait the same forever.

The decision surface is the fix, and it is deliberately **not a new source of
truth**. It is a projection (principle 2): a further fold over the same events
`backlog.readiness`, `escalations`, and `approvals` already write. It grants no
authority (principle 7), it never auto-approves a side effect (principle 6),
and it gives every escalation a forward transition on a bounded delay
(principle 3). Everything below is grounded in machinery that already exists;
where it proposes something new, it says so.

---

## 1. The queue

### Hard cap: 12

The queue holds at most **twelve** decision requests. The number is a policy
value that belongs beside the other deterministic caps in `internal/safety`
(`steady` 12, `bring-up` 8 while reconcilers are still standing up), governed
by the active policy profile exactly like `MaxChildrenPerItem` (32),
`MaxConcurrentRunningPerToken` (8), and `MaxDelegationDepth` (5).

The defense:

- **It is a foreground attention budget, not a backlog.** There is one owner.
  The queue is the small set the owner is asked to look at *now*; the backlog
  of everything blocked stays where it is, unbounded, in `backlog.readiness`.
  The cap answers "how many decisions can one human hold in a sitting," not
  "how many decisions exist."
- **It sits below the observed pile on purpose.** Eighteen-plus blocked items
  is the state we are fixing; a cap of twelve makes the queue a *filter that
  forces triage*, not a *mirror that reproduces the pile*. If the queue could
  grow to hold everything, it would reproduce exactly the "read the whole
  backlog" problem it exists to solve.
- **It is in the register of the existing caps.** meristem's resource limits
  are small per-scope integers (8, 32, 5), not hundreds. A per-owner decision
  cap of twelve is the same shape: roughly a review sitting's worth of
  distinct with-context calls — enough to batch, small enough that the
  thirteenth decision must *earn* its slot by out-ranking one already queued.
- **It fails closed.** When more than twelve requests qualify, the surplus is
  not lost and not auto-resolved: it degrades to today's behavior (see
  "Overflow"). Overflow is the safe floor, so a low cap can never cause harm,
  only less batching.

Alternatives considered: an uncapped queue (rejected — it is the current
`blockers` scan renamed); a Miller-style 7±2 (defensible, but too tight to
batch a real sitting and it starves under the observed load); a per-*class*
cap instead of a global one (deferred — it complicates ranking without
evidence any class dominates; revisit if one does).

### Admission: what earns a slot

Candidate decisions are *derived*, not separately authored. Each worker tick
folds four existing sources into a candidate set:

- `backlog.readiness` `blockers` — visible `blocked` / `awaiting_approval`
  items;
- open `escalation.requested` (an escalation whose origin epoch is still the
  one recorded in the payload — the `PatienceAttention` open/resolved test
  applied to escalations);
- pending `approval.created` (status `pending`, not yet expired);
- unmet `human-ack:` convergence checks awaiting an owner decision.

A candidate is **admissible** only if it is a genuine, answerable decision:

1. it names a specific owner question, not merely "this is blocked";
2. it carries a recommendation, a default, and a stated blast radius (§2) —
   a bare block with no recommended answer is a backlog item, not a queue
   entry;
3. it is not a duplicate of one already queued (coalescing, below).

The admitted set is then ranked (below) and the top twelve occupy the queue.

### Dedup and coalescing

Two mechanisms, one already in the tree and one proposed:

- **Escalations already dedup.** `escalation_id` is derived from
  `(work_item_id, reason, summary)`, and a repeat `Request` returns the
  existing human work item instead of recording a second event. The queue
  inherits this for free.
- **Decisions about the same thing coalesce by a decision key.** Multiple
  escalations, approvals, and checks can reduce to one decision. Propose a
  `decision_key = (work_item_id, decision_class, target)` — e.g. two
  escalations about the same deploy of the same item share a key. Candidates
  with the same key collapse to a single queue entry that carries *all* the
  provenance (every contributing escalation/approval id), so the owner answers
  once and every contributor resolves. This mirrors how a signal dedupe-links
  onto an existing live item rather than creating a second (`internal/signals`,
  `docs/safety.md`): the coalesced-away candidate is recorded for audit, not
  dropped.

### Ordering and eviction: deadline-aware priority

Neither pure FIFO nor pure priority. Sort by, in order:

1. **class priority** (a small fixed integer per decision class; safety
   classes rank highest — see §2's table);
2. **deadline ascending** (soonest to expire first);
3. **admission sequence ascending** (FIFO within a tie).

The rationale is the tension between stakes and urgency. Pure FIFO buries a
deploy decision that is minutes from its default behind week-old trivia. Pure
deadline lets a trivial auto-apply jump ahead of a high-stakes safety
decision. Deadline-*within*-priority-band surfaces the highest-stakes
decisions first, and within a band surfaces the one the owner is about to lose
the chance to influence (an auto-apply default about to fire, or a lapse about
to revert). FIFO is only the final tie-break, so nothing starves.

Eviction is a consequence of ranking, not a separate rule: when a
higher-ranked candidate is admitted and the queue is full, the lowest-ranked
current entry drops to overflow. Because the queue is re-derived each tick,
eviction and promotion are just "the ranked fold changed"; no entry is
destroyed, and an evicted entry re-enters the moment it out-ranks a slot again.

### Overflow: not queued, still discoverable

Decisions that do not fit remain fully discoverable exactly where they live
today — the `blockers` group of `backlog.readiness` and the `owner-attention`
feed — because the queue is a projection over the same events, not a container
that removes them from anywhere. Overflow is the low-priority / far-deadline
tail. Nothing is hidden.

One safety rule makes overflow the safe floor: **a default may auto-apply only
on an entry that has actually held a queue slot for its full patience window.**
Visibility is a precondition for auto-apply. An auto-apply default therefore
never fires on a decision the owner never had a chance to see; while a decision
sits in overflow, its only expiry behavior is the lapse-back-to-`blocked` that
`approvals.Expire` already performs — i.e. today's behavior. This means a low
cap can delay batching but can never cause a silent auto-decision on an unseen
item.

---

## 2. Recommendations with defaults

Every queued decision carries four things:

- **the question** — the owner-facing decision, in prose (Direction: "the
  operator interacts with prose");
- **a recommended answer** — the agent's advice (§3: provenance
  agent-recommended);
- **the default** — what applies if the request reaches its deadline
  unanswered;
- **the blast radius of that default** — the set of subjects the default's
  effect would touch.

Blast radius is the load-bearing field. Define it as the subjects a default's
effect mutates. A default is **contained** iff its blast radius is a subset of
`{the item itself, its own subtree}` *and* its effect is reversible by a later
owner event (no terminal `done|failed|canceled` transition, no token/scope
change, no `human_review_status` change). Everything else is **wide**.

### Two designs for the default

**Design A — auto-apply on expiry.** At the deadline the default *becomes* the
decision; the item moves.

- For: genuine bounded patience — the fleet does not stall on the owner's
  absence; the item reaches a forward transition on a bounded delay
  (principle 3) instead of sitting in `blocked`.
- Against: the system takes an action the owner did not take. If the effect is
  a side effect, this is a direct violation of default-deny (principle 6). If
  the effect is wide, a wrong default is expensive and not cleanly reversible.

**Design B — lapse back to blocked.** At the deadline the *request* expires and
the origin reverts to plain `blocked`, re-entering the normal backlog. This is
exactly what `approvals.Expire` does today: it appends `approval.expired` and
transitions the work item back to `blocked` with reason `approval_expired` —
it never approves.

- For: the system never acts without the owner; always safe; already
  implemented for approvals, so it is a proven shape.
- Against: not really convergence — the item just returns to the pile it came
  from and will be re-queued later. Lapse-back alone reproduces the rot; its
  value is that it is *safe*, and that it clears the queue slot for something
  actionable.

### Recommendation per decision class

Auto-apply is permitted **only for contained defaults**. Wide defaults —
always including the four named safety-relevant classes — lapse back and
**never auto-apply**. This is principle 6 and principle 7 made structural: the
system does not decide side effects or widen authority on a timer.

| Decision class | Example question | Default on expiry | Auto-apply? | Blast radius |
|---|---|---|---|---|
| `deploy_scheduling` | Deploy the v1 tip to a node now, or hold? | hold (no deploy) | **never** | fleet / node — wide |
| `commit_pinning` | Pin a cultivar's worker to commit `abc`? | leave unpinned | **never** | fleet — wide |
| `human_review_status` | Approve this cultivar activation / grant? | remain `blocked` | **never** | the gate itself — wide |
| `authority_widening` | Grant `work_items.write_all` to token Y? | deny / remain `blocked` | **never** | tokens / authority — wide |
| `prioritization` | Work item A or B next in this subtree? | recommended pick | auto-apply | own subtree — contained |
| `check_set_acceptance` | Accept the scribe's proposed checks? | accept recommended set | auto-apply | own item — contained |
| `routing_choice` | Route to `reviewer` or `checklist-worker`? | recommended cultivar (non-widening) | auto-apply | own subtree — contained |
| `stale_disposition` | Cancel this stale item? | **keep** (no terminal move) | never (cancel is terminal, irreversible) | own item, but irreversible — wide |

Two rules generate the whole table and generalize to classes not yet named:

- **The four safety-relevant classes never auto-apply, full stop:** deploy
  scheduling, commit pinning, `human_review_status` changes, authority
  widening. Their defaults are lapse-back; the recommendation is advisory and
  waits for the owner.
- **A default may auto-apply only a reversible, non-terminal effect within the
  item's own subtree.** So `stale_disposition` cannot auto-*cancel* (terminal),
  though its recommendation may still *be* "cancel"; the default keeps the item
  and lapses. `check_set_acceptance` may auto-apply because setting
  `suggested_convergence_checks` is reversible via a later
  `work_item.metadata_updated`.

---

## 3. Provenance

**An expired default must never be recorded as an owner decision.** This is the
non-negotiable of the whole design, and it is the seam with `c2830e82`
("Owner-ack provenance"), which owns the constraint that only a genuine owner
decision may stamp `acked_by = owner`.

The constraint is already enforced, in three places, by exactly the machinery
this design reuses:

- `approvals.Decide` refuses any actor that is not a human non-root token
  (`ErrHumanDecisionToken`) and refuses the requester deciding its own approval
  (`ErrSeparationOfDuties`).
- `cultivaractivation.approvalSeparated` requires an explicit
  `human_review_status = approved` event whose `actor_token_id` is **not** the
  proposer's before an activation may proceed.
- The `human_ack` reducer folds only a genuine `human_ack.decision` signal;
  **absent that signal it returns `escalate`, never `accept`** — a decision
  cannot pass by the *absence* of evidence.

The reason all three already hold without new trust assumptions is principle 5:
`actor_token_id` and `source` come from the request context, never from a body
or header, and cannot be forged. A default applied by the reconciler is
`source = system` and can never be mistaken for `source = human`. The design
therefore does not invent an enforcement mechanism; it forbids the system path
from ever touching owner-only fields.

Three distinct provenances, never conflated:

| Provenance | `source` | Kind (see §5) | May it stamp owner authority? |
|---|---|---|---|
| **owner-decided** | `human`, non-root, ≠ requester | `decision_request.decided` | **Yes** — the only one. May set `human_review_status = approved`, emit `human_ack.decision: pass`, stamp `acked_by = owner`. |
| **default-applied-on-expiry** | `system` (reconciler) | `decision_request.default_applied` | **Never.** Records `owner_decision: false`. May drive a contained, reversible in-subtree effect only. Never emits `human_ack.decision: pass`, never sets `approved`, never stamps `acked_by = owner`. |
| **agent-recommended** | `agent` | `decision_request.recommended` | **Never.** Advice only; it never disposes. It is the recommendation the owner (or a contained default) acts upon. |

The practical test the surface must satisfy: reading the event log, an auditor
can always tell whether a decision was *made by the owner* or *fell to its
default*, because they are different kinds with different `source` values, and
only the owner-decided kind is wired to the owner-authority fields. A
`default_applied` event that carried `human_ack.decision: pass` would be a bug
of the most serious kind — the check must be that such a signal can only be
emitted on the owner-decided path.

---

## 4. Self-expiring escalations

### Expiry relative to the checks it gates

An escalation gates a convergence check — today the `human-attention` child's
`human_response_recorded` / `human-ack:` check. The governing rule:

**An expired escalation must not silently satisfy the human-ack check it
gates.**

This is already true in the reducer and must stay true: `HumanAck.Reduce`
returns `accept` only on a `human_ack.decision` signal with `pass = true`; with
no such signal it returns `escalate`. So an escalation that expires produces
**no** passing signal, and the gated check remains unmet. Expiry may change the
*item's* disposition, but never the *check's* verdict:

- **Lapse-back classes** (all safety classes, all wide defaults): expiry
  reverts the origin to plain `blocked` — the `approvals.Expire` shape — so it
  re-enters the backlog. The human-ack check stays unsatisfied. The decision is
  not made; it is merely no longer holding a queue slot.
- **Auto-apply classes** (contained defaults only): the default drives the item
  forward and records a `decision_request.default_applied` event. Because that
  path is `source = system` and forbidden from emitting `human_ack.decision`,
  the item's forward motion is justified by a *class-appropriate* verdict
  (a `query:` or `event:` check on the applied effect), **never** by a faked
  owner ack. A `human-ack:` check is by definition owner-gated and therefore
  belongs to a lapse-back class; it can never be discharged by a default.

### Re-prompt cadence

Today blocked-on-owner items are silent forever. The decision surface replaces
"silent forever" with "bounded, decaying, visible" — without reintroducing the
escalation storm the fixed-point exemption exists to prevent:

- While an escalation is queued, it re-surfaces on `owner-attention` at the
  worker-tick cadence, with an *aging* indicator derived from
  `now - raised_at` against its deadline. Like `PatienceAttention`, the
  re-prompt is a **derived correlation, not a new event per prompt** — the feed
  keeps showing the open request until its epoch changes; no storm, no
  per-prompt row.
- At the deadline: auto-apply classes fire their contained default; lapse-back
  classes revert to `blocked`; safety classes re-prompt at a longer cadence and
  keep waiting (visibly, aging), never firing a default.

### Escalation levels

Two levels, both derived (no stored escalation-level column):

- **Level 1 — attention.** Normal `owner-attention` placement.
- **Level 2 — imminent.** The deadline is near (an auto-apply default is about
  to fire, or a safety decision has aged past a threshold number of
  re-prompts). Level 2 sorts to the top of the queue (it lowers the effective
  deadline key in §1's ordering).

No pager or human-transport is in scope here, consistent with `escalations.md`
("It does not ... add an approval table or notification transport in this
slice"). A level is an ordering signal, not a new delivery channel.

---

## 5. Event vocabulary

A new subject kind `decision_request`, event-sourced and projected, in the
exact shape of the existing `escalation` / `approval` / `subactor_grant`
families: its own subject id, `<noun>.<verb_past>` kinds, deterministic ids,
and the idempotency discriminator where a payload can legitimately repeat. Add
`SubjectDecisionRequest = "decision_request"` to `internal/domain` and the
kinds to `AllEventKinds`.

Subject id is derived, so raising the same decision twice collapses:
`decision_request_id = uuid5(ns, "decision_request|" + decision_key)` where
`decision_key = work_item_id || decision_class || target`.

```
kind: decision_request.raised          class: decision
subject_kind: decision_request
subject_id: <decision_request_id>
payload:
  work_item_id:   <origin work item>
  decision_key:   "<work_item_id>|deploy_scheduling|node-lump"
  decision_class: "deploy_scheduling"
  question:       "Deploy the v1 tip to Lump now, or hold?"   -- prose, scrub-eligible
  recommendation: "hold until the smoke protocol is green"
  recommended_by: "agent"                                      -- see .recommended
  default:
    disposition:  "hold"
    effect:       "none"        -- contained|none|<reversible in-subtree effect>
    auto_apply:   false          -- false for wide/safety classes
  blast_radius:   "node"         -- item|subtree|node|fleet|authority
  contributes:    ["<escalation_id>", "<approval_id>"]   -- coalesced provenance
  expires_at:     "2026-07-16T00:00:00Z"
```

```
kind: decision_request.recommended     class: decision    source: agent
payload:
  decision_request_id: <id>
  recommendation:      "hold until the smoke protocol is green"
  rationale:           "<why>"          -- prose, scrub-eligible
  -- advice only; never disposes. May be folded into .raised, or appended
  -- later when an advisor revises its recommendation.
```

```
kind: decision_request.coalesced       class: lifecycle
payload:
  decision_request_id: <surviving id>
  from_work_item_id:   <coalesced-away origin>
  from_escalation_id:  <id>             -- recorded for audit, not dropped
  reason:              "same decision_key"
```

```
kind: decision_request.decided         class: decision    source: human (non-root, != requester)
payload:
  decision_request_id: <id>
  disposition:         "hold" | "deploy" | ...   -- the owner's answer
  reason:              "<owner note>"             -- prose, scrub-eligible
  -- the ONLY provenance permitted to stamp owner authority (§3).
```

```
kind: decision_request.default_applied class: lifecycle    source: system
payload:
  decision_request_id: <id>
  disposition:         "hold"           -- the default that fired
  effect:              "none"           -- contained, reversible, in-subtree only
  owner_decision:      false            -- explicit, always false
  expired_at:          "2026-07-16T00:00:00Z"
  -- MUST NOT emit human_ack.decision:pass, set human_review_status=approved,
  -- or stamp acked_by=owner.
```

```
kind: decision_request.lapsed          class: lifecycle    source: system
payload:
  decision_request_id: <id>
  reason:              "decision_request_expired"
  reverted_to:         "blocked"        -- the approvals.Expire shape
```

Taxonomy obligation: `internal/feed/taxonomy.go` classifies every kind exactly
once and its partition test names any unclassified newcomer. Slot the new kinds
as marked above — `.raised`, `.recommended`, `.decided` are `decision`;
`.coalesced`, `.default_applied`, `.lapsed` are `lifecycle` (they move the
item's disposition without themselves being a judgment). None are `admin`, so
all are projectable into feeds.

---

## 6. Integration with backlog readiness

### Derive, do not duplicate

The queue **feeds from** the backlog readiness fold; it does not maintain a
parallel state. `backlog.readiness` already folds the `work_items` projection
into groups, `blockers` among them. The decision surface is one further fold:
it takes `blockers` plus open `escalation.requested`, pending
`approval.created`, unmet `human-ack:` checks, and `decision_request.*`, and
projects the bounded, ranked queue. It reads the same events; it stores no
independent truth. If the two disagree, the system has lied (principle 2), and
`meristem rebuild` would catch it.

### Projection vs. table

Two shapes, per `projections-spec.md` and the `PatienceAttention` precedent:

- **Derived read model (fold), à la `PatienceAttention`.** Compute the queue on
  read by folding `decision_request.*` against the current `work_items`
  projection; open/closed and in-queue/overflow are *correlation verdicts*, not
  stored columns, resolved without a second event when the item's epoch moves.
  For: rebuild-safe by construction, no second source of truth. Against:
  recompute per read; ranking/cap computed each time.
- **Materialized projection table (`decision_requests`), à la `approvals`.** A
  projector writes rows on `decision_request.*`. For: cheap indexed reads.
  Against: the ranking and the cap depend on wall-clock deadline comparisons,
  and a projector must be **pure with respect to the event payload — no clock
  reads** (AGENTS.md, "How to write a projection writer"). A queue position
  that depends on `now` cannot live in a projected row.

**Recommendation: hybrid.** Materialize the durable, clock-independent facts as
a `decision_requests` projection (identity, class, question, recommendation,
default, blast radius, `expires_at`, decided/lapsed status) exactly like
`approvals`. Compute the **bounded, ranked, in-queue-vs-overflow view** as a
read-time fold, because it is clock-dependent — the same split
`PatienceAttention` draws between the stored `patience.breached` fact and the
derived open/resolved verdict. This keeps projectors pure while giving the API
indexed reads.

### Named projection and surfaces

Add an R6 projection-as-data entry — `owner-decisions@1` selecting
`decision_request.raised`, `.decided`, `.default_applied`, `.lapsed` (or extend
`owner-attention@1` to include `.raised`). Per `projections-spec.md`,
**projections select content, never grant or narrow authority**: the queue read
still passes through the caller's access reduction, and cursors are
per-projection. The read surfaces mirror `backlog.readiness`: a
`decision.surface.v1` contract on `GET /v1/decisions` and MCP `decisions.list`,
with the owner decision recorded through `decisions.decide` /
`POST /v1/decisions/{id}/decide` — the human-only, separation-of-duties path
from `approvals.Decide`, not a metadata write.

---

## 7. Open questions for the owner

- **The cap.** Is twelve right, and should it be one number or per policy
  profile (`steady` 12 / `bring-up` 8)?
- **Class taxonomy completeness.** Are the four named safety classes the whole
  set, or are there others (e.g. is *canceling a stale item* — terminal and
  irreversible — safety-relevant enough to name explicitly)?
- **Default patience windows.** Per-class deadlines, or reuse the existing
  per-state `PatienceBudgets`? How long is a deploy-scheduling decision allowed
  to age before Level 2?
- **Auto-apply in bring-up.** Should auto-apply be disabled entirely until
  `steady`, so first-live operation is lapse-back-only?
- **Re-prompt cadence.** Tie the re-prompt to the worker tick, or a separate
  decaying schedule; and what re-prompt count trips Level 2 for a safety
  decision?
- **Unify with approvals or not.** Should `decision_request.decided` be a new
  kind, or should the owner-decision path *be* `approval.decided` for classes
  that are already approvals — i.e. how much of this is a new family versus a
  recommendation-and-deadline layer over `approvals`?
- **Who authors the recommendation.** The existing `reviewer` / scribe
  cultivars, or a new advisory cultivar? The recommendation is
  agent-recommended provenance either way, but the source cultivar affects the
  briefings.
