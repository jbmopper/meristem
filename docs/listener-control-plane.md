# Listener Control Plane

> Status: owner-authorized implementation release candidate for work item
> `588544c9-2fe3-5173-aeb9-33829fd19cec`, incorporating Fable findings
> `LDR-M1` and `LDR-M2`. `docs/spec.md` remains normative. Slices 0-3 are on
> `v1`; slice 4 is implemented on the listener release train and still requires
> exact-commit review plus the explicit runtime cutover gate. Networking and
> assignment-bound token exchange remain deferred until after the OS update.

## Decision summary

Meristem will treat listening as durable desired state owned by core, not as a
set of launcher environment variables. A stable listener registration declares
what bounded capability demand it is willing and authorized to claim. The
listener's base policy may select all eligible demand or narrow by originating
principal, work-item tree, event kind, or another normalized deterministic
predicate. A listener with no assignment evaluates its base policy; after it
atomically claims one work item, its effective lens narrows to that assignment
until terminal completion, yield, or lease expiry, then returns automatically
to the latest base policy.

The existing Codex bridge is a bootstrap adapter, not the architecture. Its
feed consumption, cursor, policy, queue, and delivery-journal responsibilities
move into the generic listener control plane. The Codex-specific remainder is
a fail-closed, metadata-only activation adapter that resumes one locally bound
Codex task and returns an unambiguous activation outcome. Meristem never puts
event bodies into the wake prompt and never treats a wake as new owner
authority.

This design does **not** add an `agent` object or agent-kind enum. A listener is
an attributed client endpoint capable of accepting temporary assignments.
Writer, reviewer, critic, and similar behaviors remain assignment data.

## Owner-visible contract

The prose translation layer must be able to normalize these instructions:

| Owner instruction | Durable base policy | Behavior while assigned |
| --- | --- | --- |
| "Listen for everything" | All currently open capability demand visible to and eligible for this listener | Focus on the claimed assignment; restore all-eligible afterward |
| "Listen to Fable" | All eligible demand whose originating principal resolves to Fable | Focus on the claimed assignment; restore the Fable predicate afterward |
| "Listen for networking work" | A resolved work-item tree and/or explicit event-kind/capability predicate | Focus on the claimed assignment; restore the topic predicate afterward |
| "Pick up one thing, finish it, then listen again" | Any selected base policy with `max_concurrent_assignments=1` | Claim one finite lease; ignore unrelated demand until release; then rescan current open demand and resume |

"Everything" means all **eligible capability demand**, not every event in the
audit log. Prose classification may propose a normalized policy, but only the
persisted deterministic predicates control delivery.

## Current-state evidence

Evidence was refreshed against repository state on 2026-08-07 MDT.

| Capability | State | Evidence and disposition |
| --- | --- | --- |
| Assigned/addressed feed lane | Shipped | Shared REST/MCP reducer and SSE tests are in `internal/api/assigned_feed_integration_test.go`; reuse unchanged |
| Normalized actor, kind, item, and tree predicates | Shipped | `internal/feed/predicates.go`; reuse as the policy predicate vocabulary |
| Filter-bound durable cursor and SSE resume | Shipped | `meristem feed --watch --cursor-file`; reuse in the generic supervisor |
| Wake-hook redelivery on failure | Legacy compatibility | `meristem feed --watch --exec` remains available during guarded cutover; the generic listener owns new delivery state |
| Atomic work-item Claim/Yield, lease expiry, and terminal handback | Shipped | `internal/workitems/assignments.go`, migrations 0035-0036, and canonical REST/MCP transports |
| Claim/Yield/GetAssignment REST and MCP surfaces | Shipped | Listener slice 1; external scoped clients share the domain reducer |
| Durable listener registration and base policy | Shipped | Listener slice 2, migration 0037, and `internal/listeners` |
| Stable listener addressing | Shipped | Producers address listener UUID/name; bearer rebinding does not change the address |
| Claim-bound focus and automatic policy restoration | Shipped | `meristem listener` derives IDLE/FOCUSED from registration, assignment, and feed projections |
| Codex task activation | Release candidate | `scripts/codex-thread-nudge.py activate` is metadata-only, journal-free, idle-only, and fail-closed on unattended requests. The isolated app-server pre-approves only the three assignment-bound tools already filtered by the task profile; every approval, elicitation, permission, or other server request is still declined. |
| Durable activation request/outcome | Release candidate | Migration 0039 plus `internal/listeneractivation`; REST, MCP, assigned-feed control, restart, and rebuild proofs are included |
| Assignment-bound token exchange | Designed separately, not implemented | Required only when a claimed role needs narrower temporary authority than the listener's stable credential already has |

### Runtime correction to the initial review evidence

Runtime release state is intentionally not frozen in this design record. Use
`meristem status`, `/readyz`, `meristem build-guard-status`, and MCP
`initialize` to compare the mapped build with the reviewed `v1` pin. Restart
remains an acceptance test and a release action, never the presumed fix for
missing behavior.

## Domain model

### Listener registration

A listener registration is a durable client endpoint, not a persona.

Proposed fields in the `listeners` projection:

- `id`: stable listener UUID and routing address;
- `name`: operator-facing unique name such as `codex-review`;
- `principal_token_id`: stable credential currently accountable for claims;
- `provider`: optional routing datum, never an authority grant;
- `capabilities`: normalized set of capability names the listener offers;
- `max_concurrent_assignments`: initially exactly `1` for app listeners;
- `policy_event_id`: event that produced the current base policy;
- `created_at` and `updated_at`: event timestamps.

Token rotation or OAuth rebinding changes the credential bound to the stable
listener registration through an attributed event; it does not change the
listener address. Producers never choose a bearer UUID.

Proposed authoritative event kinds:

- `listener.registered`
- `listener.credential_bound`
- `listener.policy_set`
- `listener.retired`

All projections are written synchronously by event projectors. Policy payloads
carry `payload_version`, and their canonical normalized shape participates in
deterministic event identity.

### Base policy

`listener.policy_set` contains a complete replacement, not an incremental
patch:

```json
{
  "payload_version": 1,
  "listener_id": "uuid",
  "projection": "dispatch",
  "predicates": [
    {"kind": "actor", "token_ids": ["uuid"]},
    {"kind": "work_item_tree", "work_item_id": "uuid"},
    {"kind": "kind_include", "event_kinds": ["dispatch.requested"]}
  ],
  "capabilities": ["review.exact_artifact"],
  "max_concurrent_assignments": 1,
  "focus": "claimed_work_item_tree",
  "effective_after": "policy_event"
}
```

Predicates use the existing normalized feed vocabulary. Empty predicate lists
mean all eligible demand in the selected projection. Unknown predicates,
capabilities, projections, or focus modes fail closed. The first release does
not add free-form topic tags: the prose layer resolves a topic to an existing
tree, principal set, capability, and/or event-kind set before persisting the
policy.

Policy changes while a listener is focused replace the pending base policy but
do not interrupt the active assignment. Explicit cancellation remains a
separate owner-authorized work-item operation.

### Work item as the first capability-demand envelope

The first release continues using a work item plus `dispatch.requested` or an
explicit structured handoff as the demand envelope. It does not introduce a
second capability-demand lifecycle table. This deliberately reuses the shipped
assignment ledger. A later work item may introduce a separate demand object if
one work item must host simultaneous independent capabilities.

Dispatch payloads gain normalized routing metadata where required:

- semantic capability name, resolved from the exact cultivar profile's
  `dispatch_capability` (never copied from the cultivar name);
- exact versioned cultivar as separate launch metadata;
- exact artifact identity for artifact-bound work;
- implementation-author principal when self-review exclusion applies;
- optional listener addressee chosen by the deterministic router;
- policy constraints such as distinct-provider review;
- patience and fallback rule.

Raw clients request capability; they do not resolve listener credentials. The
router selects an eligible listener registration and appends the addressed
activation event.

The shipped rootstock mapping is total and versioned:

| Cultivar | Dispatch capability |
| --- | --- |
| `checklist-worker@1` | `work_items.execute_checks` |
| `convergence-scribe@1` | `convergence.propose_checks` |
| `reviewer@1` | `review.exact_artifact` |
| `human-attention@1` | `human.attention` |

Legacy/custom cultivar events that lack `profile.dispatch_capability` map to
`cultivar.<name>.v<version>`. That exact-version fallback is deliberately
narrow: it preserves replay and rolling-upgrade compatibility without
pretending an old launch profile offered a broader semantic role. New
cultivar definitions declare `profile.dispatch_capability` explicitly.

An addressed activation is **exclusive to its addressee for that finite
routing-patience epoch**, not advisory. While that epoch is open, a different
otherwise-eligible listener receives a pure claim conflict and no event is
appended. If the addressee does not claim or accept activation before the
deadline, the deterministic router appends a new routing decision that either
names a different listener, removes the addressee for an open race, selects an
allowed fallback adapter, or escalates. Addressing therefore has observable
meaning without permitting an unavailable listener to strand the demand.

### Assignment identity

The existing `work_item.assigned` event remains the authoritative lease. Its
event ID is the assignment generation. A payload-version extension records the
claiming `listener_id` in addition to the attributed bearer token. Claim is
allowed only when the bearer is the registration's principal credential or an
unexpired delegated credential whose chain resolves to it.

Credential rebinding while an assignment is active does not create a new
assignment generation, reset its lease, or transfer it to another listener.
Authorization for every completion, verdict, failure, or yield is resolved at
event time through the listener's **current** credential binding and the exact
`assignment_event_id`. The newly bound credential may complete the existing
generation; the old credential fails closed immediately after unbinding and
appends no task event.

Every completion, review verdict, failure, or yield that satisfies routed work
must reference:

- `work_item_id`;
- `listener_id`;
- `assignment_event_id`;
- exact artifact identity when applicable.

Reducers reject stale generations, mismatched artifacts, and self-review.

## Canonical transport surface

REST remains canonical; MCP mirrors request and response bodies.

### Listener operations

- `POST /v1/listeners` / `listeners.create`
- `GET /v1/listeners` / `listeners.list`
- `GET /v1/listeners/{id}` / `listeners.get`
- `POST /v1/listeners/{id}/policy` / `listeners.set_policy`
- `POST /v1/listeners/{id}/credential-bindings` /
  `listeners.bind_credential`

Listener administration requires a dedicated non-root human scope. A listener
may narrow its own policy but may not widen its capabilities, scopes, or
credential binding. Root remains mint/revoke-only.

### Assignment operations

- `POST /v1/work-items/{id}/claim` / `work_items.claim`
- `GET /v1/work-items/{id}/assignment` / `work_items.get_assignment`
- `POST /v1/work-items/{id}/yield` / `work_items.yield`

Claim accepts `listener_id` and the observed `policy_event_id`. The service
revalidates registration, authorization, eligibility, current policy revision,
human-review state, work-item claimability, author exclusion, and capacity
inside the same transaction that appends `work_item.assigned`. A stale policy
revision is a pure conflict and appends no event. Existing same-holder
idempotency and competing-holder conflict semantics remain intact.

Yield names the exact `assignment_event_id`; stale yield attempts append
nothing. GET reads the existing projection.

## Listener state machine

The effective state is derived; it is never a process-local source of truth.

```text
START
  -> read listener registration and latest base policy
  -> read active assignments for listener principal/delegation chain

no active assignment
  -> IDLE(policy_event_id)
  -> mint cursor for the policy lens at head H
  -> snapshot current open eligible demand
  -> attempt deterministic candidates in order
  -> consume events after H
  -> atomically claim one candidate

claim succeeds
  -> FOCUSED(work_item_id, assignment_event_id)
  -> retain the stable control lane
  -> use assigned/addressed + work-item-tree lens for task traffic
  -> request activation

terminal result | yield | expiry
  -> assignment projection closes the exact generation
  -> discard the focus cursor only after the release is observed
  -> return to IDLE using the latest base policy
```

Mint-before-snapshot deliberately permits duplicates and prevents gaps. An
open demand at or before H appears in the snapshot; one after H appears in the
stream; a demand racing both may appear twice, but deterministic ordering,
idempotent claim, and the assignment conflict reducer collapse it.

The stable control lane is never replaced by a topic/tree lens. It carries
policy revisions, cancellation, assignment control, activation outcomes, and
terminal handback. Content lenses may narrow demand and focused work, but
cannot hide control traffic.

Restart runs `START`. If an active assignment exists, the listener resumes
FOCUSED before considering new demand. If none exists, it resumes IDLE and
rescans current open demand. Lease expiry is worker-owned and returns the
listener to IDLE even when the client vanished.

## Deterministic eligibility and selection

Eligibility is a pure reduction over durable inputs:

1. demand is open, nonterminal, and visible to the listener;
2. work item is not human-review blocked;
3. demanded capability is registered and allowed by the current policy;
4. listener capacity is available;
5. listener and credential scopes cover claim plus the required tree;
6. assignment author-exclusion and provider-diversity constraints pass;
7. if the current routing epoch names an addressee, this listener is that
   addressee;
8. no unexpired assignment already exists.

Candidate order is `(dispatch_event_seq, work_item_id)` on the home node. A
listener attempts candidates in that order. Competing eligible listeners may
race; the existing assignment row lock chooses one winner. A claim event records
the reducer identity, policy event, routing inputs, and selected listener. Free-
form model judgment never grants the lease.

## Activation and the Codex shim

### Durable activation contract

Claiming creates or causes a deterministic `listener.activation_requested`
event keyed by assignment generation and adapter attempt. A projection records:

- activation ID;
- listener and assignment IDs;
- adapter kind and binding generation;
- state: `requested | dispatching | accepted | completed | failed | ambiguous`;
- bounded retry count and next retry;
- last outcome event.

The generic listener supervisor claims pending activations through the same
Postgres-backed lease discipline used elsewhere. Before contacting an external
application it records `dispatching`. It uses a deterministic external client
message ID derived from the activation ID. It then records an attributed
accepted/completed/failure receipt. An ambiguous result is never blindly
resubmitted; the adapter first reconciles using the deterministic client
message ID. If reconciliation cannot establish an outcome within patience,
the reducer reassigns, chooses an allowed fallback, or escalates.

No second queue or durable file database is introduced.

### What remains Codex-specific

The Codex adapter may:

- resolve one adapter-local binding from listener ID to Codex task ID;
- resume that task through the supported app-server lifecycle;
- reconcile a deterministic client message ID against task history;
- start a turn only when the dedicated task is idle;
- deliver a metadata-only wake containing activation, work-item, assignment,
  and separately bound task-actor IDs;
- decline every unattended approval, elicitation, or permission request;
- report accepted, completed, retryable, terminal-failure, or ambiguous.

It may not:

- select actors or capabilities;
- inspect or copy Meristem event bodies into the prompt;
- decide whether an event is authoritative owner instruction;
- own feed filters or cursor identity;
- grant Meristem authority;
- launch a fresh reviewer as the canonical path;
- retry an uncertain admission without reconciliation.

The adapter-local Codex task binding is vendor transport configuration, not a
Meristem routing address. It may remain a local configuration value in the
first release. The stable Meristem listener registration is what other actors
address. Rebinding the adapter does not change listener policy or attribution.

### Disposition of the current shim

| Existing responsibility | Disposition |
| --- | --- |
| SSE connect/reconnect and filter-bound cursor | Replace with generic Go listener/feed code already shipped in `meristem feed --watch` |
| Static actor allowlist and `listen_for` UUID | Delete after listener policy and stable routing are live |
| Queue, delivery, seen IDs, and quarantine files | Move to event-backed activation requests and receipts; retain files only during guarded cutover |
| Wake coalescing | Optional generic optimization keyed by listener and assignment; never correctness-critical |
| Codex app-server protocol and negative approval handling | Retain in the one-shot Codex adapter initially |
| Deterministic client message ID and history reconciliation | Retain; derive from durable activation ID |
| Hard-coded thread ID | Keep only as adapter-local binding, outside routing semantics |
| Separate listener-supervisor and task MCP credentials | Retain least-privilege separation; the first Codex profile stores an inert task marker and derives one exact tree scope only inside a live assignment-bound MCP process |

The old bridge remains available until the new supervisor passes parity and
restart tests. Cutover is one listener at a time. A listener has exactly one
active activation consumer generation, so old and new adapters cannot both
accept the same activation.

## Authority and token exchange

The stable listener credential authenticates the principal and may read the
control/demand surfaces needed to decide whether to claim. It does not
automatically inherit every temporary role's write authority.

The first Codex task profile provisions a distinct marker-only credential with
no ordinary business authority. A reviewed MCP launcher authenticates its exact
actor UUID and asks the activation service to validate the current activation,
work item, assignment event, listener principal, and leases. Only inside that
process does the deterministic reducer derive the exact tree-scoped read/write
set and three-tool surface. The task actor remains the attributed actor; the
listener principal and its bearer never cross the adapter wake boundary. Every
tool listing and call repeats the live-generation validation, so yield, expiry,
terminal activation, rebind, revocation, or token replacement fails closed.

This is one local assignment-bound exchange profile, not the general delegated
credential design. Where future roles require broader or remote exchange, the
issued credential must still be:

- is bound to listener ID, work-item tree, assignment event, audience, and
  role/lens;
- expires no later than the assignment lease;
- is no broader than the source authorization and owner-approved policy;
- preserves source, actor, and delegation chain;
- omits unrelated authority from both source and target;
- fails closed after yield, terminal handback, expiry, or generation mismatch.

This dependency is owned by work item
`37d442e8-4132-54a2-8a40-9e59d12cd357`. It is not permission to create
permanent writer/reviewer identities.

## Bounded patience and failure behavior

Every nonterminal state has a finite exit:

- open demand not claimed: retry eligibility, then allowed fallback, then human;
- claimed but activation not accepted: reconcile/retry, then expire/reassign;
- bound app task busy before admission: record structural
  `adapter_target_busy`, retry without consuming the ordinary adapter-failure
  budget, and remain bounded by the assignment lease/patience;
- activation accepted but task does not report completion: assignment lease
  expires, then reassign/fallback;
- ambiguous external admission: reconcile only; never blind duplicate;
- focused task yields or fails: release exact generation and return listener to
  base policy;
- invalid policy or binding: fail closed, append deterministic error, and
  retain the last valid policy where one exists.

Patience escalation may change operational lifecycle and create an attention
item, but must not revoke an existing owner permission decision. That separate
bug is tracked by `96154ad5-a30f-55df-9d56-aa3460639967`.

## Implementation plan

The owner authorized implementation on 2026-08-07. Exact-commit review remains
required before merge, and replacing the live feed bridge remains a separate
human-gated cutover.

### Slice 0 — readiness proof and baseline (shipped)

- Pin the reviewed `v1` commit and binary build guard.
- Run existing feed-watch, assignment, rebuild, and bridge test suites.
- Add a non-mutating runtime status command that reports compiled commit,
  listener binding health, policy revision, effective mode, assignment
  generation, cursor identity, and last activation outcome without reading
  token material.
- Reproduce the three-token misaddressing failure as a contract test.

Exit: evidence distinguishes configuration/restart issues from missing code;
no restart is prescribed without a failed executable probe.

### Slice 1 — canonical assignment transports (shipped)

- Add REST/MCP claim, assignment read, and yield surfaces over the existing
  service.
- Add idempotency, access, lease-generation, same-holder retry, competing
  claimant, expired claim, and provider-safe tool-advertisement tests.
- Mark stale-policy and other proven zero-write validation conflicts with the
  typed pure-refusal disposition so a corrected retry may reuse its key.
- Add no new assignment table.

Exit: two external scoped clients race for one item; exactly one assignment
event exists, the loser gets a replayable conflict, and yield/expiry/terminal
handback are visible on the assigned lane.

### Slice 2 — listener registration and policy (shipped)

- Add listener events, projections, domain service, REST, and MCP.
- Reuse the normalized predicate implementation and pin policy fingerprints.
- Add stable name/ID resolution and credential binding with separation of
  duties.
- Add deterministic policy translation fixtures for the four owner-visible
  instructions above.

Exit: Fable and Codex have stable listener registrations in the routing
fixtures; either may request or receive a capability without naming a bearer
UUID, and routing resolves the addressed listener deterministically.

### Slice 3 — idle/focused supervisor (shipped)

- Add a generic `meristem listener` runtime using the existing feed client.
- Maintain the stable control lane and revision-specific demand/focus cursors.
- Implement mint-before-snapshot candidate recovery, deterministic ordering,
  atomic claim, restart derivation, and automatic base-policy restoration.
- Record worker-visible status and deterministic errors through events.

Exit: restart in IDLE and FOCUSED loses no open demand, accepts no duplicate
assignment, and restores the latest base policy after release.

### Slice 4 — durable activation and Codex adapter cutdown (release candidate)

- Add activation events/projection and bounded activation reconciler.
- Move delivery state from shell files into the event-backed projection.
- Keep the tested app-server driver as a one-shot adapter; remove policy,
  actor allowlist, feed cursor, and queue ownership from it.
- Run old/new parity, ambiguous-admission, crash-window, approval-denial, and
  metadata-boundary tests.

Exit: the old bridge can be stopped for one listener without loss; restart
reconciles an accepted Codex turn by deterministic client message ID.

### Slice 5 — narrowed assignment authority

- Implement the independently reviewed assignment-bound exchange contract only
  for roles that need it.
- Bind expiry and revocation to the exact assignment generation.
- Prove the child credential cannot read or write outside its assigned tree or
  survive terminal handback.

Exit: ad-hoc reviewer/writer behavior requires no permanent role identity and
  cannot broaden authority.

### Slice 6 — Fable complementary-review smoke and cutover

1. Register Fable and Codex as complementary listeners and set Codex base
   policy to all eligible complementary-review demand.
2. Fable publishes an exact-artifact review demand addressed through the
   Codex listener registration, without a token UUID.
3. Codex listener is selected, wakes, and atomically claims generation G.
4. Codex focuses on the work-item tree and records a G-fenced verdict.
5. A blocking verdict wakes Fable for revision; an accepted verdict feeds the
   deterministic convergence reducer.
6. Codex observes terminal handback and returns to all-eligible listening.
7. Repeat with an API restart, listener restart, duplicate event, first-wake
   failure, lease expiry, and competing eligible listener.

Exit: the complete loop requires no owner relay, uses no guessed token or
thread ID, and reaches a deterministic terminal or bounded escalation.

## Verification matrix

Required automated coverage:

- pure policy normalization and stable fingerprints;
- projector replay byte-for-byte;
- REST/MCP parity and idempotency;
- policy scope cannot widen authorization;
- stable listener ID survives credential rebinding;
- rebinding while FOCUSED preserves the assignment generation: the new
  credential may record terminal handback and the unbound credential is
  rejected without appending an event;
- assigned/addressed control survives content predicates;
- an addressed routing epoch rejects a racing non-addressed eligible listener;
  after bounded expiry and an explicit reroute, the new addressee or open race
  may claim;
- mint-before-snapshot has duplicate-safe no-loss behavior;
- concurrent claim has exactly one winner;
- restart derives IDLE versus FOCUSED from projections;
- focus ignores unrelated demand but retains control traffic;
- terminal/yield/expiry restores the newest base policy;
- exact-artifact, no-self-review, and assignment-generation fences;
- activation dispatching/accepted/completed/ambiguous recovery;
- no event body enters a Codex wake prompt;
- unattended Codex requests never approve permissions or external writes;
- full Postgres integration suite and `meristem rebuild` remain green.

## Review questions

The independent reviewer should explicitly accept or reject these decisions:

1. Is a durable listener registration the smallest correct stable address, or
   can the existing token model provide equivalent rotation-safe routing
   without making producers guess bearer IDs?
2. Is work item plus dispatch metadata sufficient for the first release, or is
   a separate capability-demand lifecycle already required?
3. Does mint-before-snapshot plus idempotent claim prove no eligible open demand
   is lost across policy changes and focus restoration?
4. Is adapter-local Codex task binding consistent with vendor-neutral core and
   bounded recovery, or must binding metadata be event-backed immediately?
5. Which portions of the current shell bridge remain necessary after the
   shipped feed watcher and durable activation projection are composed?
6. Can assignment-bound token exchange remain a later slice without weakening
   the first end-to-end proof?
