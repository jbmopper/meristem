# Remote reference anchors — design proposal

Status: **revised proposal for owner decision and independent design review**.
Nothing here is implemented, and nothing here is authorized. Work item
`319c8dc8` forbids migrations and new event kinds; this design needs both,
which is exactly why it remains a proposal rather than an implementation.

## The problem

The accepted cross-node read rule (D+B, recorded on `319c8dc8`) requires that
the exact canonical reference already exist in an authoritative local record
attached to an object the caller may read. A caller-supplied qualified
reference is explicitly **not** an anchor.

No such record can exist today:

- `work_item_relations` is `parent_id` / `child_id` as local UUID foreign keys.
  It cannot name an object on another node at all.
- `command_queue` (with `0032_network_provenance`) carries `origin_node_id` and
  `causing_work_item_id`. The causing item is the **local** one; this anchors a
  local item to a remote *node*, not to a remote *object*.
- `crossnode_outcome_observations` carries `target_node_id` and
  `remote_terminal_event_id` — a remote *event*, with no path to a work-item
  uuid on this node.
- `FormatCanonicalRef` has no callers, so no canonical reference is minted
  anywhere.

So the anchor condition is unsatisfiable in production: every remote read
refuses, and the success path could only ever be exercised by a test that
manufactures its own precondition.

## Proposal: a separate remote-reference projection

### Authoritative events

Two new kinds, both subject-bound to the **local** work item. Anchoring on the
local subject is what gives the local ACL something to evaluate.

- `work_item.remote_reference_recorded`
- `work_item.remote_reference_retired`

### Payload v1

```
payload_version: 1            // absent means 1, per docs/payload-versioning.md
work_item_id:    <uuid>       // must equal event.subject_id
canonical_ref:   "mrs://<node_id>/work-items/<uuid>"
home_node_id:    <node_id>    // must equal the ref's authority component
remote_object_id: <uuid>      // must equal the ref's id component
relation:        <enum>       // see vocabulary
note:            <string>     // optional, human context, never load-bearing
```

`home_node_id` and `remote_object_id` are stored redundantly with
`canonical_ref` **and validated against it** at projection time. They exist so
queries can filter by home without parsing strings; the reference remains the
single source of truth, and a payload whose parts disagree with its ref fails
closed rather than being reconciled.

The retire payload carries `work_item_id`, `canonical_ref`, `relation`, and a
`reason`.

### Relation vocabulary and lifecycle boundary

A small closed set, rejected if unknown:

- `tracks` — this item observes the remote one; no convergence coupling.
- `continues_at` — records where work continued after a handoff; informational
  only in this slice.

Neither relation changes local lifecycle, convergence checks, patience, or
terminal state. In particular, `continues_at` does not automatically turn the
local item into a terminal stub. Consumers may render or follow the reference,
but only an ordinary local lifecycle event can change the local item.

Deliberately **not** included:

- `depends_on`. Making a local item wait for remote convergence requires a
  separately designed event-backed remote-state observation, a deterministic
  reducer and verdict event, and bounded patience with partition escalation.
  A live remote read cannot be a replay-safe convergence input.
- `parent_of` / `child_of`. Parenthood implies tree traversal, and the local
  tree must stay local (see the comparison below).

### Projection

`work_item_remote_references`:

| column | note |
| --- | --- |
| `work_item_id` | FK to the local `work_items` row |
| `canonical_ref` | the durable spelling |
| `home_node_id`, `remote_object_id` | derived, validated against the ref |
| `relation` | enum |
| `recorded_event_id`, `recorded_at` | provenance |
| `retired_event_id`, `retired_at` | null while live |

Primary key `(work_item_id, canonical_ref, relation)`.

### Projector and rebuild contract

Every column is a deterministic fold of the event log. `retired_at` comes from
the retiring event's `occurred_at`, never from `now()` — a projector that reads
the clock produces a different table on replay, and `meristem rebuild` compares
table signatures. No triggers: `rebuild` clones with `LIKE ... INCLUDING ALL`,
which does not copy triggers, so trigger-maintained columns diverge in the
sandbox and fail the check for no real defect.

A retire event for a reference that was never recorded matches zero rows and is
a no-op, in the same way `node.route_updated` is a no-op for an unregistered
node.

### Idempotency, and the cycle trap

Event identity is subject + kind + canonical payload. Recording the same
`(work_item_id, canonical_ref, relation)` twice collapses, which is correct for
a retry.

But **record → retire → record** is three distinct logical actions whose first
and third payloads are byte-identical. Without a discriminator the third event
reuses the first event's id, the append reports success, the projector never
runs, and the reference stays retired while the caller believes it was
restored. This is exactly the defect codex found on the listener control plane
(`LCP2-R2-B1`) and it must be designed in from the start: both kinds take an
action discriminator from the idempotency identity when present, with the
repository's stable predecessor fallback (the current reference-state event id)
for direct service calls.

### Who may create and retire an anchor

This is the security crux, and it is stricter than ordinary local write.

An anchor is what names *which* remote object may be read. If any actor who can
write to any local item could mint one, then D collapses into B: an actor
holding `crossnode.work_items.read` could anchor an arbitrary reference to an
item it controls and read anything on any peer. The anchor would stop being
independent evidence and become a formality the caller supplies itself — which
is precisely the "caller-supplied reference is not an anchor" rule, re-entered
through the side door.

So recording or retiring an anchor requires **both**:

1. ordinary local mutation authority on the subject work item, and
2. a dedicated `crossnode.references.record` scope — non-root, non-revoked,
   identified, like the existing cross-node scopes.

Ordinary tracker-write scopes do **not** imply it.

### Normalization and the local ACL reduction for a remote read

A remote-read surface may accept either qualified spelling defined by the
network contract: the canonical `mrs://<node_id>/work-items/<uuid>` or the
compact `<node_id>:<uuid>` alias. Before any anchor lookup or outbound I/O, the
service must parse the input, require a non-empty remote node id, and re-emit it
with `FormatCanonicalRef`. A bare UUID is local and is not a remote reference.
Malformed or unformattable input fails closed.

Only the normalized canonical string is carried past that boundary. Events and
the projection store canonical strings exclusively; compact aliases are input
compatibility, never durable identity.

A remote read of the normalized `<canonical-ref>` is permitted only when all
of these hold:

1. There is a **non-retired** row in `work_item_remote_references` whose
   `canonical_ref` equals `<canonical-ref>` by exact string comparison.
2. The caller passes the ordinary local **read** reducer for that row's
   `work_item_id` — the anchor's subject, a local object with a local ACL.
3. The caller holds `crossnode.work_items.read` (`access.RemoteReadAllowed`).
4. The peer credential independently authorizes the exact read at the home.

Any missing element refuses **before** outbound I/O and appends no event.

Exact string comparison in (1) is safe only *after* normalization. The parser
accepts the compact alias for compatibility, while canonical URI parsing
rejects decorations and non-standard UUID spellings. Re-emitting the parsed
tuple produces one durable string per object, so both accepted qualified inputs
select the same anchor and decorated variants fail before lookup.

### What produces an anchor — never the read itself

An anchor is created only by an explicit operator or domain action:

- an operator recording a cross-node reference (`meristem work-item remote-ref
  add`, REST, MCP);
- a domain flow that *already* has authority over both sides, e.g. an addressed
  push recording the informational `continues_at` relation on the local item it
  just dispatched from.

The remote read never mints its own anchor. That would make D circular: the
read would authorize itself.

## Alternative considered: extend `work_item_relations`

Rejected, because it does not preserve the local-tree invariants.

`work_item_relations` is `(parent_id, child_id)` as local UUID FKs, and the
tree is walked with recursive CTEs that assume every id resolves to a local
`work_items` row. Adding a nullable `home_node_id` breaks that assumption in a
way the type system cannot catch:

- Every existing recursive traversal would need a `home_node_id IS NULL` filter
  to stay local. Each one that is missed silently walks a remote id as if it
  were local — and since no local row exists, an inner join drops the branch
  while a left join yields NULL columns. Both are wrong, and neither raises an
  error.
- The FK itself must be dropped or made conditional, which removes the database
  guarantee that a relation names a real local object.
- Parent/child implies convergence and patience propagation up and down the
  tree. Extending it across nodes silently extends those semantics too, which
  is a much larger decision than "we can name a remote object."

A separate projection keeps the tree exactly as it is: local FKs intact,
recursive traversal unchanged, no existing query touched. The cost is one more
table; the benefit is that no existing correctness property has to be re-proved.

## Migration cost

One migration for the projection, plus two event kinds. Both are currently
forbidden by `319c8dc8`. Migration numbering is integration state, not part of
this design: assign the next dense number only when an implementation child is
rebased onto the then-current `v1` line.

## What the owner is being asked

1. Accept the separate-projection approach, or direct the `work_item_relations`
   extension despite the traversal hazard above.
2. Widen `319c8dc8`'s Forbidden list to permit one migration and two event
   kinds, or spawn a separate item to carry them.
3. Confirm the stricter create authority (`crossnode.references.record` on top
   of local mutation authority), or accept that ordinary local write is enough
   to mint an anchor — which weakens D to roughly B.
4. Confirm the first slice is observational (`tracks` and informational
   `continues_at`) and defer cross-node convergence coupling to a separate
   design with its own reducer, verdict event, patience budget, and escalation.
