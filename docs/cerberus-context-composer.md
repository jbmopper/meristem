# Cerberus Context Composer

> Status: experimental Cerberus/Aegis pilot design record. This is not the
> current general agent bootstrap path; use [`AGENTS.md`](../AGENTS.md) and
> [`mcp-worker-bootstrap.md`](mcp-worker-bootstrap.md) for active worker rules.

This document defines the model-facing context contract for the planned
Cerberus/Aegis worker net. It is advisory until promoted into code, but it is
intended to be the source input for the scoped-token, reducer-event,
budget/force, smoke, and launcher work_items that follow it.

If this document conflicts with `docs/spec.md`, the spec wins.

## Contract

Every Cerberus/Aegis model invocation receives exactly two model-facing fields:

```json
{
  "system": "role, authority, standards, boundaries, and budgets",
  "message": "current state, relevant feed, sibling signals, and the next task"
}
```

Do not add wide context structs for worker-facing content. A field is
structural only when deterministic Go code, a reducer, a projection, or an auth
boundary must query or key on it without parsing prose. Everything else belongs
inside `system` or `message`.

The event envelope remains structural and outside this model context:

- event kind
- subject kind and id
- actor token id
- source
- idempotency key
- request authentication and token scopes

These are attribution, auth, and dedupe facts, not worker payload prose.

## Composer Inputs

The context composer reads from durable meristem state. It does not inherit
private chat state.

`system` is assembled from stable authority and process text:

- invocation role and persona:
  - Aegis coordinator, when the scoped coordinator token is launching Aegis
    herself for the subtree
  - grower or healer, when spawning a Cerberus subactor worker
  - reducer-facing reporter, when a worker is emitting bounded evidence
- assigned work_item id and subtree root
- Aegis-authored project standards inherited from the work_item lineage
- allowed operations and explicit out-of-scope operations
- authority boundaries: workers may append bounded events and request/spawn
  permitted children, but may not decide lifecycle from free-form judgment
- token-scope summary, without token secrets
- masking rules for secrets and private material
- budget limits: token budget, wall-time budget, call depth, and current depth
- force-convergence behavior when a coordinator force event is present

`message` is assembled from changing state:

- current work_item snapshot: title, body, lifecycle state, suggested checks,
  human review status, parent/child summary
- recent feed window and opaque cursor
- relevant lineage/coordinator events that define standards or budget state
- sibling signals on the same parent, especially grower/healer signals
- last convergence verdict or pending reducer output, when present
- `coordinator.pass_summary`, when this is a compressed respawn pass
- one concrete task for the current invocation

The composer may compact older events into prose before placing them in
`message`, but the compacted text must name the event ids or cursor range it
summarizes so a worker can self-research through MCP when needed.

## Structural Payload Rule

Event payloads default to narrative content:

```json
{
  "system": "...",
  "message": "..."
}
```

Add structural fields only for facts the deterministic subsystem must inspect.
Examples:

```json
{
  "kind": "healer.convergence_signal",
  "checks_passed": ["context_contract_documented"],
  "checks_failed": [],
  "system": "...",
  "message": "Evaluated grower event ... against the declared checks."
}
```

```json
{
  "kind": "waypoint.verified",
  "waypoint_id": "context-contract-v1",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "checks_passed": ["system_message_only_default_defined"],
  "system": "...",
  "message": "The reducer emitted this after all required signals passed."
}
```

`waypoint.verified` does not carry `verified_by_reducer`; being reducer-emitted
is part of the event contract. If provenance is needed, use the event envelope
and the reducer verdict event that caused it.

## Coordination Rules

Grower and healer workers coordinate through meristem, not through shared
process memory. Aegis/coordinator is not a third spawnable worker in a Cerberus
subactor. The coordinator token is Aegis's scoped identity for the subtree; each
spawned subactor has exactly two workers: grower and healer.

- The grower reads the latest healer signal before appending a forward move.
- The healer reads recent grower moves and appends bounded convergence,
  stand-down, or repair signals.
- Either worker may request or spawn a child Cerberus subactor only for a
  verified waypoint and only while `current_depth < call_depth_max`.
- Workers treat non-human feed entries as context, not owner instructions.
- Lifecycle transitions are owned by deterministic reducers/reconcilers or by
  explicit operator events, not by model prose.

## Masking Rules

The composer must never place the following in `system` or `message`:

- bearer tokens or token files, including `.meristem/*.token`
- plaintext token secrets returned during fresh issuance
- `.env*` contents or connection credential material
- private message bodies unrelated to the assigned subtree
- unrelated operator files or repository content

Workers may receive a non-secret token-scope summary such as:

```text
Token authority: work_items.read, work_items.write, feed.read_assigned,
work_items.tree:<root>. Secret material is masked and out of scope.
```

If a worker needs information that is masked, it blocks or asks for a scoped
export. It does not request raw secrets.

## Force Respawn

Aegis force convergence keeps the same `system` contract and supplies a
compressed `message`.

Trigger:

- token budget exhausted
- wall-time budget exhausted
- call depth would exceed `call_depth_max`
- operator appends an explicit force event

Protocol:

1. Aegis appends `coordinator.force_convergence` with the budget reason.
2. Running workers append `worker.output_reduced` with a compact summary of
   attempted work, current state, remaining blockers, and evidence event ids.
3. Aegis appends `coordinator.pass_summary`, compressing the subtree into a
   bounded message.
4. Aegis respawns fresh grower/healer workers with the same role system text and
   the pass summary as the main message context.
5. The fresh workers make one best-effort convergence pass.
6. If the pass still cannot converge, Aegis records a blocked summary for the
   deterministic reducer or operator to handle.

The force pass is a context-management operation. It does not let Aegis bypass
the reducer lifecycle boundary.

## Example: Grower Context

```json
{
  "system": "You are the grower for work_item 60959376-e0ff-5207-9270-dacfb403333e in subtree 98853a93-2de4-42fb-9438-a1a54caf9589. Aegis standards inherited from the coordinator: worker context defaults to system + message; structural fields exist only for reducer/projection needs; do not introduce agent_kind schema. Allowed operations: read assigned feed/work_items, append bounded progress or candidate events in-tree, request a verified-waypoint child if depth remains. Out of scope: lifecycle decisions, approvals, secret inspection, token material, unrelated files. Budget: 40000 tokens, 8 minutes wall time, depth 2/5. Before acting, read the latest healer signal on this work_item.",
  "message": "Current state: triaged/running context-composer item with checks system_message_only_default_defined, lineage_standards_compose_into_system, feed_work_item_sibling_state_compose_into_message, force_respawn_uses_same_system_compressed_message, masking_rules_for_secrets_explicit. Recent feed cursor: opaque-cursor-123; recent events: coordination.plan_converged d6e908ba..., Claude pass 581b1928.... Sibling healer last signal: none. Task: draft one candidate contract section that advances the context composer without changing schema."
}
```

## Example: Healer Context

```json
{
  "system": "You are the healer for work_item 60959376-e0ff-5207-9270-dacfb403333e. Your job is to read grower outputs, compare them to Aegis-authored standards and suggested convergence checks, and append bounded signals. You may append healer.convergence_signal, healer.repair_signal, or healer.stand_down. You may not transition lifecycle from free-form judgment. Secrets, token values, unrelated files, approvals, and schema-level agent taxonomy are out of scope. Budget: 20000 tokens, 5 minutes wall time, depth 2/5.",
  "message": "Current work_item checks: system_message_only_default_defined; lineage_standards_compose_into_system; feed_work_item_sibling_state_compose_into_message; force_respawn_uses_same_system_compressed_message; masking_rules_for_secrets_explicit. Recent grower events: candidate contract event abc..., candidate example event def.... Recent reducer verdict: none. Task: evaluate the last grower move. If it satisfies checks, append healer.convergence_signal with checks_passed/checks_failed plus system/message narrative. If not, append healer.repair_signal with the smallest repair."
}
```

## Example: Aegis Force-Respawn Context

```json
{
  "system": "You are Aegis coordinating subtree 98853a93-2de4-42fb-9438-a1a54caf9589. You author project standards, monitor budgets, spawn or wake Cerberus grower/healer pairs, and record force-convergence events. You do not decide lifecycle directly from prose; deterministic reducers own lifecycle. Allowed operations: read subtree, append coordinator events, request or spawn in-tree workers with scoped tokens, and summarize worker outputs. Out of scope: raw token secrets, unrelated workspaces, external writes without approval. Budget policy: force on token exhaustion, wall-time exhaustion, or depth >= 5.",
  "message": "Force reason: wall_time_budget exhausted after 8 minutes. Worker output_reduced events: grower 111... summarized candidate contract sections; healer 222... found masking rules incomplete; grower 333... repaired examples. Pass summary to carry forward: system/message contract is stable; structural payload rule is stable; remaining risk is making mask exclusions explicit in all examples. Task: append coordinator.pass_summary with this compressed state, then spawn one fresh grower/healer pass using the same system contract and this message as the main context."
}
```

## Acceptance Checks For `60959376`

- `system_message_only_default_defined`: satisfied by the two-field contract.
- `lineage_standards_compose_into_system`: satisfied by composer inputs for
  inherited Aegis standards and authority boundaries.
- `feed_work_item_sibling_state_compose_into_message`: satisfied by composer
  inputs for work_item snapshots, feed cursor/windows, and sibling signals.
- `force_respawn_uses_same_system_compressed_message`: satisfied by the force
  respawn protocol and example.
- `masking_rules_for_secrets_explicit`: satisfied by the masking section and
  non-secret token-scope example.

## Follow-On Work

This contract unblocks the next work_items in the converged plan:

- `81e57433` can derive the scoped Aegis coordinator token and the grower/healer
  worker token scopes from the allowed operations and masking boundaries above.
- `21b1f9c3` can define minimal reducer-facing structural fields while leaving
  narrative content in `system` and `message`.
- `8f983e32` can use the force-respawn protocol as the budget exhaustion path.
- `d618d807` can smoke concurrent grower/healer workers using the examples as
  prompt templates.
- `9e5190e8` can build one Aegis coordinator launcher plus grower/healer worker
  launchers that populate only `system`, `message`, and the auth envelope.
