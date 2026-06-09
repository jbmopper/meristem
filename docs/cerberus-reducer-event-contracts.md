# Cerberus Reducer Event Contracts

This document executes work_item `21b1f9c3`: define field-minimal event
contracts for Cerberus/Aegis coordination.

If this document conflicts with `docs/spec.md`, the spec wins.

## Rule

A payload field is structural if and only if deterministic Go code, a reducer,
a projection, or an auth/idempotency boundary must read it without parsing
prose. Otherwise the content belongs in `system` and `message`, as defined in
`docs/cerberus-context-composer.md`.

The envelope remains structural and outside these payload contracts:

- event kind
- subject kind and id
- actor token id
- source
- idempotency key
- token scopes and request authentication

## Event Kinds

The first Cerberus pilot uses these inner kinds under
`work_item.event_appended` unless and until a later work_item promotes one into
a canonical top-level event kind:

- `aegis.standards_declared`
- `worker.grower_signal`
- `worker.healer_signal`
- `waypoint.verified`
- `coordinator.budget_status`
- `coordinator.force_convergence`
- `worker.output_reduced`
- `coordinator.pass_summary`

No migration is required for this first contract. These facts are feed-visible
coordination events, and the deterministic reducer can read their JSON payloads
from the event log. Add a projection only when a later reducer or UI needs
indexed queries over these fields.

## Contracts

### `aegis.standards_declared`

Purpose: Aegis-authored project standards and process rules inherited by a
subtree.

Structural fields:

```json
{
  "standard_set_id": "cerberus-context-v1",
  "applies_to_work_item_id": "98853a93-2de4-42fb-9438-a1a54caf9589",
  "checks": ["system_message_only_default_defined"],
  "system": "...",
  "message": "..."
}
```

Justification:

- `standard_set_id` lets later signals name the standard without parsing text.
- `applies_to_work_item_id` lets reducers determine lineage applicability.
- `checks` is reducer-readable checklist vocabulary.

### `worker.grower_signal`

Purpose: candidate/progress output from a grower.

Structural fields:

```json
{
  "signal_id": "grower-context-draft-001",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "produced_check_ids": ["system_message_only_default_defined"],
  "depends_on_event_ids": ["d6e908ba-b296-59d3-892f-5a169d102817"],
  "system": "...",
  "message": "..."
}
```

Justification:

- `signal_id` gives healer/reducer references a stable handle.
- `target_work_item_id` makes the signal unambiguous when emitted by child
  workers.
- `produced_check_ids` is optional reducer input for checklist matching.
- `depends_on_event_ids` makes compacted context auditable.

### `worker.healer_signal`

Purpose: bounded evaluation, repair, or stand-down signal from a healer.

Structural fields:

```json
{
  "signal_id": "healer-context-review-001",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "checks_passed": ["system_message_only_default_defined"],
  "checks_failed": [],
  "disposition": "pass",
  "depends_on_event_ids": ["<grower-event-id>"],
  "system": "...",
  "message": "..."
}
```

Allowed `disposition` values for the pilot:

- `pass`
- `repair`
- `stand_down`
- `blocked`

Justification:

- Reducers need `checks_passed`, `checks_failed`, and `disposition` without
  parsing prose.
- `depends_on_event_ids` ties the signal to reviewed evidence.

### `waypoint.verified`

Purpose: reducer-emitted gate that permits a child Cerberus subactor.

Structural fields:

```json
{
  "waypoint_id": "context-contract-v1",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "checks_passed": ["system_message_only_default_defined"],
  "system": "...",
  "message": "..."
}
```

There is no `verified_by_reducer` field. This event is reducer-emitted by
contract; provenance comes from the event envelope and the preceding
`convergence.verdict_recorded` event.

### `coordinator.budget_status`

Purpose: Aegis budget observation for a subtree.

Structural fields:

```json
{
  "subtree_root_work_item_id": "98853a93-2de4-42fb-9438-a1a54caf9589",
  "token_budget_remaining": 40000,
  "wall_time_remaining_seconds": 480,
  "current_depth": 2,
  "call_depth_max": 5,
  "force_required": false,
  "system": "...",
  "message": "..."
}
```

Justification:

- Reducers and monitors need budget numbers and force status directly.
- Narrative explanation, usage breakdown, and assumptions stay in `message`.

### `coordinator.force_convergence`

Purpose: Aegis requests a bounded force pass because a budget or operator force
condition fired.

Structural fields:

```json
{
  "subtree_root_work_item_id": "98853a93-2de4-42fb-9438-a1a54caf9589",
  "reason": "wall_time_budget_exhausted",
  "force_pass": 1,
  "current_depth": 5,
  "call_depth_max": 5,
  "system": "...",
  "message": "..."
}
```

Allowed `reason` values for the pilot:

- `token_budget_exhausted`
- `wall_time_budget_exhausted`
- `call_depth_exhausted`
- `operator_requested`

Justification:

- Reducers and workers need a structural reason and pass number.
- The detailed compacted context stays in `message`.

### `worker.output_reduced`

Purpose: worker shutdown summary during force convergence.

Structural fields:

```json
{
  "worker_role": "grower",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "force_pass": 1,
  "status": "partial",
  "evidence_event_ids": ["<event-id>"],
  "remaining_check_ids": ["masking_rules_for_secrets_explicit"],
  "system": "...",
  "message": "..."
}
```

Allowed `worker_role` values for the pilot:

- `grower`
- `healer`
- `reader`

Allowed `status` values:

- `complete`
- `partial`
- `blocked`
- `failed`

Justification:

- Aegis needs role, target, pass, status, evidence ids, and remaining checks to
  compose `coordinator.pass_summary` deterministically enough for replay.

### `coordinator.pass_summary`

Purpose: compressed force-pass context for respawn or final blocker.

Structural fields:

```json
{
  "subtree_root_work_item_id": "98853a93-2de4-42fb-9438-a1a54caf9589",
  "force_pass": 1,
  "source_event_ids": ["<worker-output-reduced-event-id>"],
  "remaining_check_ids": ["masking_rules_for_secrets_explicit"],
  "next_action": "respawn_best_effort",
  "system": "...",
  "message": "..."
}
```

Allowed `next_action` values:

- `respawn_best_effort`
- `block_with_summary`
- `await_operator`

Justification:

- Aegis and later reducers need source ids, remaining checks, and next action
  without parsing the prose summary.

## Reducer Inputs

The pilot deterministic reducer reads:

- target work_item suggested convergence checks
- `aegis.standards_declared.checks` inherited through lineage
- latest relevant `worker.grower_signal.produced_check_ids`
- latest relevant `worker.healer_signal.checks_passed/checks_failed`
- `waypoint.verified` for child-spawn gates
- `coordinator.budget_status.force_required`
- `coordinator.force_convergence.reason`
- `worker.output_reduced.status` and `remaining_check_ids`
- `coordinator.pass_summary.next_action`

It does not parse `system` or `message` for lifecycle decisions. Those fields
are evidence and model context.

## Narrative Example

```json
{
  "signal_id": "healer-context-review-001",
  "target_work_item_id": "60959376-e0ff-5207-9270-dacfb403333e",
  "checks_passed": ["system_message_only_default_defined"],
  "checks_failed": [],
  "disposition": "pass",
  "depends_on_event_ids": ["4591961e-7520-5249-9a4c-025a7dea4cc5"],
  "system": "You are the healer. Evaluate the last grower event against Aegis standards. Do not transition lifecycle.",
  "message": "I reviewed the context composer summary event and found the two-field contract explicit. No repair is needed for this check."
}
```

## Acceptance Checks

- `event_kinds_listed`: satisfied by the Event Kinds section.
- `structural_fields_justified_by_reducer_or_projection_need`: satisfied by
  per-contract justification notes.
- `waypoint_verified_fields_are_waypoint_id_target_id_checks_passed_only`:
  satisfied by the `waypoint.verified` contract.
- `no_verified_by_reducer_field`: satisfied explicitly in `waypoint.verified`.
- `system_message_narrative_examples_included`: satisfied by each contract and
  the narrative example.
- `envelope_vs_payload_boundary_explicit`: satisfied by the Rule section.
- `no_unnecessary_migration_added`: satisfied by the no-migration-first
  recommendation.
