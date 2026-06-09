# Cerberus Pilot Smoke Protocol

This document executes work_item `d618d807`: define the end-to-end smoke
scenario for the Cerberus grower/healer loop before launch wrappers run.

If this document conflicts with `docs/spec.md`, the spec wins.

## Purpose

The smoke proves that cheap workers can coordinate through meristem state rather
than inherited chat, and that lifecycle authority remains with deterministic
reducers/reconcilers rather than model prose.

It validates:

- one coordinator item with Aegis standards
- one concurrent grower signal
- one concurrent healer signal
- reducer-readable `checks_passed` / `checks_failed`
- one `waypoint.verified` or failed-check path
- one budget/force path
- one `worker.output_reduced`
- one `coordinator.pass_summary`

## Preconditions

- `docs/cerberus-context-composer.md` exists.
- `docs/cerberus-scoped-token-bootstrap.md` exists.
- `docs/cerberus-reducer-event-contracts.md` exists.
- `docs/cerberus-budget-force-protocol.md` exists.
- Meristem API/MCP is reachable.
- Test workers use separate scoped tokens when available; otherwise this smoke
  remains a dry-run protocol until phase-4 launchers are installed.
- The smoke target is inside root
  `98853a93-2de4-42fb-9438-a1a54caf9589`.

## Actors

The pilot uses concurrent grower and healer workers.

- Aegis coordinator: reads the subtree, declares standards, monitors budgets,
  and appends force/pass summary events.
- Grower: reads work_item/feed context and appends `worker.grower_signal`.
- Healer: reads the same work_item/feed context, including grower signals, and
  appends `worker.healer_signal`.
- Deterministic reducer/reconciler: reads structural fields and records
  lifecycle-relevant verdicts.

The grower and healer are not sequential role passes. They are separate workers
that coordinate by feed-visible sibling signals.

## Smoke Work Item

Create or choose a narrow child work_item under the Cerberus root:

```text
title: Cerberus pilot smoke target
body: Prove the grower/healer loop can coordinate through meristem by producing
      a tiny documented contract change or no-op evidence event.
checks:
  smoke_grower_signal_seen
  smoke_healer_signal_seen
  smoke_force_summary_seen
```

This smoke item is disposable. Its value is the event sequence, not the payload
content.

## Step-By-Step Protocol

1. Aegis reads the root item, smoke item, and recent feed cursor.
2. Aegis appends `aegis.standards_declared` on the smoke item:
   - `standard_set_id = cerberus-smoke-v1`
   - `applies_to_work_item_id = <smoke_item_id>`
   - `checks = ["smoke_grower_signal_seen", "smoke_healer_signal_seen",
     "smoke_force_summary_seen"]`
   - `system` and `message` follow the context composer contract.
3. Aegis appends `coordinator.budget_status`:
   - positive token and wall-time remaining
   - `current_depth = 0`
   - `call_depth_max = 5`
   - `force_required = false`
4. Aegis starts or wakes grower and healer concurrently with separate contexts
   over the same smoke item and feed cursor.
5. Grower reads the latest healer signal. If none exists, it appends
   `worker.grower_signal` with:
   - `target_work_item_id = <smoke_item_id>`
   - `produced_check_ids = ["smoke_grower_signal_seen"]`
   - `depends_on_event_ids` naming the standards event
   - narrative `system` and `message`
6. Healer reads the feed, sees the grower signal, and appends
   `worker.healer_signal` with:
   - `target_work_item_id = <smoke_item_id>`
   - `checks_passed = ["smoke_grower_signal_seen",
     "smoke_healer_signal_seen"]`
   - `checks_failed = []`
   - `disposition = "pass"`
   - `depends_on_event_ids` naming the grower event
7. Reducer/reconciler observes the structural fields. If all non-force checks
   are satisfied, it may append `waypoint.verified` for
   `waypoint_id = cerberus-smoke-grower-healer-loop`.
8. Aegis appends a forced budget status for the same smoke item:
   - `token_budget_remaining = 0` or `wall_time_remaining_seconds = 0`
   - `force_required = true`
9. Aegis appends `coordinator.force_convergence` with reason
   `token_budget_exhausted` or `wall_time_budget_exhausted`.
10. Grower and healer stop forward work and each append `worker.output_reduced`
    with:
    - `worker_role = grower|healer`
    - `status = complete|partial`
    - evidence event ids
    - remaining check ids
11. Aegis reads both output summaries and appends `coordinator.pass_summary`
    with:
    - `source_event_ids` naming worker outputs
    - `remaining_check_ids = []` when smoke evidence is complete
    - `next_action = block_with_summary` for the smoke, unless a best-effort
      respawn is explicitly being tested
12. Reducer/reconciler records the final observation:
    - accept when all smoke checks are present, or
    - block with pass summary when the force path intentionally stops the loop.

## Expected Events In Order

The exact event ids are deterministic but not known before append. The expected
relative order is:

1. `work_item.created` and relation events for the smoke item, if newly created
2. `work_item.transitioned` to `running`
3. `work_item.event_appended` / `aegis.standards_declared`
4. `work_item.event_appended` / `coordinator.budget_status`
5. `work_item.event_appended` / `worker.grower_signal`
6. `work_item.event_appended` / `worker.healer_signal`
7. `convergence.verdict_recorded` or
   `work_item.event_appended` / `waypoint.verified`
8. `work_item.event_appended` / `coordinator.budget_status` with
   `force_required = true`
9. `work_item.event_appended` / `coordinator.force_convergence`
10. `work_item.event_appended` / `worker.output_reduced` from grower
11. `work_item.event_appended` / `worker.output_reduced` from healer
12. `work_item.event_appended` / `coordinator.pass_summary`
13. final `convergence.verdict_recorded` and lifecycle transition, if the
    deterministic worker is active for this target

If the deterministic worker is not running, the smoke still passes as a protocol
dry run only when all feed-visible events through `coordinator.pass_summary`
appear with the required structural fields.

## Reducer And Projection Observations

The reducer must be able to read these without parsing prose:

- declared checks from `aegis.standards_declared.checks`
- grower contribution from `worker.grower_signal.produced_check_ids`
- healer verdict from `worker.healer_signal.checks_passed/checks_failed`
- force requirement from `coordinator.budget_status.force_required`
- force reason from `coordinator.force_convergence.reason`
- worker shutdown status from `worker.output_reduced.status`
- final compressed state from `coordinator.pass_summary.next_action` and
  `remaining_check_ids`

Projection observations:

- feed shows all coordination events in chronological order
- `convergence_verdicts`, when active, records reducer identity, version,
  attempt, inputs digest, disposition, and raw signals
- work_item state changes, when any occur, are separate events from verdicts

## Sibling Coordination Check

The grower and healer must prove they coordinated through meristem:

- Grower message says it read the latest healer signal and found none or found a
  specific event id.
- Healer `depends_on_event_ids` names the grower event id it evaluated.
- Any grower second move must name the healer event id it read before moving.

If these references are absent, the smoke fails even if both workers produced
plausible prose.

## Exit Conditions

Pass:

- grower and healer events exist with structural target ids
- healer signal contains reducer-readable passed/failed check arrays
- force path emits at least one `worker.output_reduced`
- Aegis emits `coordinator.pass_summary`
- no worker claims lifecycle authority from free-form prose

Block:

- scoped worker token cannot read/write the smoke item
- MCP is unreachable
- deterministic reducer is required for the run but not active
- force summary cannot be produced from worker outputs

Fail:

- token secret appears in an event, doc, config, or message
- grower/healer coordinate only through inherited chat
- worker transitions lifecycle directly based on model judgment
- child spawn occurs without `waypoint.verified` or at depth >= 5

## Failure Modes And Blocker Summary

Use this blocker summary shape when the smoke cannot complete:

```json
{
  "blocked_at_step": 6,
  "blocking_condition": "healer could not see grower event via feed",
  "evidence_event_ids": ["<standards-event>", "<grower-event>"],
  "missing_evidence": ["worker.healer_signal with depends_on_event_ids"],
  "recommended_next_step": "fix feed cursor or scoped token visibility before launch wrappers"
}
```

The blocker is appended as `coordinator.pass_summary` with
`next_action = "block_with_summary"` when force convergence has started, or as a
normal `worker.summary` event if the smoke never reached the force path.

## Acceptance Checks

- `smoke_steps_written`: satisfied by Step-By-Step Protocol.
- `concurrent_grower_healer_workers_selected`: satisfied by Actors.
- `expected_event_sequence_defined`: satisfied by Expected Events In Order.
- `sibling_signal_coordination_via_feed_defined`: satisfied by Sibling
  Coordination Check.
- `reducer_observations_defined`: satisfied by Reducer And Projection
  Observations.
- `exit_conditions_include_terminal_budget_and_force_paths`: satisfied by Exit
  Conditions.
- `failure_modes_and_blocker_summary_defined`: satisfied by Failure Modes And
  Blocker Summary.
