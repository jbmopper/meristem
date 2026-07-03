# Cerberus Budget And Force-Convergence Protocol

This document executes work_item `8f983e32`: define budget tracking and
force-convergence for cheap-model Aegis goal/loop mode.

If this document conflicts with `docs/spec.md`, the spec wins.

## Budget Dimensions

Every Cerberus subtree has three budget dimensions:

```json
{
  "token_budget_total": 100000,
  "token_budget_remaining": 100000,
  "wall_time_budget_seconds": 900,
  "wall_time_remaining_seconds": 900,
  "current_depth": 0,
  "call_depth_max": 5
}
```

Defaults for the first pilot:

- `call_depth_max = 5`
- wall time is measured from the coordinator pass start
- token budget is operator-supplied or estimated by the launcher when provider
  accounting is unavailable

Aegis records observations with `coordinator.budget_status`. Budget accounting
can be approximate at the model-provider boundary, but the budget status event
must state what was measured, what was estimated, and what remains.

## Force Triggers

Aegis appends `coordinator.force_convergence` when any of these is true:

- `token_budget_remaining <= 0`
- `wall_time_remaining_seconds <= 0`
- `current_depth >= call_depth_max`
- operator explicitly requests force convergence

The event includes a structural `reason`:

- `token_budget_exhausted`
- `wall_time_budget_exhausted`
- `call_depth_exhausted`
- `operator_requested`

Force convergence is a context compression and bounded retry mechanism. It does
not let a model decide lifecycle directly.

## Sequence

1. Aegis reads the subtree, recent feed, and current budget status.
2. Aegis appends `coordinator.budget_status`.
3. If a trigger is active, Aegis appends `coordinator.force_convergence`.
4. Running workers observe the force event through feed reads.
5. Each worker stops forward work and appends `worker.output_reduced`.
6. Aegis waits for expected worker summaries or bounded patience expiry.
7. Aegis appends `coordinator.pass_summary`.
8. If `current_depth < call_depth_max`, Aegis may spawn one fresh grower/healer
   pair for a compressed best-effort pass.
9. If the best-effort pass converges, workers emit signals for the reducer.
10. If it does not converge, Aegis records a blocked summary and leaves
    lifecycle to the deterministic reducer or operator.

## Worker Shutdown Behavior

On `coordinator.force_convergence`, workers:

- stop creating new forward events
- do not spawn children
- summarize only facts already known from their assigned state and feed window
- append one `worker.output_reduced` event
- include evidence event ids and remaining checks structurally
- exit or wait idle after the summary

`worker.output_reduced.message` should be short enough to fit into the next
respawn context without reintroducing the full historical feed.

## Pass Summary Boundary

`coordinator.pass_summary` is the only context carried across a force respawn by
default. It may include:

- current state in prose
- evidence event ids
- checks satisfied
- checks remaining
- known blockers
- exact next task for the best-effort pass

It must not include:

- token secrets
- unrelated feed history
- raw private message bodies outside the subtree
- full transcripts when evidence ids and a compact summary are enough

The pass summary is a lossy context compression for model workers. It is not a
loss of durable truth because the original events remain in the event log.

## Depth And Waypoint Gate

Child Cerberus subactors may be spawned only when both are true:

```text
waypoint.verified exists for the target waypoint
current_depth < call_depth_max
```

The default `call_depth_max` is `5`. At depth `5`, Aegis must force convergence
instead of spawning another child. If force convergence still cannot produce
adequate signals, the item is blocked with the latest pass summary.

The grower or healer may request a child, but the spawn gate depends on the
verified waypoint and depth, not on the requester's free-form confidence.

## Cheap-Model Loop Contract

Aegis in cheap-model goal/loop mode repeats:

1. read subtree state and feed
2. update budget status
3. wake or spawn eligible grower/healer workers
4. observe worker signals
5. request deterministic reduction when required signals exist or patience
   expires
6. force convergence on budget exhaustion
7. stop when all children are terminal, force has blocked, or operator
   intervenes

The loop may "jabber" through meristem events at steady state, but each event
must be attributed, scoped, idempotent, and bounded by budget.

## Structural Fields

Budget/force events use the field-minimal contracts in
`docs/cerberus-reducer-event-contracts.md`.

Reducer-facing fields:

- subtree root work_item id
- token budget remaining
- wall time remaining
- current depth
- call depth max
- force required
- force reason
- force pass number
- remaining check ids
- next action

Narrative context, accounting assumptions, and model instructions stay in
`system` and `message`.

## Exit Conditions

Terminal loop outcomes:

- all required checks pass and deterministic reducer accepts
- force pass completes and deterministic reducer blocks with summary
- operator explicitly cancels or redirects
- panic revoke removes the coordinator token and no replacement is active

Non-terminal continuation outcomes:

- budget remains and new worker signal is expected
- force summary requests one best-effort respawn with depth remaining
- reducer requests retry under bounded patience

No state waits forever; every non-terminal path has a next scan, timeout, force,
or block.

## Acceptance Checks

- `token_wall_time_call_depth_budgets_defined`: satisfied by Budget Dimensions.
- `force_convergence_trigger_and_sequence_defined`: satisfied by Force Triggers
  and Sequence.
- `worker_output_reduced_shutdown_behavior_defined`: satisfied by Worker
  Shutdown Behavior.
- `pass_summary_compression_boundary_defined`: satisfied by Pass Summary
  Boundary.
- `depth_guard_and_waypoint_spawn_gate_defined`: satisfied by Depth And
  Waypoint Gate.
