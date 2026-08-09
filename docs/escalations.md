# Human Escalations

Escalation is a durable convergence action, not a chat side channel. When a
reducer, worker, or grant issuance path cannot dispose a work item safely, it
records the need for human attention in the event log and creates a normal
human-visible `work_item`.

## Current Contract

`internal/escalations.Service.Request` is the first shared path:

1. Read the origin `work_item` projection.
2. Derive a stable `escalation_id` from `(work_item_id, reason, summary)`.
3. If that escalation already exists, return the existing human work item id.
4. Append `escalation.requested`.
5. Append `work_item.created` for a child item titled `Human attention: ...`.
6. Append `work_item.relation_added` from the origin to the human item.
7. Preserve the origin's `human_review_status`; escalation is not a new human
   decision and therefore cannot revoke `waved_through` or `approved`.
8. Append `work_item.transitioned` to move the origin to `blocked` unless it is
   already blocked.

All appends happen in one transaction. Projection writes are performed only by
the registered projectors fired by `internal/events`.

## Event Payload

`escalation.requested` uses:

```json
{
  "work_item_id": "origin-work-item-uuid",
  "human_work_item_id": "human-attention-work-item-uuid",
  "reason": "short deterministic reason",
  "summary": "operator-facing summary",
  "origin_state": "running",
  "origin_state_reason": "optional previous state reason"
}
```

The payload records the state seen by the first request. Replays with the same
`work_item_id`, `reason`, and `summary` return the original escalation instead
of recording a second event after the origin has moved to `blocked`.

## Human-Visible Item

The generated child item is ordinary work item state:

- `state = captured`
- `human_review_status = blocked`
- `suggested_convergence_checks = ["human_response_recorded"]`

This makes escalations visible through existing work-item and feed paths without
adding an approval table or notification transport in this slice.

The child is the new question awaiting a human decision. The origin retains
the owner's prior decision even while lifecycle progress is blocked.

## What This Is Not

- It is not an approval decision. External writes still require the approval
  system and must default deny until that system exists.
- It does not mint tokens. Subactor grant issuance should call this service
  when the grant reducer returns `escalate`.
- It does not unblock the origin item. A later slice should define the
  resolution event and the reducer that moves the origin out of `blocked` after
  the human response is recorded.

## Intended Callers

- Convergence reducers that return `escalate`.
- Subactor grant issuance when a template requires human review.
- Bounded-patience worker paths that exhaust their budget and choose a human
  handoff rather than terminal failure.
