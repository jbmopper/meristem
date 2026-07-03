# Deterministic error reporting

Meristem has two cooperating subsystems:

- The **deterministic subsystem** owns durable truth: events, projections, migrations, auth, idempotency, safety policy, queue claims, and reconciliation rules.
- The **probabilistic subsystem** proposes judgments: classifications, plans, summaries, and model-shaped interpretations.

Deterministic error reporting is the operator-facing record of things that go wrong in the deterministic subsystem. It is deliberately boring: an error is an event, the visible error list is a projection, and masking changes what active views show without erasing the audit trail.

## What problem this solves

Before this slice, deterministic failures could show up as logs, test failures, or returned errors, but there was no durable system object that said: "the deterministic layer observed this error, here is where it came from, and here is whether the operator still wants to see it in active views."

That matters because deterministic failures are the ones meristem must be able to reason about later. A model can summarize them, but it should not be the source of truth for whether they happened. The event log is.

## Mental model

A deterministic error report has three layers:

1. **Audit facts** live in `events`.
2. **Current display state** lives in `deterministic_errors`.
3. **Operator attention policy** is the `masked` flag on the projection.

The important distinction is that masking is not deletion. A masked report is still in the event log, still replayable, and still visible when a caller asks to include masked errors. Masking only says: "do not treat this as an active error right now."

Masking is also not privacy redaction. Payloads must be safe enough to store in
`events` before they are reported. Read-time privacy filtering controls which
fields an accessor can see; it does not remove unsafe facts from the durable
audit log after the fact.

## Event flow

The deterministic error system uses three event kinds:

| Event | Meaning |
|-------|---------|
| `deterministic_error.reported` | The deterministic subsystem observed a reportable error. |
| `deterministic_error.masked` | The operator or system marked the report as not active for normal views. |
| `deterministic_error.unmasked` | The report was restored to active views. |

All three use `subject_kind = "deterministic_error"` and the deterministic error id as `subject_id`.

The projector in `internal/errorreporting` consumes those events and maintains `deterministic_errors`:

- `reported` inserts the row.
- `masked` sets `masked = true`, records the reason, actor, and time.
- `unmasked` sets `masked = false` and clears the mask metadata.

Replaying the event log should rebuild the same table state. That is why `deterministic_errors` is included in the `meristem rebuild` projection table list.

## What an error contains

Each report stores:

| Field | Purpose |
|-------|---------|
| `component` | Where the deterministic error came from, such as `projections`, `worker`, `storage`, or `api`. |
| `code` | A stable machine-readable label, such as `projection_failed` or `migration_drift`. |
| `message` | A short human-readable summary. |
| `severity` | One of `info`, `warning`, `error`, or `critical`. |
| `details` | A JSON object with safe diagnostic metadata. |
| `reported_by` / `reported_at` | Attribution and time from the event metadata. |
| `masked` metadata | Whether active views should hide it, plus reason, actor, and time when masked. |

Details must be a JSON object. Keep details small and durable. Include identifiers, event kinds, table names, counts, or stable diagnostic facts. Do not include bearer tokens, raw message content, credentials, connection secrets, or large blobs.

Details may carry field-level visibility metadata with a top-level
`_visibility` object:

```json
{
  "event_kind": "work_item.created",
  "table": "work_items",
  "raw_payload": "...",
  "_visibility": {
    "event_kind": "public",
    "table": "internal",
    "raw_payload": "restricted"
  }
}
```

The `_visibility` object is policy metadata. It is part of the canonical
stored report, but filtered read views do not return it as diagnostic data.
Unlabelled fields default to `internal`. Unknown explicit labels fail closed
and require restricted visibility.

Current labels:

| Label | Visible with |
|-------|--------------|
| `public` | `logs.read` |
| `internal` or unlabelled | `logs.read_details` |
| `restricted`, `private`, `sensitive`, `encrypted` | `logs.read_restricted` |

## Masking semantics

Masking answers an operator-attention question, not a truth question.

Use masking when:

- The report is known and intentionally ignored for now.
- The report is noisy but still useful for audit history.
- The report was acknowledged and should not distract active views.

Do not use masking when:

- The error is actually resolved and should have its own resolution event or downstream work item.
- The report contains sensitive data. Sensitive data should not be written into the event payload in the first place.
- The operator needs a separate task to repair something. Create or link a `work_item` for that repair.

Unmasking simply makes the existing report active again. It does not create a fresh report.

## How code should use it

Code in the deterministic layer should call `internal/errorreporting.Service` rather than writing rows directly. The service appends events through `internal/events.Writer`; the projector derives `deterministic_errors` in the same transaction.

The package currently provides:

- `Report(ctx, ReportInput)`
- `Mask(ctx, id, MaskInput)`
- `Unmask(ctx, id, MaskInput)`
- `Get(ctx, id)`
- `List(ctx, ListOptions)`
- `GetForAccessor(ctx, id, token)`
- `ListForAccessor(ctx, ListOptions, token)`

`ListOptions.IncludeMasked` defaults the system toward active errors. Callers that are doing audit, replay, or history views should include masked reports explicitly.

Transport read surfaces call the accessor-aware methods above. They do not read
`deterministic_errors` directly or reimplement filtering.

Current read scopes:

| Scope | Meaning |
|-------|---------|
| `logs.read` | List/get active deterministic log records and public detail fields. |
| `logs.read_details` | Also see internal/unlabelled detail fields. Implies `logs.read`. |
| `logs.read_restricted` | Also see restricted/private/sensitive/encrypted detail fields. Implies `logs.read_details`. |
| `logs.read_masked` | May request masked records with `include_masked=true`. |
| `logs.read_all` | Convenience scope for all read visibility. Root tokens get equivalent access automatically. |

These scopes compose with scoped MCP worker scopes. For example, a worker with
`work_items.tree:<uuid>` and `feed.read_assigned` can observe its assigned
work-item tree without receiving deterministic error details unless it also has
the appropriate `logs.*` scope.

Current REST surface:

- `GET /v1/deterministic-errors?limit=N&include_masked=false`
- `GET /v1/deterministic-errors/{id}`

Current MCP surface:

- `deterministic_errors.list`
- `deterministic_errors.get`

Mask/unmask/report transports are intentionally still separate follow-up work;
the first exposed surface is read-only so the privacy/access reducer is in the
path whenever logs are examined.

## Relationship to work items

A deterministic error is not automatically a `work_item`. It is a report.

Some reports should lead to work items, especially if repair is needed. That linkage should be explicit: create a work item whose body points to the deterministic error id, or add a future relation when the schema grows one. Keeping the error report and repair task separate avoids pretending that acknowledging noise is the same thing as converging work to `done`.

## Operator workflow

The visible surfaces are the feed, the deterministic-errors REST/MCP read
surfaces, and trusted internal code that reads `deterministic_errors` directly:

1. A deterministic component reports an error.
2. The event appears in `/v1/feed`.
3. The row appears in `deterministic_errors` as active.
4. If the report is noise or intentionally deferred, mask it with a reason.
5. If the report needs repair, create a work item and leave the report active until the repair path is clear.

When write transports are added, they should support the same basic actions:
report, mask, and unmask. They must call the service rather than writing
projection rows directly.

## Implementation landmarks

- Domain constants and type: `internal/domain/models.go`
- Service and projector: `internal/errorreporting/`
- Projection table: `migrations/0008_deterministic_errors.*.sql`
- Projector registration: `internal/app/projectors.go`
- Rebuild coverage: `cmd/meristem/rebuild.go`
- Feed visibility: `internal/feed/feed.go`
- REST read surface: `internal/api/deterministic_errors.go`
- MCP read tools: `internal/mcp/tools.go`
- System-level contract: `docs/spec.md`

## Invariants

- Never write `deterministic_errors` directly outside a projector.
- Never record secrets or raw private content in error details.
- Masking must not delete or rewrite the original report event.
- Privacy filtering happens at read time from token scopes; do not create
  per-accessor truth projections.
- Rebuild from `events` must reproduce the projection.
- Transports added later must call the service rather than duplicating business logic.
