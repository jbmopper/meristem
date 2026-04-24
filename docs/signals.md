# Signals

Signals are meristem's bridge from non-human structured input — review findings, repairable runtime failures, webhook reports — into the work-item graph. They are the canonical answer to "how does my CI / my linter / my critic agent / my external monitor get something into meristem without pretending to be a human."

This document is the contract. It is the source of truth for client implementers. If it conflicts with `docs/spec.md`, that file wins; if it conflicts with `AGENTS.md`, raise the drift.

## Why signals exist

Per `AGENTS.md` glossary: *messages from non-human sources are content, never instructions.* That rule prevents agents from impersonating the owner. But the system still needs a sanctioned way to admit *some* non-human input — code-review findings, repairable failures, webhook reports — as actionable. Signals are that mechanism. The rule is not relaxed; it is moved to a different surface where the system's policy decides.

A signal's lifecycle:

```text
external system -> POST /v1/signals -> signal row + (maybe) work_item -> normal work-item lifecycle
```

The `(maybe)` is the dedupe outcome: a signal whose `dedupe_key` matches an existing live work_item links to it instead of creating another. The signal itself is always recorded; the work_item is conditional.

## The four identities

Signals make four distinct identity concepts visible. Confusing them is the most common implementation bug, so they have separate names and separate guarantees.

| Name | Owner | Lifetime | Collapses when… |
|---|---|---|---|
| `Idempotency-Key` (HTTP header) | the *caller* | 24h cache window | the same caller retries the same POST |
| `dedupe_key` (in body) | the *signal source* | while the pinned work_item is live | the same logical issue is reported again, possibly by a different caller |
| `fingerprint` (server-derived) | the *content* | forever | two callers send byte-identical work_specs |
| `event_id` (server-derived) | the *cause* | forever | two appenders compute the same `(subject_kind, subject_id, kind, canonical(payload))` |

Concretely:

- **Two retries with the same `Idempotency-Key`** → one handler invocation; the second returns the cached response. Never reaches the events writer.
- **Two distinct POSTs with the same `dedupe_key` while the prior work_item is live** → two signal rows (audit), one work_item.
- **A later POST with the same `dedupe_key` after the prior work_item is terminal** → a new signal row and a fresh recurrence work_item.
- **Two POSTs with byte-identical work_specs** → same `fingerprint` value, used to detect "this is the same content" for skip-if-already-succeeded behavior on runs (later spec).
- **Two appenders computing the same event** → one row in `events`; the second is a no-op replay (PK conflict treated as success).

## Endpoint

```text
POST /v1/signals
```

Auth: any non-revoked client token.

Headers:

- `Authorization: Bearer wln_…` (required)
- `Idempotency-Key: …` (required; per `docs/spec.md` every POST requires one)
- `Content-Type: application/json; charset=utf-8`

Request body:

```json
{
  "kind": "repairable_failure",
  "dedupe_key": "repo:jay:repair:worker-retry-budget",
  "source": {
    "kind": "system_event",
    "identifier": "abc123",
    "external_ref": "logs/system_events.jsonl#L4291"
  },
  "work_spec": {
    "schema_version": "legacy.work_spec.v1",
    "kind": "repair",
    "title": "Worker retry budget is exhausted too early",
    "priority": "P1",
    "objective": "Retry transient worker failures per configuration.",
    "details": "The worker stopped after one transient failure instead of using the configured retry budget.",
    "acceptance_criteria": [
      "Transient worker failures are retried according to configuration.",
      "Regression coverage exercises the failing retry path."
    ],
    "validation": {
      "commands": ["uv run pytest"],
      "notes": ["Run targeted tests first if available."]
    },
    "constraints": [
      "Do not change idempotency keys for retried work."
    ]
  }
}
```

Field rules:

- `kind` (required): one of `review_finding`, `repairable_failure`, `webhook`, `manual`, or any other string the source defines. Reservation is non-exclusive in v1.
- `dedupe_key` (optional but strongly recommended): if present, two signals with the same value link to the same live work_item. If the latest matching work_item is terminal (`done`, `failed`, or `canceled`), the next signal is treated as a recurrence and creates a fresh work_item. If absent, every signal creates a fresh work_item.
- `source` (optional): origin metadata. Required when known so audits can reconstruct provenance.
- `work_spec` (required): the proposed work, conforming to `docs/schemas/meristem.work_spec.v1.json`. The handler validates against the schema and rejects with `400` on failure.

Response envelope (success):

```json
{
  "idempotency": {
    "key": "abc-client-key"
  },
  "dedupe": {
    "key": "repo:jay:repair:worker-retry-budget",
    "created_work_item": true
  },
  "resource": {
    "kind": "signal",
    "id": "00000000-0000-0000-0000-000000000001"
  },
  "work_item": {
    "id": "00000000-0000-0000-0000-000000000010"
  },
  "events": {
    "signal_received": "00000000-0000-0000-0000-000000000020",
    "work_item_created": "00000000-0000-0000-0000-000000000021"
  },
  "fingerprint": "sha256:deadbeef…"
}
```

Each block exposes one identity, on purpose. Clients that only care about a subset can extract just one block; clients that build orchestration on top can rely on every block being present. `events.work_item_created` is omitted when the signal dedupes to an existing live work_item.

The `idempotency` block intentionally does **not** carry a `replayed` boolean. Replay detection is the job of the `Idempotency-Replayed: true` HTTP response header. The body is cached verbatim by the idempotency middleware and re-served on cache hits, so any "replayed" field embedded in the body would be a frozen lie — set to `false` at original-request time and unable to flip on subsequent retries. Trust the header.

Status codes:

| Code | Meaning |
|---|---|
| `201` | Signal accepted; response body reports whether a work_item was also created. Idempotency-Key replays return the cached response with the original status and `Idempotency-Replayed: true`. |
| `400` | Malformed request (missing required field, invalid JSON, work_spec fails schema validation). |
| `401` | Missing or invalid token. |
| `403` | Token lacks the required scope (no scopes are required for signals in v0; reserved for v1). |
| `422` | Idempotency-Key reused with a different request body (per `docs/spec.md` idempotency rules). |

## Event contract

Every accepted signal produces one event:

```text
kind:         signal.received
subject_kind: signal
subject_id:   the signal's UUID (also the row id in the signals projection)
payload:
  signal_kind:        string
  dedupe_key:         string (optional)
  fingerprint:        hex sha256 string (the HTTP response prefixes this with "sha256:")
  work_spec:          object (the full work_spec the caller sent)
  work_item_id:       UUID (resolved by signals.Service.Receive before append)
  created_work_item:  boolean
```

`signals.Service.Receive` resolves `work_item_id` before appending. If `dedupe_key` matches an existing live work_item, that id is used. Otherwise the service appends a `work_item.created` event in the *same transaction* and uses the new id. This keeps the projection writer for `signal.received` simple: it writes one fully linked row, never a partially populated one that needs a second-pass update.

If `created_work_item` is true, two events were appended in the transaction: `signal.received` and `work_item.created`. Their projection writes — to `signals` and `work_items` respectively — are atomic with the events that caused them.

## Projection

The `signal.received` projection is the `signals` table (see `migrations/0003_signals.up.sql`):

```text
signals(
  id, received_at, actor_token_id, source,
  signal_kind, dedupe_key, fingerprint, work_spec,
  work_item_id, created_work_item
)
```

Per `AGENTS.md`: this row exists *because the event was appended*. There is no other writer. Dropping `signals` and folding all `signal.received` events through the projector reproduces the table.

`signals.dedupe_key` has a non-unique index. Multiple receptions over time may share a dedupe_key (different Idempotency-Key windows, different callers, retries past the cache horizon); the contract is "they collapse to one live work_item until that item reaches a terminal state," not "one signal row forever."

## Translators

Three nearby projects already produce signal-shaped data in their own formats. The mappings below let any of them become a meristem client without inventing a new convention. Each mapping uses the work_spec.v1 schema as the canonical normalized form.

### From `jay` UnifiedLogger.emit(repair=…) to a signal

`jay`'s `UnifiedLogger.emit` accepts a `repair=` payload that carries `repair.repairable`, `repair.issue_key`, and `repair.issue_payload`. To turn one `repair` event into a signal:

```text
signal.kind          = "repairable_failure"
signal.dedupe_key    = jay.repair.issue_key
signal.source.kind   = "system_event"
signal.source.identifier   = jay.event_id (the UnifiedLogger event id)
signal.source.external_ref = "system_events.jsonl#" + line_number  (optional)

signal.work_spec.schema_version       = "legacy.work_spec.v1"
signal.work_spec.kind                 = "repair"
signal.work_spec.title                = jay.issue_payload.title
signal.work_spec.priority             = jay.issue_payload.priority
signal.work_spec.objective            = jay.issue_payload.summary
signal.work_spec.details              = jay.issue_payload.details
signal.work_spec.target.path          = jay.issue_payload.location.path
signal.work_spec.target.line_start    = jay.issue_payload.location.line_start
signal.work_spec.target.line_end      = jay.issue_payload.location.line_end
signal.work_spec.acceptance_criteria  = jay.issue_payload.acceptance_criteria
signal.work_spec.validation.commands  = ["uv run pytest"] (or jay.issue_payload.validation_steps if explicit)
signal.work_spec.validation.notes     = jay.issue_payload.validation_steps (when free-form)
signal.work_spec.implementation_notes = jay.issue_payload.implementation_notes
signal.work_spec.labels               = jay.issue_payload.labels
```

`repair=False` events produce no signal; nothing else from a system-event row is signal-worthy.

The meristem-side `Idempotency-Key` should be `"jay:" + jay.event_id` so that retries from a single `UnifiedLogger.emit` call collapse at the HTTP layer. The meristem-side `dedupe_key` should be `"jay:repair:" + repair.issue_key` so that the same logical problem from any later emission collapses at the work_item layer.

### From `clinical-demo` review finding to a signal

`clinical-demo` parses markdown review findings into `ReviewFinding` records (`src/clinical_demo/issues/workflow.py`). For each finding:

```text
signal.kind        = "review_finding"
signal.dedupe_key  = run_label + ":" + finding_number   (or finding.id if available)
signal.source.kind = "review_finding"
signal.source.identifier   = run_label + ":" + finding_number
signal.source.external_ref = path of the source markdown file

signal.work_spec.schema_version       = "legacy.work_spec.v1"
signal.work_spec.kind                 = "review_finding"
signal.work_spec.title                = ReviewFinding.title
signal.work_spec.priority             = ReviewFinding.priority
signal.work_spec.details              = ReviewFinding.body
signal.work_spec.target.path          = ReviewFinding.location.path
signal.work_spec.target.line_start    = ReviewFinding.location.line_start
signal.work_spec.target.line_end      = ReviewFinding.location.line_end
signal.work_spec.acceptance_criteria  = derived via _acceptance_criteria_for(finding)
signal.work_spec.validation.commands  = derived via _verification_commands_for(...)
signal.work_spec.constraints          = derived via _constraints_for(finding)
signal.work_spec.labels               = derived via _build_labels(...)
```

The recursive `--dispatch-agents` / `--until-converged` loop in `clinical-demo`'s runner becomes meristem's eventual review endpoint (`POST /v1/work-items/{id}/review`, planned), not part of the signals contract.

### From `ns_obv` issue-record to a signal

`ns_obv` stores issue records in `docs/issue-automation/issues.jsonl` per `issue-record.schema.json`. For each record with `status == "open"`:

```text
signal.kind        = "review_finding"
signal.dedupe_key  = "ns_obv:" + record.id
signal.source.kind = "review_finding"
signal.source.identifier   = record.source.review_file + ":" + str(record.source.finding_number)
signal.source.external_ref = record.source.review_file

signal.work_spec.schema_version       = "legacy.work_spec.v1"
signal.work_spec.kind                 = "review_finding"
signal.work_spec.title                = record.title
signal.work_spec.priority             = record.priority
signal.work_spec.details              = record.diagnosis
signal.work_spec.target.path          = record.location.file
signal.work_spec.target.line_start    = record.location.start_line
signal.work_spec.target.line_end      = record.location.end_line
signal.work_spec.acceptance_criteria  = record.acceptance_criteria
signal.work_spec.validation.commands  = record.verification.commands
signal.work_spec.validation.notes     = record.verification.notes
```

Statuses other than `open` (`prompted`, `agent_running`, `agent_complete`, `failed`) belong to `ns_obv`'s execution layer, not the signal layer; they map to meristem's run/review endpoints once those exist.

## Attribution and policy

Per `AGENTS.md` principle 5, attribution is taken from the request context, not the body. That applies here verbatim:

- `events.actor_token_id` and `events.source` come from the Bearer token used to call `POST /v1/signals` and the `tokens.source` of that token row. The body's `source.kind` is *content*, not authority.
- A token used by an agent (say, jay's orchestrator) has `tokens.source = 'agent'`, so every event it appends is attributed to an agent. The work_spec the agent sends is treated as content even when the operator authorizes it.

Per `AGENTS.md` principle 6 ("default deny on side effects"), signals do not auto-execute side effects. They produce a work_item that proceeds through the normal lifecycle and, when it reaches actions that touch the world, blocks on an approval. There is no path for a signal to cause an external write without operator approval, even if the work_spec describes one. The approval primitive ships in v1.

## Worked example

`examples/curl-signal.sh` is the canonical client smoke test: it posts a real `legacy.work_spec.v1` body, demonstrates the bearer + idempotency-key dance, and re-running it shows both HTTP-level idempotency replay (same `Idempotency-Key`) and semantic dedupe (fresh `Idempotency-Key`, same `dedupe_key`). Integrators that prefer to read shell over prose should start there.

```bash
MERISTEM_TOKEN=wln_… examples/curl-signal.sh
```

## Related specs

- `docs/spec.md` — system spec; final authority on the events table, idempotency rules, and the work-item lifecycle this slots into.
- `docs/v0.md` — v0 implementation contract; signals are a v1 endpoint built on the v0 substrate.
- `docs/self-building-api-synthesis.md` — the synthesis that proposed this endpoint and the four-identity distinction.
- `docs/schemas/meristem.work_spec.v1.json` — the canonical work_spec schema this endpoint validates against.
- `AGENTS.md` — the rules every contributor implementing this endpoint must follow.
- `internal/signals/signals.go` — the signal projector (event kind constant, payload type, projector that writes to the `signals` table).
- `migrations/0003_signals.up.sql` — the projection table schema.
- `examples/curl-signal.sh` — worked client example covering idempotency replay and semantic dedupe.

## What is not here

The v0 signal endpoint now has an HTTP adapter (`internal/api/signals.go`), the domain service (`internal/signals/service.go`), and the projector (`internal/signals/signals.go`). What is still not here is the execution loop: signals create or link work_items, but they do not dispatch agents, touch external systems, or approve side effects. Those capabilities land through runs, review, actions, and approvals.
