# External Project Intake

External work becomes a normal meristem `work_item`. That is the whole design:

```text
external source -> POST /v1/signals -> signal row + work_item -> normal lifecycle
```

One POST, then the ordinary machinery — triage, scribe-authored convergence
checks, budgets, approvals — treats it like any other item. There is no
separate external pipeline. (Owner decision 2026-07-07: this document was
trimmed from a richer draft; intake requirements beyond this file were judged
ceremony. If this file conflicts with [`docs/spec.md`](spec.md), the spec wins.)

## What "external work" means

Three different things get called external. They are orthogonal, and only the
first one is intake's concern:

1. **External source** — the *requester* is outside this conversation: a
   webhook, CI, a review tool, another agent or person. The subject may well
   be meristem itself (a CI failure in this repo is external-source,
   internal-subject). Intake exists for this axis: provenance must be
   recorded, and a signal must never carry authority.
2. **External subject** — the *thing to be changed* lives outside this
   checkout: another repository, a running service. This is just target
   metadata on an otherwise normal work item (`target.repo`, `target.path`).
3. **External executor** — the *agent that will do the work* is outside the
   trust boundary (a provider-facing worker). That is launch-time machinery —
   provider-context export, scope packet, approval — and has nothing to do
   with how the work item was born.

Any combination occurs. Conflating the axes is what makes intake look like it
needs a heavyweight contract; separated, each is already served by existing
machinery.

## Intake contract (the load-bearing minimum)

A signal needs exactly:

- `kind`: signal class (`review_finding`, `repairable_failure`, `webhook`,
  `manual`)
- `dedupe_key`: semantic identity of the external issue — external systems
  retry and multiple reporters see the same problem; this is the same
  duplicate-suppression defense the event log uses internally, applied at the
  boundary
- `source.kind` / `source.identifier` / `source.external_ref`: enough for an
  operator to answer *what reported this, which record was it, where is the
  original evidence*
- a `work_spec` with `title` and enough body (`objective`/`details`) to triage,
  plus `target.repo` (and `target.path`) when the subject is external

That is the whole required surface. Everything else — priority, acceptance
criteria, validation commands, constraints — is **optional hints**. Do not
require external clients to pre-author convergence checks: the scribe pass
(R1, self-defining convergence) exists precisely to write checks for items
that arrive without them. An item as bare as any internal capture is an
acceptable intake.

## Dedupe conventions

The intake client owns the semantic key: stable across retries, portable
across hosts, narrow enough not to collapse unrelated work, broad enough not
to duplicate live items. Recommended forms:

- `repo:<project>:issue:<tracker-id>`
- `repo:<project>:review:<review-id>:finding:<n>`
- `repo:<project>:repair:<failure-class>`
- `service:<project>:incident:<external-id>`

`target.repo` is the stable project identity (`github.com/acme/payments`,
`srv://prod/wayline`) — never a local absolute path; paths belong in
`target.path` or `source.external_ref`.

## Authority and admission

Signals create work; they do **not** grant authority. The rules that gate
external work are enforced structurally, not per-item:

- **per-source scoped tokens**: each external source (webhook, CI, helper)
  gets its own token, so attribution and scope are structural facts of the
  event log, not checklist answers
- **approval before side effects**: unchanged, the ordinary lifecycle gate
- **budgets and patience**: unchanged, from the active policy profile

The admission checklist (work item `e9f37244`) is a **one-time audit** that
each AGENTS.md rule has a structural enforcement point, plus the token-minting
policy above — not a form filled per item.

If the work will later be handed to an external *executor*, the launch-time
scope packet ([`docs/mcp-worker-bootstrap.md`](mcp-worker-bootstrap.md),
provider-context export) applies then. Intake neither requires nor replaces
it.

## Example

```json
{
  "kind": "review_finding",
  "dedupe_key": "repo:github.com/acme/payments:review:pr-481:finding:7",
  "source": {
    "kind": "review_finding",
    "identifier": "pr-481:finding-7",
    "external_ref": "https://github.com/acme/payments/pull/481#discussion_r123456"
  },
  "work_spec": {
    "schema_version": "meristem.work_spec.v1",
    "kind": "review_finding",
    "dedupe_key": "repo:github.com/acme/payments:review:pr-481:finding:7",
    "title": "Null-check missing before refund reversal",
    "objective": "Prevent nil dereference on refund reversal path.",
    "target": { "repo": "github.com/acme/payments", "path": "internal/refunds/service.go" }
  }
}
```

```bash
curl -fsS \
  -H "Authorization: Bearer $MERISTEM_TOKEN" \
  -H "Idempotency-Key: ext-intake-pr481-finding7" \
  -H "Content-Type: application/json; charset=utf-8" \
  -X POST http://127.0.0.1:8080/v1/signals \
  -d @external-project-signal.json
```

Outcome: one `signal.received` event; a new `work_item` if the `dedupe_key` is
not already pinned to a live item, otherwise a link to the existing item.

## Deliberately not built

- intake helper CLI / translator — not until a second real client exists
- richer `work_spec` target metadata — additive later if a real client needs it
- provider-context bridge and gateway-backed remote intake — downstream
  execution layers (`a03f644e`), not intake prerequisites

## Related documents

- [`docs/signals.md`](signals.md)
- [`docs/schemas/meristem.work_spec.v1.json`](schemas/meristem.work_spec.v1.json)
- [`docs/mcp-worker-bootstrap.md`](mcp-worker-bootstrap.md)
