# Self-Building API Synthesis

Validated locally on 2026-04-24 in `America/Denver`.

This document records the synthesis of `meristem`'s API direction with the auto-repair, issue-agent, and repo-repair patterns found in nearby projects. It is intended for another assistant to reproduce the same API recommendation without needing the original chat context.

This document is advisory. If it conflicts with `docs/spec.md`, `docs/spec.md` is canonical. If it conflicts with `AGENTS.md`, prefer `docs/spec.md` and then update the projection in `AGENTS.md`.

## Source Snapshot

The synthesis was checked against these local files:

- `/Users/juliusmopper/Dev/meristem/docs/spec.md`
- `/Users/juliusmopper/Dev/meristem/AGENTS.md`
- `/Users/juliusmopper/Dev/meristem/docs/thoughts.md`
- `/Users/juliusmopper/Dev/clinical-demo/issues/README.md`
- `/Users/juliusmopper/Dev/clinical-demo/src/clinical_demo/issues/workflow.py`
- `/Users/juliusmopper/Dev/jay/README.md`
- `/Users/juliusmopper/Dev/jay/docs/auto_repair_api.md`
- `/Users/juliusmopper/Dev/jay/issue_workflow/repair.py`
- `/Users/juliusmopper/Dev/ns_obv/docs/issue-automation/README.md`
- `/Users/juliusmopper/Dev/ns_obv/scripts/orchestrate-issue-agents.mjs`
- `/Users/juliusmopper/Dev/stanford-cs336/assignment1-basics/plugins/repo-portfolio-repair-agent/skills/repo-portfolio-repair/SKILL.md`
- `/Users/juliusmopper/Dev/stanford-cs336/assignment1-basics/plugins/repo-portfolio-repair-agent/scripts/repair_plan.py`

Freshness caveat: `/Users/juliusmopper/Dev/meristem` was not a git checkout when checked, so this file is current against local disk only. It has not been compared with a remote branch.

## Current Drift To Preserve

`docs/spec.md` and `AGENTS.md` still state the active implementation rule as "idempotency everywhere" and "Postgres is the system." `docs/thoughts.md` argues for a conceptual reframe: convergence should be the model, the event log should be truth, and idempotency should become a consequence rather than the headline principle.

For now, keep both truths in view:

- Implementation must follow the current spec: every write path is idempotent, Postgres is the only durable backing store, and events are append-only.
- API design should lean toward the newer convergence framing: the owner declares intent, the system records facts, projections are deterministic, and workers reconcile toward terminal state.

## Thesis

`meristem` should not add a separate auto-healing subsystem. The auto-healing pattern should be a first-class use of the normal API.

The shared grammar is:

```text
signal -> work_item -> spec -> plan/run -> artifacts/events -> review -> done | failed | child repair
```

The existing systems are examples of that grammar:

- `clinical-demo` turns review findings into issue specs and recursively dispatches agents until unresolved findings stabilize.
- `jay` turns review findings and repairable runtime events into issue specs, prompts, dispatches, and idempotent run state.
- `ns_obv` separates issue records from execution events, generates specs/prompts/manifests, and optionally launches agents.
- `assignment1-basics` uses a deterministic repair-plan generator where zero planned steps means the repo is already repaired.

`meristem` should absorb those functions as generalized coordination primitives, not copy their repo-local runners wholesale.

## Four Identities

The API should separate these identities clearly:

- `Idempotency-Key`: transport retry identity. The same request from the same token, scope, key, and request body returns the same response.
- `dedupe_key`: semantic identity. The same logical instruction, finding, repair event, or desired state maps to the same work item instead of creating duplicates.
- `fingerprint`: content identity for specs, plans, prompts, run inputs, and validation inputs. If the same fingerprint already succeeded, the run can skip safely.
- `event_id`: immutable fact identity, derived from cause plus canonical content. Replays produce no new events.

This distinction is the bridge between the existing projects and `meristem`'s substrate. It replaces repo-local dispatch ledgers and JSONL histories with first-class tables and append-only events.

## Canonical Work Spec

`work_item` remains the center. A normalized work specification should be attachable as an artifact and should be stable enough to render prompts, dispatch agents, validate work, and dedupe repeated signals.

Example:

```json
{
  "schema_version": "meristem.work_spec.v1",
  "kind": "repair",
  "dedupe_key": "repo:clinical-demo:review:2026-04-22:finding-001",
  "title": "Public HITL entry point cannot resume a paused run",
  "priority": "P1",
  "objective": "Fix the paused-run resume path.",
  "details": "Human-readable diagnosis.",
  "source": {
    "kind": "review_finding",
    "identifier": "2026-04-22-codex-review:finding-001",
    "external_ref": "issues/review_findings/2026-04-22-codex-review.md"
  },
  "target": {
    "repo": "clinical-demo",
    "path": "src/clinical_demo/...",
    "line_start": 1,
    "line_end": 80
  },
  "acceptance_criteria": [
    "Paused runs can be resumed through the public HITL entry point.",
    "Regression coverage exercises the paused-run path."
  ],
  "validation": {
    "commands": ["uv run pytest"],
    "notes": ["Run targeted tests first if available."]
  },
  "constraints": [
    "Do not fix unrelated findings.",
    "Do not revert unrelated work."
  ]
}
```

The spec is content-addressed. Its fingerprint is used by plan, prompt, run, and validation records.

## Recommended REST Surface

Keep the v0 endpoints from `docs/spec.md`, then add the self-building primitives below.

```text
POST /v1/inbox/messages
POST /v1/signals
GET  /v1/feed

POST /v1/work-items
GET  /v1/work-items
GET  /v1/work-items/{id}
POST /v1/work-items/{id}/children
POST /v1/work-items/{id}/events
POST /v1/work-items/{id}/transition
POST /v1/work-items/{id}/claims

POST /v1/work-items/{id}/artifacts
POST /v1/work-items/{id}/plans
POST /v1/work-items/{id}/runs
POST /v1/work-items/{id}/review

POST /v1/actions
POST /v1/approvals/{id}/decision

GET  /v1/runs
GET  /v1/dead-letter
GET  /v1/events
```

`POST /v1/signals` is the main missing bridge. It accepts non-human structured inputs: review findings, repairable runtime failures, failed dispatches, validation failures, webhook reports, and critic-agent findings. Messages from agents and systems are still content, not owner instructions; the signal endpoint converts safe structured inputs into work items under policy.

Example request:

```json
{
  "kind": "repairable_failure",
  "dedupe_key": "repo:jay:repair:worker-retry-budget",
  "source": {
    "kind": "system_event",
    "identifier": "abc123"
  },
  "work_item": {
    "title": "Worker retry budget is exhausted too early",
    "priority": "P1",
    "details": "The worker stopped after one transient failure.",
    "labels": ["repairable-event", "worker", "p1"],
    "acceptance_criteria": [
      "Transient failures are retried according to configuration.",
      "Regression coverage exercises the failing retry path."
    ],
    "validation": {
      "commands": ["uv run pytest"]
    }
  }
}
```

Example response envelope:

```json
{
  "idempotency": {
    "key": "jay:system-event:abc123",
    "replayed": false
  },
  "dedupe": {
    "key": "repo:jay:repair:worker-retry-budget",
    "created": true
  },
  "resource": {
    "kind": "work_item",
    "id": "00000000-0000-0000-0000-000000000000"
  },
  "event": {
    "id": "00000000-0000-0000-0000-000000000001",
    "kind": "work_item.created"
  },
  "fingerprint": "sha256:..."
}
```

## Runs

`POST /v1/work-items/{id}/runs` should replace repo-local dispatch ledgers.

Example:

```json
{
  "executor": "codex",
  "mode": "dispatch",
  "input_artifact_id": "00000000-0000-0000-0000-000000000010",
  "input_fingerprint": "sha256:...",
  "skip_if_successful_fingerprint_exists": true,
  "metadata": {
    "repo_root": "/Users/juliusmopper/Dev/jay",
    "model": "gpt-5.4"
  }
}
```

If the same work item already has a successful run for the same input fingerprint, return a successful `skipped` run result rather than launching again.

## Plans

`POST /v1/work-items/{id}/plans` represents deterministic planning. The assignment repair plugin is the cleanest reference pattern: generate a plan from current repo state, apply it only if non-empty, then regenerate. If the second plan has zero steps, the repaired state is idempotent.

Plans should include:

- `generator`
- `input_fingerprint`
- `plan_fingerprint`
- `steps`
- `status`: `empty | proposed | approved | applied | failed`
- `validation`

Applying a plan is a write action and therefore goes through policy and approvals once approvals exist.

## Reviews

`POST /v1/work-items/{id}/review` represents the recursive stabilization loop from `clinical-demo`. A review can say:

- The item is resolved.
- The item is still unresolved and should keep running.
- A child issue should be created.
- The original spec is stale and should be blocked or revised.

Review findings become signals or child work items with their own dedupe keys.

## Actions And Approvals

Agents should not own side effects directly. They should claim work, append progress, attach artifacts, propose plans, request runs, request actions, and propose transitions.

`meristem` owns:

- dedupe
- idempotency
- event attribution
- approvals
- retries
- blocked-state handling
- terminal convergence

External writes go through `POST /v1/actions`. The action either executes immediately if it is a read/non-side-effecting operation, or creates an approval and blocks until the owner decides.

## Mapping Existing Systems

| System | Local concept | meristem concept |
|---|---|---|
| `clinical-demo` | review findings markdown | `POST /v1/signals` with `kind=review_finding` |
| `clinical-demo` | recursive unresolved-finding review | `POST /v1/work-items/{id}/review` plus convergence loop |
| `clinical-demo` | per-issue Codex dispatch | `runs` against prompt/spec fingerprints |
| `jay` | repairable JSONL events | `signals` and append-only `events` |
| `jay` | `dispatch_state.json` | run records keyed by input fingerprint |
| `jay` | workflow failure emits repairable event | workflow failure creates a new signal or child work item |
| `ns_obv` | `issues.jsonl` | work-item projection or import source |
| `ns_obv` | generated specs/prompts/manifests | artifacts |
| `ns_obv` | local run logs | run artifacts plus events |
| `assignment1-basics` | deterministic repair plan | `plans` |
| `assignment1-basics` | zero-step second run | idempotent repaired-state validation |

## Implementation Order

The cheapest path that unlocks the imported behavior:

1. Add `dedupe_key` to `work_items`, with a unique index when non-null.
2. Standardize write responses around idempotency, event id, resource id, and fingerprint.
3. Add `artifacts` so specs, prompts, plans, logs, patches, and reviews have one home.
4. Add `runs` with `input_fingerprint`, `executor`, `status`, and previous-success skip behavior.
5. Add `POST /v1/signals` as the canonical bridge for repairable events and review findings.
6. Add `plans` and `review` endpoints once artifacts and runs exist.
7. Add approval-gated `actions` before any write connector ships.

## Reproduction Prompt For Another Assistant

To reproduce the API recommendation, ask the assistant to do the following:

```text
Read `/Users/juliusmopper/Dev/meristem/docs/spec.md`, `/Users/juliusmopper/Dev/meristem/AGENTS.md`, and `/Users/juliusmopper/Dev/meristem/docs/thoughts.md`.

Then inspect the auto-repair references:
- `/Users/juliusmopper/Dev/clinical-demo/issues/README.md`
- `/Users/juliusmopper/Dev/jay/docs/auto_repair_api.md`
- `/Users/juliusmopper/Dev/ns_obv/docs/issue-automation/README.md`
- `/Users/juliusmopper/Dev/stanford-cs336/assignment1-basics/plugins/repo-portfolio-repair-agent/skills/repo-portfolio-repair/SKILL.md`

Synthesize them into a meristem API direction. Preserve `docs/spec.md` as canonical, but include the `docs/thoughts.md` reframe: convergence is the model, the event log is truth, and idempotency is an implementation property. Recommend first-class `signals`, `artifacts`, `runs`, `plans`, `reviews`, `actions`, and approvals rather than separate repo-local auto-healing systems. Distinguish `Idempotency-Key`, `dedupe_key`, `fingerprint`, and `event_id`.
```

