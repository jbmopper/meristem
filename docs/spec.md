# meristem Spec

This document is the single source of truth for `meristem`. It supersedes:

- `meristem-proposal-archived.md`
- `meristem-technical-spec-and-implementation-plan-archived.md`

Both are kept for historical context only and are no longer authoritative.

## What meristem Is

`meristem` is a portable, editor-agnostic, single-operator coordination plane. The owner gives directions in any form. `meristem` normalizes them into a graph of work items, coordinates humans and agents, brokers actions into external systems (GCP, AWS, repos, CI, SSH, Kubernetes), and drives every work item to a terminal state without further owner intervention beyond approvals. `meristem` itself stays light and always-on; heavy compute and inference happen in the systems it orchestrates.

## What meristem Is Not

- Not a workflow engine. It dispatches, waits, and records; it does not execute heavy work itself.
- Not a multi-tenant product. One owner, no other humans.
- Not a replacement for GitHub, the cloud consoles, or the IDE.
- Not coupled to any one editor, model vendor, or cloud.
- Not phased beyond v0. v0 is a bootstrap — the smallest substrate that can be used to track and coordinate its own further development. Every capability past v0 is itself a `work_item` in `meristem`.

## Core Principles

- **Direction → convergence.** The owner gives instructions. The system pushes them to a terminal state — `done`, `failed`, or `canceled` — without further intervention beyond approvals. No work item sits indefinitely; if it cannot progress, it escalates, retries, or terminates.
- **Idempotency everywhere.** Every layer — HTTP requests, jobs, connector actions, events, projections, approvals, inbox ingestion — is safely retryable. Re-sending the same instruction produces the same result, not duplicate work. This is the single property that makes "run itself out" possible.
- **One owner, many client tokens.** The owner is the only human authority. Each client (iPhone, web, CLI, each MCP-connected agent) holds its own scoped token; the root token only mints and revokes others. There are no public endpoints.
- **Postgres is the system.** All durable state, all queues, all audit lives in Postgres. Object storage is a reference target, not a second source of truth.
- **One log.** All state changes append to `events` with full actor attribution. There is no separate audit ledger.
- **Editor-agnostic surfaces.** REST is canonical. CLI, MCP, and the web UI are full-featured translation layers; every REST operation has an MCP tool.
- **Portable substrate.** Go binary, Postgres, an object-storage interface. No managed cloud primitives in the core. Migration between clouds is a redeploy, not a feature gap.
- **Default deny on side effects.** Write actions wait for owner approval. The system never auto-approves. Approvals are a first-class part of the convergence loop, not a stop on it.
- **Minimum viable security; the rest is work the system does.** The substrate ships the primitives required to safely run itself. Critics, reviewers, threat models, redaction policies, formal audits, and similar elaborations are work items the system can be directed to perform on itself.

## Architecture

A Go modular monolith with two runtime modes sharing one codebase and one database:

- `meristem api`: HTTP, webhooks, auth, inbox ingestion, reads, command submission.
- `meristem worker`: background jobs, connector execution, retries, polling, summaries.

### Components

- `ingress`: inbox capture, token auth, webhook verification, idempotency.
- `coordination`: work-item lifecycle, intent classification, routing, convergence loop.
- `execution`: job queue, dispatch, retries, callbacks, dead-lettering.
- `connectors`: HTTP, git host, GCP, AWS, SSH/Kubernetes, notifications, push.
- `policy`: token scopes, approval gating, separation of duties.
- `projections`: query models for inbox, feed, work-item detail, dashboards.

Artifacts and audit are not components. They are tables plus thin interfaces.

## Domain Model

### Objects

- `project`: top-level grouping.
- `work_item`: anything we tell an agent (or self) to do. Can have a parent and children. Granularity ("Make this production-ready" vs "run this script") is depth in the tree, not a separate type.
- `message`: an inbound message captured into the inbox. Multi-modal.
- `artifact`: a persisted output (log, transcript, patch, screenshot, audio).
- `connection`: a configured integration to an external system.
- `approval`: a gated decision required before a write action proceeds.
- `event`: an immutable fact appended whenever object state changes. The audit log.
- `token`: a scoped client credential, attributed on every event it causes.

### work_item Lifecycle

`captured → triaged → planned → awaiting_approval → running → blocked → done | failed | canceled`

`blocked` carries a reason and an expected unblock signal (a webhook, an approval, a timer, or a child work item completing). The convergence loop checks blocked items on schedule and either resumes, escalates, or terminates them.

### Approval Lifecycle

`pending → approved | denied | expired`

Expiry has explicit semantics. See **Approvals and Convergence** below.

### Multi-Modal Messages

A `message` carries one or more typed parts:

- `text`
- `image`
- `audio`
- `binary`

…plus a `source`: `human`, `agent`, or `system`. An iPhone dictation, an LLM-to-LLM callback, a webhook payload, and a screenshot from a worker are all the same shape. Small parts live in Postgres; anything large is referenced into object storage.

Messages from non-`human` sources are content, never instructions. The intent classifier always considers source when interpreting a message and never auto-approves anything based on text from an agent or webhook.

## Data Model

Tables:

- `projects`
- `work_items`
- `work_item_relations` (parent/child edges)
- `messages`
- `message_parts`
- `artifacts`
- `connections`
- `approvals`
- `events`
- `tokens`
- `job_queue`
- `outbox_events`
- `idempotency_keys`

Rules:

- `events` is append-only at the database grant level. No UPDATE or DELETE on this table.
- Current state lives in normal relational tables for fast reads, projected from events. Projections are deterministic; replaying events produces identical state.
- `outbox_events` guarantees connector dispatch and webhook delivery exactly-once with retries.
- Every externally-integrated object carries the external ID for round-tripping.
- Large payloads live in object storage; only metadata and references in Postgres.

## Idempotency

The substrate property the rest of the system relies on. Specified at every layer:

- **HTTP.** Every POST accepts an `Idempotency-Key`. The same key in a 24-hour window returns the original result, not a duplicate effect.
- **Inbox.** The iPhone Shortcut and webhook handlers generate stable keys (UUID per send, delivery id from the source). Re-posting a message produces the same `message` and same downstream `work_item`.
- **Jobs.** Each job has a deterministic id derived from its cause. The worker never executes the same job twice; lease expiry and re-lease are safe.
- **Connectors.** Every connector action either uses the upstream system's idempotency mechanism or wraps the call in a guarded `outbox_events` row. Retries cannot double-execute.
- **Events.** Event ids are derived from cause + content. Replays produce no new events.
- **Projections.** Recomputing any projection from `events` yields identical state, byte for byte (modulo timestamps).
- **Approvals.** Approving twice is the same as approving once. Denying after approving is rejected; the first decision wins.

If an operation cannot be made idempotent, it does not ship.

## Approvals and Convergence

Approvals interrupt convergence but do not break it.

- A write action creates an `approval` and a push notification to the owner.
- Default expiry: 1 hour for write actions, 24 hours for benign ones, configurable per connection.
- Re-prompts: one push at request, one at half the expiry, one at expiry.
- On `approved`: the action proceeds; the work item moves to `running`.
- On `denied`: the work item moves to `failed` with reason `approval_denied`. Terminal unless owner explicitly retries.
- On `expired`: the work item moves to `blocked` with reason `approval_expired`. The convergence loop re-requests once after a backoff; second expiry escalates to `failed`.
- An approval decision is itself an event, attributed to the deciding token.
- Tokens that *create* approvals cannot *decide* them. Only owner-decision tokens (iPhone, web session) can approve.

The owner can be unreachable for a long time and the system still terminates: nothing sits in `awaiting_approval` forever.

## Execution Model

### Inbound

1. A message arrives (text, voice, image, webhook, agent callback) with an idempotency key.
2. `meristem api` authenticates the token and stores the raw message and parts.
3. Coordinator classifies intent: `capture`, `query`, `command`, or `approval`. Source is always considered.
4. A `work_item` is created or updated; events are appended; the originating token is recorded on every event.
5. If work is needed, a job is enqueued in `job_queue` with a deterministic id.

### Outbound

1. `meristem worker` leases the next ready job via `SELECT … FOR UPDATE SKIP LOCKED`.
2. Policy checks the action: read actions proceed; write actions create an approval and the job parks until the approval resolves.
3. The connector executes the action through the `outbox_events` discipline.
4. Results are stored as artifacts and events.
5. Projections update; push notifications fire if needed.
6. The convergence loop ensures the parent work item moves toward a terminal state.

### Queue and Always-On

The worker is a long-lived process that polls `job_queue` on a short interval, leases jobs with `FOR UPDATE SKIP LOCKED`, holds the lease for the duration of the run, and returns the lease on exit or expiry. Crash mid-anything is safe; the next poller picks the lease back up. Postgres is both the backing store for state and the queue. No Redis. No NATS.

## Auth and Token Model

- A single root token, held only by the owner, used only to mint and revoke other tokens.
- Each client gets its own token: iPhone, web UI, CLI, and one per MCP-connected agent (Cursor, Codex, Claude Desktop, custom workers).
- Every token carries scopes. The two scopes that matter most:
  - `can_request_writes`: token may submit write actions for approval.
  - `can_decide_approvals`: token may approve or deny. Held only by iPhone and active web sessions.
  - A token cannot hold both for the same approval (separation of duties).
- Token revocation is instant; the next request fails. A panic-revoke endpoint reachable from the iPhone invalidates every non-root token.
- Tokens are recorded on every event. Audit answers "who, via what client, when, with what authority."

## Security Primitives

The floor. Everything below ships in the substrate; elaborations are work the system later does for itself.

- TLS 1.3 only on the public endpoint, HSTS, no plaintext fallback.
- Disk encryption on the VM. Default-encrypted object storage.
- Secrets via envelope encryption: master key in the host cloud's KMS (GCP Secret Manager / OCI Vault); per-connection credentials stored as encrypted blobs in Postgres so they move with the database.
- Postgres bound to loopback; no public listener.
- Webhook verification per `connection`, with a signing scheme (`hmac-sha256`, `github`, `stripe`, etc.). Unverified inbound webhooks are rejected and recorded as security events.
- Append-only `events` with full actor attribution.
- Per-token rate limits (defaults: 60 writes/min, 600 reads/min) and per-connector concurrency caps (default 4). A circuit breaker trips any connector that errors over threshold.
- A `DELETE /v1/messages/{id}` that hard-deletes message parts and their object-storage entries, leaving an `event` saying who deleted it and when, without payload. Mechanism present from day one; retention policy is a configurable later.

## Connectors

Every connector declares each action as `read` or `write`.

- `read` actions execute immediately.
- `write` actions create an `approval` and wait.
- Every action is idempotent or guarded by an idempotency key.
- Credentials live in the encrypted `connections` blob, never in source.
- Retries: exponential backoff with jitter, capped (default 5). After cap, the work item moves to `failed`, the connector error is preserved as an artifact, and the dead-letter view surfaces it.

That is the entire policy surface. No dry-run framework, no budget caps, no allowlist DSL. Elaborations like cost-guards or critic-agents are work the system performs on itself.

## API Surface

- `POST /v1/inbox/messages`
- `GET  /v1/feed`
- `GET  /v1/work-items`
- `POST /v1/work-items`
- `GET  /v1/work-items/{id}`
- `POST /v1/work-items/{id}/children`
- `POST /v1/approvals/{id}/decision`
- `POST /v1/webhooks/{source}`
- `GET  /v1/connections`
- `POST /v1/connections`
- `POST /v1/tokens` (root only)
- `DELETE /v1/tokens/{id}` (root or self)
- `POST /v1/tokens/revoke-all` (root only, panic)
- `DELETE /v1/messages/{id}`

REST is canonical. CLI and MCP are full-featured translation layers — every REST operation has an MCP tool. The web UI reads the same projections.

## Mobile Entry and Push

- iPhone Shortcut: capture text, image, or audio; POST to `/v1/inbox/messages` with an idempotency key; show a compact response.
- APNs push for approvals (or Web Push via PWA). Payload contains a one-line summary and a link, never secrets.
- Email or SMS fallback for push failures.

## Cloud Strategy

The substrate is the same wherever it runs.

- **Initial host:** small Compute Engine VM on GCP, funded by remaining $200 GCP credit through June 1. `meristem api`, `meristem worker`, Postgres, reverse proxy with TLS, GCS for object overflow.
- **Eventual host:** OCI Ampere A1 with the same containers, OCI block storage for Postgres, OCI Object Storage for artifacts, triggered by a planned $300 / 1-month project. The migration is a redeploy and a DNS swap, not a feature change.
- **Never:** depending on managed cloud primitives in the core (Firestore, Cloud Tasks, Eventarc) or building to provider APIs early.

Workload Identity Federation (GCP) and IAM Roles Anywhere (AWS) for off-cloud credentials.

### Migration Runbook

The migration to OCI (or any future host) follows one runbook, not a feature plan:

1. Provision the new host with disk, object bucket, and KMS.
2. Stand up Postgres on the new host and replicate from the source.
3. Deploy the same containers; point at the new database.
4. Cut DNS; revoke old credentials.
5. Snapshot the old host and decommission.

If a step is not possible, it is a substrate bug, not a missing feature.

## Disaster Recovery

- Nightly `pg_dump` to object storage, encrypted with the master key.
- Weekly disk snapshot.
- A documented restore procedure that the owner has executed at least once before relying on it.
- The owner can rebuild the entire system from a backup and a root token.

## Observability

- Structured logs with request id, work item id, token id.
- OpenTelemetry traces across api, worker, and connector calls.
- Metrics: queue depth, run duration, approval wait time, connector error rate, artifact volume, convergence-loop lag.
- A small operator dashboard: open work items, blocked items, failed jobs, stale approvals, dead-lettered actions.

## Repository Shape

```text
meristem/
  cmd/
    meristem/
  internal/
    api/
    auth/
    coordination/
    execution/
    connectors/
    policy/
    projections/
    storage/
  migrations/
  web/
  deploy/
  docs/
```

## v0 — Bootstrap

v0 is the smallest version of `meristem` that can be used to build `meristem`. It ships cold (built directly, not tracked in itself). Everything past v0 is a tracked `work_item` in the running system.

The thesis: as soon as the owner can capture instructions from the iPhone, see them in a feed, and dispatch them to a Cursor agent via MCP, the rest of the substrate can be built *as* `meristem` work — by the owner, by Cursor agents, or both — flowing through the system it is building.

### v0 Scope

The minimum surface that satisfies the bootstrap thesis:

- Go binary, Docker-based local dev, Postgres migrations.
- One owner bearer token. No scopes yet, no client-token model.
- Tables: `work_items`, `work_item_relations`, `messages`, `message_parts`, `events`, `idempotency_keys`, `tokens`.
- Append-only `events` with actor attribution (token id and source).
- Idempotency on `POST /v1/inbox/messages` and `POST /v1/work-items` via `Idempotency-Key`.
- Endpoints:
  - `POST /v1/inbox/messages` — text only in v0.
  - `GET  /v1/feed`
  - `GET  /v1/work-items` and `GET /v1/work-items/{id}`
  - `POST /v1/work-items` and `POST /v1/work-items/{id}/children`
  - `POST /v1/work-items/{id}/events` — clients append progress.
  - `POST /v1/work-items/{id}/transition` — owner or agent moves the item along its lifecycle.
- MCP server with parity to the v0 REST surface — list/read work items, append to inbox, create items, spawn children, append events, transition state.
- iPhone Shortcut: dictate or type, POST to `/v1/inbox/messages` with an idempotency key.
- Always-on deploy on a small GCP VM: Caddy or nginx + Let's Encrypt, Go binary, Postgres on the same host, disk encryption, daily `pg_dump` to GCS via cron.
- Secrets via environment variables for now. KMS-wrapped envelope encryption is a v1 work item.

### What v0 Deliberately Does Not Have

These are deferred and become tracked work items in `meristem` the moment v0 is up:

- Approvals, approval lifecycle, push notifications.
- Connectors of any kind. The owner and Cursor agents are the only executors; side effects happen in their hands and are reported back as events on the work item.
- Convergence loop. Items move along their lifecycle when the owner or an agent transitions them. Nothing is automatically retried, escalated, or terminated yet.
- Multi-modal messages. Text only.
- Token scopes, separation of duties, panic-revoke.
- Web UI.
- Webhook ingress and verification.
- Rate limits, circuit breakers, dead-letter views.
- Artifact table and object-storage interface.
- Coordinator intent classification. The owner classifies by writing the instruction explicitly ("create a work item to …", "show me open items").

### v0 Acceptance

- Owner can dictate "Build the approval system in `meristem`" from the iPhone, and a `work_item` exists with that text within 15 seconds.
- A Cursor agent, via MCP, can list open work items, pick one, append progress events as it works, spawn child items, and mark items `done`.
- The same instruction submitted twice produces one `work_item`, not two.
- All state survives a process restart.
- Every `event` records which token (owner, iPhone, Cursor) caused it.
- Each item in the v1 substrate below is a tracked `work_item` in the running v0 system before any of it is built.

### Estimated v0 Build Cost

Roughly five to seven focused working days. v0 is small enough to land inside a single working week.

## v1 Substrate (first body of work in meristem)

Once v0 is running, every item below exists as a `work_item` in `meristem` and is dispatched to the owner or to Cursor agents via MCP. v1 is the agreed-upon substrate; "What meristem Builds For Itself" below is the open-ended backlog after that.

v1 is complete when every item below is true. Nothing in v1 is GCP-specific; the host happens to be GCP.

- Go repo, Docker-based local dev, Postgres migrations.
- Token model: root token, scoped client tokens, separation of duties, panic-revoke.
- All security primitives listed above.
- `work_item`, `message`, `artifact`, `approval`, `event`, `token` tables and projections.
- Append-only `events` with full attribution.
- Idempotency at every layer per the **Idempotency** section.
- `POST /v1/inbox/messages` accepting multi-modal parts; `GET /v1/feed`.
- Worker with `job_queue` and `SELECT … FOR UPDATE SKIP LOCKED`.
- Convergence loop that drives every work item to a terminal state without owner babysitting.
- Generic HTTP connector with read/write declaration, approval gate on writes, retries, and dead-lettering.
- Webhook verification.
- Approvals with expiry, re-prompt cadence, and the convergence semantics above.
- APNs (or Web Push) for approval requests, with email/SMS fallback.
- Minimal web UI: feed, work-item detail, approve/deny, dead-letter view.
- iPhone Shortcut posting to `/v1/inbox/messages`.
- Full-featured MCP server with parity to REST, including write paths and approval requests.
- Nightly Postgres dumps to object storage; documented and rehearsed restore.
- Single-VM deploy with TLS, disk encryption, and object overflow.

### Acceptance

- Owner dictates an item from the phone and sees it in the feed in under 15 seconds.
- A write action through any connector pushes an approval to the iPhone and is recorded end-to-end in `events` with the originating token id.
- A work item given an instruction reaches a terminal state without further owner intervention beyond approvals, even across restarts and connector failures.
- The same instruction submitted twice produces one work item, not two.
- An agent in Cursor can, via MCP: read the feed, create a work item, spawn a child, attach an artifact, request approval for a write action, and observe the approval decision — against the same Postgres state the iPhone path uses.
- The owner can rebuild the entire system from a backup and the root token.
- The same containers run on the GCP VM today and on an OCI Ampere A1 instance with no code change.

## What meristem Builds For Itself

Beyond v1, the system itself continues to be the agent that builds further capability. Each item below is a backlog `work_item`, not a phase. The owner directs; `meristem` converges.

- Native GCP, AWS, SSH, and Kubernetes connectors with WIF and IAM Roles Anywhere.
- CLI.
- Reusable run templates and saved actions.
- Search and richer dashboards.
- LLM-to-LLM message paths exercised end-to-end.
- Native iPhone app once inbox, approvals, and notifications are stable on the Shortcut + PWA path.
- Critic agents that review proposed write actions before approval is requested.
- Cost-guard agents that watch connector spend and propose throttles.
- Field-level encryption for sensitive `message_parts`.
- Hash-chained external attestation of the audit log.
- Formal threat model and adversarial tests.
- PII redaction policy and automated retention enforcement.
- Multi-region disaster recovery.
- Migration to OCI per the runbook.

## Risks

- Scope creep into workflow orchestration.
- Treating MCP, mobile UX, or connectors as the product before the inbox + work-item graph and the convergence loop are real.
- Underestimating approval ergonomics — if approving from the phone is annoying, the owner will weaken the gate or ignore pushes.
- Shipping any operation that cannot be made idempotent.
- Drifting back into managed cloud primitives because they are convenient.
- OCI Always Free capacity not being available when the migration is wanted.
