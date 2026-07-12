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
- **One owner, many client tokens.** The owner is the only human authority. Each client (iPhone, web, CLI, each MCP-connected agent) holds its own scoped token; the root token only mints and revokes others. An endpoint may be internet-reachable only as an explicitly configured ingress and still requires meristem authentication; there are no anonymous business endpoints.
- **One backing store per node; today it is Postgres.** All durable state, queues, and audit owned by a node live in that node's Postgres. Object storage is a reference target, not a second source of truth. Nodes never share a database or expose Postgres across a node boundary.
- **One log per node.** All state changes owned by a node append to that node's `events` log with full actor attribution. There is no separate audit ledger, global event sequence, or multi-writer object history.
- **Editor-agnostic surfaces.** REST is canonical. CLI, MCP, and the web UI are full-featured translation layers; every REST operation has an MCP tool.
- **Portable substrate.** Go binary, Postgres, an object-storage interface. No managed cloud primitives in the core. Migration between clouds is a redeploy, not a feature gap.
- **Default deny on side effects.** Write actions wait for owner approval. The system never auto-approves. Approvals are a first-class part of the convergence loop, not a stop on it.
- **Minimum viable security; the rest is work the system does.** The substrate ships the primitives required to safely run itself. Critics, reviewers, threat models, redaction policies, formal audits, and similar elaborations are work items the system can be directed to perform on itself.

## Architecture

A Go modular monolith with two runtime modes sharing one codebase and, on each
node, one backing store:

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

### Fleet and Network Contract

Meristem may run as a one-operator fleet of a few nodes. A node is one meristem
deployment and its private backing store. Each node owns one append-only event
log; `events.seq` is meaningful only as a cursor within that log. Nodes do not
replicate Postgres, merge event sequences, or elect a database leader.

Every durable domain object has exactly one **home node**, initially the node
that created it. Only the home node appends events for that object. Remote
reads are authenticated REST GETs to the home node; remote mutations are
authenticated, idempotency-keyed REST POSTs to the home node. Transport does
not create a second business-logic path. Qualified references identify the
home as `<node_id>:<uuid>` (canonical URI
`mrs://<node_id>/work-items/<uuid>`); an unqualified UUID is local to the node
interpreting it.

One owner-selected **registry home** is authoritative for node ids and routing
intent. This is a narrow control-plane authority, not the home of fleet data.
Registry changes are events on that node. Other nodes pull authenticated
snapshots, validate them, append the observed snapshot to their own log, and
derive their local routing projection from that event. There is no gossip,
ambient service discovery, or liveness consensus.

The minimum cross-node transport has two paths:

1. A direct HTTPS REST request to an explicitly approved origin for a node.
2. A durable command queue on an explicitly approved reachable queue host when
   the target accepts no inbound connection. The target polls outbound,
   executes the allowlisted REST operation locally with the original
   idempotency identity, and acknowledges a terminal outcome.

Application-level forwarding through a relay is not part of the minimum
network contract. It may be added only with a separate design covering target
binding, credential selection, refusal behavior, loop prevention, and audit.
Both direct attempts and queued commands have finite retry/expiry budgets and
deterministic escalation; an unreachable node cannot leave a non-terminal
command waiting forever.

The provider-facing MCP ingress is a separate gateway concern. It exposes the
existing API's `/mcp` route through public TLS, with OAuth 2.1 authorization,
per-client attribution, resource-bound access tokens, rotating refresh tokens,
and owner approval for each grant/authority change. It is neither the fleet's
event-log authority nor required for peer networking. REST remains canonical.
Sealed read profiles expose only provider-safe work-item/feed projections;
sealed tracker-write profiles additionally expose narrowly validated,
idempotent work-item coordination mutations. They never expose approval,
connector, inbox, registry/policy, private payload, or execution authority.

`docs/network-layer-spec.md` gives the detailed registry, origin-validation,
queue, patience, staging, and acceptance contract. In particular, Stage 1 is
peer REST plus queue fallback; provider MCP ingress is Stage 2. Remote-reference
caching, object re-homing implementation, application relay, and HTTP MCP
writes are explicit follow-up work, not implicit parts of Stage 1.

### Deterministic and Probabilistic Subsystems

The system has two cooperating subsystems. The deterministic subsystem owns the event log, projections, migrations, safety policy, auth, idempotency, queue claims, and all reconciliation rules that must replay byte-for-byte. The probabilistic subsystem proposes classifications, plans, summaries, and other model-shaped judgments; it never owns durable truth directly.

Deterministic-layer failures are reported as `deterministic_error.*` events and projected into `deterministic_errors`. Error reports are maskable: masking hides a report from active operator views without deleting or changing the immutable events that explain when it was reported, masked, or unmasked. Masking is attention policy, not privacy redaction. Error payloads must be safe for durable audit storage; secrets and raw message content do not belong in them. Read surfaces apply deterministic token-scope visibility (`logs.read*`) to details fields, and future encrypted payload envelopes are an explicit work item rather than an excuse to store plaintext sensitive content.

### Convergence Patterns

Convergence is how a `work_item` reaches a terminal state. In every pattern the probabilistic subsystem may *propose and judge* — sample at high temperature, fan out across models, draft a plan, write a patch, grade another model's patch — but a deterministic *reduction* is what disposes. Without a deterministic side, retrying is a random walk and bounded patience has nothing to escalate on.

The deterministic side is not required to be a hand-coded checker. It is required to be a deterministic *reducer over the signals it has*. The signals may themselves be probabilistic (a model's grade of another model's output, a confidence score, an LLM-emitted JSON verdict). What is deterministic — and what is logged — is the reduction: majority vote across N graders, a fixed threshold, an `all-pass` over a list of probabilistic checks. Given the same inputs, the reducer always produces the same verdict.

Every convergence pattern must satisfy three rules:

1. **A deterministic reduction.** A pure function over the available signals decides whether the candidate is accepted, rejected, or escalated. The signals may be model outputs treated as opaque text or as parsed JSON; the reducer is hand-coded and replayable. The verdict, not the candidate, advances the lifecycle.
2. **The verdict is an event.** Acceptance and rejection append events that record the reducer's identity and version, the inputs digest, and — when a probabilistic signal informed the reduction — the signal's source (model, prompt version, sample id) and the raw output. "We accepted output X because reducer Y had three of four graders pass at commit Z" must be reconstructable from `events` alone, so a stricter future reducer can re-fold the log.
3. **Bounded patience.** The pattern declares a maximum number of attempts (or a wall-clock budget) and an escalation rule when exhausted: terminate `failed`, request approval, or hand to a human. New patterns ship with their escalation rule or they do not ship.

For wall-clock patience breaches, resolution is a deterministic correlation,
not a second event. `patience.breached` records the work item, state, budget, and
`state_entered_at` epoch that were over budget. The breach remains open only
while the replayed `work_items` projection still has that same state and state
epoch; any later `work_item.transitioned` event changes the state epoch and
resolves the breach historically. A future `patience.resolved` event would be
redundant unless it carried a new decision that is not already present in the
lifecycle log.

Each `work_item` may carry `suggested_convergence_checks`, a human-readable checklist of signals a worker should try to satisfy before claiming convergence. The list is advisory until a reducer consumes it, but it is durable projection state sourced from events so workers, reviewers, and future reconcilers see the same checklist. A `work_item` also carries `human_review_status`: `blocked`, `waved_through`, or `approved`. `waved_through` is the default for ordinary project work; `blocked` means human review must be resolved before the item should be treated as converged; `approved` records explicit human clearance. This field is not a replacement for approval rows: external writes still use the approval system and default deny.

The recurring shapes — none of them privileged, all reducible to "propose N signals, deterministically combine them":

- **Resample.** Same model, same prompt, varied seed or temperature. Cheapest pattern. The reducer is typically a strict checker plus a retry budget; useful when the checker is strict and the model is mostly right.
- **Multi-model.** Different models or different prompts produce N candidates; the deterministic reducer selects (vote, schema-match, run-to-green, score-and-take-best). Useful when failure modes are model-specific.
- **Generate-and-validate.** A `generate` child (probabilistic) produces a candidate; a `validate` child (which may itself be a model run, "loaded" into grader mode by being asked to grade its pair's output) emits a probabilistic verdict. The deterministic reducer combines validator verdicts — for a single grader, threshold its score; for multiple, take a vote or require unanimity. Pure-deterministic validators (schema checks, type checks, test suites, linters, parsers, fuzzers, replay-equality against the event log) are still preferred when available, because they make the reducer trivial.
- **External signal.** A connector or human action provides the verdict (CI passed, approval granted, file landed). The reducer is the projection that observed the signal. Bounded patience here is a timeout that escalates to a different pattern, not infinite waiting.

These shapes compose: a generate-and-validate child can use multi-model internally; a multi-model selector can itself feed a generate-and-validate. They do not compose into a new mechanism — every layer still owes a deterministic reduction, an event, and a patience budget.

The deterministic reducer should be the smallest thing that does the job. A vote, a threshold, an `all` over a checklist, a regex over a model's verdict text — these are fine, *as long as the reducer is fixed and logged*. What is not fine is letting a model's free-form judgment directly drive the lifecycle: the reducer is then unspecified and the verdict is not replayable.

## Domain Model

### Objects

- `project`: top-level grouping.
- `work_item`: anything we tell an agent (or self) to do. Can have a parent and children, suggested convergence checks, and a human review status. Granularity ("Make this production-ready" vs "run this script") is depth in the tree, not a separate type.
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
- `deterministic_errors`
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
- New client tokens may carry scopes; scope-less legacy tokens retain broad v0
  access until rotated. The shipped policy scopes include work-item scopes
  (`work_items.read`, `work_items.write`, `work_items.read_all`,
  `work_items.write_all`, `work_items.tracker_write`,
  `work_items.tracker_write_all`, `work_items.create`,
  `work_items.tree:<uuid>`), feed
  scopes (`feed.read`, `feed.read_assigned`), owner posture scopes such as
  `policy_profile.switch`, registry write scope `registry.write`, log scopes,
  `oauth_clients.bind`, `oauth_clients.revoke`, and `approvals.decide`.
  OAuth-client administration requires an explicitly scoped non-root human;
  the root token remains mint/revoke-only. Write-request authority comes from
  the relevant work-item/connector write scope; approval decision authority
  requires a human non-root token with `approvals.decide`.
  - A token that requested an approval cannot decide that same approval, even if
    it also has `approvals.decide` (separation of duties).
- Scoped agent MCP access is a deterministic reducer over token scopes and
  canonical projections, not a per-agent projection. The first shipped scopes
  are `work_items.tree:<uuid>`, `work_items.read`, `work_items.write`, and
  `feed.read_assigned`; MCP tools/list hides impossible tool classes and
  object-level work_item/feed calls re-check the assigned tree before touching
  services. Denied writes append no events. Scope-less legacy tokens retain
  broad v0 access until explicitly rotated; any non-empty scope set is
  policy-bearing and fails closed on unknown or incomplete scopes.
- Provider OAuth actors carry exactly one versioned `provider.profile:*`
  marker plus its sealed Meristem scope set. Their coarse OAuth scope is
  `mcp:read` or `mcp:read mcp:tracker_write`. Dynamic registration records an
  allowed scope ceiling, not authority; the bound profile and owner-approved
  request select one exact effective scope within it. Profile, effective-scope,
  or ceiling mismatches fail at binding, authorization, token exchange,
  refresh, and access.
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

## v0 — Bootstrap (shipped)

> **Status: shipped.** v0 acceptance is met by the current binary. This section is retained as the historical contract; the implementation details are in `docs/v0.md`. Current substrate state: v0 plus the in-flight v1 items called out below.

v0 is the smallest version of `meristem` that can be used to build `meristem`. It shipped cold (built directly, not tracked in itself). Everything past v0 is a tracked `work_item` in the running system.

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

## v1 Substrate (in flight — current body of work)

> **Status: in flight.** v0 has shipped; v1 is the current substrate work. Each item below either lives in code today (marked ✅ done or 🚧 partial) or is open work (◻︎). Open items exist as `work_item`s in the running system per `meristem seed v1`; this list and the seeded backlog must not drift.

v1 is the agreed-upon substrate; "What meristem Builds For Itself" below is the open-ended backlog after that. v1 is complete when every item below is ✅. Nothing in v1 is GCP-specific; the host happens to be GCP.

- ✅ Go repo, Docker-based local dev, Postgres migrations.
- 🚧 Token model: root token ✅, client/scoped tokens ✅, root-only mint/revoke ✅, panic-revoke ✅, approval-specific separation of duties ✅.
- 🚧 Security primitives listed above (basic bearer + SHA-256 + append-only triggers in place; KMS-wrapped envelope encryption open).
- 🚧 `work_item`, `message`, `event`, `token`, and `approval` tables and projections ✅; `artifact` table open.
- ✅ Append-only `events` with full attribution.
- ✅ Idempotency at every POST per the **Idempotency** section.
- 🚧 `POST /v1/inbox/messages` ✅ (text only); multi-modal parts open. `GET /v1/feed` ✅, with SSE push.
- 🚧 Worker with `job_queue` and `SELECT … FOR UPDATE SKIP LOCKED` — `worker` daemon, `worker --once` verification tick, package, migration, dispatch enqueue, and lease-claim primitive are in place; job execution semantics remain open.
- 🚧 Convergence loop that drives every work item to a terminal state without owner babysitting — scribe, dispatch, durable queue enqueue/claim, always-on worker ticks, checklist convergence, breach detection, finite policy profiles, and minimal approval lifecycle are in place; connector retries, re-prompt cadence, and full terminal convergence remain open.
- 🚧 Generic HTTP connector with read/write declaration, approval gate on writes, retries, and dead-lettering — HTTP proof slice, write approval creation, stdio MCP tool, and approval-to-outbox dispatch are in place; retries/dead-lettering and full connector catalog remain open.
- ◻︎ Webhook verification.
- 🚧 Approvals with create/read/decision, expiry, separation of duties, REST, and stdio MCP tools ✅; re-prompt cadence and second-expiry convergence remain open.
- ◻︎ APNs (or Web Push) for approval requests, with email/SMS fallback.
- ◻︎ Minimal web UI: feed, work-item detail, approve/deny, dead-letter view.
- ✅ iPhone Shortcut posting to `/v1/inbox/messages`.
- 🚧 MCP server with REST parity ✅ for read/triage paths, feed projections,
  registry/projection reads, substrate `work_item` mutations, and approval
  request/decision tools plus approval-gated HTTP connector requests over
  stdio; provider HTTP MCP has provider-safe reads plus a sealed tracker-only
  mutation profile with durable replay and no execution/external-write tools.
  Artifact attachment remains open.
- ◻︎ Nightly Postgres dumps to object storage; documented and rehearsed restore.
- 🚧 Single-VM deploy with TLS, disk encryption, and object overflow.

Items that landed beyond the original v0 scope and are now part of the substrate: `internal/signals` (non-human structured input), `internal/errorreporting` (`deterministic_error.*` events + maskable projection), `internal/safety` (deterministic resource limits), and the resumable feed cursor with monotonic `events.seq`. They are folded into v1 because they ship the load-bearing properties this section requires.

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
