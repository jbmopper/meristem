# Network layer spec

Status: accepted design contract for Stage 1 tie-off (work item
`35592eb5-172f-5640-9009-39abcc6b1d4c`). This document elaborates the
canonical fleet contract in `docs/spec.md`. If they disagree, `docs/spec.md`
wins and this document must be corrected.

Scope: authenticated communication among a one-operator fleet of 2-5 meristem
nodes, plus the boundary between peer networking and the provider-facing MCP
gateway. OAuth and HTTP MCP protocol details belong to their own work items and
documents; this contract does not redefine them.

## 1. Decisions and boundaries

The fleet is mesh-capable but deliberately small. Each node remains useful in
isolation, and explicitly configured direct peer routes are permitted. There
is no routing protocol, multi-master database, global event ordering, or
mandatory central data node.

One owner-selected node is the **registry home**. It is authoritative only for
node identity and routing intent. It is not automatically the home of domain
objects, the provider ingress, or the durable center of the fleet. Those roles
may happen to share a machine, but the data model does not depend on that
deployment choice.

Stage 1 has exactly two cross-node delivery paths:

1. Direct authenticated REST to a target's approved peer origin.
2. A durable queue on an approved reachable queue host for a target that has
   no inbound route. The target drains the queue by outbound polling.

Application-level relay — a node accepting a request and forwarding it onward
as an online proxy — is deferred. The existing `relay_via` selector and relay
receipt path are prototype code, not part of Stage 1 acceptance. Before relay
can ship, a separate contract must specify target binding, credential
selection, scope intersection, refusal propagation, loop prevention, payload
limits, patience, and attribution. A direct request may fall back to a durable
queue after a reachability failure; it must never silently become a forwarded
request.

Provider MCP ingress is Stage 2. Stage 1 does not require a DNS name, public
listener, tunnel, OAuth flow, or provider registration. The ingress is not a
peer-network dependency.

## 2. Per-node truth and object homes

A **node** is one meristem deployment and its private backing store. Today that
store is Postgres. Each node owns exactly one append-only `events` log with its
own monotonic `events.seq`:

- `events.seq` is a cursor only within its node.
- Sequences are never compared, merged, or totaled across nodes.
- A node rebuilds its projections only from its own events.
- Postgres is never shared or replicated as a fleet coordination mechanism.
- Port 5432 never crosses a node boundary; inter-node traffic is HTTP API only.

Every `work_item`, `message`, `approval`, and its event subtree has exactly one
**home node**. Creation fixes the home for Stage 1. Only the home appends events
for the object or derives its authoritative projection.

A client operating on a remote-homed object uses the home node's canonical REST
surface:

- Reads are authenticated GETs to the home.
- Mutations are authenticated POSTs to the home and carry an
  `Idempotency-Key` generated for the logical action.
- The same domain operation backs local, direct, and queued execution. Network
  transport never owns lifecycle logic.
- A response or queued acknowledgement is evidence of the home node's outcome;
  it is not a second authoritative projection of the object.

Cross-node references use `<node_id>:<uuid>`. The canonical URI form for a work
item is `mrs://<node_id>/work-items/<uuid>`. An unqualified UUID means local to
the node interpreting it. Payloads that persist qualified references follow
`docs/payload-versioning.md`.

Remote-reference caching is deferred. Stage 1 resolves reachable remote
references by read-through. A node with no inbound route cannot serve a remote
read; the caller returns a bounded unavailable result rather than treating a
stale copy as truth. A future `remote_refs` cache must be explicitly marked
derived, rebuildable, and never folded into local projections as though its
events were local.

Object re-homing implementation is also deferred. Stage 1 never changes an
object's home. Passing work to an agent on another node means that agent reads
and mutates the original home remotely. A future re-homing design must preserve
the replayability of both logs and define the authoritative pointer and history
boundary before reserving executable migration behavior.

## 3. Registry authority and distribution

The registry home owns the authoritative `nodes` projection. Registry changes
append events there before changing the projection. The registry is routing
intent, not observed liveness, and contains no bearer tokens, token hashes, or
connection credentials.

Each entry contains:

- `node_id`: stable, unique in the operator's fleet, and DNS-label safe.
- `base_url`: optional stable API origin advertised for operator-facing or
  provider configuration.
- `direct_url`: optional origin approved for peer-to-peer REST traffic.
- `queue_via`: an ordered list of node ids approved to hold durable commands
  for this target.
- `status`: operator intent such as `active` or `disabled`, never a health
  probe result.
- the registry revision that produced the entry.

`base_url` and `direct_url` are distinct even when they contain the same
origin. `base_url` identifies the node's stable external/API seat; Stage 2 may
derive `/mcp` and OAuth endpoints from it. It is not used for peer delivery
unless the same origin is separately approved as `direct_url`. `direct_url`
is the machine-to-machine peer route used by Stage 1. Neither field contains
an endpoint path.

Both URL fields use one validator and canonicalizer:

- The value is an absolute origin: scheme, host, and optional port only.
- Userinfo, query, fragment, and non-root path are rejected.
- Host names are lower-cased, default ports are removed, and a trailing slash
  is removed before persistence and event identity calculation.
- `https` is required except for loopback origins explicitly enabled in local
  development or tests. Public plaintext and plaintext bearer transport are
  forbidden.
- Unspecified, multicast, link-local, and metadata-service addresses are
  rejected. Private-address routes require explicit operator approval; DNS
  rebinding must not widen the approved address class at request time.
- Redirects are not followed across origins. The request's target node id is
  bound to the selected registry entry and checked again by the receiver.

The current schema field named `relay_via` must not activate application relay
in Stage 1. Until a migration gives the queue-host list its accurate
`queue_via` name, implementations may interpret `relay_via` only as the
ordered queue-host allowlist. Documentation and API responses must not promise
online forwarding from that legacy name.

Other nodes obtain the registry by authenticated outbound REST from the
configured registry home. A snapshot has a source `node_id`, source
`events.seq` revision, and the complete normalized entry set. A consumer:

1. authenticates with its own registry-home-minted read token;
2. verifies the source identity, monotonically newer revision, schema, URL
   validation, and uniqueness of node ids;
3. appends one local registry-snapshot-observed event containing the validated
   snapshot and source revision; and
4. replaces its local routing projection atomically from that event.

This makes the consumer's routing view replayable from its own log. Repeated
delivery of the same source revision is idempotent. If the registry home is
unreachable, a node retains the last accepted snapshot; route attempts still
obey their own timeouts. There is no push, gossip, health consensus, or direct
write to a consumer's routing projection.

Bootstrap configuration identifies the registry home origin and its pinned
node id. Bootstrap is the only ambient input: after the first accepted
snapshot, routing changes are event-backed registry data.

## 4. Direct REST delivery

The sender selects the target by qualified object home or explicit target node
id, loads one accepted registry revision, and applies this deterministic rule:

1. Refuse a disabled or unknown target.
2. If the target has `direct_url`, call the canonical REST operation there.
3. On a qualifying reachability failure, try the target's approved durable
   queue hosts in order.
4. If no approved queue host is reachable, return a delivery failure and let
   the causing work item's patience reducer dispose of it.

There is no relay candidate between steps 2 and 3. Route selection does not
probe, gossip, or mutate the registry. A short in-memory cooldown may avoid
repeating a failed origin, but it is only a performance hint: restart may lose
it without changing correctness.

Each direct request carries:

- a bearer minted by the target/home node for that specific client;
- an `Idempotency-Key` stable for the logical action;
- the target node id and originating node id as structural request metadata;
- an ordinary canonical REST path and body.

The receiver derives actor identity and source from the authenticated target
token, never from routing headers or the request body. It refuses a target-node
mismatch, disallowed scope, unknown path, oversize body, or already-forwarded
marker before appending any event.

Authentication, authorization, validation, conflict, and rate-limit responses
are terminal refusals for that attempt and do **not** fall back to a queue.
Queue fallback is allowed only for connection/DNS/TLS timeout or an explicitly
classified transient `502`, `503`, or `504`. This prevents an alternate route
from bypassing target policy.

Default direct patience is three attempts within 60 seconds, with a 10-second
per-attempt deadline and deterministic capped backoff. Deployments may shorten
those values, but may not remove the attempt or wall-clock cap. Once the direct
budget is exhausted, the request is enqueued once if an approved queue path
exists; otherwise delivery fails structurally.

## 5. Durable queue fallback

A queue host is an ordinary reachable meristem node whose registry entry is
listed in the target's `queue_via`. The registry home is not implicitly a
queue host. Selecting it requires the same explicit entry as any other node.

The sender POSTs an idempotent command envelope to the queue host. The queue
host appends `command.queued` and derives the queue row in the same transaction.
The envelope contains only the material required for deterministic execution:

- queue command id and original idempotency identity;
- target and originating node ids;
- originating actor token id and source as resolved by the queue host;
- one allowlisted canonical REST operation, normalized path, and bounded body;
- queued-at and expires-at timestamps from the event;
- payload version.

It never contains a bearer, token hash, root credential, connection credential,
private message body, `.meristem/*.token` content, or `.env*` content.

Queue authority is deliberately narrow and implementable. The queue host
requires the enqueueing token to have target-specific queue-write scope for the
operation class plus an exact origin-node scope; a syntactically valid
`origin_node_id` without that scope is only an assertion and is refused. The
queue host records the resolved actor id. The target polls with its
own queue-read token, authenticates the configured queue host, validates the
target id and operation allowlist, and executes under one target-local non-root
spoke token scoped only to the permitted queued operations, target execution,
and the authenticated origin. No remote token
material crosses the boundary. The target's events record both the originating
actor id supplied by the authenticated queue host as structural provenance and
the local spoke token id that actually authorized the mutation. The event's
`actor_token_id` and `source` remain those of the local request context; remote
metadata never impersonates local attribution. A missing/narrower local scope
or an envelope outside the allowlist is a refusal, never a reason to use a
broader system token. Per-origin mappings to distinct target-local tokens are
optional future hardening, not a Stage 1 prerequisite.

Queue operations are an explicit allowlist of canonical REST mutations. An
arbitrary method/path/body proxy is forbidden. Approval decisions, token/root
operations, connector credentials, private message parts, registry mutation,
and any endpoint not named in the allowlist cannot be queued in Stage 1.

The target polls outbound with an interval no greater than 30 seconds, leases
commands in stable queue order, and preserves the original idempotency identity
during local execution. It acknowledges exactly one terminal structural outcome: `done`,
`refused`, `failed`, or `expired`. First terminal acknowledgement wins; later
acks are idempotent only when identical and otherwise conflict. The queue host
appends the acknowledgement event before updating the projection.

Default queue patience is 24 hours from `queued_at`, with at most five local
execution attempts for retryable failures. Validation, authorization, and
conflict refusals are not retried. At the earlier of attempt exhaustion or
`expires_at`, the reconciler appends a terminal command event and the causing
work item deterministically transitions according to its declared policy — by
default to `failed` with `cross_node_delivery_expired`. A policy may instead
escalate to a human-gated work item, but that state owes its own finite patience
budget. No pending queue row may wait forever.

Queue hosts may disappear without corrupting either object's home log. An
unacknowledged command remains pending until its finite expiry; a target restart
resumes from its durable poll cursor and idempotency identity. Queue outcomes
do not create a shadow authoritative copy of the target object.

An origin with remotely queued commands polls the queue host outbound through
`GET /v1/crossnode/outcomes?origin=<origin>&after=<seq>`. The queue host filters
terminal rows by their immutable `origin_node_id` before returning them and
orders them by the queue host's terminal-event sequence. The remote credential
requires the exact `crossnode.outcomes:<origin>` scope; root, unscoped, and
other-origin tokens are denied. The origin uses a separate local system/agent
token with `crossnode.observe:<queue_host>:<origin>` to append
`command_outcome.observed`. That event projects both the immutable observation
and the per-host cursor. An exact stale page is a no-op; conflicting terminal
facts fail closed. No bearer crosses nodes or is persisted in either payload.

Only the configured origin may apply the causing-item policy. For an observed
expiry, a non-terminal origin-homed cause transitions once to `failed` with
`cross_node_delivery_expired`. Missing and already-terminal causes still record
the observation and advance the cursor with an explicit resolution; they do
not wedge the poller or mutate another object's home. Acknowledged outcomes are
recorded as delivery evidence but do not create a target-object projection.

## 6. Identity, data, and failure boundaries

Tokens live in the Postgres of the node that minted them. A client that calls
node X directly holds an X-minted token. One agent operating on three homes has
three separately revocable tokens; a shared fleet or relay bearer is forbidden.
The root token never participates in peer delivery.

Approval separation of duties remains local to the approval's home. Approval
decisions cannot be queued or decided by proxy in Stage 1. A decision must land
directly on the home and be attributed to a qualifying home-minted human token.

The following never cross nodes in Stage 1: bearer/token material, token
hashes, root credentials, connection credential blobs, private message-part
bodies, secret files, or Postgres connections. Qualified ids, structural
metadata, allowlisted command bodies, and deliberately requested read results
may cross subject to the target token's scope and existing resource limits.

A partition leaves every node fully functional for local capture, reads,
worker ticks, and lifecycle changes to locally homed objects. Remote reads fail
within their request deadline. Remote commands either reach an approved queue
or produce a bounded delivery failure. The provider ingress being down has no
special effect on Stage 1 unless the operator explicitly chose that same node
as a direct or queue route.

## 7. Provider ingress boundary (Stage 2)

External providers need one stable HTTPS origin. The selected ingress exposes
the existing `meristem api` `/mcp` route through deployment-managed TLS; it is
not a new business-logic service. TLS termination, tunnels, reverse proxies,
and perimeter controls remain deployment choices and never enter core routing
logic.

Standard provider authentication is mandatory. Optional perimeter controls are
additive and never supply meristem actor identity. Provider-facing requests
resolve a per-client token in request context; proxy or access headers are not
identity. Public TLS is TLS 1.3 with HSTS and no plaintext fallback.

Stage 2 owns OAuth/provider compatibility and its own acceptance tests. HTTP MCP
remains read-only until its mutation idempotency and approval contract lands.
The gateway may read a reachable remote home through Stage 1 read-through, but
it does not make an inboundless home readable and does not turn the ingress
into a fleet authority.

## 8. Staged delivery

### Stage 0 — node identity and local registry projection

One node has a stable `node_id`; registry events and projection rebuild work on
that node; qualified references parse and format without changing existing
local UUID semantics.

### Stage 1 — two-node REST and queue networking

Stage 1 ships before and independently of provider ingress:

- one event-backed registry authority and authenticated snapshot distribution;
- normalized, validated `base_url` and `direct_url` origins;
- immutable object homes and qualified references;
- direct authenticated/idempotent REST reads and mutations;
- durable queue fallback for an explicitly configured inboundless target;
- narrow target-local spoke credentials and operation allowlists;
- terminal acknowledgement, retries, expiry, and patience escalation;
- replay/rebuild coverage for every registry and queue projection; and
- deployment/runbook coverage for two independent databases.

Stage 1 explicitly defers `remote_refs`, object re-homing implementation,
application-level relay, provider OAuth/registration, and HTTP MCP writes.

### Stage 2 — provider gateway

Stage 2 selects the ingress machine, exposes `/mcp` through public TLS, and
completes the provider OAuth/registration work. Its read-only provider smoke
tests are additive to Stage 1; they are not Stage 1 networking evidence.

## 9. Stage 1 acceptance

Stage 1 is tied off only when an automated or recorded two-node exercise proves
all of the following with nodes A and B using independent Postgres databases:

1. Each node's `events.seq` advances independently; rebuilding either node from
   only its own log reproduces its registry, queue, and domain projections.
2. A registry mutation on the registry home reaches the other node as one
   authenticated, validated, event-backed snapshot revision. A replay is a
   no-op; a stale, malformed, or wrong-source snapshot is refused.
3. URL tests reject paths, credentials, fragments, unsafe address classes,
   plaintext non-loopback origins, cross-origin redirects, and target mismatch.
4. An A client with a B-minted scoped token reads and mutates a B-homed work
   item over B's `direct_url`. Repeating the same idempotency key produces one
   B event/projection effect.
5. Removing B's inbound route causes an allowlisted B-homed mutation to be
   queued once on an explicit queue host, polled outbound by B, executed under
   the narrow B-local spoke token, and acknowledged terminally. Restarting B
   between fetch and acknowledgement produces no duplicate mutation.
6. A disallowed path, target mismatch, oversize payload, or narrower local or
   queue-host scope is refused before target mutation. A direct `401`, `403`,
   `409`, or `429` is not bypassed through the queue.
7. First terminal acknowledgement wins. Conflicting later acknowledgements do
   not alter the queue row or append a second state change.
8. With an accelerated test clock, direct retry exhaustion and queue expiry
   append their terminal events and drive the causing work item through its
   declared finite escalation. No pending network state survives past budget.
9. With the registry home and provider ingress unavailable, both nodes continue
   local operation from their own logs. Any still-approved direct route works;
   unavailable remote operations fail or queue within their stated budgets.
10. Logs, events, queue envelopes, registry snapshots, and test fixtures contain
    no bearer material, token hashes, connection credentials, private message
    bodies, or secret-file content.

Latency is observed but is not correctness. Initial p95 targets are 100 ms for
a local read, 150 ms for a local append, 800 ms for a direct peer command, and
the configured poll interval (default at most 30 seconds) for a healthy queued
command. Breaches are metrics; idempotency, authorization, single-writer homes,
and bounded patience are the acceptance invariants.

## 10. Explicit follow-up work

These are deliberately not hidden inside Stage 1:

- `remote_refs` cache design, invalidation, visibility, and rebuild semantics;
- object re-homing and immutable cross-log history/pointer semantics;
- application relay target/credential/refusal/loop/audit contract;
- optional per-origin queue actor to target-local token bindings;
- provider-facing OAuth and provider registration;
- HTTP MCP writes and their idempotency/approval behavior; and
- a larger-fleet discovery or routing model if route administration becomes
  material beyond the 2-5 node horizon.
