# Network layer spec

Status: draft for owner review (work item `66eb0aed`, from owner direction
`90359fc5`). Scope: inter-node communication for a one-operator fleet of 2–5
meristem nodes, external ingress, and the seat for the provider-facing MCP
gateway (`a03f644e`). Converges with thread `4c00df15` (HTTP Streamable MCP
auth/client config); makes **no** auth/client-config decisions of its own.
If this conflicts with `docs/spec.md`, the spec wins.

## 1. Topology: hub-and-spoke gateway, not mesh

**Decision: hub-and-spoke.** The home server (behind the Cloudflare tunnel)
is the hub. Every other node (M4 Mac, laptop) is a spoke that makes
**outbound-only HTTPS** to the hub's public URL. There is no spoke-to-spoke
path; spoke↔spoke traffic relays through the hub.

| Dimension | Full mesh (N×N) | Hub-and-spoke (chosen) |
| --- | --- | --- |
| Connectivity | Every node needs to reach every node; laptops need inbound holes or a full overlay (WireGuard/tailnet) | Spokes are outbound-only; exactly one public surface |
| New core dependencies | Overlay network / membership protocol | None — plain HTTPS against the existing REST API |
| Consistency pressure | Invites multi-master thinking | Single-writer-per-object falls out naturally (§2) |
| Operator burden (one human) | N tunnels, N certs, N token sets | 1 tunnel, 1 DNS name, 1 ingress policy |
| Spoke↔spoke latency | 1 hop | 2 hops via hub — acceptable at N ≤ 5 |
| Failure mode | Partial partitions, hard to reason about | Hub down ⇒ cross-node ops pause; every node stays fully functional locally |

Committed for the 2–5 node horizon. Revisit only if the fleet grows past ~5
nodes or spoke↔spoke volume dominates — that revisit is a new work_item, not
a silent drift. Inter-node transport is the **existing REST surface**
(principle: REST is canonical; transports never own business logic). No
gRPC, no message bus, no Postgres-level replication between nodes.

## 2. Cross-node consistency model

**Per-node truth.** Each node owns exactly one append-only `events` log with
its own monotonic `events.seq`. `seq` is a per-node cursor and is never
merged, compared, or totaled across nodes. There is no global log, no global
ordering, no consensus round. A node's projections are rebuilt only from its
own log — replay determinism (spec § Idempotency) is a per-node property and
stays that way.

**Object homes (single writer).** Every `work_item`, `message`, `approval`,
and their event subtrees have exactly one *home node*: the node where the
object was created. Events about an object are appended **only** on its home
node. A node that wants to mutate a remote object issues an HTTP POST to the
home node (relayed via hub) with an `Idempotency-Key` — the existing
middleware makes tunnel retries safe (converges with `beac80e1`: same
idempotency machinery, no parallel contract).

**What crosses nodes:**

- *Commands* — ordinary REST POSTs to the home node, bearer-authenticated,
  idempotency-keyed.
- *Reads* — read-through GETs against the home node (feed, work_item get,
  projections).
- *Remote-ref projections (optional, stage 1b)* — a spoke may pull
  `feed`/`events` incrementally from the hub using the existing seq cursor
  and materialize a clearly-marked `remote_refs` cache: derived, rebuildable,
  never truth, never input to local projection writers as if local.
  Pull-based only; the hub never pushes (spokes have no inbound surface).

**Naming.** Each node registers a stable, DNS-safe `node_id` (e.g. `den`,
`m4`) via a `node.registered` event on the hub, projected into a `nodes`
registry (base URL, public key/fingerprint, status). Cross-node references
use the qualified form `<node_id>:<uuid>` (canonical URI
`mrs://<node_id>/work-items/<uuid>`) in event payloads and relations.
Unqualified UUIDs always mean "this node." Payloads carrying qualified refs
use payload versioning per `docs/payload-versioning.md`.

**What never crosses:** bearer token material and token hashes;
`connections` credentials; `.meristem/*.token` and any `.env*` content;
`message_parts` bodies (private message content — only ids and metadata may
appear in cross-node refs); approval *decisions* by proxy (a decision POST
must land on the home node with the deciding human token — no relay caching,
separation of duties intact). Postgres is co-located per node and **port
5432 never crosses a node boundary** — no tunneled DSNs, resolving
`fc6a83f8`: inter-node traffic is HTTP API only.

**Commands to spoke-homed objects (the relay's missing half).** Spokes
accept no inbound (§3), so the hub cannot push a command to a spoke-homed
object. Instead: the hub **queues the command durably** (a `command.queued`
event on the hub, projected into a per-spoke command queue); the spoke
**polls its queue outbound** on its feed-poll cadence, executes the command
locally against its own log (with the original idempotency key, so replays
collapse), and acknowledges by POSTing the outcome back to the hub on its
next poll. Consequence for latency: cross-node commands to spoke-homed
targets are bounded by the spoke's poll interval (default ≤ 30 s), not the
800 ms relay budget — §5 reflects this. Stage 1 may ship without the
command queue by declaring spoke-homed objects **locally mutable only**;
that restriction must be explicit in the stage 1 exit criteria, not
implicit.

**Cross-node identity.** Tokens live in each node's own Postgres; there is
no shared token store. An actor mutating an object on node X authenticates
with a token **minted by node X** — a spoke-based agent that needs to write
hub-homed objects holds its own hub-minted token, and vice versa. The §4
rule extends to relays: one token per agent **per node**, never a shared
relay token — a shared relay bearer would collapse attribution exactly
where it matters most (writes crossing trust boundaries). Queued commands
(above) record the originating actor's token id on the hub *and* execute
under the spoke-local agent token, with both ids in the command's event
trail.

**Partition behavior.** Tunnel down ⇒ each node keeps full local function
(capture, triage, worker ticks). Cross-node commands queue as ordinary
work_items on the issuing node under bounded patience: a stalled cross-node
call gets a patience budget and an escalation rule like any non-terminal
state. No new infrastructure; the convergence loop already models this.

## 3. Ingress: Cloudflare tunnel to the home server

- **Termination.** Public TLS terminates at the Cloudflare edge;
  `cloudflared` on the home server holds an outbound connection and forwards
  to `meristem api` on `localhost:8080`. The api binary is unchanged and
  never listens publicly. The tunnel is **deployment configuration, not
  core** (AGENTS.md portability rule): swapping to Caddy + DNS, WireGuard,
  or a plain VPS reverse proxy is a redeploy, not a code change. Nothing in
  `internal/` may reference Cloudflare.
- **Auth composition.** Two independent layers, additive, never
  substitutive: (outer) optional Cloudflare Access policy / service tokens
  as a perimeter; (inner) mandatory meristem bearer (`mrs_` PAT) — the only
  source of identity. Attribution comes from the resolved token in request
  context; `Cf-Access-*` headers are never read for identity (spec
  principle 5). Losing the perimeter layer degrades to bearer-only, which
  must remain sufficient.
- **Spokes expose nothing inbound.** M4 and laptop nodes accept no inbound
  connections — no tunnel, no port-forward, api bound to loopback. They
  reach the hub outbound-only and poll/long-poll (`GET /v1/feed` with
  cursor, within `internal/safety` feed-wait limits) for anything
  hub-originated.

## 4. Seat for the external MCP gateway (`a03f644e`)

The gateway is **not a new service**: it is the hub's `/mcp` Streamable-HTTP
route (from thread `4c00df15`) exposed through the tunnel and registered
with providers (Claude, ChatGPT). This spec assigns it a place in the
topology and nothing else. It **inherits, unmodified, from `4c00df15` and
its interjections**:

1. Transport shape — `/mcp` on the already-running `meristem api`, same
   domain dispatch as stdio, not a REST proxy.
2. The auth option decision (static bearer / session exchange / local broker
   / OAuth) — still open in that thread; this spec does not pick. Note for
   that thread: provider-registered remote connectors will likely force the
   OAuth option forward — that re-prioritization belongs to `4c00df15`.
3. Client config shapes (Codex/Cursor/Claude Code stanzas) and the **no
   secrets in shared JSON** rule (`0814fe00`).
4. The read-only gate: mutating tools stay rejected on HTTP MCP until the
   mutation idempotency contract (`edf84c83`) lands (`beac80e1`;
   `docs/mcp-parity.md` Transport Policy).
5. Per-session bearer resolution for attribution — one token per agent,
   never a shared remote bearer.

This layer adds only: the gateway URL points at the **hub**; gateway tool
calls touching remote-homed objects resolve via §2 (read-through or
qualified refs), invisible to the provider client.

## 5. Latency budgets

| Operation | Budget (p95) | Notes |
| --- | --- | --- |
| Local feed read (same node) | ≤ 100 ms | loopback; unchanged from today |
| Local event append (POST) | ≤ 150 ms | includes synchronous projection writers |
| Hub read via tunnel (spoke or gateway client) | ≤ 500 ms | edge RTT + tunnel hop + hub processing |
| Cross-node command to a hub-homed object (spoke → hub append) | ≤ 800 ms | one relay hop; idempotent retry on breach |
| Cross-node command to a spoke-homed object (hub queue → spoke poll) | ≤ spoke poll interval (default 30 s) | durable queue + outbound poll; not a sub-second path |
| Cross-node reference resolution — cached in `remote_refs` | ≤ 100 ms | local projection read |
| Cross-node reference resolution — cache miss (read-through) | ≤ 750 ms | falls back to hub read |
| Remote-ref projection staleness | ≤ 30 s steady-state | pull interval configurable; staleness is visible, never silent |

Breaching a budget is an observability signal (spec § Observability
metrics), not a correctness failure — correctness rests on idempotency and
single-writer homes.

## 6. Staged rollout

- **Stage 0 — today (single node).** One Postgres, one api on :8080, stdio
  MCP + read-only HTTP MCP. Adopt now, cheaply: assign this node its
  `node_id` via `node.registered` so every future reference is stable;
  reserve the qualified-ref payload shape. Exit: node registry projection
  exists with one row.
- **Stage 1 — two nodes.** Home server becomes hub behind the Cloudflare
  tunnel; M4 (item `575414ca` seed) joins as outbound-only spoke. Ship:
  nodes registry, qualified refs, cross-node POST relay with
  Idempotency-Key, spoke feed polling; `remote_refs` cache optional (stage
  1b). Stage 1 restriction, explicit: spoke-homed objects are **locally
  mutable only** — the hub command queue is stage 1b+. Exit: a work_item
  homed on the hub is claimed and progressed by an agent on the M4 using an
  M4-agent token minted on the hub (per-node identity, §2); killing the
  tunnel leaves both nodes fully functional locally; replaying either
  node's log reproduces its projections byte-for-byte.
- **Stage 2 — gateway live.** `/mcp` on the hub exposed through the tunnel,
  registered as a remote connector per the `4c00df15` auth decision.
  Read-only until `edf84c83` closes. Exit: vanilla Claude (and one second
  provider) reads the feed and work_items through the registered gateway,
  each client on its own token; zero secrets in any shared client config.

## 7. Open questions for the owner

1. **Hub permanence.** Is the home server the hub indefinitely, with hub
   relocation handled as a manual redeploy (migration-runbook style), or
   must the design keep hub re-homing cheap from day one?
2. **Perimeter on `/mcp`.** Require Cloudflare Access service tokens in
   front of the gateway route, or bearer-only? Provider connector clients
   may not support Access headers — bearer-only for `/mcp` with Access on
   everything else is the likely compromise; confirm.
3. **Stage 1 caching.** Start pure read-through (simpler, chattier) and add
   the `remote_refs` projection only when latency data demands it —
   acceptable?
4. **Object re-homing.** Moving a work_item's home node: permanently out of
   scope, or reserve a `work_item.migrated` event kind now so the door stays
   open?
