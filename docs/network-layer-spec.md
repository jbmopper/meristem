# Network layer spec

Status: draft for owner review (work item `66eb0aed`, from owner direction
`90359fc5`), updated after owner correction on 2026-07-06: the registered
DNS/MCP box is ingress, not the topology's durable center. Scope: inter-node
communication for a one-operator fleet of 2-5 meristem nodes, external
ingress, and the seat for the provider-facing MCP gateway (`a03f644e`).
Converges with thread `4c00df15` (HTTP Streamable MCP auth/client config);
makes **no** auth/client-config decisions of its own. If this conflicts with
`docs/spec.md`, the spec wins.

## 1. Topology: mesh-capable fleet with a registered ingress

**Decision: mesh-capable, with one registered ingress.** The DNS-registered
box exists because external providers need a stable URL for `/mcp`. It is a
rendezvous/ingress point, not the permanent authority for the fleet. M4,
laptop, and home-server nodes remain peers at the data-model level: each owns
its local event log, and cross-node communication uses whichever approved HTTP
route is currently reachable. Direct node-to-node paths are allowed when they
exist; ingress relay is the default for provider-facing and NAT-constrained
paths, not the definition of the topology.

| Dimension | Permanent hub-and-spoke | Mesh-capable with registered ingress (chosen) |
| --- | --- | --- |
| Connectivity | Every remote operation depends on the hub being reachable | Provider ingress has one stable URL; peer routes may be direct, relayed, or temporarily absent |
| New core dependencies | None beyond HTTPS, but the hub becomes a behavioral center | None beyond HTTPS; optional operator deployment may add tunnels or overlay routes outside core |
| Consistency pressure | Single-writer-per-object falls out naturally, but hub language invites centralization | Single-writer-per-object remains explicit; mesh reachability does not imply multi-master |
| Operator burden (one human) | 1 tunnel, 1 DNS name, 1 ingress policy | 1 registered ingress for providers; additional peer paths only where they reduce fragility |
| Peer latency | 2 hops via hub | 1 hop when a direct approved route exists, otherwise relay/poll |
| Failure mode | Hub down => cross-node ops pause | Ingress down => provider ingress pauses; local nodes keep operating and peer paths can continue if available |

Committed for the 2-5 node horizon. The system is a mesh of event-log-owning
nodes with an ingress seat for provider MCP. Revisit only if the fleet grows
past ~5 nodes or peer-route management becomes the dominant problem — that
revisit is a new work_item, not silent drift. Inter-node transport is the
**existing REST surface** (principle: REST is canonical; transports never own
business logic). No gRPC, no message bus, no Postgres-level replication
between nodes.

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
home node over the best available approved route (direct peer route, ingress
relay, or queued pull) with an `Idempotency-Key` — the existing middleware
makes retries safe (converges with `beac80e1`: same idempotency machinery, no
parallel contract).

**What crosses nodes:**

- *Commands* — ordinary REST POSTs to the home node, bearer-authenticated,
  idempotency-keyed, carried by the currently available route.
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

**Commands to nodes without inbound reachability.** Some nodes, including the
M4 during interim bring-up, may accept no inbound route. A peer cannot push a
command to such a node. Instead, the reachable side **queues the command
durably** (a `command.queued` event projected into a per-target command queue);
the target node **polls its queue outbound** on its feed-poll cadence, executes
the command locally against its own log (with the original idempotency key, so
replays collapse), and acknowledges by POSTing the outcome back on its next
poll. Consequence for latency: cross-node commands to pull-only targets are
bounded by the target's poll interval (default <= 30s), not the sub-second
direct-route budget — §5 reflects this. Stage 1 may ship without the command
queue by declaring pull-only objects **locally mutable only**; that restriction
must be explicit in the stage 1 exit criteria, not implicit.

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

## 3. Ingress: registered box for provider MCP

- **Termination.** Public TLS terminates at the chosen ingress provider;
  `cloudflared` on the registered box is the current candidate and forwards
  to `meristem api` on `localhost:8080`. The api binary is unchanged and
  never listens publicly. The tunnel is **deployment configuration, not
  core** (AGENTS.md portability rule): swapping to Caddy + DNS, WireGuard,
  Tailscale Funnel, or a plain VPS reverse proxy is a redeploy, not a code
  change. Nothing in `internal/` may reference Cloudflare.
- **Auth composition.** Two independent layers, additive, never
  substitutive: (outer) optional Cloudflare Access policy / service tokens
  as a perimeter; (inner) mandatory meristem bearer (`mrs_` PAT) — the only
  source of identity. Attribution comes from the resolved token in request
  context; `Cf-Access-*` headers are never read for identity (spec
  principle 5). Losing the perimeter layer degrades to bearer-only, which
  must remain sufficient.
- **Ingress instability is assumed.** The registered box is allowed to be
  flaky or absent during interim M4 bring-up. That failure pauses registered
  provider MCP and any routes that depend on that box; it must not prevent the
  M4 from running its own local meristem or from communicating over another
  explicitly configured peer route.
- **Nodes may be inboundless.** M4 and laptop nodes can start with no inbound
  connections — no tunnel, no port-forward, api bound to loopback. They use
  outbound polling for queued commands until a direct peer route is explicitly
  configured.

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

This layer adds only: the provider-facing gateway URL points at the
**registered ingress**; gateway tool calls touching remote-homed objects
resolve via §2 (read-through, qualified refs, direct route, or queued pull),
invisible to the provider client.

## 5. Latency budgets

| Operation | Budget (p95) | Notes |
| --- | --- | --- |
| Local feed read (same node) | ≤ 100 ms | loopback; unchanged from today |
| Local event append (POST) | ≤ 150 ms | includes synchronous projection writers |
| Ingress read via tunnel (peer or gateway client) | <= 500 ms | edge RTT + tunnel hop + ingress processing |
| Cross-node command over direct reachable route | <= 800 ms | one HTTP route; idempotent retry on breach |
| Cross-node command to pull-only target | <= target poll interval (default 30s) | durable queue + outbound poll; not a sub-second path |
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
- **Stage 1 — two nodes.** The registered ingress box exposes provider MCP;
  M4 (item `575414ca` seed) joins as a peer that may be pull-only at first.
  Ship: nodes registry, qualified refs, cross-node POST over whichever route
  is available, Idempotency-Key, outbound polling for pull-only targets;
  `remote_refs` cache optional (stage 1b). Stage 1 restriction, explicit:
  pull-only objects are **locally mutable only** until the command queue
  lands. Exit: a work_item homed on the ingress node is claimed and progressed
  by an agent on the M4 using an M4-agent token minted on the home node
  (per-node identity, §2); killing ingress leaves both nodes fully functional
  locally and does not make either log unreplayable.
- **Stage 2 — gateway live.** `/mcp` on the hub exposed through the tunnel,
  registered as a remote connector per the `4c00df15` auth decision.
  Read-only until `edf84c83` closes. Exit: vanilla Claude (and one second
  provider) reads the feed and work_items through the registered gateway,
  each client on its own token; zero secrets in any shared client config.

## 7. Open questions for the owner

1. **Ingress candidate.** Which physical box gets the registered DNS/MCP
   ingress first, knowing it is allowed to be unstable and replaceable?
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
