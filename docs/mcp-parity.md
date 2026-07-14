# MCP Parity Matrix

Status date: 2026-07-12. Source of truth remains the implementation plus
`docs/v0.md`; this matrix is the current gap map for the A-bar MCP parity item.

## Transport Policy

| Surface | Current status | Write policy |
| --- | --- | --- |
| REST | Canonical transport for every operation. | POST routes use bearer auth plus `Idempotency-Key`. |
| Stdio MCP | Full agent-oriented MCP surface, subject to token scopes. | Mutating tools require an MCP `idempotency_key` argument; handlers call the same services as REST. |
| HTTP MCP | Streamable HTTP POST; the API route still selects the read-only profile. | An explicit provider-tracker profile is available for narrow work-item writes, but is not active until API/OAuth integration deliberately selects it. |
| Provider-tracker HTTP MCP | Implemented transport policy, pending API/OAuth integration. | Allows only create, spawn child, append event, metadata, and transition through the actor-scoped MCP idempotency executor. Hidden tools are rejected before dispatch. |

## Capability Matrix

| Capability | REST | Stdio MCP | HTTP MCP today | Remaining gap |
| --- | --- | --- | --- | --- |
| Inbox capture | `POST /v1/inbox/messages` | `inbox.capture` | Hidden | HTTP MCP write policy, if this ever needs HTTP MCP. |
| Feed and backlog reads | `GET /v1/feed`, `GET /v1/backlog/readiness` | `feed.read`, `backlog.readiness` | Visible | None for read-only parity. |
| Work item reads | `GET /v1/work-items`, `GET /v1/work-items/{id}` | `work_items.list`, `work_items.get` | Visible | None for read-only parity. |
| Work item mutations | create, spawn child, append event, metadata, transition, convergence proposal, cultivar activation | `work_items.create`, `work_items.spawn_child`, `work_items.append_event`, `work_items.update_metadata`, `work_items.transition`, `convergence.propose_checks`, `registry.activate_cultivar` | API route remains read-only; provider-tracker profile implements the five tracker mutations only | Wire the profile after OAuth authority selection. Convergence and cultivar activation remain hidden. |
| Registry and projection reads | registry/projection GET routes | `registry.list`, `registry.get`, `projections.list`, `projections.get` | Visible when token policy allows | None for read-only parity. |
| Registry and projection writes | registry/projection POST routes | `registry.define_tropism`, `registry.define_cultivar`, `projections.define` | Rejected | HTTP MCP mutation idempotency if these are exposed over HTTP. |
| Approval reads | approval GET/list routes | `approvals.get`, `approvals.list_for_work_item` | Visible | None for read-only parity. |
| Approval request/decision | `POST /v1/work-items/{id}/approvals`, `POST /v1/approvals/{id}/decision` | `approvals.request`, `approvals.decide` | Rejected | HTTP MCP mutation policy only; stdio parity is closed. |
| OAuth client bind/revoke | `POST /v1/oauth/clients/{client_id}/actor`, `POST /v1/oauth/clients/{client_id}/revoke` | `oauth_clients.bind_actor`, `oauth_clients.revoke` | Rejected | None for stdio parity. Both transports require explicit non-root human `oauth_clients.bind` / `oauth_clients.revoke` scopes; provider HTTP profiles hide and reject both tools. |
| OAuth grant revoke | `POST /v1/oauth/grants/{grant_id}/revoke` | `oauth_grants.revoke` | Rejected | None for stdio parity. Reuses the `oauth_clients.revoke` scope (a grant is a strict subset of its client's authority); provider HTTP profiles hide and reject the tool. |
| Approval-gated HTTP connector request | `POST /v1/work-items/{id}/http-connector/actions` | `connectors.http_request` | Rejected | Retries/dead-lettering remain connector substrate work; HTTP MCP exposure still waits on mutation idempotency. |
| Artifact attachment | Not shipped | Not shipped | Not shipped | Open substrate and parity gap. Do not advertise placeholder tools. |
| Signals ingress | `POST /v1/signals` | Intentionally absent | Intentionally absent | REST-only external-system ingress; not an MCP parity bug. |
| Feed stream | `GET /v1/feed/stream` | `feed.read` with cursor/wait | HTTP MCP GET SSE unavailable | Transport capability, not a separate domain operation. |

## Attribution And Client Config

Stdio MCP reads `MERISTEM_TOKEN` from the process environment. Each agent
instance should get its own token row, so events carry that token id as actor.
Cursor compatibility mode may advertise underscore aliases, but dispatch accepts
canonical dot names and aliases against the same actor.

HTTP MCP authenticates with the request bearer token. Shared client JSON must
not embed bearer secrets; use local environment, OS credential storage, or an
operator-owned private config path. If HTTP MCP mutation support lands later,
the transport must keep per-client attribution and must not make a shared
remote bearer a substitute for individual agent tokens.

The provider-tracker HTTP profile fails closed against latent execution
authority. Created children/items must be explicitly `human_review_status=blocked`
and cannot name a cultivar; metadata writes cannot wave work through; transitions
may only block or terminalize work. This is intentionally narrower than the
stdio surface. Broader tracker transitions require a durable, domain-level
non-dispatchable marker that every worker reconciler enforces, rather than an
assumption that job execution will remain paused.

## Linked Work

- `3eb5c8c4-f0f9-5720-8c65-2c949252074c` — A-bar MCP parity gap map.
- `5e96aefb-9a57-51f1-b107-83ffcbb526f8` — full-featured MCP parity umbrella.
- Artifact substrate remains open under the v1 substrate list.
- `34a34c86-2e1c-51a6-bb34-175e52488dae` — provider tracker HTTP mutation
  profile and idempotency proof; API/OAuth integration remains separate.

## Local-Agent HTTP MCP Parity Audit (2026-07-14, item `4473e765`)

This section is the first slice of item
`4473e765-a3b9-5714-aa61-a142fd063567` ("migrate local agent MCP access to the
server's HTTP MCP surface; retire spawned-instance wrappers"). The endorsed
direction is that a local agent (Claude, Codex, Cursor — `source=agent`, broad
or scoped local policy) becomes an ordinary MCP-over-HTTP client of the one
running server instead of spawning its own `meristem mcp` stdio instance. This
audit fixes exactly which tools reach a local agent token on each transport
today, and which gaps must wait on the two owner-court items:

- `95c24a80-f8f9-5b1e-9525-150d037b3841` — HTTP MCP mutations: durable replay
  and write-policy gate (whether/how mutating tools run over HTTP).
- `07417203-ea12-5139-9e06-46f681a08e8a` — transport compatibility: POST vs
  GET/SSE.

Their decisions are out of scope for this slice and are not pre-empted here.

### How a token reaches an HTTP tool today

The API mounts one `/mcp` route (`internal/api/mcp.go`). It authenticates the
request bearer, then calls `providerHTTPProfile(actor)` to pick a
`*mcp.HTTPToolProfile`:

- A token carrying one valid sealed `provider.profile:*` marker maps to the
  matching provider profile (`ProviderSafeReadHTTPProfile` for the read
  profiles, `ProviderTrackerHTTPProfile` for the write profiles).
- A token carrying a marker that fails to parse fails closed with
  `403 provider_authority_denied`.
- **A token with no provider marker — i.e. every ordinary local agent token,
  legacy scope-less broad or scoped worker — falls through to
  `ProviderSafeReadHTTPProfile()`.** This is the crux of the parity gap: there
  is no *local-agent* HTTP profile. Local tokens are routed onto the
  provider-safe read profile, which advertises only
  `feed.read, backlog.readiness, work_items.list, work_items.get` and reshapes
  their responses through the provider-safe context reducer
  (`provider_safe_feed.v1`, `provider_safe_work_items.v1`).

On both transports the same `access.ToolVisible(actor, tool)` reducer runs
first; the HTTP path then *intersects* that visibility with the profile
allowlist (`handleListToolsFiltered`) and rejects any non-allowlisted
`tools/call` before dispatch (`checkHTTPToolAllowed`). So the HTTP surface for a
local token is `stdio-visible-tools ∩ {the four provider-safe reads}` — never
wider, regardless of how broad the token's scopes are.

### Parity table (local agent token)

Two representative local tokens:

- **Broad** — legacy scope-less `source=agent` token (empty `Scopes`, not root);
  `access.legacyUnscoped` grants the legacy broad surface until rotation.
- **Scoped** — a read-tree worker: `source=agent`, `Scopes = {work_items.read,
  feed.read_assigned, work_items.tree:<uuid>}`, not root.

"HTTP" is what either local token gets over `/mcp` today (the provider-safe read
fallback). `✓/✗` is advertisement **and** dispatch (they move together on both
transports). Human-only tools are unreachable by any `agent` token on either
transport and are listed for completeness.

| Tool | R/M | stdio (broad) | stdio (scoped) | HTTP (local) | Gap reason |
| --- | --- | --- | --- | --- | --- |
| `feed.read` | R | ✓ | ✓ | ✓ | Availability parity. DTO reshaped to `provider_safe_feed.v1`; cursor/`wait` streaming (SSE) blocked on `07417203`. |
| `backlog.readiness` | R | ✓ | ✓ | ✓ | Availability parity (in the provider-safe read allowlist). |
| `work_items.list` | R | ✓ | ✓ | ✓ | Availability parity. DTO reshaped to `provider_safe_work_items.v1` (narrower than the stdio operator DTO). |
| `work_items.get` | R | ✓ | ✓ | ✓ | Availability parity. DTO reshaped to `provider_safe_work_items.v1`. |
| `registry.list` | R | ✓ | ✓ | ✗ | **Recorded gap.** No local-agent HTTP profile; local tokens fall back to the provider-safe read profile, which deliberately excludes the registry surface. Not separable into "never wired" (see decision below). |
| `registry.get` | R | ✓ | ✓ | ✗ | Same as `registry.list`. |
| `projections.list` | R | ✓ | ✓ | ✗ | Same — projection reads excluded from provider-safe read; no local profile. |
| `projections.get` | R | ✓ | ✓ | ✗ | Same. |
| `approvals.get` | R | ✓ | ✓ | ✗ | Same — approval surface excluded from provider-safe read; no local profile. |
| `approvals.list_for_work_item` | R | ✓ | ✓ | ✗ | Same. |
| `deterministic_errors.list` | R | ✓ | ✗ (needs `logs.*`) | ✗ | Excluded from provider-safe read; on stdio also `logs.*`-scoped, so the scoped read-worker never sees it either. |
| `deterministic_errors.get` | R | ✓ | ✗ (needs `logs.*`) | ✗ | Same. |
| `inbox.capture` | M | ✓ | ✗ | ✗ | Mutation over HTTP → blocked on `95c24a80` write gate. Not in any local-reachable profile. (Scoped path is human-source anyway.) |
| `work_items.create` | M | ✓ | ✗ | ✗ | Exists over HTTP only for provider *write*-marker tokens via `ProviderTrackerHTTPProfile`. Extending to local tokens is the write-policy decision → `95c24a80`. |
| `work_items.spawn_child` | M | ✓ | ✗ | ✗ | Provider write-marker only; local surface blocked on `95c24a80`. |
| `work_items.append_event` | M | ✓ | ✗ | ✗ | Provider write-marker only; local surface blocked on `95c24a80`. |
| `work_items.update_metadata` | M | ✓ | ✗ | ✗ | Provider write-marker only; local surface blocked on `95c24a80`. |
| `work_items.transition` | M | ✓ | ✗ | ✗ | Provider write-marker only; local surface blocked on `95c24a80`. |
| `convergence.propose_checks` | M | ✓ | ✗ | ✗ | Hidden even from the provider tracker profile; blocked on `95c24a80`. |
| `registry.activate_cultivar` | M | ✓ | ✗ | ✗ | Mutation; hidden from provider profiles; blocked on `95c24a80`. |
| `registry.define_tropism` | M | ✓ | ✗ | ✗ | Mutation; blocked on `95c24a80`. |
| `registry.define_cultivar` | M | ✓ | ✗ | ✗ | Mutation; blocked on `95c24a80`. |
| `projections.define` | M | ✓ | ✗ | ✗ | Mutation; blocked on `95c24a80`. |
| `approvals.request` | M | ✓ | ✗ | ✗ | Mutation; blocked on `95c24a80`. |
| `connectors.http_request` | M | ✓ | ✗ | ✗ | Mutation with an approval-gated external side effect; blocked on `95c24a80` and connector substrate. |
| `approvals.decide` | M | ✗ (human-only) | ✗ | ✗ | Human, non-root only on both transports; HTTP write also gated by `95c24a80`. |
| `policy_profile.switch` | M | ✗ (human-only) | ✗ | ✗ | Human, non-root only on both transports; HTTP write also gated by `95c24a80`. |
| `oauth_clients.bind_actor` | M | ✗ (human-only) | ✗ | ✗ | Human, non-root, scoped only; provider HTTP profiles hide and reject it; HTTP write gated by `95c24a80`. |
| `oauth_clients.revoke` | M | ✗ (human-only) | ✗ | ✗ | Same as `oauth_clients.bind_actor`. |
| `oauth_grants.revoke` | M | ✗ (human-only) | ✗ | ✗ | Same (reuses the `oauth_clients.revoke` scope). |

Summary for a local agent token: stdio advertises the full read surface (12
reads for a broad token, 10 for the scoped read-worker) plus its permitted
mutations; HTTP advertises exactly the four provider-safe reads and rejects
everything else. Every read gap collapses to one root cause — **no local-agent
HTTP profile exists** — and every mutation gap collapses to the `95c24a80`
write gate. `07417203` bites only the streaming/SSE shape of `feed.read` and the
`405` on `GET /mcp`.

### Safe-implementation decision: recorded as a gap, nothing wired over HTTP

The read tools stdio advertises for a local token but HTTP does not
(`registry.*`, `projections.*`, `approvals.get`, `approvals.list_for_work_item`,
`deterministic_errors.*`) are **not** missing because someone wired a
local-agent read surface and forgot them. They are missing because there is no
local-agent HTTP surface at all: the API routes every unmarked token onto
`ProviderSafeReadHTTPProfile`, whose doc comment deliberately excludes the
registry, approval, policy, inbox, connector, convergence, and
deterministic-error surfaces, and whose selection also flips on the
provider-safe context reducer so responses come back reshaped rather than as the
stdio operator DTOs.

Building genuine local parity therefore cannot be cleanly separated from
"deliberately gated," so per the item's instruction this slice **wires no new
tools over HTTP**. Doing it right requires three parent-item decisions this
slice must not make:

1. A way to classify a bearer as *local agent* vs *provider* at the HTTP layer
   (today the only signal is the presence/absence of a sealed
   `provider.profile:*` marker, which is a provider concept).
2. Decoupling the provider-safe context reducer from "any profile is set":
   `HandleHTTPMessageWithOptions` applies `withProviderSafeContext` whenever
   `opts.Profile != nil`, so a local read profile that returned ordinary
   (stdio-parity) DTOs would have to change provider-safe-adjacent behavior —
   explicitly out of scope here.
3. Whether any mutating tool rides the same surface (`95c24a80`) and which
   transport verbs it uses (`07417203`).

What this slice does land is a parity **guard** test
(`internal/mcp/http_local_parity_integration_test.go`) that pins the current
contract so the parent item cannot widen the surface silently:

- Advertisement: a broad local token sees the full read surface on stdio but
  exactly `{feed.read, backlog.readiness, work_items.list, work_items.get}` over
  the provider-safe read fallback — the four-tool cap that a local token hits
  today.
- Denied tools append no events: `registry.list` (a stdio-visible read) and
  `work_items.transition` (a mutation) are both rejected at the HTTP profile
  boundary and leave the `events` table byte-count unchanged.
- Per-agent attribution: an event created over the one already-permitted HTTP
  write path (the provider-tracker profile) carries `actor_token_id` equal to
  the calling bearer's token id, and two distinct bearers produce two distinct
  actor attributions — the property local agents must inherit once writes are
  enabled.

### Attribution and per-agent auth: HTTP vs stdio

- **stdio** reads `MERISTEM_TOKEN` from the process environment once, at launch
  (`Server.Authenticate`), and stores that one token row as the actor for every
  call the process makes (`s.actorToken()`). Per-agent attribution therefore
  requires each stdio agent instance to be launched with its **own**
  `MERISTEM_TOKEN`. Denied calls return `insufficient_scope` before the handler
  runs, so they append no events.
- **HTTP** takes the bearer per request in `mcpProtected`: `mcpat_*` secrets go
  through `oauthTokens.AuthenticateAccess(secret, <base>/mcp)` (audience-bound
  OAuth access tokens), everything else through `authenticator.Authenticate`.
  The resolved token is put on the request context and passed as `actor` into
  `dispatchWithActor`, so attribution is per-request-bearer and is naturally
  per-agent when each agent holds its own token — no shared process-wide
  `MERISTEM_TOKEN` is involved. Events append with `actor_token_id` = that
  bearer's token id (proven for the provider-tracker write path; reads append
  nothing).

Both transports call the identical `access.ToolVisible` reducer and the identical
domain services; HTTP layers the profile allowlist and the provider-safe context
reducer on top. Attribution parity already holds for whatever a local token is
permitted to run over HTTP today; only the *breadth* of that permitted set is
the gap.

### Local-agent client config, once parity exists

Today a local agent launches its own stdio server, e.g. a Claude/Cursor MCP
entry that runs the binary with a per-agent token in the environment:

```jsonc
// stdio (today): one meristem instance per agent, spawned over stdio
{
  "mcpServers": {
    "meristem": {
      "command": "meristem",
      "args": ["mcp"],
      "env": { "MERISTEM_TOKEN": "<this agent's own token>" }
    }
  }
}
```

Once local HTTP parity lands, the same agent becomes a plain HTTP MCP client of
the already-running server — no spawned instance, one shared process, per-agent
bearer:

```jsonc
// HTTP (target): ordinary MCP-over-HTTP client of the one running server
{
  "mcpServers": {
    "meristem": {
      "type": "http",
      "url": "https://<host>/mcp",
      "headers": { "Authorization": "Bearer <this agent's own token>" }
    }
  }
}
```

The bearer must never be a shared remote secret standing in for individual
agent tokens; per-agent attribution depends on each client presenting its own
token. Client JSON must not embed the secret in a shared/committed file — use
local environment, OS credential storage, or an operator-owned private config
path. `POST /mcp` requires `Accept: application/json, text/event-stream`;
`GET /mcp` (server-initiated SSE) remains `405` pending `07417203`.

### Dev-instance exception path

Spawning a local `meristem mcp` instance from a worktree remains a **labeled
exception**, never the default: it exists only to exercise unmerged tool changes
against a throwaway or dev database before they reach the one running server,
and it must be recorded as such (per AGENTS.md's external-worker rules — target
workspace, allowed areas, worktree base, work item, and `human_review_status`).
Once a tool change is merged, local agents reach it as ordinary HTTP MCP clients
of the running server; the spawned-instance wrapper is not a parallel
production surface. The full policy for retiring the wrappers belongs to the
parent item, not this slice.
