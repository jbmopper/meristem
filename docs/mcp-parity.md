# MCP Parity Matrix

Status date: 2026-07-17. Source of truth remains the implementation plus
`docs/v0.md`; this matrix records the current transport contract.

## Transport Policy

| Surface | Current status | Write policy |
| --- | --- | --- |
| REST | Canonical transport for every operation. | POST routes use bearer auth plus `Idempotency-Key`. |
| Stdio MCP | Full agent-oriented MCP surface, filtered by token policy and object access. | Mutating tools require an MCP `idempotency_key` argument and use the durable idempotency executor. |
| Local HTTP MCP | An unmarked, non-root `source=agent` static bearer gets ordinary stdio-equivalent dispatch and DTOs over Streamable HTTP POST. Tool advertisement, calls, and object reads/writes use the same token policy as stdio. | Mutating tools use the same actor-scoped MCP `idempotency_key` executor as stdio. |
| Provider-safe HTTP MCP | A bearer carrying one exact sealed `provider.profile:*` marker gets the matching provider read or tracker-write profile and provider-safe DTO rendering. | Tracker-write profiles allow only their narrow, validated, idempotent coordination mutations. Unknown, malformed, or ambiguous markers fail closed. |

`GET /mcp` remains `405` until server-initiated SSE support lands. POST clients
must send `Accept: application/json, text/event-stream`.

## Capability Matrix

The local HTTP column describes unmarked static bearers. Their exact surface is
not a separate allowlist: it is the stdio surface after the same
`access.ToolVisible` and object-level reducers have run.

| Capability | REST | Stdio MCP | Local HTTP MCP | Provider HTTP MCP |
| --- | --- | --- | --- | --- |
| Inbox capture | `POST /v1/inbox/messages` | `inbox.capture` | Same as stdio when token policy permits | Hidden |
| Feed and backlog reads | `GET /v1/feed`, `GET /v1/backlog/readiness` | `feed.read`, `backlog.readiness` | Same as stdio; ordinary DTO | Provider-safe structural DTO |
| Work item reads | `GET /v1/work-items`, `GET /v1/work-items/{id}` | `work_items.list`, `work_items.get` | Same as stdio; ordinary DTO and object filtering | Provider-safe structural DTO and sealed authority filtering |
| Work item mutations | create, spawn child, append event, metadata, transition, convergence proposal, cultivar activation | Matching MCP tools | Same as stdio when token policy permits | Only the five validated tracker mutations on tracker-write profiles |
| Registry and projection reads | Registry/projection GET routes | `registry.*`, `projections.*` reads | Same as stdio when token policy permits | Hidden |
| Registry and projection writes | Registry/projection POST routes | Define/activate tools | Same as stdio when token policy permits | Hidden |
| Approval reads and requests | Approval GET/POST routes | Approval tools | Same as stdio when token policy permits | Hidden |
| Approval decisions | `POST /v1/approvals/{id}/decision` | `approvals.decide` | Policy-authorized human, non-root tokens, including legacy unscoped tokens until rotation | Hidden |
| OAuth administration | OAuth client/grant routes | OAuth administration tools | Human, non-root, explicitly scoped tokens only | Hidden |
| Approval-gated HTTP connector | Connector action route | `connectors.http_request` | Same as stdio when token policy permits; external writes remain approval-gated | Hidden |
| Signals ingress | `POST /v1/signals` | Intentionally absent | Intentionally absent | Intentionally absent |
| Feed stream | `GET /v1/feed/stream` | `feed.read` cursor/wait | POST request/response only; GET/SSE pending | POST request/response only; GET/SSE pending |

## Local Static Bearer Contract

`internal/api/mcp.go` authenticates each HTTP request and passes the resolved
token into the MCP dispatcher:

- No `provider.profile:*` marker on a non-root `source=agent` token means an
  ordinary local actor. The API passes no HTTP profile, so `tools/list`,
  `tools/call`, idempotency, and response DTOs are the same as stdio.
- Unmarked human, system, and root tokens retain the prior provider-safe
  read-only fallback. This parity seam grants no new authority to credentials
  intended for another role; in particular, the root credential is never an
  ordinary MCP mutation bearer.
- One valid sealed provider marker selects `ProviderSafeReadHTTPProfile` or
  `ProviderTrackerHTTPProfile`. The restricted allowlist and provider-safe
  response reducer remain active.
- Any marker that is malformed, unknown, duplicated, or inconsistent with its
  sealed scopes returns `403 provider_authority_denied`. It never falls back to
  local authority.

The absence of an HTTP profile is not an authorization bypass. Both transports
run `access.ToolVisible(actor, tool)` before dispatch. Work-item and feed tools
then recheck their object/tree access through `access.Service`. A scope-denied
tool reaches neither the handler nor the idempotency executor and appends no
events. An object-denied mutation appends no domain mutation event.

## Idempotency And Attribution

Stdio reads one token at process launch. HTTP resolves a bearer per request.
Both paths call the same mutation executor with identity
`(actor_token_id, MCP:<tool>, idempotency_key)`:

- replaying the same actor/tool/key/arguments returns the recorded result;
- reusing the key with different arguments conflicts before the domain write;
- two different bearer tokens using the same tool/key remain distinct acts;
- every appended event carries the exact calling bearer as `actor_token_id`.

Local HTTP uses ordinary operator DTOs in both fresh and replayed results.
Provider profiles retain the versioned provider-safe response contract, so a
provider request cannot replay an ordinary local DTO under the same logical
idempotency identity.

Each local agent must continue to hold its own token. Centralizing the MCP
server removes per-client Meristem processes; it does not introduce a shared
agent identity. Bearers belong in local environment or credential storage,
never committed client JSON.

## Regression Coverage

`internal/api/mcp_test.go` pins route selection: broad and scoped unmarked
bearers get ordinary policy-filtered dispatch, valid sealed profiles keep their
narrow surfaces, and malformed/unknown markers fail closed.

`internal/mcp/http_local_parity_integration_test.go` proves:

- broad and tree-scoped local actors advertise identical tools over stdio and
  HTTP;
- local HTTP and stdio return the same ordinary work-item DTO;
- a scope-denied local HTTP call appends zero events;
- an out-of-tree mutation appends no transition event;
- local HTTP mutation replay, conflict handling, exact bearer attribution, and
  distinct per-bearer identity.

Existing provider profile, provider-safe rendering, and malformed-authority
tests remain the regression boundary for the sealed provider path.

## Remaining Transport Work

- `07417203-ea12-5139-9e06-46f681a08e8a` — GET/SSE transport compatibility.
- `4473e765-a3b9-5714-aa61-a142fd063567` — migrate local client configuration
  to the centralized HTTP server and retire spawned stdio wrappers. That is an
  operational cutover, separate from this code-only parity seam.
- Public ingress and OAuth runtime activation remain separately gated. This
  local parity change neither enables them nor changes provider authority.
