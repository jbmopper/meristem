# MCP Parity Matrix

Status date: 2026-08-07. REST remains canonical; MCP transports translate into
the same domain services. This document describes the current transport/profile
contract rather than preserving the superseded local-HTTP gap audit.

## Transport And Profile Selection

| Transport / actor | Tool and response policy |
| --- | --- |
| REST | Canonical operation surface. POST routes use bearer attribution and `Idempotency-Key`. |
| Stdio, unmarked | Compatibility behavior: ordinary token-scope filtering and ordinary DTOs. |
| HTTP, unmarked | Provider-safe read fallback retained for compatibility. |
| Stdio or HTTP, exact `mcp.profile:local_agent_v1` | Ordinary token-scope filtering, ordinary DTOs, and object-level scope checks. |
| Stdio or HTTP, exact `provider.profile:*` | Sealed provider allowlist, argument validator, and provider-safe response reducer for that exact profile. |
| Either transport, malformed profile | Fail closed. Unknown, repeated, mixed local/provider, or otherwise inexact markers are rejected. |

The local marker is accepted only for an active, non-root `source=agent` token.
It selects a transport profile; it grants no authority. The token's other scopes
still determine advertisement, dispatch, and object-level visibility. Only the
root-controlled token creation path may issue the marker; delegated issuance
rejects it.

The provider profiles remain sealed. Local HTTP parity does not widen a
provider actor's tool set or replace provider-safe response reduction.

## Local-Agent HTTP Parity

For an explicitly marked local actor, HTTP and stdio share these invariants:

- `tools/list` is filtered from the same `access.ToolVisible` decision and
  fails closed if the internal list result has an unexpected shape.
- `tools/call` is checked at the HTTP route and again in the shared dispatcher.
  The second gate independently re-derives the authenticated actor profile, so
  a caller-supplied route profile cannot weaken a sealed validator.
- Broad and scoped actors receive the same advertised canonical tool set on
  both transports. A tree-scoped actor remains unable to read or mutate work
  outside its `work_items.tree:<uuid>` boundary.
- Responses use ordinary local DTOs. They are not rewritten into
  `provider_safe_*` envelopes.
- Mutations call the same canonical services, attribute events to the request
  bearer, and require the same MCP argument-level `idempotency_key` used by
  stdio.
- Canonical and underscore aliases share one idempotency identity. Replaying
  the same call through the alternate spelling returns the recorded result;
  changed arguments conflict and cannot append a second authoritative event.

The HTTP `Idempotency-Key` header is not the identity of a JSON-RPC tool call.
Tool-call replay remains keyed by actor, canonical `MCP:<tool_name>` scope, the
argument-level key, and the canonicalized arguments.

## Tool Names

Canonical names remain dot-separated (`feed.read`, `work_items.get`). The
underscore aliases exist for hosts that reject dots. Alias and canonical names
are jointly injective: duplicate canonical registrations, alias collisions, and
canonical/alias collisions fail server construction.

- Stdio selects its advertised spelling with `MERISTEM_MCP_TOOL_NAMES`.
- HTTP selects it per request with
  `X-Meristem-Tool-Names: canonical|cursor` (absent means canonical).

HTTP naming is request-local. Concurrent clients using different modes cannot
change one another's `tools/list` result. Dispatch accepts both spellings.

## Capability Matrix

| Capability | REST | Local stdio / marked local HTTP | Sealed provider HTTP |
| --- | --- | --- | --- |
| Feed, backlog, work-item reads | Canonical GET routes | Scope-derived tools and ordinary DTOs | Profile allowlist and provider-safe DTOs |
| Registry, projection, approval, deterministic-error reads | Canonical GET routes | Visible only when the actor's scopes permit | Hidden unless the exact provider profile deliberately allows them |
| Work-item mutations | Canonical POST routes | Scope-derived; durable MCP argument idempotency | Only tracker-safe mutations in tracker profiles, with sealed validation |
| Approval, connector, registry/policy mutations | Canonical POST routes | Scope-derived; existing separation-of-duties and approval gates remain | Hidden and rejected |
| OAuth bind/revoke | Canonical POST routes | Explicit non-root human scopes; not available to agent tokens | Hidden and rejected |
| Artifact attachment | Not shipped | Not shipped | Not shipped |
| Signals ingress | `POST /v1/signals` | Intentionally absent | Intentionally absent |
| Feed stream | `GET /v1/feed/stream` | `feed.read` cursor plus bounded `wait` | Profile-filtered `feed.read`; `GET /mcp` SSE is not implemented |

Signals are external-system ingress rather than an agent tool. HTTP MCP
server-initiated SSE is a transport capability, not a separate business
operation. Artifact attachment remains the open REST/MCP parity item; no
transport advertises a placeholder.

## Bounded Feed Wait

`POST /mcp` establishes a response write deadline longer than the configured
maximum `feed.read` wait before dispatch. If the server cannot establish that
deadline it returns 503 without running the MCP request. This keeps a legal
maximum wait usable while preserving bounded patience. The handler clears the
connection-scoped deadline on exit so a keep-alive connection cannot poison a
later request; clear failure is logged but cannot rewrite a committed response.
`GET /mcp` continues to return 405.

## Regression Evidence

The load-bearing checks live alongside the implementation:

- `internal/mcp/http_local_parity_integration_test.go` proves broad and
  tree-scoped stdio/HTTP surface parity, object filtering, alias-stable replay,
  changed-body conflict, and per-bearer event attribution.
- `internal/mcp/http_test.go` proves route/dispatcher double gating, actor-derived
  tracker validation, fail-closed list shapes, and concurrent request-local
  tool naming.
- `internal/api/mcp_test.go` proves marker selection, strict naming-header
  parsing, response deadlines, and pre-dispatch failure.
- `internal/api/mcp_local_profile_integration_test.go` proves a maximum legal
  `feed.read` wait completes through the mounted API route.

## Linked Work

- `4473e765-a3b9-5714-aa61-a142fd063567` — local-agent HTTP MCP parity.
- `95c24a80-f8f9-5b1e-9525-150d037b3841` — HTTP MCP durable mutation gate.
- `07417203-ea12-5139-9e06-46f681a08e8a` — transport compatibility and
  server-initiated SSE decision.
- Artifact substrate remains open under the v1 substrate list.
