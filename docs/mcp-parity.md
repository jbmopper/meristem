# MCP Parity Matrix

Status date: 2026-07-06. Source of truth remains the implementation plus
`docs/v0.md`; this matrix is the current gap map for the A-bar MCP parity item.

## Transport Policy

| Surface | Current status | Write policy |
| --- | --- | --- |
| REST | Canonical transport for every operation. | POST routes use bearer auth plus `Idempotency-Key`. |
| Stdio MCP | Full agent-oriented MCP surface, subject to token scopes. | Mutating tools require an MCP `idempotency_key` argument; handlers call the same services as REST. |
| HTTP MCP | Streamable HTTP POST with read-only tool advertisement. | Mutating tools are intentionally rejected until HTTP MCP mutation idempotency is specified. |
| Future HTTP MCP mutation | Open design target, not an implicit capability. | Must preserve request-context attribution, idempotency, and token scope filtering before any write tool is advertised. |

## Capability Matrix

| Capability | REST | Stdio MCP | HTTP MCP today | Remaining gap |
| --- | --- | --- | --- | --- |
| Inbox capture | `POST /v1/inbox/messages` | `inbox.capture` | Hidden | HTTP MCP write policy, if this ever needs HTTP MCP. |
| Feed and backlog reads | `GET /v1/feed`, `GET /v1/backlog/readiness` | `feed.read`, `backlog.readiness` | Visible | None for read-only parity. |
| Work item reads | `GET /v1/work-items`, `GET /v1/work-items/{id}` | `work_items.list`, `work_items.get` | Visible | None for read-only parity. |
| Work item mutations | create, spawn child, append event, metadata, transition, convergence proposal, cultivar activation | `work_items.create`, `work_items.spawn_child`, `work_items.append_event`, `work_items.update_metadata`, `work_items.transition`, `convergence.propose_checks`, `registry.activate_cultivar` | Rejected | HTTP MCP mutation idempotency if these are exposed over HTTP. |
| Registry and projection reads | registry/projection GET routes | `registry.list`, `registry.get`, `projections.list`, `projections.get` | Visible when token policy allows | None for read-only parity. |
| Registry and projection writes | registry/projection POST routes | `registry.define_tropism`, `registry.define_cultivar`, `projections.define` | Rejected | HTTP MCP mutation idempotency if these are exposed over HTTP. |
| Approval reads | approval GET/list routes | `approvals.get`, `approvals.list_for_work_item` | Visible | None for read-only parity. |
| Approval request/decision | `POST /v1/work-items/{id}/approvals`, `POST /v1/approvals/{id}/decision` | `approvals.request`, `approvals.decide` | Rejected | HTTP MCP mutation policy only; stdio parity is closed. |
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

## Linked Work

- `3eb5c8c4-f0f9-5720-8c65-2c949252074c` — A-bar MCP parity gap map.
- `5e96aefb-9a57-51f1-b107-83ffcbb526f8` — full-featured MCP parity umbrella.
- Artifact substrate remains open under the v1 substrate list.
- HTTP MCP mutation idempotency remains a future design/implementation slice.
