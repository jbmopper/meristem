# Provider MCP gateway contract

Status: implemented gateway contract under umbrella `a03f644e`, updated
2026-07-12. If this document conflicts with `docs/spec.md`, `docs/spec.md`
wins.

## Scope and dependencies

This contract narrows four existing threads:

- `a03f644e` - external MCP gateway umbrella. Owns the provider-facing gateway
  rollout and the real provider session smoke.
- `4c00df15` - HTTP Streamable MCP auth/client-config thread. The
  provider-facing decision is now OAuth-compatible auth for vanilla Claude and
  ChatGPT registration.
- `66eb0aed` - network-layer spec. Provides the registered ingress seat and
  confirms that the DNS/MCP box is ingress, not the durable topology center.
- `beac80e1` - HTTP MCP mutation idempotency/write gate dependency. Its durable
  replay contract now backs the sealed tracker-only mutation surface.

Remaining rollout work:

- configure public TLS ingress and the documented perimeter rate limits;
- complete a real Claude or ChatGPT registration/read/write smoke;
- add provider-specific transport compatibility only if that smoke proves the
  existing Streamable HTTP implementation insufficient.

## Endpoint contract

The provider-facing endpoint is:

```text
https://<registered-ingress-host>/mcp
```

It is public HTTPS and implements MCP Streamable HTTP. It is the `/mcp` route
on the existing `meristem api` process, dispatched through `internal/mcp`; it is
not a new service and not a proxy to `/v1/...` REST handlers.

The registered ingress host is deployment configuration. Cloudflare Tunnel is
an acceptable current deployment choice, but the core contract is only HTTPS
ingress to `meristem api`. Swapping Cloudflare for another TLS reverse proxy
must not require core code changes.

Remote provider OAuth requires both an explicit HTTPS
`MERISTEM_PUBLIC_BASE_URL` and `MERISTEM_OAUTH_SYSTEM_ACTOR_TOKEN_ID` naming an
active non-root system token. OAuth issuer/resource values are never derived
from forwarded proxy headers in that mode. The provider endpoint is
`${MERISTEM_PUBLIC_BASE_URL}/mcp`; see
[`provider-oauth-operations.md`](provider-oauth-operations.md) for the complete
bring-up and smoke runbook.

Transport compatibility for a first vanilla provider registration remains an
acceptance question. If a provider requires server-initiated `GET /mcp` SSE
behavior before it will register, record and implement that concrete gap. This
contract still names `/mcp` as the provider endpoint either way.

## Auth contract

Vanilla provider registration for Claude and ChatGPT requires
OAuth-compatible MCP authorization. A provider registry must not be configured
with a long-lived static meristem bearer token as its production credential.

The static bearer path remains valid only for local, internal, and debug
clients, including:

- `meristem mcp` stdio launched with `MERISTEM_TOKEN`;
- Claude Code, Cursor, Codex, and custom local workers using private
  per-process configuration;
- local curl/debug clients and controlled internal API callers.

Provider-facing OAuth must preserve meristem attribution. A provider session or
provider-issued access token must resolve to a meristem actor in request
context before any tool call is authorized. It must not collapse all remote
provider traffic onto one shared bearer or one shared actor.

No shared provider registration JSON may contain meristem bearer material,
OAuth client secrets, access tokens, refresh tokens, cookies, Cloudflare Access
JWTs, or any `.meristem/*.token` content.

## Cloudflare and perimeter policy

Cloudflare may terminate TLS, run WAF rules, and apply rate limits for
provider-facing `/mcp`.

Provider-facing `/mcp` must not require Cloudflare Access service-token
headers, including `CF-Access-Client-Id`, `CF-Access-Client-Secret`, or
`CF-Access-Jwt-Assertion`, unless a specific provider client is later proven to
support that requirement and the exception is recorded as a new work item.

Non-provider routes may remain behind Cloudflare Access or another perimeter.
That perimeter is additive only. Meristem identity still comes from the
resolved meristem auth context, never from `Cf-Access-*` headers.

## Tool surface contract

The provider-facing gateway always reduces reads to versioned provider-safe
work-item and structural feed DTOs. Raw event payloads, private messages,
approvals, connectors, registry/policy state, credentials, and encrypted
private content are excluded.

Read profiles advertise only `feed.read`, `work_items.list`, and
`work_items.get`. Tracker-write profiles add five idempotent work-item tools:
create, spawn child, append a tracker-safe note/progress event, update blocked
metadata, and block or terminalize. New items must be
`human_review_status=blocked`; no cultivar or execution-shaped transition is
accepted. The worker independently excludes human-review-blocked work from
scribe, dispatch, review, and convergence lanes.

Approval decisions, connector actions, inbox capture, registry/policy writes,
convergence proposals/signals, and job execution remain hidden and rejected
before dispatch. A read grant cannot acquire tracker writes when new tools are
added because the exact versioned authority profile and OAuth scope are stored
and revalidated throughout the grant lifecycle.

## Evidence event

The first real provider smoke appends a secret-free evidence event. Emit it as
a `work_item.event_appended` on the relevant gateway smoke item with
`inner_kind = "provider_gateway.session_verified"` and this payload shape:

```json
{
  "payload_version": 1,
  "provider": "claude",
  "provider_client": {
    "kind": "vanilla_provider_connector",
    "registration_ref": "opaque-nonsecret-reference"
  },
  "endpoint": {
    "url": "https://example.com/mcp",
    "transport": "mcp_streamable_http",
    "tls_terminated_by": "cloudflare",
    "cloudflare_access_required": false
  },
  "auth": {
    "scheme": "oauth",
    "flow": "provider-supported-oauth",
    "meristem_actor_token_id": "00000000-0000-0000-0000-000000000000",
    "provider_subject_ref": "opaque-nonsecret-reference",
    "token_material_recorded": false
  },
  "session": {
    "started_at": "2026-07-07T00:00:00Z",
    "completed_at": "2026-07-07T00:00:00Z",
    "provider_session_ref": "opaque-nonsecret-reference"
  },
  "tool_calls": [
    {
      "name": "initialize",
      "result": "ok",
      "read_only": true
    },
    {
      "name": "tools/list",
      "result": "ok",
      "advertised_write_tools": 0,
      "read_only": true
    },
    {
      "name": "feed.read",
      "result": "ok",
      "read_only": true
    },
    {
      "name": "work_items.get",
      "result": "ok",
      "read_only": true
    }
  ],
  "assertions": {
    "no_cloudflare_access_service_token_headers": true,
    "no_static_bearer_shared_with_provider": true,
    "no_write_tools_advertised_or_called": true,
    "no_secret_values_recorded": true
  },
  "notes": "short human-readable summary without secrets"
}
```

The evidence may include opaque references or one-way hashes for correlation.
It must not include raw `Authorization` headers, bearer tokens, OAuth access or
refresh tokens, client secrets, cookies, Cloudflare Access tokens, private
message bodies, provider transcript content, or connector credential material.

The event's normal meristem wrapper supplies `actor_token_id`, `source`, and
`occurred_at`; do not duplicate those fields from user-controlled provider
input.
