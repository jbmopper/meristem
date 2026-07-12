# Provider OAuth operations

This runbook brings up the remote MCP OAuth boundary for one Meristem owner.
The local REST API, static-bearer HTTP MCP, and stdio MCP remain available when
remote OAuth is disabled. Provider OAuth is enabled only when both of these are
set on `meristem api`:

```text
MERISTEM_PUBLIC_BASE_URL=https://mcp.example.com
MERISTEM_OAUTH_SYSTEM_ACTOR_TOKEN_ID=<uuid of an active non-root source=system token>
```

The public base URL must be explicit HTTPS. It is the OAuth issuer and MCP
resource origin; Meristem does not derive either value from `Host` or
`X-Forwarded-*` in this mode. Setting only one variable, using HTTP, supplying a
malformed URL/UUID, or naming a missing, revoked, root, human, or agent token
makes `/readyz` fail and returns the same generic `503 oauth_unavailable` from
discovery, registration, authorization, and token endpoints. With both
variables absent, `/readyz` remains healthy and reports OAuth disabled.

## Token roles

Keep these four credentials distinct. None belongs in provider registration
JSON, Cloudflare configuration, screenshots, logs, or prompts.

1. **Root:** `source=human`, root=true. It only mints and revokes Meristem
   tokens. Do not use it for OAuth client binding or approval decisions.
2. **OAuth lifecycle system actor:** `source=system`, non-root, no provider
   authority scopes. Its token ID goes in
   `MERISTEM_OAUTH_SYSTEM_ACTOR_TOKEN_ID`; its bearer secret does not. It
   attributes untrusted DCR and authorization lifecycle events.
3. **OAuth client administrator:** `source=human`, non-root, with exactly the
   administrative capabilities needed here:
   `oauth_clients.bind,oauth_clients.revoke`. It binds one DCR client to one
   pre-minted provider actor and may later revoke the client.
4. **Approval decider:** `source=human`, non-root, separate from the client
   administrator and provider actor. Give it `approvals.decide` plus the
   work-item read scope required by its owner UI (normally
   `work_items.read_all`). The token that caused an approval cannot decide it.

The **provider actor** is a fifth, exact identity: `source=agent`, non-root, and
one sealed authority profile. Do not share it across active DCR clients. For
the portfolio owner tracker, mint one of these exact scope sets:

```text
# Read-only owner tracker
provider.profile:owner_tracker_read_v1,feed.read,work_items.read_all

# Tracker write (coordination mutations only)
provider.profile:owner_tracker_write_v1,feed.read,work_items.read_all,work_items.tracker_write_all
```

Tracker-write authority cannot approve, use connectors, change registry or
policy, expose private message bodies, or start execution. Provider-created
items must remain `human_review_status=blocked`.

## One-time setup

With `BIN` pointing at the Meristem binary and `ROOT_TOKEN` held only in the
current trusted shell:

```bash
MERISTEM_TOKEN="$ROOT_TOKEN" "$BIN" tokens create \
  --name oauth-lifecycle --source system

MERISTEM_TOKEN="$ROOT_TOKEN" "$BIN" tokens create \
  --name oauth-client-admin --source human \
  --scopes oauth_clients.bind,oauth_clients.revoke

MERISTEM_TOKEN="$ROOT_TOKEN" "$BIN" tokens create \
  --name oauth-approval-decider --source human \
  --scopes approvals.decide,work_items.read_all

MERISTEM_TOKEN="$ROOT_TOKEN" "$BIN" tokens create \
  --name provider-owner-tracker --source agent \
  --scopes provider.profile:owner_tracker_write_v1,feed.read,work_items.read_all,work_items.tracker_write_all
```

Record each secret once in the normal protected token store. Configure only
the lifecycle token's printed `id=...`, not its `secret=...`, as the system
actor ID. Restart the API with the two OAuth variables and verify:

```bash
curl -fsS https://mcp.example.com/readyz
curl -fsS https://mcp.example.com/.well-known/oauth-authorization-server
curl -fsS https://mcp.example.com/.well-known/oauth-protected-resource/mcp
```

`/readyz` must contain `"oauth":"ok"`; discovery must name exactly
`https://mcp.example.com` and `https://mcp.example.com/mcp`.

## Ingress boundary

Cloudflare (or another replaceable reverse proxy) terminates public TLS and
forwards to `meristem api`. Meristem OAuth remains the application identity
boundary. Do not require Cloudflare Access service-token headers on provider
OAuth or `/mcp`; vanilla provider connectors do not supply them. Do not expose
Postgres or a direct clear-text API listener publicly.

Before enabling the hostname, install per-source-IP rate-limit rules returning
HTTP 429 at these ceilings:

| Endpoint | Required initial ceiling |
|---|---:|
| `POST /oauth/register` | 5 requests/minute |
| `GET /oauth/authorize` | 30 requests/minute |
| `POST /oauth/token` | 60 requests/minute |

Keep `/oauth/register` the tightest: DCR is unauthenticated and creates durable
events. The authorize ceiling accommodates approval-page retries, and the
token ceiling accommodates code exchange plus rotating refresh. A change to
these limits is an operational work item, not an ad-hoc bypass.

## DCR, binding, and consent

DCR metadata is untrusted self-assertion. A `client_name`, redirect URI, or
provider-looking hostname does not establish provider identity or owner
consent. PKCE binds the authorization code exchange; the owner-side actor
binding supplies Meristem authority.

If DCR omits `scope`, registration records the server's supported ceiling
(`mcp:read mcp:tracker_write`) for compatibility with vanilla clients. That
ceiling grants nothing by itself. The bound actor's sealed profile and the
owner-approved authorization request select the exact effective scope. A
client that explicitly registers only `mcp:read` cannot later receive tracker
writes.

On the first provider registration, let the provider dynamically register. If
it immediately attempts authorization before binding, a temporary failure is
expected; retry after binding. Obtain the new `client_id` from the DCR response
in a controlled smoke or from this read-only operator query:

```sql
SELECT client_id, client_name, redirect_uris, created_at
FROM oauth_clients
WHERE revoked_at IS NULL
ORDER BY created_at DESC;
```

Inspect the registered redirect URI against the provider's current official
connector documentation. Then bind the exact provider actor with the scoped
human administrator:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $OAUTH_CLIENT_ADMIN_TOKEN" \
  -H 'Idempotency-Key: bind-provider-client-1' \
  -H 'Content-Type: application/json' \
  "https://mcp.example.com/v1/oauth/clients/$CLIENT_ID/actor" \
  -d "{\"actor_token_id\":\"$PROVIDER_ACTOR_ID\",\"authority_profile\":\"owner_tracker_write_v1\"}"
```

Retry authorization. Meristem creates a ten-minute authorization work item and
approval. From the trusted owner client, find the approval attached to the
work item shown on the pending page and decide it with the separate decider:

```bash
curl -fsS \
  -H "Authorization: Bearer $APPROVAL_DECIDER_TOKEN" \
  "https://mcp.example.com/v1/work-items/$AUTH_WORK_ITEM_ID/approvals"

curl -fsS -X POST \
  -H "Authorization: Bearer $APPROVAL_DECIDER_TOKEN" \
  -H 'Idempotency-Key: decide-provider-oauth-1' \
  -H 'Content-Type: application/json' \
  "https://mcp.example.com/v1/approvals/$APPROVAL_ID/decision" \
  -d '{"decision":"approved","reason":"owner approved this provider client and sealed profile"}'
```

Continue the pending authorization page. Authorization codes are single-use
and the authorization request expires after 10 minutes. Access tokens live for
1 hour. Refresh tokens live for 30 days and rotate on every use. Reuse of an
old refresh token is recorded and rejected as `invalid_grant` without revoking
the healthy successor (so a lost token response does not destroy the session).
Approval is per new grant or authority change, not every access-token refresh.

## First read/write smoke

In the provider connector, register `https://mcp.example.com/mcp`, complete the
flow above, and run these in order:

1. List tools. Confirm the surface contains structural work-item/feed reads
   and tracker mutations only; it must not advertise approvals, connectors,
   registry/policy writes, inbox capture, private messages, or execution.
2. Call `work_items.list` and inspect a known non-private item. Confirm private
   message bodies and raw event payloads are absent.
3. Call `work_items.create` once with arguments equivalent to:

   ```json
   {
     "title": "Provider tracker smoke",
     "body": "Created through the provider-safe MCP tracker surface.",
     "state": "captured",
     "human_review_status": "blocked",
     "idempotency_key": "provider-tracker-smoke-1"
   }
   ```

4. Repeat the identical call with the same idempotency key. It must return the
   same work item and append no duplicate event. Reuse the key with a changed
   body; it must fail with an idempotency conflict.
5. Confirm the created item is human-review-blocked and that a worker scan
   creates no dispatch event or job for it. Terminalize or cancel the smoke
   item from the provider, then record the secret-free gateway evidence event
   described in [provider-mcp-gateway.md](provider-mcp-gateway.md).

To revoke access, use the scoped client administrator on
`POST /v1/oauth/clients/{client_id}/revoke` and revoke the exact provider actor
with the root token. Either action must cause subsequent access/refresh use to
fail; never rotate or expose the root as a shortcut.
