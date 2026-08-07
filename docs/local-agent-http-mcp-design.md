# Local-Agent HTTP MCP Parity and Client Cutover

Status: proposed for Claude review  
Work item: `35991736-bdae-53ac-9760-1121a1855189`  
Release base: `e7bc6dd32367dd1bd62e806360cb78b696835eaa`

## Decision

Codex, Claude, and Cursor become ordinary Streamable HTTP clients of the one
`meristem api` process at `/mcp`. They stop spawning a second `meristem mcp`
process against the shared database after the cutover gate passes. Stdio
remains a development-only compatibility path for unmerged code and isolated
databases.

Local HTTP access is selected by one exact, versioned token-scope marker:

```text
mcp.profile:local_agent_v1
```

The marker classifies the transport/data profile; it grants no business
authority. The token's other scopes continue to drive `access.ToolVisible` and
all object-level checks. Every migrated local client receives its own
`source=agent`, non-root token with the marker plus explicit scopes. No migrated
client uses the legacy empty-scope compatibility path.

The route keeps the current provider-safe fallback for unmarked static
credentials. This avoids turning every old or accidentally reused agent bearer
into a full HTTP credential merely because it has no `provider.profile:*`
marker.

## Goals

- Give Codex, Claude, and Cursor the same tools and ordinary operator response
  shapes over HTTP that the same scoped actor receives over stdio.
- Keep one actor token per client or session so every appended event retains
  exact attribution.
- Preserve the sealed provider profiles and their reduced response shapes.
- Reuse the existing MCP argument-level idempotency executor for every
  mutation; do not invent an HTTP-header idempotency path.
- Remove local agents' routine dependency on a per-client Meristem process and
  direct database access.
- Preserve a tested, explicit rollback to the existing stdio wrappers until
  all three clients pass installed-client smoke tests.

## Non-goals

- `GET /mcp` server-initiated SSE. `feed.read` already provides cursor-based,
  bounded long polling over POST and is sufficient for this cutover.
- OAuth or token exchange for local agents. Those belong to the later token
  ergonomics work.
- Removing the root-only local administration path used to mint and revoke
  credentials, apply migrations, or perform emergency diagnostics.
- A public HTTP ingress. The first cutover targets the loopback API. Any later
  non-loopback endpoint must independently meet the TLS and origin requirements
  in `docs/spec.md`.
- A typed agent/persona object. Persona remains prompt/artifact state; this
  marker describes only an MCP presentation boundary.

## Authentication and profile selection

The authenticated token projection, never a request body or client-supplied
profile header, selects the profile.

| Token shape | `/mcp` result |
| --- | --- |
| One exact valid `provider.profile:*` marker and its sealed scopes | Existing provider read or tracker profile, including provider-safe reducers |
| Exact `mcp.profile:local_agent_v1`, active non-root `source=agent`, no provider marker | Local-agent profile; ordinary response shapes and scope-derived tools |
| Both provider and local markers, repeated markers, unknown `mcp.profile:*`, or invalid actor kind | `403`, before dispatch |
| No profile marker | Existing provider-safe read fallback |

`mcp.profile:local_agent_v1` is not a sealed authority bundle: local agents may
be portfolio-wide or tree-scoped. Its validator checks the actor shape and
marker grammar only. `access.ToolVisible`, `CanReadWorkItem`,
`CanWriteWorkItem`, listener binding checks, human-only reducers, and the
services remain authoritative. A marker-only token therefore sees no business
tools rather than gaining a default surface.

Provider and local profile parsers must fail closed independently. The route
must reject ambiguity before it chooses either profile; precedence is not a
security policy.

## Separating profile selection from data reduction

Today `HTTPOptions.Profile != nil` means two things at once:

1. intersect tools with a fixed allowlist; and
2. enable provider-safe response rendering.

The implementation must make those decisions explicit. The proposed internal
shape is equivalent to:

```go
type HTTPToolProfile struct {
    name                  string
    restrictTools         bool
    allowedTools          map[string]bool
    providerSafeResponses bool
    validateCall          func(string, json.RawMessage) error
}
```

Provider profiles set both booleans and keep their fixed allowlists,
argument validators, and fail-closed renderer-registration gate. The local
profile sets neither boolean: advertisement and dispatch are reduced solely by
the ordinary token policy, and results retain the ordinary stdio-equivalent
DTOs. The explicit local profile still appears in structured telemetry and
tests; the API must not implement it as an undocumented `nil` shortcut.

Every `tools/call`, including an alias, is canonicalized before policy and
idempotency evaluation. Denial occurs before a handler, event append, queue
write, or outbox write is reachable.

## Tool-name compatibility

The shared API cannot use the current process-wide
`MERISTEM_MCP_TOOL_NAMES` switch because Codex, Claude, and Cursor connect to
the same process. HTTP therefore accepts this representation-only request
header:

```text
X-Meristem-Tool-Names: canonical | cursor
```

- Absent or `canonical` advertises dot names such as `work_items.get`.
- `cursor` advertises the existing underscore aliases such as
  `work_items_get`; it is used by Claude and Cursor.
- Any other value is a `400` on MCP POST.
- Dispatch continues accepting both spellings in either mode.

The header changes only `tools/list` representation. It never changes actor,
tool visibility, object authority, response reduction, or idempotency scope.
The implementation passes a request-local name mode into list rendering; it
must not mutate `Server.toolNameMode` and create cross-client state. `Mcp-Name`
continues to match the spelling actually supplied in `tools/call`.

Client metadata such as `clientInfo.name` remains observational and does not
select either authority or naming mode.

## Mutation contract

Local HTTP mutations use the existing shared MCP dispatcher and durable
argument-level idempotency executor:

```text
(actor token id, MCP:<canonical-tool>, idempotency_key, canonical arguments)
```

The bearer is resolved on every request and passed as the actor. The client
must include `idempotency_key` in mutating tool arguments, reuse it only for an
identical retry, and receive the recorded result on replay. Reuse with changed
arguments conflicts before mutation. An ambient HTTP `Idempotency-Key` header
is neither required nor consulted for JSON-RPC tool calls.

This is already proven on the provider-tracker HTTP path; local parity widens
admission to the same path rather than adding a second writer. Tests must prove
two local bearers produce distinct `actor_token_id` values and that denied or
conflicting calls append no events.

## Client configurations and secret handling

The canonical secret remains one mode-0600 file per principal under the
operator-owned `.meristem/` directory. Generated config is private and
uncommitted. No bearer appears in a project file, review artifact, command-line
argument, or log.

`scripts/provision-assistant-access.sh` gains Codex, Claude, and Cursor HTTP
targets and emits config plus launch helpers under `.meristem/generated/`.
Generation is side-effect-free outside that directory. Applying user config,
minting/revoking tokens, and restarting clients are separate owner-approved
cutover operations.

### Codex

Codex uses its native Streamable HTTP configuration:

```toml
[mcp_servers.meristem]
url = "http://127.0.0.1:8080/mcp"
bearer_token_env_var = "MERISTEM_CODEX_TOKEN"
http_headers = { "X-Meristem-Tool-Names" = "canonical" }
```

A generated launcher reads `.meristem/codex.token`, exports only
`MERISTEM_CODEX_TOKEN`, and executes the Codex CLI or desktop binary. This
keeps the shared TOML secret-free and works around GUI sessions that do not
inherit a shell environment. The launcher does not start Meristem and has no
database URL.

### Claude Code

Claude uses a user-scoped HTTP entry with `type: "http"`. A generated
`headersHelper` reads `.meristem/claude-code.token` and returns the
`Authorization` and `X-Meristem-Tool-Names: cursor` headers as JSON at connect
or reconnect time. The helper is mode 0700, contains no bearer, starts no
Meristem process, and has no database URL. If the installed Claude build does
not support `headersHelper`, the tested fallback is the same token-file-backed
environment launcher pattern used for Codex; embedding the bearer in shared
Claude config is not an acceptable fallback.

### Cursor

Cursor uses a user-scoped entry with `type: "http"` (not the incompatible
`streamable-http` alias), the loopback URL, and:

```json
{
  "headers": {
    "Authorization": "Bearer ${env:MERISTEM_CURSOR_TOKEN}",
    "X-Meristem-Tool-Names": "cursor"
  }
}
```

A generated launcher reads `.meristem/cursor.token`, exports only
`MERISTEM_CURSOR_TOKEN`, and executes the Cursor desktop binary. The
installed Cursor build must demonstrate that its GUI resolves the environment
placeholder and completes the smoke test before its stdio entry is retired.
If it does not, the release remains blocked for Cursor; the design does not
silently copy its bearer into committed or shared JSON and does not introduce
an authentication proxy.

## Provisioning and rotation

Migration mints or rotates distinct credentials for each long-lived client and
each unattended listener principal. Each credential has:

- `source=agent`, non-root;
- exactly one `mcp.profile:local_agent_v1` marker;
- explicit least-authority business scopes for that principal; and
- no `provider.profile:*` marker.

Adding the marker makes the token policy-bearing, so the provisioning script
must never add it to an otherwise empty scope list. Session credentials retain
unique names and files and receive the marker plus the requested explicit
scopes. Rotation writes the new file atomically with mode 0600, reconnects one
client, verifies attribution, and only then revokes the prior token. A failed
reconnect leaves the prior credential and stdio config available for rollback.

## Rollout gate

This feature and the listener changes ride one reviewed release tip, but the
cutover is sequential:

1. Land code, docs, integration tests, and generated-config tests on the
   listener release branch.
2. Obtain an exact-commit review of the combined tip.
3. With owner approval, build once from that tip and publish the build pin.
4. Start the API/worker/listener stack and require readiness plus compiled-tip
   equality before changing any client.
5. Mint/rotate explicitly scoped local-profile credentials.
6. Reconnect Codex, Claude, then Cursor one at a time. For each client prove:
   initialize, expected tool names, a scoped read, an idempotent work-item
   mutation, event attribution to that client's token, and bounded
   `feed.read`.
7. Disable that client's stdio entry only after its HTTP proof passes.
8. After all three pass, remove routine direct-database wrappers from generated
   active config. Keep the labeled development fallback documented and
   unregistered by default.

No two Meristem binaries may act as normal writers against the shared store
during cutover. A client failure rolls back that client's config; a server or
build-pin failure rolls back the whole release before any further credential
changes.

## Required tests

- Profile matrix: valid local, valid provider read/write, unmarked fallback,
  unknown/repeated/both markers, root/human/system/revoked local marker.
- Local broad-scope and tree-scoped tokens advertise exactly their
  `ToolVisible` surface and receive ordinary DTOs.
- Provider profiles retain their current fixed allowlists, validators, and
  provider-safe DTOs over both HTTP and stdio.
- Canonical and cursor list modes are request-local under concurrent calls;
  dispatch accepts either alias while policy/idempotency use the canonical
  tool.
- HTTP local mutation replay, changed-body conflict, in-flight behavior, and
  per-bearer event attribution.
- Denied profile, scope, object, build-pin, and malformed-name calls append no
  events and reach no queue or outbox writer.
- Provisioning generation tests contain no bearer or database URL, accept all
  three targets, refuse empty effective scopes, and preserve mode 0600/0700.
- Installed-client smoke tests for the exact Codex, Claude, and Cursor builds
  used at cutover.

## Documentation updates in the implementation commit

`docs/spec.md`, `docs/v0.md`, `docs/mcp-parity.md`, `README.md`, the Cursor
example, and the worker bootstrap must be updated together. In particular,
current statements that HTTP MCP is read-only or provider-only become false
once local-profile parity lands. The live parent and continuation work items
must receive the same decision and exact-tip evidence so the specification and
running backlog do not drift.

## Review questions

Claude should fail the design for any of these:

1. Can the local marker accidentally grant authority, revive legacy unscoped
   access, or interact ambiguously with a provider marker?
2. Does separating fixed allowlists from provider-safe response rendering keep
   provider behavior fail closed?
3. Can the request-local naming header affect anything beyond representation,
   or race between clients?
4. Are the Codex, Claude, and Cursor credential paths workable without a
   bearer in committed/shared config or a second Meristem/DB process?
5. Do rollout and rollback prevent mixed writer versions and preserve exact
   attribution?
6. Are any existing specification claims or tests missing from the required
   implementation update?
