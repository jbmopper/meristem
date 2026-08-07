# Local-Agent HTTP MCP Parity and Client Cutover

Status: revised after Claude round-one review; pending round-two review
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

- `GET /mcp` server-initiated SSE. `feed.read` provides cursor-based, bounded
  long polling over POST and is sufficient for this cutover. Before dispatch,
  `/mcp` POST extends its write deadline to the configured maximum feed wait
  plus a five-second response margin through `http.ResponseController`. Failure
  to establish that deadline rejects the request before tool dispatch; the
  transport never accepts a wait it cannot return as a JSON-RPC response.
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

The local marker is transport-independent. An exact local marker over stdio
selects the same unrestricted, scope-derived local profile and ordinary DTOs;
an unknown, repeated, or malformed `mcp.profile:*` marker fails authentication
or dispatch rather than becoming an inert unknown scope. Unmarked legacy stdio
credentials retain their existing compatibility semantics until rotation.

Only the root-controlled local token mint/provisioning path may attach
`mcp.profile:local_agent_v1`. OAuth dynamic registration, OAuth profile
reduction, token exchange, provider binding, and delegated provider flows must
never emit or accept it. Their only profile markers remain the exact sealed
`provider.profile:*` expansions.

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

Profile selection is deliberately double-gated on HTTP. The API route derives
one profile from the authenticated actor and passes it as transport policy;
the shared dispatcher independently derives the actor profile again for both
`tools/list` and `tools/call`. An explicit actor marker must match the route
profile exactly. An unmarked actor is valid only with the exact provider-safe
fallback. Any other mismatch rejects before advertisement or dispatch. A route
may never widen the actor-derived result.

`handleListToolsFiltered` and its replacement must fail closed on every
internal result-shape mismatch. A failed type assertion returns an error (or an
explicitly empty list), never the unfiltered result. Provider tools/list thus
retains both actor-derived sealing and structural response reduction even if
the internal list representation changes.

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

Server construction also proves that canonical names and compatibility aliases
are jointly injective. Registering a canonical name or cursor alias already
owned by another tool is a deterministic startup failure rather than a silent
`toolsByName` overwrite. Policy and idempotency may canonicalize only after
this guard passes.

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

Environment-based clients carry a known residual risk: their bearer is present
in the Codex, Cursor, or fallback-Claude process environment and is inherited
by child shells, extensions, and other subprocesses. That runtime surface is
wider than today's
stdio wrapper, which injects the bearer only into the child Meristem process.
The cutover accepts this temporarily because neither client offers a tested
connect-time file helper. Mitigations are mandatory: least-authority per-client
tokens, no database URL in the launcher, rotation at least weekly and
immediately on suspected exposure, and launch-only injection. The variable
must never be placed in a shell profile, `launchctl setenv`, a global user
environment, shared config, or project config. Token exchange or an OS-backed
connect-time helper replaces this residual in the later ergonomics work.

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

Codex samples that variable at process start. Rotation therefore requires a
full Codex process restart, not an MCP reconnect.

### Claude Code

Claude uses a user-scoped HTTP entry with `type: "http"`. A generated
`headersHelper` reads `.meristem/claude-code.token` and returns the
`Authorization` and `X-Meristem-Tool-Names: cursor` headers as JSON at connect
or reconnect time. The helper is mode 0700, contains no bearer, starts no
Meristem process, and has no database URL. If the installed Claude build does
not support `headersHelper`, the tested fallback is the same token-file-backed
environment launcher pattern used for Codex; embedding the bearer in shared
Claude config is not an acceptable fallback.

Claude's helper reads the file at connection time, so a tested disconnect and
reconnect is sufficient for rotation unless the installed client proves
otherwise.

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

Cursor samples the environment placeholder from its launched process.
Rotation therefore requires a full Cursor process restart, not an MCP
reconnect.

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
scopes. Rotation writes the new file atomically with mode 0600, then reconnects
Claude or fully restarts Codex/Cursor as specified above. The prior token stays
active until the new client instance proves its local profile, expected tool
surface, one attributed read, and one attributed idempotent mutation; only then
is the prior token revoked. A failed proof restores the prior file and client
configuration before retrying.

## Rollout gate

This feature and the listener changes ride one reviewed release tip, but the
cutover is sequential:

1. Land code, docs, integration tests, and generated-config tests on the
   listener release branch.
2. Obtain an exact-commit review of the combined tip.
3. With owner approval, build once from that tip and publish the build pin.
4. Set `MERISTEM_HTTP_ADDR=127.0.0.1:8080`, start the API/worker/listener stack,
   and require readiness, compiled-tip equality, and an operating-system socket
   check proving the effective listener is loopback-only before changing any
   client. Enabling the local-agent profile on a non-loopback plaintext bind is
   a release failure and violates the TLS/ingress requirements in
   `docs/spec.md`.
5. Mint/rotate explicitly scoped local-profile credentials.
6. Restart Codex, reconnect Claude, then restart Cursor one at a time. For each
   client prove:
   initialize, expected tool names, a scoped read, an idempotent work-item
   mutation, event attribution to that client's token, maximum configured
   `feed.read`, and a local-only tool/ordinary DTO that distinguishes the local
   profile from the four-tool provider-safe fallback.
7. Disable that client's stdio entry only after its HTTP proof passes.
8. After all three pass, remove routine direct-database wrappers from generated
   active config. Keep the labeled development fallback documented and
   unregistered by default.

No two Meristem binaries may act as normal writers against the shared store
during cutover. A client failure rolls back that client's config; a server or
build-pin failure stops further migration and restores every already-migrated
client to its preserved stdio configuration before service rollback is called
complete. The restored client is smoked against the rolled-back server. A
working-looking four-tool provider-safe HTTP surface is a failed rollback, not
success.

## Round-one finding disposition

| Finding | Design response |
| --- | --- |
| `HMCP-R1-B1` | `/mcp` POST establishes a write deadline of maximum feed wait plus five seconds and rejects before dispatch if it cannot. |
| `HMCP-R1-B2` | Cutover sets and proves `127.0.0.1:8080`; a non-loopback plaintext bind fails the release gate. |
| `HMCP-R1-B3` | Both HTTP methods receive an actor-derived second gate; list-shape drift fails closed. |
| `HMCP-R1-B4` | Environment inheritance is an explicit accepted residual with least scopes, launch-only injection, weekly rotation, and no global environment persistence. |
| `HMCP-R1-B5` | Rotation requires full Codex/Cursor restart and a Claude reconnect, with old-token revocation after attribution proof. |
| `HMCP-R1-B6` | Whole-release rollback restores and smokes every migrated client on stdio; four-tool degradation is failure. |
| `HMCP-R1-B7` | Server construction rejects canonical/alias collisions before registration. |
| `HMCP-R1-B8` | Local-marker issuance excludes OAuth/exchange/provider paths; exact and malformed stdio semantics are specified and tested. |

## Required tests

- Profile matrix: valid local, valid provider read/write, unmarked fallback,
  unknown/repeated/both markers, root/human/system/revoked local marker.
- Exact local markers select the same unrestricted scope-derived semantics on
  stdio; unknown/malformed local markers fail closed on stdio; OAuth,
  registration, exchange, and provider reducers cannot issue the local marker.
- Local broad-scope and tree-scoped tokens advertise exactly their
  `ToolVisible` surface and receive ordinary DTOs.
- Provider profiles retain their current fixed allowlists, validators, and
  provider-safe DTOs over both HTTP and stdio.
- HTTP `tools/list` independently re-derives the actor profile, rejects a route
  mismatch, and returns no unfiltered tools on every synthetic internal-shape
  mismatch.
- Canonical and cursor list modes are request-local under concurrent calls;
  dispatch accepts either alias while policy/idempotency use the canonical
  tool.
- Construction rejects collisions across canonical names and all cursor
  aliases; a synthetic `a.b_c`/`a_b.c` pair cannot overwrite registration.
- HTTP local mutation replay, changed-body conflict, in-flight behavior, and
  per-bearer event attribution.
- A maximum-wait `feed.read` over `/mcp` completes within the transport's
  extended write deadline; inability to set that deadline rejects before
  dispatch.
- Denied profile, scope, object, build-pin, and malformed-name calls append no
  events and reach no queue or outbox writer.
- Provisioning generation tests contain no bearer or database URL, accept all
  three targets, refuse empty effective scopes, preserve mode 0600/0700, and
  never persist a bearer in global environment setup.
- Installed-client smoke tests for the exact Codex, Claude, and Cursor builds
  used at cutover, including full-restart rotation for Codex/Cursor,
  reconnect-time rotation for Claude, local-vs-fallback discrimination, and
  restoration of every migrated client during server rollback.

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
7. Does maximum-wait `feed.read` have a transport deadline that can return its
   JSON-RPC result?
8. Does rollout prove a loopback-only bind, and does rollback restore every
   already-migrated client rather than accepting provider-safe degradation?
9. Are environment inheritance, client-specific rotation, alias injectivity,
   marker issuance, and stdio marker semantics explicit and tested?
