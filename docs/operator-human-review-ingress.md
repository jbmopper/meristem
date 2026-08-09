# Operator Human-Review Ingress

Status: proposed for independent review
Work item: `01f73c29-f2e5-5a63-8d8a-1c1707c3e6e2`
Implementation base: `ff87be7df8f604c56152701f0dc318d31e5f48de`

## Decision

Meristem will separate **requesting** a human-review decision from **making**
one.

An agent may request that the owner wave through, approve, or keep blocked one
exact work item. That request is inert: it grants no authority and changes no
review status. The owner commits the decision through a first-party operator
surface which verifies a one-time, user-present authenticator assertion bound
to:

- one work item;
- one requested decision;
- the work item's current human-review generation;
- one short expiry; and
- one home node.

The first portable operator surface is a browser page reached from a safe link
in the conversation and confirmed with a WebAuthn platform authenticator. The
link is an identifier, not a bearer capability. No token, token file, JSON
body, shell command, or command output crosses the conversation.

The verified authenticator maps to an active, non-root `source=human` token
holding `work_items.review_decide`. Only then may the home node append the
decision event with that human token as `actor_token_id` and `source=human`.
The agent credential which requested or relayed the decision is never reused
as the deciding actor.

Host-native MCP elicitation, an embedded MCP App, iPhone, or another future UI
may render the same durable request. A renderer does not become an authority
boundary merely because it displayed a confirmation prompt. It must either
return the same user-presence proof or provide an independently reviewed,
non-forgeable host attestation which distinguishes a real user decision from
configuration, automation, and model-based approval.

The backed-out strict gate is reintroduced only in the same release that
ships and proves this ingress end to end. Denial and usable human ingress are
one release invariant.

## Why host approval alone is insufficient

Codex can prompt for side-effecting app and MCP tool calls, and its app server
has client-mediated approval and elicitation flows. Codex also supports
automatic approval review. The current MCP call delivered to meristem does not
contain a proof which meristem can verify as "this exact decision was made by
the owner rather than by an automatic reviewer or client configuration."

Consequently:

- a destructive annotation is useful presentation metadata, not identity;
- a successful MCP call remains attributable to its authenticated MCP token;
- `clientInfo`, request metadata, tool arguments, and prose saying "the user
  approved" are not authority;
- an agent token must never become `source=human` because a host may have shown
  a prompt; and
- a shim which copies an app approval into a human event is forbidden until
  the host supplies a verifiable human-origin attestation.

This is why the first slice uses an authenticator assertion verified by
meristem itself rather than treating a conversation transcript or MCP prompt
as a signature.

## Goals

- Let the owner wave through or approve a visible work item from the normal
  conversation/UI flow with one explicit confirmation.
- Keep all bearer credentials and credential files out of owner and agent
  handling.
- Attribute the decision to an exact active human identity, never to the
  requesting agent.
- Bind a confirmation to one immutable request and one current work-item
  generation so it cannot be replayed after state changes.
- Make the request, decision, expiry, and refusal reconstructable from the
  event log.
- Preserve REST as canonical. MCP, browser, embedded UI, and mobile surfaces
  translate into the same domain service.
- Keep the core editor- and provider-agnostic.

## Non-goals

- Treating a normal agent message as a human signature.
- Giving a human bearer token to Codex, Claude, Cursor, a shell, or an MCP
  subprocess.
- Reusing approval rows for human-review metadata. External side-effect
  approvals remain a separate lifecycle and separation-of-duties boundary.
- Building a general identity platform or multi-user RBAC system.
- Requiring every client to implement WebAuthn. Clients may open the operator
  URL in the system browser.
- Releasing the historical gate commits as-is.

## Domain contract

### Request

Canonical REST:

```text
POST /v1/work-items/{id}/human-review/requests
```

Request body:

```json
{
  "decision": "waved_through",
  "reason": "optional owner-facing explanation"
}
```

The ordinary authenticated caller supplies an `Idempotency-Key`. The service
requires visibility of the work item and enough ordinary write authority to
request attention, but not `work_items.review_decide`.

The service locks the current item and appends
`human_review.decision_requested` with:

```json
{
  "work_item_id": "uuid",
  "decision": "waved_through|approved|blocked",
  "from_status": "blocked",
  "expected_review_generation": "event-uuid-or-created-event-uuid",
  "expires_at": "RFC3339",
  "reason": "optional owner-facing explanation"
}
```

The event id is the public `request_id`. The response returns the request
snapshot and an operator URL such as:

```text
http://localhost:8080/operator/human-review/{request_id}
```

The URL contains no secret. Anyone may read the same bounded operator-safe
summary that an authorized work-item reader can see. Possessing or opening the
URL cannot decide anything.

The first slice permits only one live request for the tuple
`(work_item_id, decision, expected_review_generation, requester_token_id)`.
An identical idempotent retry returns it. A newer request does not invalidate a
different caller's audit history, but the decision reducer accepts at most one
request for the current generation and makes all siblings stale after a
decision.

### Review generation

`expected_review_generation` is the event id of the latest event which set or
decided `human_review_status`; for an untouched item it is the
`work_item.created` event id. It is derived projection state, not a mutable
counter.

Every accepted human-review decision advances the generation to the decision
event id. A system escalation which only moves lifecycle state does not change
it. A metadata event which changes convergence checks without changing review
status does not change it. This preserves the fixed escalation invariant:
system patience may block lifecycle progress but cannot revoke or fabricate an
owner decision.

### Operator challenge

Canonical REST:

```text
GET  /v1/operator/human-review/{request_id}
GET  /v1/operator/human-review/{request_id}/challenge
POST /v1/operator/human-review/{request_id}/decision
```

The GET renders server-owned content only: exact work-item id, title, current
status, proposed decision, requester identity label, reason, expiry, and a
single confirm action. Agent-provided rich HTML is never rendered.

The challenge endpoint is read-only and returns a fresh, stateless WebAuthn
assertion challenge whose canonical bytes commit to:

```text
home_node_id || request_event_id || work_item_id || decision ||
expected_review_generation || expires_at || nonce
```

The response carries a server-authenticated challenge envelope. The envelope
is signed with a supervisor-managed authentication key, contains an
unpredictable nonce and short expiry, and is not authority: it can only be
completed by the registered authenticator. Challenge issuance changes no
durable state, so it appends no event and needs no projection. Multiple
challenges for one request are safe because the first accepted decision
consumes the request generation; later assertions are idempotent replays or
stale conflicts.

The browser requires `userVerification`, sends the WebAuthn assertion and
signed envelope directly to the decision endpoint, and supplies a generated
`Idempotency-Key`. Neither the model nor the MCP response receives the
assertion. Rotating or losing the challenge-signing key merely invalidates
outstanding short-lived envelopes; it cannot change durable decisions.

### Decision

The assertion-authentication middleware first verifies the cryptographic
envelope and assertion, resolves the mapped human token, and places that token
plus the verified assertion facts in the request context. It does not mutate
state. The idempotency middleware then runs under that verified actor, so a
byte-identical retry can return the original committed result before command
state is reconsidered.

The home-node domain service then verifies under one transaction:

1. the request exists, is unexpired, and is unconsumed;
2. the request names the work item's home node;
3. the item still has the expected review generation and `from_status`;
4. the credential mapping is still active and still names the context actor;
5. that actor is still an active non-root human token holding
   `work_items.review_decide`;
6. the authenticated envelope and assertion facts name this exact request,
   item, decision, generation, node, origin, and RP id; and
7. the requested transition is one of the explicit review decisions.

The handler never derives `actor_token_id`, `source`, user-presence, or
user-verification from a claimed request field. Durable state and authority
are rechecked under the same snapshot as the append; cryptographic verification
is completed before the transaction so it cannot hold a database lock while
waiting on expensive parsing or signature work.

It then appends one `human_review.decided` event with the mapped human token as
`actor_token_id` and `source=human`:

```json
{
  "work_item_id": "uuid",
  "request_event_id": "uuid",
  "from_status": "blocked",
  "decision": "waved_through",
  "expected_review_generation": "uuid",
  "authenticator": "webauthn",
  "credential_id_digest": "sha256-hex",
  "challenge_digest": "sha256-hex",
  "assertion_digest": "sha256-hex",
  "previous_sign_count": 0,
  "sign_count": 0,
  "user_present": true,
  "user_verified": true,
  "origin": "http://localhost:8080",
  "rp_id": "localhost"
}
```

The registered projector updates the work item's `human_review_status` and
review generation, consumes the request, advances the authenticator signature
counter when the credential supplies one, and marks sibling requests for the
old generation stale in the same transaction. One event drives all of those
projection writes.

The browser displays success from the committed projection. The conversation
learns the result through the normal feed/listener path; it does not relay
command output.

An identical retry under the same idempotency key returns the already committed
result. A changed decision, changed assertion, expired envelope, consumed
request, authenticator-counter regression, or stale review generation is a
pure conflict and appends nothing.

## Human authenticator registration

A `human_authenticator` durable object maps one WebAuthn credential public key
to one existing non-root human token. Its projection contains only public
verification material and lifecycle state; no bearer or private key is stored.

Registration and revocation append:

```text
human_authenticator.registered
human_authenticator.revoked
```

Registration is a bootstrap/admin operation, not a review decision. It must be
performed through a short-lived, loopback-only enrollment ceremony started by
the supervisor and completed with owner user verification. The ceremony maps
the credential to a pre-existing, narrowly scoped human decision token. It
must not accept an actor-selected token id, `source`, public key, or claimed
verification flags in an ordinary request body.

The implementation gate must include a concrete, reviewed bootstrap procedure
which an unattended workspace agent cannot complete. Until that procedure is
proven, the decision endpoint remains disabled and the strict review gate must
not ship. A root or operator bearer stored in the workspace is not an
acceptable substitute.

Authenticator revocation takes effect before the next decision verification.
Revocation never changes prior event attribution.

## Projection and replay rules

New projections:

- `human_review_requests`, keyed by request event id;
- `human_authenticators`, keyed by credential-id digest; and
- `work_items.human_review_generation`, derived from existing history plus new
  decision events.

All three are rebuilt solely by folding events. Current state is never inferred
from browser cookies, process memory, or an authenticator signature counter
held outside the log.

The migration must define an explicit historical fold for
`human_review_generation`. For pre-feature history, the latest
`work_item.created` or `work_item.metadata_updated` event which changes
`human_review_status` supplies the generation. The existing projection
boundary from the backed-out commits must not be reintroduced as a one-time
table rewrite which loses accepted history.

Request expiry is a deterministic correlation. A request is open only while
its projection is unconsumed, the item generation still matches, and
`events.occurred_at + ttl` is after the evaluation time. If a durable expiry
event is needed for feed visibility, the worker appends
`human_review.request_expired` once using the request id as its deterministic
cause; it never changes `human_review_status`.

## Access policy

New scopes:

```text
work_items.review_request
work_items.review_decide
human_authenticators.admin
```

- Agent and tracker profiles may receive `work_items.review_request` within
  their existing portfolio/tree visibility. It grants no decision authority.
- `work_items.review_decide` is valid only on active, non-root
  `source=human` identities reached through a registered human authenticator.
- The ordinary bearer REST/MCP metadata-update path cannot use
  `work_items.review_decide` to clear a block. Human-review decisions use the
  dedicated verified service so a stolen bearer alone is insufficient.
- `human_authenticators.admin` registers or revokes public credentials; it
  does not decide reviews.
- The root token remains mint/revoke/bootstrap-only and cannot decide.
- Provider OAuth, token exchange, listener, tracker, and local-agent profiles
  cannot mint, inherit, or impersonate the human-authenticator authority.

The historical `work_items.update_metadata` request must stop forcing callers
to restate review status merely to update convergence checks. The field becomes
optional (or the checklist receives a separate canonical update operation):
omission preserves review status without claiming a decision. The ordinary
metadata path may still move review status toward `blocked`. Once the strict
gate is restored, it refuses every transition from `blocked` to
`waved_through` and every new transition to `approved`, regardless of bearer
source. Preserving an already-approved projection while changing only checks
is not a new decision and appends no review-decision event. Actual review
transitions exist only in the verified decision service.

Ordinary create/spawn operations may continue to default new work to
`waved_through` or conservatively create it `blocked`; they may not create an
item as `approved`. A UI which combines creation and explicit approval still
records two causal events through one verified owner flow: creation first,
then `human_review.decided`. This keeps approval attribution visible instead
of burying it in an agent- or transport-shaped create payload.

## MCP and UI presentation

MCP adds only the safe request tool in the first slice:

```text
work_items.request_human_review
```

It mirrors the canonical REST request and returns the operator-safe snapshot
plus URL. It is idempotent and may be called by an agent. It never accepts an
assertion, human token, actor id, source, or `approved_by` field.

If a client supports MCP elicitation, the tool may ask whether to open the
operator URL. That answer is navigation consent only. It is not the meristem
decision and is never recorded as one.

An MCP App component may render the same server-owned snapshot and invoke the
challenge ceremony. Its private component tool still cannot bypass WebAuthn.
Hosts without embedded UI open the URL in a browser. This keeps Codex, Claude,
Cursor, mobile, and a future meristem UI on one domain contract.

## Bounded patience

- Decision request TTL: five minutes by default, capped by safety policy.
- Challenge-envelope TTL: two minutes and never later than the request expiry.
- Expiry changes no work-item permission. The item remains blocked.
- The requesting worker may issue one new request after a bounded backoff. It
  may not loop indefinitely or create repeated human-attention children.
- After the request retry budget is exhausted, the originating item remains
  blocked with one owner-attention projection entry. No child storm.

## Failure behavior

| Failure | Result |
| --- | --- |
| Agent only requests a decision | Request event only; review status unchanged |
| Agent opens or drives the page | No decision without verified user presence |
| Host auto-reviews an MCP prompt | Navigation at most; no decision authority |
| Request or challenge expires | Pure refusal or one deterministic expiry event; status unchanged |
| Work item changes before confirmation | `409 stale_review_generation`; no append |
| Assertion replay | Original result only for byte-identical replay; otherwise conflict |
| Wrong origin/RP, absent UV/UP, bad signature | `403 invalid_human_assertion`; no append |
| Credential revoked or token inactive | `403 human_decision_authority_inactive`; no append |
| Projection/replay mismatch | Build/release blocker; no fallback table write |
| Operator ingress unavailable | Strict agent-denial gate remains unreleased |

## Release invariants

The implementation release is blocked until one exact combined tip proves:

1. an agent bearer cannot clear `blocked` or assert/retain `approved` through
   REST, stdio MCP, HTTP MCP, tracker, provider, listener, replay, or direct
   metadata services;
2. an agent can create an inert, bounded decision request and receive only an
   operator-safe link;
3. the owner can open that link and confirm without shell, token file, bearer,
   JSON, or output relay;
4. the committed decision event names the registered non-root human token and
   `source=human`;
5. the assertion is bound to the exact item, decision, generation, node, and
   expiry and cannot be replayed or reused;
6. an unattended agent, automatic approval reviewer, alternate MCP client,
   and browser automation without user verification cannot decide;
7. event replay reproduces authenticator, request, consumption, generation,
   and work-item projections exactly;
8. escalation preserves prior human-review status and does not manufacture a
   new decision request for every worker tick;
9. the pre-gate historical fixture rebuilds without changing accepted review
   history;
10. an independent exact-commit review accepts both the operator workflow and
    the authority boundary; and
11. the release procedure verifies the operator ingress on the deployed
    build before enabling the strict gate.

The end-to-end regression begins with an agent-authenticated request, pauses at
the operator UI, consumes a real test authenticator assertion, and ends by
reading the durable feed/work-item projection. A unit test which injects a
`source=human` token directly into the service is necessary but not sufficient.

## Implementation slices

1. **Event and reducer contract.** Add request/decision/authenticator events,
   projections, review generation, replay, and pure authorization reducers.
   Keep the decision endpoint disabled.
2. **Authenticator bootstrap and verifier.** Add the loopback enrollment
   ceremony, public-key projection, revocation, WebAuthn verification, and
   negative user-presence tests. Review this slice independently because it is
   the human identity root.
3. **Operator surface.** Add the server-owned page, challenge/decision REST
   endpoints, CSP/origin protections, and browser regression. No framework is
   required for the first page.
4. **Request translations.** Add the safe REST and MCP request surfaces and
   operator-safe feed rendering. Add optional open-link elicitation without
   treating it as a decision.
5. **Combined gate.** Reapply the strict service/access denials against the new
   dedicated decision path, migrate historical review generations, and run the
   full end-to-end and rebuild lanes on one exact commit.
6. **Client presentation follow-ups.** Embed the same contract in MCP Apps,
   mobile, or provider-native UI only after each adapter proves it cannot
   weaken user presence or attribution.

Slices 1-4 are dark substrate. Slice 5 is the only release which changes the
existing operator behavior. Partial deployment of denial without ingress is
forbidden.

## Rejected alternatives

### Let the agent relay the owner's prose

The event would still be agent-attributed and the agent could fabricate or
replay it. A transcript is evidence for a human to inspect, not an
authentication factor.

### Give the operator bearer to the MCP client

The model can call the same tool unattended and the credential returns to the
process/environment problem which caused the incident. Tool descriptions are
not access control.

### Mark the decision tool destructive and trust the prompt

Hosts may auto-review prompts, and the call reaching meristem does not provide
a verifiable user-origin proof. This is useful UX only after a trustworthy
broker boundary exists.

### Treat an MCP elicitation response as `source=human`

A generic MCP client can answer server requests, and current host behavior may
route elicitations through automatic review. Without a signed, registered host
attestation which distinguishes those cases, the response is not human
identity.

### Use a bearer in an HTTP-only cookie

It hides the token from ordinary page JavaScript but does not prove current
user presence; browser automation can exercise a live session. A cookie may
help bind UI state, but it cannot replace the authenticator assertion.

### Keep the gate backed out permanently

That restores usability by permitting silent agent self-clearance. The
original invariant was correct; the release failed because it shipped only
the denial half.

## Source notes

- OpenAI documents that Codex can elicit approval for side-effecting app/MCP
  tools and that automatic approval review can handle eligible approval
  requests: <https://learn.chatgpt.com/docs/agent-approvals-security>.
- OpenAI's app-server contract documents client-mediated command, file,
  permission, MCP elicitation, and app tool-call approvals, including user
  input prompts: <https://learn.chatgpt.com/docs/app-server>.

Those capabilities justify a future host-native renderer. Neither document
defines a meristem-verifiable human identity attestation on the resulting MCP
tool call, so this design does not treat the prompt itself as authority.
