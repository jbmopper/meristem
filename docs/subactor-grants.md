# Subactor Grant Reducer

Child B defines the first deterministic rule and issuance path for
agent-created subactors. The reducer remains pure; the REST route resolves
database facts, records grant lifecycle events, and only mints a token after a
`grant` decision.

## Current State

Root can mint scoped tokens today with `meristem tokens create`. Scoped token
use is enforced by `internal/access` across REST and MCP:

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" \
  go run ./cmd/meristem tokens create \
    --name worker-<work-item-prefix> \
    --source agent \
    --scopes 'work_items.read,work_items.write,feed.read_assigned,work_items.tree:<work_item_uuid>'
```

Agents can request subactor tokens through:

```http
POST /v1/subactor-grants
```

The route is bearer-authenticated and runs behind the standard HTTP
idempotency middleware. Request body:

```json
{
  "template": "same_tree_read_progress",
  "work_item_id": "00000000-0000-0000-0000-000000000000",
  "requested_scopes": [],
  "name": "optional-token-name"
}
```

`work_item_id` is the requested tree root. `requested_scopes` is optional; when
omitted, the named template supplies the scopes. Requested token source is fixed
to `agent`, and `human_review_status` is read from the target `work_item`
projection rather than accepted from the request body.

## Reducer Inputs

`internal/grants.Reduce` is pure. Callers resolve database facts first and pass
them in:

- parent token projection
- requested grant template
- requested token source
- requested `work_items.tree:<uuid>` root
- requested scopes, if the caller supplied them explicitly
- tree relation between the parent assignment and requested root:
  `same | descendant | outside | unknown`
- delegation depth facts resolved from the parent token's `work_items.tree`
  root(s): whether depth is known, the target depth, the max depth, and whether
  that max came from the target work item's cultivar `xylem.max_depth` or the
  safety-policy fallback
- human review status attached to the grant request
- whether logs visibility or approval authority was requested

The reducer reads no database, clock, environment, or process-local state.

## Templates

### `same_tree_read_progress`

Read-only observer/progress token for the same work-item tree:

- `feed.read_assigned`
- `work_items.read`
- `work_items.tree:<requested_root>`

This is the first automated grant template. It may be granted without human
approval only when it is a subset of the parent token and the requested root is
the same tree or a descendant.

### `same_tree_worker`

Write-capable worker token for the same work-item tree:

- `feed.read_assigned`
- `work_items.read`
- `work_items.write`
- `work_items.tree:<requested_root>`

This template exists in the reducer, but it escalates unless the grant request
has `human_review_status=approved`. That preserves separation of duties while
the delegation path is new.

## Outcomes

- `grant` means the request matched a known template, stayed in-tree, stayed
  within the parent token's effective authority, and satisfied the template's
  review rule.
- `deny` is for malformed or unknown requests that cannot become valid by human
  approval, such as a missing parent token or unknown template.
- `escalate` is for requests that might be valid with human judgment but must
  not self-issue: out-of-tree roots, unknown ancestry, legacy unscoped parents,
  root delegation, non-agent source changes, logs visibility, approval
  authority, scope widening, over-budget delegation depth, or write-capable
  grants without approval.

Reducer decisions are recorded as feed-visible events:

- `subactor_grant.requested`
- `subactor_grant.granted`
- `subactor_grant.denied`
- `subactor_grant.escalated`

There is no `subactor_grants` projection table in this slice. Token projection
state still comes from `token.created`; escalation state still comes from
`escalation.requested` plus the work_item events emitted by the escalation
service.

## Issuance Contract

1. Append `subactor_grant.requested`.
2. Run the reducer.
3. Append `subactor_grant.granted`, `subactor_grant.denied`, or
   `subactor_grant.escalated`.
4. If granted, append `token.created` through auth's token creation path in the
   same transaction so the `tokens` row remains a projection.
5. If escalated, call `escalations.RequestInTx` in the same transaction so the
   human-visible escalation work_item is durable.
6. Return the plaintext secret exactly once in the immediate fresh response.

The idempotency cache records a redacted replay body for granted responses:
fresh callers receive `token_secret`, but `idempotency.recorded` and replayed
responses do not contain the plaintext secret. Durable retries after the
24-hour HTTP cache window use the deterministic grant id to return the existing
grant outcome without minting a second token; existing granted outcomes return
token metadata but no secret.

Do not write token rows directly. Do not record plaintext token secrets in
events. Do not expose this over HTTP MCP write tools until the MCP mutation
idempotency contract is resolved.

`subactor_grant.requested` records the depth-budget facts used by the reducer:
`delegation_depth_known`, `delegation_depth` when known,
`max_delegation_depth`, and `depth_budget_source`. A cultivar-scoped source is
recorded as `cultivar:<name>@<version>`; otherwise it is `safety_policy`.

## R5 cultivar activation gate

R5 reuses `internal/grants.Reduce` as an authority gate without using the token
issuance contract above. Worker-proposed cultivar activation records
`cultivar_activation.*` events, evaluates the proposal as `same_tree_worker`,
and appends `cultivar.defined` only after the reducer grants and the proposal
work item has an explicit separated `human_review_status=approved` event. This
path does not append `subactor_grant.granted` and does not mint a token.

## Expiry

Cursor's recommended default for future subactor tokens is a short fixed expiry
such as 24 hours. The current token projection has no expiry field, so this
slice documents the policy but does not add a schema migration. Adding
`expires_at` belongs in the issuance slice if the operator wants time-bounded
subactor credentials immediately.
