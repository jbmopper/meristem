# Subactor Grant Reducer

Child B defines the first deterministic rule for agent-created subactors. This
slice is a reducer contract only: no REST route, MCP tool, or secret-returning
API is added here.

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

Agents cannot yet mint their own subactor tokens. That is deliberate: token
issuance is a side effect and must pass through a deterministic reducer before
any automation path exists.

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
  authority, scope widening, or write-capable grants without approval.

Reducer decisions are deterministic and safe to record as events in a later
slice.

## Future Issuance Contract

A future issuance service should:

1. Append `subactor_grant.requested`.
2. Run the reducer.
3. Append `subactor_grant.granted`, `subactor_grant.denied`, or
   `subactor_grant.escalated`.
4. If granted, call the existing auth token creation path so `token.created`
   remains the source of the token projection.
5. Return the plaintext secret exactly once in the immediate response.

Do not write token rows directly. Do not record plaintext token secrets in
events. Do not expose this over HTTP MCP write tools until the MCP mutation
idempotency contract is resolved.

## Expiry

Cursor's recommended default for future subactor tokens is a short fixed expiry
such as 24 hours. The current token projection has no expiry field, so this
slice documents the policy but does not add a schema migration. Adding
`expires_at` belongs in the issuance slice if the operator wants time-bounded
subactor credentials immediately.
