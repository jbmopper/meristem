# Cerberus Scoped-Token Bootstrap

This document executes work_item `81e57433`: define the bootstrap path from the
legacy unscoped Aegis launcher to a scoped Aegis coordinator token plus scoped
grower/healer worker tokens.

If this document conflicts with `docs/spec.md`, the spec wins.

## Chosen Root

The Cerberus root for the first pilot is:

```text
98853a93-2de4-42fb-9438-a1a54caf9589
```

Title: `Codex Spark bicameral subactor net`.

Rationale:

- It is the umbrella item that already owns the converged Aegis/Cerberus plan.
- Phase 1 completed under this root.
- Phase 2, smoke, and launch wrappers are all children or planned descendants
  of this coordination subtree.

## Bootstrap Sequence

Bootstrap is deliberately explicit and root-token gated. The existing
`aegis.token` may coordinate documentation work, but it is not the authority
that creates the first scoped Aegis coordinator token.

1. Operator or root-token holder chooses the Cerberus root above.
2. Root token mints a scoped source=`agent` coordinator token named
   `aegis-cerberus-coordinator-98853a93`.
3. The plaintext secret is written once to a local token file with mode `0600`:
   `.meristem/aegis-cerberus-coordinator-98853a93.token`.
4. A generated MCP launcher reads only that file at process start and exports
   `MERISTEM_TOKEN`.
5. Aegis uses the coordinator token to request or mint scoped same-tree worker
   tokens for the grower and healer. The coordinator is not a spawned worker.
6. After the scoped launcher is smoke-tested, the legacy
   `.meristem/aegis.token` launcher is retired for Cerberus coordination.

No step records plaintext secrets in events, docs, shell history, generated JSON,
or MCP config. Token files are local operator material.

## Coordinator Token

Name:

```text
aegis-cerberus-coordinator-98853a93
```

Source:

```text
agent
```

Scopes:

```text
work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589
work_items.read
work_items.write
feed.read_assigned
```

These are the minimal currently shipped scopes that let Aegis coordinate inside
one subtree. They intentionally do not include:

- root authority
- approval decision authority
- log visibility scopes
- global work item read/write scopes
- inbox capture
- access to token secrets

The coordinator can append coordinator events, transition in-tree work_items
where the scoped access policy allows writes, and request same-tree subactor
grants. It may not use free-form judgment to bypass deterministic reducers.

Manual minting command shape:

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" \
  go run ./cmd/meristem tokens create \
    --name aegis-cerberus-coordinator-98853a93 \
    --source agent \
    --scopes 'work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589,work_items.read,work_items.write,feed.read_assigned'
```

The command output includes a plaintext secret exactly once. Redirect it to a
temporary file, extract only the `secret=` value into the token file, then delete
the temporary file.

## Worker Token Shapes

The context contract in `docs/cerberus-context-composer.md` defines the
operation boundaries. A Cerberus subactor spawns exactly two workers: grower and
healer. Current shipped scopes are coarse, so grower and healer tokens use the
same same-tree write scope but differ by launcher prompt and token name. Narrower
operation-specific scopes can land later as normal work_items.

Grower:

```text
name: cerberus-grower-98853a93
source: agent
scopes:
  work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589
  work_items.read
  work_items.write
  feed.read_assigned
```

Allowed by prompt and review:

- append bounded grower/candidate events
- request a verified-waypoint child when depth permits
- read latest healer signal before moving

Not allowed:

- lifecycle decisions from prose
- approval decisions
- token minting outside approved grant paths
- secret inspection

Healer:

```text
name: cerberus-healer-98853a93
source: agent
scopes:
  work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589
  work_items.read
  work_items.write
  feed.read_assigned
```

Allowed by prompt and review:

- append `worker.healer_signal` or related bounded repair/stand-down events
- evaluate grower events against Aegis-authored standards
- request a verified-waypoint child when depth permits

Not allowed:

- lifecycle decisions from prose
- approval decisions
- raw token secret access
- unrelated subtree reads

Reader/auditor:

```text
name: cerberus-reader-98853a93
source: agent
scopes:
  work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589
  work_items.read
  feed.read_assigned
```

## Subactor Grant Path

The current reducer in `docs/subactor-grants.md` supports:

- `same_tree_read_progress`
- `same_tree_worker`

Near-term use:

- The coordinator can request `same_tree_read_progress` for read-only auditors
  inside the root subtree.
- Write-capable `same_tree_worker` currently escalates unless the request has
  `human_review_status=approved`; that is acceptable for early Cerberus
  bootstrap because it preserves separation of duties while issuance is new.
- Until approval ergonomics improve, the root/manual minting path may be used
  for the first grower/healer pilot tokens, with the same scopes listed above.

This keeps token issuance deterministic and fail-closed without adding a new
runtime agent taxonomy.

## Launcher Shape

Generated launchers are secret-free scripts except for reading their own token
file at runtime. They run from a dedicated worktree and read token files from
the primary checkout:

```bash
#!/usr/bin/env bash
set -euo pipefail
primary_repo="/Users/juliusmopper/Dev/meristem"
workspace_root="/Users/juliusmopper/Dev/meristem-cerberus-coordinator-98853a93"
cd "$workspace_root"
export MERISTEM_DATABASE_URL="postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable"
export MERISTEM_TOKEN="$(tr -d '\n' < "$primary_repo/.meristem/aegis-cerberus-coordinator-98853a93.token")"
exec go run ./cmd/meristem mcp
```

Rules:

- No bearer secret appears in the script body.
- The script `cd`s into the head's dedicated worktree before running
  `meristem mcp`.
- Missing token file is a hard failure.
- Empty token file is a hard failure.
- The script does not fall back to `.meristem/codex.token`,
  `.meristem/aegis.token`, or any environment token.
- Per-head launchers differ by token file and operator-facing name, not by
  schema.

## Panic Revoke

Panic revoke is root-token/manual and explicit:

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" \
  go run ./cmd/meristem tokens revoke --id <token_uuid>
```

After revocation:

1. Stop the affected MCP session.
2. Delete or quarantine its local token file.
3. Read the feed for in-flight events attributed to the revoked token.
4. Append a coordination event on the root item summarizing the revoke reason
   and affected token ids, without recording token secrets.
5. Mint replacement scoped tokens only after the root item is back in a known
   state.

There is no silent fallback to generic Codex or Aegis tokens after revoke.

## Retiring Legacy Aegis

The legacy `.meristem/aegis.token` launcher is retired for Cerberus when all of
these are true:

- scoped coordinator launcher can authenticate and read `98853a93`
- scoped coordinator can append an in-tree progress event
- grower and healer token files exist or their grant path is approved
- old Aegis launcher is removed from operator MCP config for this role
- a meristem event records the cutover and names the replacement launcher

The old token need not be globally revoked immediately if it still serves other
manual operator workflows, but it must not be the Cerberus coordinator identity.

## Acceptance Checks

- `depends_on_context_composer_60959376_recorded`: satisfied by referencing the
  completed context contract as the source of role and authority boundaries.
- `cerberus_root_chosen`: satisfied by choosing root `98853a93`.
- `coordinator_grower_healer_scopes_derive_from_context_contract`: satisfied by
  mapping context roles to scoped token shapes above.
- `coordinator_scopes_minimal`: satisfied by using only tree, read, write, and
  assigned-feed scopes.
- `secret_handling_defined`: satisfied by the bootstrap and launcher rules.
- `panic_revoke_recovery_defined`: satisfied by the revoke section.
- `legacy_unscoped_aegis_retirement_path_defined`: satisfied by the retirement
  section.
