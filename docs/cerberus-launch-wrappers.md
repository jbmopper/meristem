# Cerberus Launch Wrappers

This document executes work_item `9e5190e8`: define and generate MCP launch
wrappers for Aegis's scoped coordinator identity and the two Cerberus worker
roles.

If this document conflicts with `docs/spec.md`, the spec wins.

## Root And Roles

Root:

```text
98853a93-2de4-42fb-9438-a1a54caf9589
```

Launch roles:

- coordinator: Aegis herself, running with a scoped token for this subtree
- grower: forward progress worker
- healer: convergence/repair worker

The older feed/triage language maps to Aegis's coordinator identity. The older
grower/driver and critic/converger language maps to the two spawnable worker
roles: grower and healer. Per-subactor spawn produces exactly two workers:
grower + healer. No coordinator is spawned as a worker, and no `agent_kind`
schema is introduced.

Model selection is launch metadata, not durable identity:

- coordinator/Aegis session: `gpt-5.4-mini` high, or the current cheapest model
  that can reliably run the goal loop
- grower/healer worker sessions: `gpt-5.3-codex-spark` high for the first pilot
- durable identity: token id, source, scopes, work_item assignment, and events

Record model choices in operator/session launch notes or wrapper-adjacent config,
not in meristem schema.

## Token Files

Each launch role has its own token file:

```text
.meristem/aegis-cerberus-coordinator-98853a93.token
.meristem/cerberus-grower-98853a93.token
.meristem/cerberus-healer-98853a93.token
```

The files are local secrets, mode `0600`, and are not committed. The wrapper
scripts read only their own token file. The coordinator file belongs to Aegis's
scoped identity, not to a third subactor worker.

## Wrapper Paths

Prepare one worktree per head before using the generated wrappers:

```bash
scripts/prepare-agent-worktree.sh --target cerberus-coordinator-98853a93
scripts/prepare-agent-worktree.sh --target cerberus-grower-98853a93
scripts/prepare-agent-worktree.sh --target cerberus-healer-98853a93
```

Generate wrappers with:

```bash
bash scripts/generate-cerberus-launchers.sh
```

Expected generated paths:

```text
.meristem/generated/cerberus-98853a93/coordinator-meristem-command.sh
.meristem/generated/cerberus-98853a93/grower-meristem-command.sh
.meristem/generated/cerberus-98853a93/healer-meristem-command.sh
```

These generated scripts are secret-free except for reading their own token file
at runtime. They set:

- `MERISTEM_DATABASE_URL`
- `CERBERUS_ROOT_ID`
- `CERBERUS_HEAD`
- `MERISTEM_TOKEN`

Each generated script `cd`s into the matching dedicated worktree before running
`meristem mcp`:

- coordinator: `../meristem-cerberus-coordinator-98853a93`
- grower: `../meristem-cerberus-grower-98853a93`
- healer: `../meristem-cerberus-healer-98853a93`

They do not read or fall back to:

- `.meristem/codex.token`
- `.meristem/aegis.token`
- `.meristem/cursor-cli.token`
- inherited `MERISTEM_TOKEN`

Missing or empty token files are hard failures.

## Config Snippets

Per-session MCP config should point each session at its own command.

Coordinator:

```toml
[mcp_servers.meristem]
command = "/Users/juliusmopper/Dev/meristem/.meristem/generated/cerberus-98853a93/coordinator-meristem-command.sh"
enabled = true
```

Grower:

```toml
[mcp_servers.meristem]
command = "/Users/juliusmopper/Dev/meristem/.meristem/generated/cerberus-98853a93/grower-meristem-command.sh"
enabled = true
```

Healer:

```toml
[mcp_servers.meristem]
command = "/Users/juliusmopper/Dev/meristem/.meristem/generated/cerberus-98853a93/healer-meristem-command.sh"
enabled = true
```

Do not configure all three launch roles in one inherited global Codex config
unless the host supports per-session server selection. The safe operator
ceremony is one Aegis/coordinator session plus separate grower and healer worker
sessions, each with the matching wrapper.

## Smoke Checks

After token files are minted, run:

```bash
bash scripts/generate-cerberus-launchers.sh
bash scripts/smoke-cerberus-mcp.sh
```

The smoke script sends MCP `initialize` and `tools/list` requests to each
wrapper, then verifies required tools are present and out-of-scope tools are
hidden. It should print:

```text
coordinator: ok
grower: ok
healer: ok
```

Expected visibility:

- coordinator is Aegis's scoped session and can see `feed.read`,
  `work_items.get`, `work_items.list`, `work_items.append_event`,
  `work_items.transition`, and metadata tools for the assigned tree.
- grower can read assigned feed/work_items and append in-tree progress or
  grower signals.
- healer can read assigned feed/work_items and append in-tree healer, repair, or
  stand-down signals.
- no head sees tools outside its token policy.

If a head authenticates but sees no work_item tools, check that the token has:

```text
work_items.tree:98853a93-2de4-42fb-9438-a1a54caf9589
work_items.read
work_items.write
feed.read_assigned
```

If a head can read/write outside the root tree, revoke the token and stop the
pilot.

## Operator Cutover

1. Mint the scoped Aegis coordinator token plus the grower/healer worker tokens
   and generate wrappers:
   `bash scripts/provision-cerberus-access.sh`.
2. If provisioning manually instead, follow
   `docs/cerberus-scoped-token-bootstrap.md` and write each plaintext secret
   exactly once to its token file.
3. Run `bash scripts/generate-cerberus-launchers.sh` after any manual token
   changes.
4. Point each head's session config at its wrapper path.
5. Verify MCP handshake and tool visibility per head.
6. Append a meristem cutover event naming the wrapper paths.
7. Stop using `.meristem/generated/aegis-meristem-command.sh` for Cerberus.

## Acceptance Checks

- `per_head_wrapper_paths_defined`: satisfied by Wrapper Paths.
- `no_fallback_to_generic_codex_token`: satisfied by generator behavior and
  no-fallback rules.
- `mcp_handshake_smoked_per_head`: satisfied by
  `bash scripts/smoke-cerberus-mcp.sh` after provisioning.
- `tool_visibility_matches_scopes`: satisfied by the same smoke script checking
  required and hidden tools.
- `docs_or_operator_snippet_added`: satisfied by Config Snippets and Operator
  Cutover.
