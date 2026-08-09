# Agent Worktrees

Meristem agent sessions must not share the primary source checkout. The primary
checkout owns local state under `.meristem/`; each assistant works from a
dedicated git worktree based on `v1`.

This prevents one agent from committing another agent's dirty files or building
`meristem-bin` from the wrong ref.

## Prepare A Worktree

Use the helper from the primary checkout:

```bash
scripts/prepare-agent-worktree.sh --target codex
scripts/prepare-agent-worktree.sh --target claude-code-gui
scripts/prepare-agent-worktree.sh --target cursor-mcp
```

Defaults:

- worktree path: `../meristem-<target>`
- branch: `codex/<target>-worktree` for `codex*`, `claude/<target>-worktree`
  for `claude*`, otherwise `agent/<target>-worktree`
- base ref for new branches: `v1`

The helper links `<worktree>/.meristem` back to the primary checkout's
`.meristem/`. Token files remain physically in the primary checkout, but tools
running from a worktree can still find the shared local state.

## HTTP MCP is independent of source worktrees

Codex, Claude Code, and Cursor connect to the one loopback `meristem api`
process over Streamable HTTP. Their generated launch/helpers read per-client
token files but do not start another Meristem process, export a database URL,
or depend on the current source worktree:

```text
.meristem/generated/codex-http-mcp.toml
.meristem/generated/codex-http-launch.sh
.meristem/generated/claude-code-http-mcp.json
.meristem/generated/claude-code-http-headers.sh
.meristem/generated/cursor-http-mcp.json
.meristem/generated/cursor-http-launch.sh
```

Worktrees remain mandatory for editing and reviewing code. They are no longer a
prerequisite for MCP connectivity. The old stdio command wrappers, including
`scripts/cursor-mcp-command.sh`, are development/rollback compatibility paths
for isolated databases; they must not remain active beside the same client's
HTTP `meristem` entry against the shared store.

Cerberus generated wrappers use per-head worktrees:

```text
../meristem-cerberus-coordinator-98853a93
../meristem-cerberus-grower-98853a93
../meristem-cerberus-healer-98853a93
```

Create them with:

```bash
scripts/prepare-agent-worktree.sh --target cerberus-coordinator-98853a93
scripts/prepare-agent-worktree.sh --target cerberus-grower-98853a93
scripts/prepare-agent-worktree.sh --target cerberus-healer-98853a93
```

## Rebuilding meristem-bin

`.meristem/generated/meristem-bin` is the single artifact that backs the API,
worker, listener, and any explicit development-only stdio compatibility
wrapper; routine Codex, Claude, and Cursor MCP access uses the central API.
One rebuild covers the server stack (work item a9374bdd). The one-command path is
`scripts/rebuild-meristem-bin.sh`: it fetches `origin/v1`, refuses a dirty tree
or a HEAD that is not the fetched `v1` tip, embeds the exact commit, publishes
the reviewed-v1 sibling pin, and on macOS ad-hoc code-signs the artifact
(`codesign -s - --force`). `--force` can only write an explicit alternate
artifact whose pin keeps it non-authoritative. Note an
ad-hoc identity is hash-based and changes per rebuild, so expect the Application
Firewall to still re-prompt for the API listener after a rebuild; a stable real
signing identity is the durable fix. Running server or explicit stdio sessions
keep their old process until restarted. After the one-time guarded-runtime
activation, they refuse
authoritative work after observing the new pin; pre-guard sessions cannot do so
and must be drained/stopped before the first guarded publish under work item
`835e0dbf`.

Use the throwaway-worktree procedure below when the primary checkout is busy.
The shared artifact still comes only from the fetched `v1` tip; arbitrary refs
must use `--force` with a separate output and intentionally fail the runtime
guard.

```bash
build_tree=/tmp/meristem-bin-build
git worktree add --detach "$build_tree" origin/v1
git -C "$build_tree" status --short
MERISTEM_BIN_OUT=/Users/juliusmopper/Dev/meristem/.meristem/generated/meristem-bin \
  "$build_tree/scripts/rebuild-meristem-bin.sh"
git worktree remove "$build_tree"
```

If you need a branch or exact commit for local testing, use `--force` with an
explicit alternate `MERISTEM_BIN_OUT`; it intentionally fails the reviewed-pin
guard and cannot replace the shared binary. Do not rebuild the shared binary
from a dirty agent worktree or from whichever branch happens to be checked out
in the primary checkout.
