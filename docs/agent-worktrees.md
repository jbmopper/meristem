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

## Launch Wrappers

`scripts/provision-assistant-access.sh` generates secret-free MCP command
wrappers for Codex and Claude Code. The generated wrappers now fail closed until
their expected worktree exists:

```text
.meristem/generated/codex-meristem-command.sh        -> ../meristem-codex
.meristem/generated/claude-code-meristem-command.sh  -> ../meristem-claude-code-gui
```

`scripts/cursor-mcp-command.sh` likewise defaults to `../meristem-cursor-mcp`.
Set `MERISTEM_AGENT_WORKTREE` or `MERISTEM_CURSOR_WORKTREE` only for an explicit
operator override.

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

Only rebuild `.meristem/generated/meristem-bin` from a clean worktree at the
intended ref.

```bash
build_tree=/tmp/meristem-bin-build
git worktree add --detach "$build_tree" v1
git -C "$build_tree" status --short
git -C "$build_tree" diff --quiet
git -C "$build_tree" diff --cached --quiet
GOCACHE=/tmp/meristem-go-cache go build \
  -C "$build_tree" \
  -o /Users/juliusmopper/Dev/meristem/.meristem/generated/meristem-bin \
  ./cmd/meristem
git worktree remove "$build_tree"
```

If you need a branch or exact commit, replace `v1` with that ref and verify
`git -C "$build_tree" rev-parse --short HEAD` before building. Do not rebuild
the shared binary from a dirty agent worktree or from whichever branch happens
to be checked out in the primary checkout.
