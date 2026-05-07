# Cursor CLI Provider

The Cursor CLI provider is the local handoff path for assigning a meristem
`work_item` to a Cursor CLI worker, usually running the `composer-2` model.
It is a provider adapter, not a new identity type: worker attribution remains
the agent-source bearer token used by the MCP process.

Use `--model spark` or `--model 5.3-spark` to launch Cursor Agent with
`gpt-5.3-codex-spark-preview`. The provider normalizes the older local label
`composer2` to Cursor's installed `composer-2` name.

## Current Slice

`meristem provider cursor-cli scaffold` prints a secret-free handoff packet:

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@127.0.0.1:5432/meristem?sslmode=disable' \
  meristem provider cursor-cli scaffold \
    --work-item <uuid> \
    --scope 'Implement one narrow change.' \
    --allowed-area internal/example \
    --out-of-scope 'External writes without approval.'
```

The command reads the `work_item` projection and includes:

- assigned work item id and title
- requested scope, allowed areas, and out-of-scope boundaries
- `human_review_status`
- `suggested_convergence_checks`
- MCP setup that reads `.meristem/cursor-cli.token` at runtime
- a copy/paste worker prompt
- an AGENTS.md overlay for a per-worker workspace

The scaffold never prints token contents. It references token files by path.

For workers that edit a project outside the meristem repo, configure that
target workspace to launch meristem's MCP server from the control-plane repo:

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@127.0.0.1:5432/meristem?sslmode=disable' \
  meristem provider cursor-cli mcp-config \
    --workspace /path/to/target-project \
    --meristem-root /Users/juliusmopper/Dev/meristem \
    --apply
```

The command writes `/path/to/target-project/.cursor/mcp.json` with a command
that `cd`s back to the meristem repo and reads `.meristem/cursor-cli.token` at
runtime. It refuses to replace an existing Cursor MCP config unless `--force`
is set.

To launch a Cursor worker directly:

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@127.0.0.1:5432/meristem?sslmode=disable' \
  meristem provider cursor-cli launch \
    --work-item <uuid> \
    --workspace /path/to/target-project \
    --scope 'Implement one narrow change.' \
    --allowed-area internal/example \
    --model spark \
    --apply-mcp \
    --worktree meristem-<uuid-prefix> \
    --worktree-base <target-project-base-ref>
```

Run `go run ./cmd/meristem ...` from the meristem repo, or use an installed
`meristem` binary and pass `--meristem-root /Users/juliusmopper/Dev/meristem`.
`--workspace`, `--allowed-area`, `--worktree`, and `--worktree-base` refer to
the target project that Cursor will edit. If the target project is meristem
itself, use `--workspace /Users/juliusmopper/Dev/meristem` and
`--worktree-base v1`. For a different project, use that repository's normal
base ref, often `main`.

Use `--dry-run` to inspect the `cursor-agent` argv and prompt without launching.
Use `--mode print --trust --approve-mcps` only for explicitly approved headless
runs; interactive launches let Cursor surface trust and MCP prompts to the
operator.

## Provider Contract

- Use one `source=agent` token per Cursor CLI worker identity. Provision the
  default local token with `scripts/provision-assistant-access.sh --targets cursor-cli`.
- Launch meristem MCP with `MERISTEM_MCP_TOOL_NAMES=cursor` so Cursor sees
  underscore aliases if it filters dot-namespaced tool names.
- A target workspace's `.cursor/mcp.json` may point back to the meristem repo;
  worker edits still happen in the target workspace passed to `cursor-agent
  --workspace`.
- The worker must fetch the assigned work item through MCP before editing.
- `human_review_status=blocked` means the worker stops and asks for human input.
- `human_review_status=waved_through` is the ordinary project-work default.
- `human_review_status=approved` records explicit human clearance.
- External write actions still require approvals; review status does not bypass
  default-deny side-effect policy.
- Completion means the worker either satisfies every suggested convergence
  check or records which check could not be satisfied and why.

## Out Of Scope

This slice does not launch Cursor processes, lease jobs, manage branches, or
build semantic/vector memory. Those are follow-on `work_item`s.
