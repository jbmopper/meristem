# Cursor CLI Provider

The Cursor CLI provider is the local handoff path for assigning a meristem
`work_item` to a Cursor CLI worker, usually running the `composer2` model.
It is a provider adapter, not a new identity type: worker attribution remains
the agent-source bearer token used by the MCP process.

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

## Provider Contract

- Use one `source=agent` token per Cursor CLI worker identity. Provision the
  default local token with `scripts/provision-assistant-access.sh --targets cursor-cli`.
- Launch meristem MCP with `MERISTEM_MCP_TOOL_NAMES=cursor` so Cursor sees
  underscore aliases if it filters dot-namespaced tool names.
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
