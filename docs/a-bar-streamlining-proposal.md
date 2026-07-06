# A-Bar Streamlining Proposal

Status: approved for constrained, non-destructive cleanup. The event log and
live `work_item`s remain the source of truth. This document is the cleanup map
for `09ec78f2-3b6a-5662-b807-bbaf051b0756`; destructive doc archival/deletion
or backlog cancellation remains split into explicit follow-up changes.

## A-Bar Review Map

| Review bullet | Live item | Current state | Proposed action |
| --- | --- | --- | --- |
| Always-on reconciliation | `9fcccf75-9bf6-5f7f-b8be-db7397b9157a` - A-bar daemon worker runner | done | Keep as landed evidence. Remaining full convergence work stays under the v1 substrate parent, not this slice. |
| Job queue + `SKIP LOCKED` | `7e4cfde6-f6b1-59ed-b407-be26f7411757` - A-bar job_queue enqueue and claim | done | Keep. Do not cancel seeded umbrella `2cae21e0` until its remaining "job execution semantics" scope is clarified. |
| Minimal approvals | `fc98dac5-2f13-5631-a9e0-97a46c172bcd` - minimal approval lifecycle | done | Keep as proof of approval object/projection/API/MCP/separation-of-duties. Remaining re-prompt/push ergonomics stay v1 substrate work. |
| Approval-gated connector proof | `f45a014d-6287-5a21-979a-7dc29ce90a9e` - HTTP connector stub | done | Keep. Retries/dead-lettering remain open under generic connector substrate scope. |
| Operational closure on bring-up | `873491a9-051e-5e1c-8438-aa9c841544dc` - doc/state projection cleanup | done | Keep. Owner path now lives in `docs/owner-quickstart.md`, `docs/owner-deep-dive.md`, and `docs/operations.md`. |
| Integration test story | `83bd3fc7-72cc-59e1-b066-ac811ba2d74a` and `c35ba2a3-1f2b-53a0-b01a-6212c56bfb65` | done | Keep both: CI/rebuild gate and shared local harness are separate evidence. |
| R8/R9 refresh parent closure | `c6ba707b-ab22-59d2-8044-86caa34e1d59`, `cdd4f240-a30e-5840-8df7-de3e31465d86`, `6acb3e73-7d02-593f-ac31-73255ce90ac6` | captured / captured / triaged | Treat as the main remaining A-bar closure cluster. Prefer one active R8 closure item plus a parent status note, not three ambiguous active items. |
| Doc/code drift on load-bearing claims | `873491a9`, `1285a949-a355-5e9d-acfc-8ce18d7bac72`, `c35ba2a3` | done | Keep. Add a docs index/banner pass if the owner approves this proposal. |
| Typed transport errors | `f6a47209-2d88-550d-a1a3-5181366aea2d` | done | Keep. |
| Projection rebuild routine | `83bd3fc7` | done | Keep. CI and local harness both now exercise the rebuild path. |
| Scoped agent surface | `3eb5c8c4-f0f9-5720-8c65-2c949252074c`, `5e96aefb-9a57-51f1-b107-83ffcbb526f8` | done / captured | Keep the done matrix. Keep `5e96aefb` open as the true umbrella for artifacts and future HTTP MCP mutations. |
| Patience resolution edge | `e41d7ab0-654e-5a85-b034-b09807176840` | done | Keep. Resolution is documented as state-epoch correlation. |

## Canonical Reading Path

For the operator:

1. `README.md` - project status, topologies, bootstrap entry point.
2. `docs/operations.md` - bring-up and shutdown procedure.
3. `docs/owner-quickstart.md` - command-only live-operation path.
4. `docs/owner-deep-dive.md` - why the quickstart steps exist.
5. `docs/backlog-readiness.md` - how to read the live board.
6. `docs/mcp-parity.md` - current REST/MCP/HTTP MCP parity matrix.

For agents:

1. `AGENTS.md` - required working rules.
2. `docs/spec.md` - final authority when a rule or projection disagrees.
3. `docs/mcp-worker-bootstrap.md` - copy/paste entry prompt for scoped workers.
4. Feature specs only as needed: `docs/scribe-spec.md`,
   `docs/registry-spec.md`, `docs/projections-spec.md`,
   `docs/escalations.md`, `docs/subactor-grants.md`,
   `docs/convergence-engine.md`, `docs/signals.md`.

The short version: `README.md` or `docs/operations.md` gets a human running;
`docs/owner-quickstart.md` gets a human operating; `AGENTS.md` gets an agent
aligned; `docs/spec.md` settles disputes.

## Proposed Doc Cleanup

| Path | Proposed action | Rationale |
| --- | --- | --- |
| `docs/owner-quickstart.codex.md` | Archive or delete after owner approval. | Already marked non-canonical comparison draft; retained only for review material. |
| `docs/owner-deep-dive.codex.md` | Archive or delete after owner approval. | Same as above. |
| `docs/mcp-spec-parity-todos.md` | Keep only if old links still matter; otherwise archive after verifying all listed items are terminal or represented in `docs/mcp-parity.md`. | It is explicitly a migration index, not the current checklist. |
| `docs/2026-04-24-roadmap-notes.md` | Add a historical/advisory banner or move to a docs archive. | Much of its content has been promoted into live items/specs; it should not look like current operator procedure. |
| `docs/agent-coordination-slice.md` | Add a stronger status banner or split durable requirements into current specs, then archive. | It is advisory and partly superseded by shipped feed cursor, backlog readiness, dispatch, and worker behavior. |
| `docs/self-building-api-synthesis.md` | Add a stronger status banner or archive after any still-current API ideas are linked to work items. | It is a useful synthesis, not a contract. |
| `docs/thoughts.md` | Add a historical/philosophy banner. | It informed the current convergence framing but should not compete with `docs/spec.md`. |
| `docs/cerberus-*.md` | Group under a clearly marked experimental/historical section or move to a docs archive after checking links from current briefings. | The Cerberus role topology was superseded by refresh/R1-R9 discipline and canceled items; the docs remain valuable as design history, not current operating path. |
| `docs/refresh-requirements.md` | Keep until refresh parent is terminal, then mark as completed historical requirements. | Owner docs and R1-R9 closure still reference it. |
| `docs/projection-taxonomy-validation.md` | Keep. | It is a committed validation artifact for R6, not an operator path. |

Do not use `docs/coord/` for general archival. It is outage fallback plus
historical coordination archive. If a docs archive is useful, create a separate
`docs/archive/` or use top-of-file status banners first.

## Proposed Backlog Cleanup

These are owner-approval actions, not changes made by this proposal.

| Item | Proposed action |
| --- | --- |
| `c6ba707b` - Refresh parent | Keep as parent until all R1-R9 requirement items are terminal and docs are on trunk. Append a status event summarizing which child or trunk checks remain. |
| `cdd4f240` - R8 parent | Decide whether this is still the active parent or whether `6acb3e73` is the closure item. If `6acb3e73` is active, append a supersession note to `cdd4f240` or transition it once its checks are represented elsewhere. |
| `6acb3e73` - R8 archive replay/export validation | Treat as the active R8 closure slice if archive replay remains undone. |
| `2cae21e0` - Worker with job_queue | Do not cancel just because the A-bar queue slice landed. Either update the reason/body to name remaining job execution semantics, or transition only after spec/substrate acceptance proves the umbrella is complete. |
| `32ff2764` - Token model | Audit against current spec. The visible spec says most token primitives are done; if no residual scope remains, transition with evidence. |
| `5e96aefb` - full MCP parity | Keep open as umbrella for artifact attachment and future HTTP MCP mutation idempotency. It is not a duplicate of the completed A-bar parity matrix. |
| `4fbae441` - lexicon decision | Keep separate. Naming changes touch durable/public surfaces and should not be hidden inside a cleanup pass. |

## Approval Gate

Before any destructive cleanup or backlog cancellation:

- append this proposal to `09ec78f2`;
- ask the owner to approve, reject, or narrow the cleanup scope;
- if approved, implement as small PRs:
  1. docs status banners / optional `docs/index.md`;
  2. owner-path deduplication between `README.md`, `docs/operations.md`, and
     `docs/owner-quickstart.md`;
  3. live backlog status events or transitions for the R8/job_queue/token
     umbrellas.

No event rows, historical work items, or archived coordination notes should be
deleted or rewritten. The cleanup is about labels, links, and current-path
friction.
