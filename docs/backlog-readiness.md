# Backlog Readiness

`GET /v1/backlog/readiness` and MCP `backlog.readiness` expose a deterministic
summary of the visible backlog. The surface is read-only and derived from the
`work_items` projection, which is itself derived from `events`.

## Contract

Response contract: `backlog.readiness.v1`.

Source:

```text
events -> work_items projection -> existing access reducer -> readiness fold
```

The endpoint does not write events, create a new table, or maintain a second
source of truth. Tokens see only the work items they could already see through
`work_items.list`.

## Groups

- `v1_substrate`: visible work items that match the current substrate/refresh
  backlog naming scheme: `Refresh: disciplined spin-up`, `R1:` through `R9:`,
  `R3 remainder:`, `Token model:`, `MCP/spec parity:`,
  `Backlog readiness projection`, `Self-building gate`, `First slice:`, and
  `Roadmap extraction:`.
- `ready_next`: visible `captured`, `triaged`, or `planned` items that are not
  classified as stale/noise.
- `blockers`: visible `blocked` or `awaiting_approval` items.
- `running`: visible `running` items.
- `stale_noise`: visible canceled/failed items, obvious demo/test/scratch
  titles, or non-terminal items whose current state epoch exceeds the declared
  stale threshold.

Stale thresholds:

```text
running:            > 1 day in state
blocked/approval:   > 7 days in state
captured/triaged/planned non-substrate: > 30 days in state
```

## Drift

`spec_seed_drift` checks for a *partial* refresh backlog: if visible work
items include at least one of the refresh items `R1` through `R9`, every
missing sibling is reported as `missing_refresh_item:Rn`. A fresh bring-up
seed carrying **zero** refresh items raises no drift — the R1–R9 refresh is a
completed one-time initiative (parent `c6ba707b`), so a new node is not
expected to carry it. This surfaces genuine spec/seed drift (a half-seeded or
corrupted refresh) without nine false positives on every clean seed, and
without mutating the backlog or hiding audit history.

## Surfaces

REST:

```bash
curl -fsS 'http://127.0.0.1:8080/v1/backlog/readiness?limit=200' \
  -H "Authorization: Bearer $(cat .meristem/codex.token)"
```

MCP:

```json
{
  "name": "backlog.readiness",
  "arguments": { "limit": 200 }
}
```

The `limit` defaults to `200` and is capped at `200`, matching the current
work-item listing cap.
