# Resource safety

Meristem enforces **deterministic, code-owned limits** on resource use before the API, worker, MCP server, or non–dry-run seed are allowed to run. The goal is to avoid restarting into an unbounded configuration: oversized HTTP bodies, unbounded feed long-polls, unbounded pool fan-out, runaway worker polling, or missing patience budgets for non-terminal `work_item` states.

This slice is **not** operator-tunable via environment variables. Policy lives in `internal/safety`, with the active named profile selected through the event-sourced `policy_profile` switch. Later work may project additional policy from the event log; even then, startup should fail closed if the effective policy is absent or invalid.

## Policy contents

| Control | Role |
|--------|------|
| `MaxRequestBodyBytes` | Upper bound on JSON request bodies for handlers that use the shared JSON decoder (e.g. inbox, signals, work-item commands). Requests over the limit receive **413** with `request_too_large`. |
| `MaxFeedWait` | Maximum `wait` query duration on **`GET /v1/feed`** watcher mode. Larger values receive **400** with `wait_too_large`. |
| `PoolMaxConns` / `PoolMinConns` | Profile-governed pgx pool bounds for long-lived processes. Startup resolves the active profile first, then re-opens the real pool with these bounds. Validation rejects non-positive values, `min > max`, and any `max` above the code-owned ceiling. |
| `WorkerTickInterval` | Default cadence for `meristem worker` daemon mode. Operators may still pass `--interval` explicitly for incident tuning, but the active profile owns the unattended default. |
| `PatienceBudgets` | Positive duration per **non-terminal** `work_item` state. Used by `internal/worker` default budgets and validated at startup so the bounded-patience invariant has explicit numbers. |
| `MaxChildrenPerItem` | Fallback maximum counted child work items under one parent when a parent has no cultivar-specific `xylem.max_children_per_item`. Over-budget spawn attempts append `xylem.exhausted`, block/escalate the parent, and do not create the requested child. |
| `MaxConcurrentRunningPerToken` | Fallback maximum work items one token may hold in `running` at once when the target item has no cultivar-specific `xylem.max_concurrent_running_items_per_token`. Over-budget running transitions append `xylem.exhausted`, block/escalate the target item, and do not enter `running`. |
| `MaxEventsPerItemPerHourByClass` | Fallback maximum metered work-item events one work item may receive per hour, keyed by R6 taxonomy class (`decision`, `progress`, `lifecycle`). Public work-item event, metadata, and transition writes are metered; internal xylem/escalation recovery events bypass the meter so exhaustion can always reach a fixed point. Cultivars may override individual classes with `xylem.max_events_per_item_per_hour_by_class`; zero or absent class entries use the safety fallback. |
| `MaxDelegationDepth` | Fallback maximum subactor delegation depth when a target work item has no cultivar-specific `xylem.max_depth`. Over-budget grant requests escalate and mint no token. |
| `MaxSignalItemsPerTokenPerHour` | Maximum number of **new** `work_items` one source token may create through **`POST /v1/signals`** per rolling hour, counted from recorded signal rows (`created_work_item = true`, trailing hour). Signals that dedupe-link onto an existing live item are **not** metered. Over-budget admission still records the signal row for audit (with a NULL `work_item_id`), appends `xylem.exhausted` on the signal subject, raises one owner escalation per token per hour, and refuses item creation with **409** `signal_item_budget_exhausted`. Enforced against the active profile's value in `internal/signals`. |

Default values are defined in `internal/safety/policy.go`. `steady` keeps the historic operational posture: 1 MiB bodies, 60s max feed wait, pool `10/1`, worker cadence `30s`, max children per item 32, max concurrent running items per token 8, max event rates per item per hour of lifecycle 120 / decision 120 / progress 240, max delegation depth 5, signal item admission 30/token/hour, and the original per-state patience defaults. `bring-up` keeps the relaxed patience budgets introduced for first-live operation and also lowers the pool to `4/1` with a `60s` worker default for resource-constrained hosts, and tightens signal item admission to 10/token/hour while backlog and reconcilers are still standing up. `MaxPatienceBudget` is the shared finite ceiling: policy profiles, explicit work-item `patience_budget_seconds`, and cultivar-derived xylem wall-clock budgets for running items must not create an effectively infinite wait. `MaxPoolMaxConns` is the code-owned ceiling for profile-declared pool fan-out. Pre-claim waits use explicit item patience when declared, otherwise the active profile's per-state patience budget.

## Fingerprint

`Policy.Fingerprint()` returns a short stable hex id derived from the canonical JSON of the policy. It appears in:

- Structured logs when the API listens (`safety_policy`).
- **`GET /readyz`** as `safety` / `safety_policy` alongside database readiness.
- Output of **`meristem safety check`**.

Use the fingerprint in runbooks (“which policy build is this binary running?”).

## Commands and gates

- **`meristem safety check`** — Validates the default policy and prints JSON to stdout. Does **not** connect to Postgres. Safe for CI and pre-migrate checks.

The following **re-validate** policy on startup (after `safety check` in bootstrap, each command still calls the same validation internally):

- `meristem api`
- `meristem mcp`
- `meristem worker`
- `meristem worker --once`
- `meristem seed v1` (not **`--dry-run`**)

`migrate`, `tokens`, `rebuild`, `healthcheck`, and `feed` CLI are unchanged by the safety gate at the `cmd` layer; only the long-running or mutating automation entry points above require a valid policy envelope.

## Changing the policy

1. Edit `internal/safety/policy.go` (and tests).
2. Run `go test ./internal/safety/... ./internal/storage/... ./cmd/meristem/...`.
3. Note the new fingerprint from `meristem safety check` in release notes or coordination docs if operators compare fingerprints across deploys.

## Related documents

- [`docs/operations.md`](operations.md) — bring-up order (includes safety check).
- [`docs/spec.md`](spec.md) — bounded patience principle.
- [`AGENTS.md`](../AGENTS.md) — repository layout (`internal/safety`).
