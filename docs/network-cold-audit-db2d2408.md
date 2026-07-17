# Boring Network cold audit: mesh fault matrix and child split (db2d2408)

Status: read-only audit, first deliverable of work item
`db2d2408-8945-5dc4-8cfe-e96562b8095c` ("Boring Network: robustness and
ergonomics are one contract"). Base audited: meristem `origin/v1 @ dbfc915`.
Author: `claude-code-session-4007a223` (non-fork implementer lane; codex
reviews). No code or live network change is made here.

## 0. Scope and context

The inter-node mesh this item is about is the **meristem crossnode layer**:
direct authenticated REST plus durable queue fallback over HTTPS
(`internal/crossnode`, `internal/spoke`, contract in
`docs/network-layer-spec.md`). It does not ride WireGuard.

Context the owner set for this work: the mesh's first real fleet host — the
Arch box — is not a simple machine. It runs multiple access points and layered
policy routing (split-tunnel VPN egress, per-subnet rule tables, its own
firewall), and will also carry the public ingress tunnel. On a host like that,
"the mesh call failed" is not actionable unless the node itself can say which
route it tried, what happened, and when it will try again. That is why
robustness and ergonomics are one contract here: the transport must fail in
bounded, deterministic ways (robustness), and the operator must be able to see
those ways without SSH archaeology or raw SQL (ergonomics).

Out of scope by owner directive: the Arch box's own egress stack
(`~/Dev/wg-control`) is not audited here. One naming correction is retained
for the record because it changes what "the mesh" means: wg-control is a
Mullvad split-tunnel **egress controller** for the box's WiFi clients — its
only WireGuard peer is a Mullvad relay; there is no inter-node peering in it.
It is the underlay environment the meristem node must be legible *on top of*,
not the mesh itself.

## 1. Fault matrix — Stage 1 delivery path

Paths: **D** = direct REST (sender walk), **Q** = durable queue host
(enqueue + ack), **S** = spoke drain (pull-only target). Budgets, verified at
`dbfc915`: direct = 3 attempts (`defaultDirectAttempts`), 10s per attempt
(`defaultHTTPTimeout`), 60s wall (`defaultDirectPatience`), backoff 1s then
2s, route cooldown 60s (`CooldownWindow`); queue = 24h deadline
(`CommandQueuePatience`), 5 local attempts (`MaxCommandAttempts`). Each cell:
behavior / where handled / deterministic test evidence (all test names
verified present).

| Fault | D: direct REST | Q: queue host | S: spoke drain |
|---|---|---|---|
| Transport failure (conn/DNS/TLS) | Retry within budgets, cool route 60s, advance to queue candidates (`deliver.go` retryable branch). Tests: `TestDeliverAdvancesPastTransportFailure`, `TestDeliverAllRoutesFail` | Sender walks queue hosts in registry order; each failure cools that route key only (`select.go`). Tests: `TestSelect`, `TestSelectRouteKeysAreStableAndDistinct` | Hub unreachable → warn + retry next tick, process stays up (`TestTickHubDownNoOp`); local API down → command stays pending, no ack (`TestTickLocalDownLeavesPending`) |
| Timeout | Per-attempt ctx (10s) nested in wall-clock ctx (60s), both enforced in `DeliverWithPolicy` | Enqueue POST under the same walk and budgets | Per-request deadlines from the spoke's HTTP client; cadence owned by the poll loop |
| 4xx (auth/validation/conflict/429) | Definitive: stops walk, surfaces status, **never** bypassed via queue (spec §4). Tests: `TestDeliverStopsOnDefinitiveNon2xx`, `TestDeliverNeverFallsBackOnDefinitiveDirectResponses` | Enqueue refused by target/operation scope checks (`authz.go`). Tests: `TestCrossnodeAuthorizationIsTargetAndOperationScoped`, `TestCrossnodeAuthorizationRejectsRootLegacyAndRevokedTokens` | Refusals acked `refused`, not retried; invalid command path refused before execution (`TestTickRefusesInvalidCommandPath`) |
| 5xx | 502/503/504 retryable + cooldown; unclassified 500 definitive (`TestDeliverAdvancesPast5xxToQueue`, `TestDeliverDoesNotBypassUnclassified500ThroughQueue`) | Same classification at enqueue | Retryable 5xx consumes one of 5 attempts without acking (`TestTickRetryable5xxConsumesAttemptWithoutAck`) |
| Stale route | Cooldown is 60s, local-only, lost on restart (spec-sanctioned hint); disabled/unknown target refused by `Select`; registry snapshot is monotonic, atomically replaced, retained through hub outage | `relay_via` list read from one snapshot per operation (`TestDispatcherLoadsOneSnapshotAndUsesProductionSelection`) | Spoke config origins validated at load (`TestLoadConfigRejectsUnsafeOrigins`, `TestLoadConfigAcceptsCredentialSafeOrigins`) |
| Duplicate delivery | `Idempotency-Key` on every attempt; retry collapses at home middleware (`TestDirectRouteTwoNodeAcceptance`) | `origin_idempotency_key` preserved on the queued row; replayed enqueue folds to the same row (deterministic event id, `ON CONFLICT DO NOTHING`); first terminal ack wins, later conflicting acks refused (`TestAckProjectorIntegration`) | Local retry reuses the original key; exec-ok-ack-fail replays without a second mutation (`TestQueueFirstTwoNodeAcceptance`) |
| Restart | Cooldowns lost (sanctioned); no long-lived sender process exists yet (see G1) | Queue rows event-backed and durable; expiry clock runs from `queued_at`, so restart cannot extend patience | Durable event-backed poll cursor + idempotency resume (`TestEventCursorStoreIntegration`, `TestTickFeedCursorAdvances`) |
| Budget exhaustion | `ErrAllRoutesFailed` after the walk; caller decides disposition | 24h / 5 attempts → terminal `expired`; causing work item fails with `cross_node_delivery_expired` (`TestQueuePatienceIntegration`) | Outcome return: origin observes terminal facts once, one origin transition, conflicting terminal facts fail closed (`TestOutcomeReturnTwoNodeAcceptance`) |

Check "legacy relay_via is queue-host allowlist only; application relay
deferred": **satisfied in code** — `Select` emits only direct and queue
candidates, `KindRelay` returns `ErrUnsupportedRoute`
(`TestDeliverRejectsApplicationRelay`). The naming debt itself remains (G3).

**Verdict on the engine:** solid. Every matrix cell has code and nearly every
cell has a named deterministic test. The robustness gaps found are *around*
the engine, not in it.

## 2. Findings, ordered by how much "boring" they buy

**G1 — the engine has no drivetrain.** `Dispatcher.DispatchMutation` and
`Dispatcher.ReadWorkItem` have zero production callers (verified by grep at
`dbfc915`: definitions and tests only). No CLI, worker, or domain flow issues
a cross-node mutation or read today; everything above the transport is
potential energy. This is not a defect in this item's scope — the designed
first consumer is **addressed push**
(`6f7fafb2-07ae-508d-a9f3-a844497e9620`, state `captured`), whose
assignment/lease/assigned-feed flows are exactly what would drive the
dispatcher. Flagged so the dependency is explicit: Boring Network hardens the
transport; addressed push is what makes it load-bearing.

**G2 — the live state is invisible without psql (check #4, unmet).** All the
state an operator needs already exists in projections: `command_queue` carries
state/attempt_count/last_attempt_at/expires_at/terminal_reason per command;
`crossnode_outcome_observations` + `crossnode_outcome_cursors` carry the
origin-side view; `nodes` carries declared routes; `spoke_state` carries the
drain cursor. But no supported surface reads any of it:
`docs/network-operations.md` §"Verify and operate" is a five-step manual
checklist, `meristem node list` shows registry *intent* only, and `/readyz`
reports build/db/oauth, not routing or queue depth. Answering "what is my
route to node X, what is queued, what failed last, when is the next retry"
requires raw SQL today. → child **A1**.

**G3 — relay_via naming debt.** The field means "ordered queue-host
allowlist" (scope clarification recorded on the stem 2026-07-17). Until it is
renamed `queue_via`, every new reader re-derives that relay is not a thing.
Bounded rename across registry payloads, projection, and docs. → child
**A3** (unclaimed; can wait).

**G4 — matrix cells leaning on acceptance tests.** Three semantics are
covered only implicitly by the two-node acceptance suites rather than by
focused deterministic tests: (a) restart between spoke fetch and ack (covered
via ack-failure injection, not a literal restart), (b) per-attempt vs
wall-clock timeout interplay at the boundary (accelerated-clock coverage
only), (c) duplicate `command.queued` enqueue folding under concurrent
replay. Acceptance coverage is real coverage, but focused tests are what keep
these semantics pinned when the acceptance suites evolve. → child **A2**.

## 3. Child split (structure-and-dispatch)

Each child is bounded, code+tests only, no live network, no infrastructure,
no merge without its own gate — per the parent contract. Reviewer for every
child: codex, against the exact delivered commit.

- **A1 — mesh diagnostics surface** (G2; parent check #4). A read-only
  operator surface answering: current route plan to each node (what `Select`
  would emit from the live registry snapshot), queue state per target
  (pending/attempts/oldest/expiry), last failure (last_attempt_at,
  terminal_reason, outcome status), and next retry (spoke cadence /
  expires_at). Allowed: new read-only code in `cmd/meristem` (subcommand),
  read helpers in `internal/crossnode` / `internal/nodes`, tests, and a
  rewrite of the manual checklist section in `docs/network-operations.md`.
  Forbidden: any mutation path, new event kinds, live network calls,
  credentials. **Claimed by non-fork in this lane; highest value, fully
  independent.**
- **A2 — fault-injection test hardening** (G4; parent check #5). Add the
  focused deterministic tests named in G4 as `internal/crossnode` /
  `internal/spoke` test files only. Independent of A1.
- **A3 — rename relay_via → queue_via** (G3). Mechanical rename with
  compatibility reading of the legacy field, plus docs. Unclaimed; lowest
  urgency; should not start until A1/A2 land to avoid churn under review.

Not created as children here: G1's drivetrain belongs to addressed push
(`6f7fafb2`), already captured with its own design record; anything touching
the Arch box's own network stack is out of scope by owner directive.

## 4. What this audit deliberately did not do

No live network change, no firewall/routing change, no infrastructure or Arch
bring-up, no credential or secret access, no merge, no managed-queue or
provider coupling proposed. `relay_via` remains inert as an application relay.
The audit is read-only against `dbfc915` and every claim above names its code
or test evidence.
