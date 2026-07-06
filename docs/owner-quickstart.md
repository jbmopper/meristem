# Owner quickstart

Terse path from the built `v1` binary to live operation. Commands and expected
outputs only. For the *why*, read [`owner-deep-dive.md`](owner-deep-dive.md);
this file and that one share the same eight-step spine.

Assumes: Stage 0 bootstrap already ran (`scripts/bootstrap.sh` — Postgres up,
migrations applied, root token minted at `.meristem/root.token` mode 0600, seed
token minted, `meristem seed v1` planted the substrate backlog + rootstock
cultivars + named projections). The API binary lives at
`.meristem/generated/meristem-bin`. Every command below runs from
`~/Dev/meristem` with `MERISTEM_DATABASE_URL` exported.

```bash
cd ~/Dev/meristem
export MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable'
BIN=.meristem/generated/meristem-bin
```

## 1. Per-agent worktrees (human-ack on worktree discipline)

Agents must not share the primary checkout. See [`agent-worktrees.md`](agent-worktrees.md).

```bash
for t in codex claude-code-gui; do
  scripts/prepare-agent-worktree.sh --target "$t"
done                                   # -> target/worktree/branch/base per target
scripts/provision-assistant-access.sh --targets codex,claude-code-gui --print-remote
# Optional Cursor local MCP:
# scripts/prepare-agent-worktree.sh --target cursor-mcp
# If .meristem/cursor-mcp.token is missing, mint it with root:
# MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" "$BIN" tokens create --name cursor-mcp --source agent
# Store the printed secret in .meristem/cursor-mcp.token mode 0600.
# Point Cursor at scripts/cursor-mcp-command.sh; it uses ../meristem-cursor-mcp.
```
Then relaunch each agent so it picks up its wrapper. Rebuild the shared binary
only from a clean worktree at `v1` (procedure in `agent-worktrees.md`).

## 2. Mint the operator token (root only mints/revokes)

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" "$BIN" tokens create \
  --name operator --source human \
  --scopes 'policy_profile.switch,registry.write,inbox.capture,feed.read,work_items.read_all,work_items.write_all'
# -> id=... name=operator root=false source=human secret=mrs_...
umask 177; printf '%s' 'mrs_...' > .meristem/operator.token   # mode 0600
```
Root cannot switch profiles or write the registry by design (separation of
duties). The operator token is your day-to-day human authority.

## 3. Serve the API and switch to bring-up

Start (or confirm) the API on `:8080`:
```bash
"$BIN" api &                                   # validates safety policy, then listens
until "$BIN" healthcheck >/dev/null 2>&1; do sleep 0.2; done
curl -sS localhost:8080/readyz                 # -> {"status":"ok",...,"policy_profile":"steady","safety_policy":"<fp>"}
```
Switch the policy profile to `bring-up` (human, non-root, needs `policy_profile.switch`):
```bash
curl -s -X POST localhost:8080/v1/policy-profile \
  -H "Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)" \
  -H "Idempotency-Key: bringup-$(date +%s)" -H "Content-Type: application/json" \
  -d '{"profile":"bring-up"}'
# -> {"profile":"bring-up","fingerprint":"<fp>","switched":true}
curl -sS localhost:8080/readyz                 # -> "policy_profile":"bring-up" with the new fingerprint
```
(Stdio MCP path instead of HTTP: `MERISTEM_TOKEN=... "$BIN" mcp`, tool `policy_profile.switch`.)

## 4. Start the worker

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/seed.token)" "$BIN" worker --once
# -> worker --once: scanned=N emitted=M already_recorded=K ... dispatch_requested=D ...

MERISTEM_TOKEN="$(tr -d '\n' < .meristem/seed.token)" "$BIN" worker --interval=30s &
# -> JSON logs: "worker daemon starting", then "worker tick complete" per pass
```
The `--once` line is the verification tick; the daemon is the live postcondition.
Expect, per pass (all idempotent on re-run — a second tick emits ~0 fresh):
- scribe: one `convergence-scribe` child per checkless captured/triaged item;
- dispatch: `dispatch.requested` entries naming the handling cultivar;
- convergence: verdicts on running items with checks;
- breach: `patience.breached` per over-budget epoch, escalated to human-attention
  items under bring-up budgets. Items already `human_review_status=blocked` are
  the fixed point — recorded, never re-escalated.
  A breach is open only while the item is still in the same state epoch named
  by the breach payload; later lifecycle transitions make it historical.

## 5. Read the system

```bash
OP="Authorization: Bearer $(tr -d '\n' < .meristem/operator.token)"
curl -sS -H "$OP" 'localhost:8080/v1/backlog/readiness?limit=200'           # grouped board
curl -sS -H "$OP" 'localhost:8080/v1/feed?projection=activity'              # default activity log
curl -sS -H "$OP" 'localhost:8080/v1/feed?projection=owner-attention'       # escalations + breach history
curl -sS -H "$OP" 'localhost:8080/v1/feed?projection=dispatch'              # launcher work queue
```
Feeds page by opaque **cursor**, never by timestamp; a cursor is bound to its
projection (cross-projection reuse is refused). MCP equivalents:
`backlog.readiness`, `feed.read` (with a `projection` arg).

## 6. Export the publishable corpus (R8)

```bash
"$BIN" export > corpus.jsonl        # allowlisted kinds only; free-text scrubbed
"$BIN" export --validate            # non-sensitive JSON validation report
```
Guarantees: no token names, no `message.captured` inbox bodies, no
`actor_token_id` attribution. Validate private backups by replay:
```bash
"$BIN" rebuild                      # fold events -> sandbox schema, diff vs live; expect no drift
```
Raw dumps in `.meristem/backups/` stay private; only `corpus.jsonl` is shareable.
For an archived dump, restore it into scratch Postgres, point
`MERISTEM_DATABASE_URL` at that scratch database, then run `"$BIN" rebuild`
and `"$BIN" export --validate`. The validation report contains counts only,
not token names or message bodies.

## 7. Trunk hygiene

```bash
"$BIN" git checkout footgun && "$BIN" git merge --ff-only v1
"$BIN" git push origin footgun      # when satisfied
```
The refresh parent's last convergence check is these docs reaching trunk.

## 8. Ongoing operation

- **Approve escalations:** work `owner-attention`; record your decision as an
  event on each `human_review_status=blocked` item (exempt from escalation storms).
- **Worker liveness:** keep one `"$BIN" worker` process supervised beside the API;
  use SIGTERM/SIGINT for graceful stop and restart with the same source=system token.
- **Panic revoke** (root only): `curl -s -X POST localhost:8080/v1/tokens/revoke-all -H "Authorization: Bearer $(tr -d '\n' < .meristem/root.token)" -H "Idempotency-Key: panic-$(date +%s)"` -> `{"revoked_count":N,...}`.
- **Back to steady** when bring-up exit criteria are met: repeat step 3 with `{"profile":"steady"}`.
- **Substrate down:** coordinate in `docs/coord/outage-YYYYMMDD.md`, then replay
  entries into the log on reconnect. See [`coord/outage-protocol.md`](coord/outage-protocol.md).
