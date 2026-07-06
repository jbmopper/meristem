# Owner deep dive

This document walks the same eight owner steps as
[`owner-quickstart.md`](owner-quickstart.md), but uses each step as a spine to
explain the machinery it touches and why that machinery exists. Read the
quickstart when you want the commands; read this when you want to understand
what you are doing to the system and why it is shaped this way.

`meristem` is a portable, editor-agnostic, single-operator coordination plane:
Go binary, one Postgres database, event-sourced. The owner declares intent in
prose; the system normalizes each instruction into a graph of `work_item`s and
drives every one to a terminal state (`done | failed | canceled`) with no
further intervention beyond approvals. Two runtime modes share the one binary:
`meristem api` (HTTP) and `meristem worker` (the deterministic reconciler).

The register is botanical, matching the name. Four terms recur, each defined on
first use below and collected here for reference (full lexicon in
[`refresh-requirements.md`](refresh-requirements.md)):

- **xylem** — the *budgeted, consumable* flow the deterministic layer meters:
  event writes, wall-clock time, delegation depth, spawn count, chatter rate.
- **phloem** — the *rich, directed* flow the generative layer exchanges:
  context briefs, findings, plans, decisions, escalation summaries. Projections
  load and route phloem.
- **tropism** — a named convergence *pattern*: a declared direction of growth
  toward a fixed point, paired with a pure, replayable reducer.
- **cultivar** — a named, versioned *bundle*: worker profile + tropism + xylem
  budget + phloem projection. Every agent-executed work item runs under exactly
  one cultivar.
- **rootstock** — the small predefined set of cultivars that self-definition
  grafts onto; the recursion base case. Rootstock is immutable except by
  owner-approved migration.

The specs behind each subsystem are cross-linked from the relevant section:
R1 scribe ([`scribe-spec.md`](scribe-spec.md)), R2 registry
([`registry-spec.md`](registry-spec.md)), R6 projections
([`projections-spec.md`](projections-spec.md)), plus
[`safety.md`](safety.md), [`operations.md`](operations.md),
[`backlog-readiness.md`](backlog-readiness.md), and
[`dogma-conformance.md`](dogma-conformance.md).

Throughout, `$BIN` is `.meristem/generated/meristem-bin` and
`MERISTEM_DATABASE_URL` points at the running Postgres.

---

## Step 1 — Per-agent worktrees

**What you run.** Prepare worktrees for the generated-wrapper agents
(`codex`, `claude-code-gui`), then run
`scripts/provision-assistant-access.sh --targets codex,claude-code-gui --print-remote`
to regenerate the launch wrappers, then relaunch the agents. If you use Cursor
local MCP, prepare `cursor-mcp` separately; it uses
`scripts/cursor-mcp-command.sh` and `.meristem/cursor-mcp.token`, not the
generated-wrapper provisioner. This is the human acknowledgement on the
worktree-discipline work item — a step no agent can take for itself, because
the whole point is to stop agents from stepping on each other.

**The collision history.** Early on, multiple assistants shared the primary
source checkout. The primary checkout owns local state under `.meristem/`
(token files, the generated binary). When two agents work in one checkout, one
commits another's dirty files, and — worse — someone rebuilds the shared
`meristem-bin` from whatever branch happens to be checked out, so the running
binary silently diverges from the ref everyone thinks they are on. That "binary
drift" is exactly the class of bug that erodes trust in an always-on system: the
audit log says one thing, the binary does another.

**The ritual that fixes it** (see [`agent-worktrees.md`](agent-worktrees.md)).
Each agent gets its own git worktree based on `v1`, with its own branch
(`codex/<t>-worktree`, `claude/<t>-worktree`, else `agent/<t>-worktree`). The
helper symlinks `<worktree>/.meristem` back to the primary checkout so token and
state files live in exactly one place while the *source* checkout is isolated
per agent. `provision-assistant-access.sh` then writes **secret-free** MCP
command wrappers under `.meristem/generated/` that read token files at runtime
rather than embedding bearer secrets in shared JSON — and those wrappers fail
closed until their expected worktree exists. The shared binary is rebuilt only
from a clean, detached worktree at a known ref (`git diff --quiet` before
`go build`), never from a dirty agent tree. The discipline is not bureaucracy;
it is what makes "which binary is running which code" a question with one answer.

---

## Step 2 — Mint the operator token

**What you run.** With the root bearer in `MERISTEM_TOKEN`, mint a non-root,
human-source token scoped for operator work, and store the printed secret at
`.meristem/operator.token` mode 0600:

```bash
MERISTEM_TOKEN="$(tr -d '\n' < .meristem/root.token)" "$BIN" tokens create \
  --name operator --source human \
  --scopes 'policy_profile.switch,registry.write,inbox.capture,feed.read,work_items.read_all,work_items.write_all'
```

**Event-sourced auth.** Tokens are not config rows you edit; they are projected
from `events`. `tokens create` appends a `token.created` event attributed to the
root actor, and the `tokens` projection derives the row. Secrets are 32 bytes
from `crypto/rand`, stored only as a SHA-256 hash, compared in constant time
(tokens are random, not passwords — bcrypt's cost would be wasted). The binary
shows the secret once (`secret=mrs_...`) and keeps only the hash; there is no
recovery, only re-mint. Revocation is instant because the next request re-reads
the projection.

**Scopes and tree scoping.** A token carries scopes (defined in
`internal/access`): read scopes (`work_items.read`, `work_items.read_all`,
`feed.read`, `feed.read_assigned`), write scopes (`work_items.write`,
`work_items.write_all`, `work_items.create`, `inbox.capture`), and the two
governance scopes you are granting here — `policy_profile.switch` (step 3) and
`registry.write` (defining tropisms/cultivars/projections). A token can also be
narrowed to a subtree with `work_items.tree:<root>`, so a leaf worker sees only
its own branch of the graph. The access reducers are transport-independent: REST,
MCP, and CLI all call the same `internal/access` functions, so a scope means the
same thing everywhere.

**Why root is deliberately weak.** Principle 7: *the root token only mints and
revokes tokens.* Root **cannot** switch policy profiles (`root_token_forbidden`)
and **cannot** write the registry — those refusals are enforced in
`internal/access` before any handler runs. This is separation of duties made
structural, and it is why bring-up and registry work must flow through the
operator token you just minted, not through root. The narrow root is also what
makes step 8's panic-revoke safe: the one credential that can invalidate every
other token has almost no other power to abuse. (The R4/R2 review findings
hardened exactly these seams — a root or agent token attempting a profile switch
is denied with a named, structured error, not a silent allow.)

Full-attribution follows from all of this: `actor_token_id` and `source` come
from the request context on every event, never from a body or header, so the
audit answers "who, via what client, when, with what authority."

---

## Step 3 — Serve the API and switch to bring-up

**What you run.** Confirm `meristem api` is serving on `:8080` (or use the stdio
MCP path), read `/readyz`, then POST `{"profile":"bring-up"}` to
`/v1/policy-profile` with the operator token and an idempotency key. Re-read
`/readyz` to confirm the profile and its fingerprint moved.

**Bounded patience is THE core invariant.** Principle 3: no non-terminal state
may wait forever. Every state has either a forward transition the reconciler
takes after a bounded delay, or a transition gated on an external signal with a
timeout that triggers the forward one. This is the property that guarantees
repeated application of the convergence loop reaches a fixed point. It is not a
feature you toggle; it is the reason the system can be trusted to "run itself
out" while you are unreachable.

**Finite caps and fingerprinting.** The deterministic resource policy lives in
`internal/safety` (see [`safety.md`](safety.md)): per-non-terminal-state patience
budgets, plus body-size, feed-wait, child-count, delegation-depth, concurrency,
and per-class event-rate ceilings. Crucially, `MaxPatienceBudget` is a shared
*finite* ceiling: no profile, explicit `patience_budget_seconds`, or
cultivar-derived wall-clock budget for a running item may create an effectively
infinite wait.
`Policy.Fingerprint()` is a short stable hex id over the canonical JSON of the
policy; it appears in logs, in `/readyz`, and in `meristem safety check`, so a
runbook can answer "which policy build is this binary running?" at a glance.

**Bring-up vs steady.** A policy profile (`internal/policyprofile`) is the
owner's declared operating posture — a *fact* recorded as an event, not lore in
someone's head. Two profiles ship: `bring-up` relaxes patience (long but still
finite) and routes escalation to the owner feed instead of straight to `failed`,
with generous xylem; `steady` is the spec-normal envelope. The default, with no
switch recorded, is `steady` — the service fails *closed* to steady if the
profile projection is absent or unreadable, never open to something unbounded.
`/readyz` reports the active profile name and its fingerprint; the switch is a
`policy_profile.switched` event attributed to your operator token. Bring-up's
*exit* criteria are themselves convergence checks on named substrate items (R9),
so "we are done mellowing" is a claim the system can verify, not a vibe. You
switch to bring-up first so that the first live worker tick (step 4) evaluates
patience under forgiving budgets while the seeded backlog is still cold.

---

## Step 4 — First live worker tick

**What you run.** First run
`MERISTEM_TOKEN=<seed/system token> "$BIN" worker --once` as the verification
tick. It prints `scanned=N emitted=M already_recorded=K`. Run it again and the
fresh counts collapse toward zero — every pass is idempotent. Then keep
`MERISTEM_TOKEN=<seed/system token> "$BIN" worker --interval=30s` supervised
beside the API. `SIGINT`/`SIGTERM` is the graceful shutdown path; restart with
the same dedicated `system`-source token.

**The noticing/acting split.** R3's central idea: the deterministic layer owns
**noticing**; agents own **acting**. `meristem worker` runs deterministic ticks
that scan non-terminal items and act only *mechanically* — transition on
timeout exactly as an item's declared escalation rule states, spawn the
escalation item the rule names, append a dispatch entry saying "this needs
attention." The reconciler never authors content, plans, or judgments. Launchers
(agent wrappers, or your own session) consume the dispatch feed and wake workers
with phloem loaded from the item's projection. The metronome and the tripwire
are deterministic; the hands are agents. Requiring a `system`-source token (root
refused) keeps this automation attributable to a worker process, distinct from
you.

**The metronome passes.** The daemon runs one tick immediately and then repeats
serially after the configured interval, so ticks never overlap in one process.
Each tick resolves the active policy profile before scanning, then `ScanOnce`
runs four passes in order (`internal/worker`):

1. **Scribe** (R1). A work item that arrives with no convergence checks is a
   *state*, not an error. The scribe pass finds checkless `captured`/`triaged`
   items and mechanically spawns one `convergence-scribe` child per parent to
   propose checks. The child id is derived — `uuid5(ns, parent_id ||
   "|convergence-scribe|v1")` — so there is one scribe child per parent, ever,
   and the spawn is idempotent across concurrent workers. A scribe agent then
   proposes checks via `POST /v1/work-items/{id}/convergence-proposal`
   (`convergence.checks_proposed`); a pure reducer validates them. Every check
   must carry a class prefix — `cmd:`, `event:`, `query:` (machine-verifiable)
   or `human-ack:` (owner-gated) — and unprefixed prose is refused
   (`unclassified_check`). The parent cannot leave `triaged` until valid checks
   land. See [`scribe-spec.md`](scribe-spec.md).
2. **Dispatch** (R3 remainder). For items that *have* checks and are eligible for
   pickup, the pass appends `dispatch.requested`, whose payload names the
   handling **cultivar**, the state, the epoch, and a reason
   (`agent_attention_requested`). This is the queue launchers read.
3. **Convergence**. For running items with checks, it records
   `convergence.verdict_recorded` under a bounded attempt budget, then transitions
   (accept -> `done`) or escalates on exhaustion. Stale identical inputs do not
   burn an attempt.
4. **Breach**. For every still-non-terminal item with a budget, it emits one
   `patience.breached` per over-budget state epoch. Pre-claim
   agent-cultivar waits converge on `dispatch.requested`; other breached
   epochs route through the human-escalation path — *unless* the item is
   already at the fixed point.
   Breach resolution is not another event: consumers correlate the breach
   payload's state epoch with the replayed `work_items` projection. If the item
   has since transitioned, the breach is historical.

**Deterministic event ids with discriminators.** Every event id is
`uuid(sha256(subject_kind || ':' || subject_id || ':' || kind ||
':' || canonical(payload))[:16])`. Replays produce no new rows; a PK conflict is
treated as success. That is why re-running the worker is a no-op on the wire, and
why the `emitted` vs `already_recorded` split in the output is meaningful: it
tells you "the scan saw N facts; M were new this run." A separate *discriminator*
distinguishes genuinely distinct actions (a scribe re-proposing under a new
idempotency key) from replays of the same one, so retries don't collapse a real
second attempt.

**Recursion guards and the fixed point.** Self-definition must terminate. The
scribe child is *born* with its own check (`query:parent_checks_defined`), so it
never matches the checkless predicate — no scribe-for-a-scribe is possible
structurally, not by filter. And the escalation loop has a base case:
`human_review_status = blocked` is THE fixed point. An item waiting on owner
input still gets its `patience.breached` recorded, but the worker does **not**
recursively escalate it (`PatienceEscalationsSkippedAwaitingHuman`). Blocked-on-
you items are exempt from escalation storms; they wait for you, quietly, forever
being not-your-problem-yet. This is what keeps a backlog full of owner-gated
items from generating an unbounded cascade of "please look at this" work.

---

## Step 5 — Reading the system

**What you run.** `GET /v1/backlog/readiness` for a grouped board; the feed
projections `activity`, `owner-attention`, and `dispatch` via
`GET /v1/feed?projection=<name>`; the equivalent MCP tools
(`backlog.readiness`, `feed.read` with a `projection` argument).

**The event log is truth; projections are pure folds.** Principle 2: `events` is
the system. Every other table — `work_items`, the registry, the projection
catalog, the readiness fold — is a deterministic projection of the log. Replay
produces identical rows. Projection writers are the *only* code that writes
non-`events` rows, they run synchronously in the same transaction as the event
append, and they are pure with respect to the event payload (no clock reads, no
random ids). This is why "read the system" and "the system is honest" are the
same statement: a projection cannot diverge from the log without the system
having lied, and `meristem rebuild` (step 6) exists to prove it hasn't.

**Named projections as data** (R6). A feed view is a *stored filter expression*
over event kinds and taxonomy classes — added by a write
(`projection.defined`, scope `registry.write`), not a deploy. The seed plants
`activity@1` (the default feed — the no-argument read path is literally this
projection), `owner-attention@1` (`escalation.requested` plus
`patience.breached` history — your nudge feed), and `dispatch@1` (the launcher
queue).
Critically, **projections select content; they never grant or narrow
authority.** Every projection read still passes through your token's access
reduction, so a projection can show you nothing your scopes would hide and hide
nothing your scopes would reveal — visibility has exactly one owner, the access
reducer. See [`projections-spec.md`](projections-spec.md).

**The taxonomy classes.** R6 also classifies every event kind exactly once
(`internal/feed`): `lifecycle` (creates, transitions, relations, breaches,
signals), `decision` (verdicts, proposals, grants, captures), `progress`
(heartbeats and tool logs — cheap to drop from briefs), and `admin` (token and
idempotency events, never in agent-facing views). These classes are the unit
that step-4's xylem event-rate budgets meter, and they are what lets a worker's
context brief carry decisions while shedding chatter.

**Cursors, never timestamps.** Feeds page by an opaque cursor that embeds the
projection name and version. A cursor from one projection is refused on another
(`cursor_projection_mismatch`). This is deliberate: an earlier coordination miss
came from reading a feed by wall-clock cutoff and silently dropping events at the
boundary. Making the cursor the only paging mechanism makes the correct thing the
only thing. The agent briefings ([`briefings/*.md`](briefings/checklist-worker.md))
encode the same rule as a non-negotiable: read the feed by cursor, never by
timestamp.

---

## Step 6 — Export the publishable corpus

**What you run.** `"$BIN" export > corpus.jsonl` for the scrubbed, shareable
corpus; `"$BIN" export --validate` for a non-sensitive proof that the export
contains only allowlisted kinds and no token names or inbox bodies; then
`"$BIN" rebuild` to validate that folding events through the projectors into a
sandbox schema matches live (no drift), which is also your archive-dump /
restore validation against private backups in `.meristem/backups/`.

**Privacy posture: two corpora, one log.** The refresh doc (R8) draws a hard
line. Raw database dumps are the owner's *private legible planning diary* —
inbox captures verbatim, tooling topology, working hours, everything. They never
leave the private log. The **publishable** corpus is a separate artifact produced
by a deterministic exporter (`internal/export`): a pure, read-only fold over the
events table. Nothing about an export run appears in the log it exports — the
exporter holds no writer and opens no writing transaction.

**The allowlist philosophy.** Export is *positive* policy: a kind is in the
corpus only if it is on `KindAllowlist`, mirroring the feed's included/excluded
partition discipline. `token.*` and `idempotency.*` kinds are not allowlisted, so
token administration never appears. `message.captured` — which carries your
verbatim inbox prose — is *deliberately excluded*. On top of that, a scrub pass
replaces free-text payload fields (`title`, `body`, `reason`, `summary`,
`rationale`, `text`, ...) with length-preserving markers, and `actor_token_id`
is simply never exported. `source` (human/agent/system) is kept because it has
research value without identifying anyone.

**What a stranger could and could not learn.** From `corpus.jsonl` a stranger
sees the *shape* of the system's self-construction: which work items existed,
their lifecycle transitions and relations, when patience breached, what verdicts
reducers issued, which cultivars and projections were defined, when the profile
switched. They can study cadence, tree structure, and convergence dynamics. They
*cannot* learn any work item's title or body, any inbox message text, any token's
name, or which token did what. The corpus is the first realized instance of "the
system can be assessed by being asked" — a fold over the log that answers
questions about behavior without leaking the owner's diary.

**Rebuild as proof.** `meristem rebuild` drops the projection tables into a
sandbox schema, replays every event through the writers, and diffs against live.
Green means the projections are exactly what the log implies — the same guarantee
that lets you rebuild the entire system from a backup and a root token
(disaster-recovery, per the spec). Run it against each archived dump in
`.meristem/backups/` before you rely on that backup.

---

## Step 7 — Trunk hygiene

**What you run.** Fast-forward `footgun` (the default branch) to `v1`, and push
to origin when satisfied. The refresh parent's last convergence check is these
very docs reaching trunk.

**The cross-agent review convention that built this.** meristem's own
development follows one loop, and it is worth understanding because it is why the
system is trustworthy: **spec -> implement -> independent review -> fix-forward**,
all in-band as work items. A slice starts as a spec doc (the R-item specs you
have been reading), gets implemented by one agent, is reviewed by a *different*
agent, and defects are fixed forward — never by rewriting history. The
convergence checks are machine-grammar: `cmd:` (a command a worker can run),
`event:` (a fact that must appear in the log), `query:` (a pure predicate over
projections), `human-ack:` (a decision only you can make). Because those checks
are executable, "is this slice done?" is a question the running system answers,
not a human judgment call — which is exactly why R9 retired the notion of a
milestone gate. Trunk hygiene is the terminal `human-ack:` on the whole refresh:
you, the owner, confirm the docs landed on `footgun`.

**Why fast-forward only.** The agent worktrees all branch from `v1`; landing
work is a `--ff-only` merge so trunk history stays linear and every commit on
`footgun` is a commit that existed, reviewed, on `v1`. No merge commits paper
over a divergence. (`meristem git ...` forwards to real `git` so version control
is invoked consistently through the one binary.)

---

## Step 8 — Ongoing operation

Once bring-up settles, your standing duties are few, by design — the system is
built so that "further intervention beyond approvals" is rare.

**Approving escalations.** Work the `owner-attention` feed. Items escalated to
you land as `human-attention` cultivar work items (tropism `human-ack@1`: the
verdict follows an explicit owner decision event). You record your decision as an
event on the item; you never let the system decide for you. Remember the fixed
point from step 4: items at `human_review_status = blocked` are *waiting on you*
and are exempt from escalation storms — they will not nag, and they will not
cascade. That is a feature: a large backlog of owner-gated questions stays quiet
until you get to it.

**Xylem budgets and what exhaustion does.** The finite caps from
[`safety.md`](safety.md) are the substrate's spending limits, referenced by
cultivars: max children per item (fallback **32**), max concurrent running items
per token (fallback **8**), max delegation/grant depth (fallback **5**), and
per-class event rates per item per hour (fallback lifecycle 120 / decision 120 /
progress 240). A cultivar can tighten any of these; zero or absent entries fall
back to the safety policy. The essential property: **exhaustion is never a silent
drop.** Over-budget spawn, over-budget concurrency, or an over-deep grant chain
appends a `xylem.exhausted` event and moves the item to `blocked` with an
escalation per its rule — it *escalates, never drops*. Bounded patience then
guarantees even the blocked item reaches a fixed point. The internal
xylem/escalation recovery events themselves bypass the meter, so exhaustion can
always reach the owner even when everything else is throttled.

**Registry and rootstock immutability.** Tropisms, cultivars, and projections are
data (R2/R6): open sets of named, versioned things over a *closed* set of reducer
semantics. Defining one is a `registry.write`; redefining a name requires
`version = current + 1` (`version_conflict` otherwise). But **rootstock** entries
(`rootstock: true`) refuse redefinition entirely — changing the recursion base
case is an owner-approved migration, not a token write, so no amount of scope
lets an agent (or you, casually) mutate the seed the whole system grafts onto.

**Cultivar activation gating (R5).** The self-extension flow is now gated in
the substrate. A worker that discovers a possible new worker files a scoped
subtask proposing a cultivar; activation then goes through
`registry.activate_cultivar` or
`POST /v1/work-items/{id}/cultivar-activations`. The gate resolves the
cultivar's `profile.scopes_template`, evaluates the subactor-grant reducer as a
same-tree worker check, and requires `human_review_status=approved` from a token
other than the proposer. Rootstock self-modification is refused before any
`cultivar.defined` event, and the granted path still does not mint a token.
That keeps profile creation, token issuance, and approval authority separated.

**Panic revoke.** `POST /v1/tokens/revoke-all` with the **root** token
invalidates every non-root token at once (`{"revoked_count":N}`). This is the one
place root's narrow power is exactly what you want: the credential with almost no
other authority is the kill switch. Root itself survives, so you can re-mint an
operator token and continue.

**Profile switch back to steady.** When bring-up's exit criteria (convergence
checks on named substrate items) are green, switch back with step 3's command and
`{"profile":"steady"}`. `/readyz` will report the steady fingerprint.

**Outage protocol basics.** The only time coordination leaves the event log is
when the *substrate itself* is unavailable (Postgres down, migration in flight,
host restart) — because then no work-item write can succeed. In that window,
agents append claim-and-handoff lines to `docs/coord/outage-YYYYMMDD.md`
(git commit is the durability and the attribution). The moment writes succeed
again, each agent replays its outage lines into the log as
`coordination.outage_note` events *before* doing anything else; the last to
replay writes a `resolved` footer. Truth converges back into the log; the
markdown is a buffer, never a second source of truth. An outage file without a
`resolved` footer means replay is still owed — treat it as blocking on
reconnection. See [`coord/outage-protocol.md`](coord/outage-protocol.md).

---

## Where to go next

- Command-only version of these eight steps: [`owner-quickstart.md`](owner-quickstart.md).
- The invariant everything rests on: bounded patience, in [`spec.md`](spec.md) and
  [`refresh-requirements.md`](refresh-requirements.md).
- Bring-up/shutdown order and the safety gate: [`operations.md`](operations.md),
  [`safety.md`](safety.md).
- The dogma agents run under, mapped to tests: [`dogma-conformance.md`](dogma-conformance.md)
  and the per-cultivar [`briefings/`](briefings/convergence-scribe.md).
