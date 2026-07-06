# Owner deep dive (Codex draft)

> Non-canonical comparison draft. The maintained owner deep dive is
> [`owner-deep-dive.md`](owner-deep-dive.md); keep this file only as review
> material until the owner chooses to archive or delete it.

This is the machinery behind [`owner-quickstart.codex.md`](owner-quickstart.codex.md).
The quickstart is command-shaped; this document explains what each command is
touching and where the current substrate still needs owner judgment.

## 1. Bootstrap and worktrees

`scripts/bootstrap.sh` does the host-local foundation work in the least magical
order: validate safety policy, start Postgres, apply embedded migrations, mint
root if needed, mint a source=`system` seed token, and run `meristem seed v1`.
Every step is intended to be idempotent. A repeat bootstrap should either do
nothing or append deterministic events that collapse as replays.

Root custody stays human. The root token exists to mint and revoke other
tokens; it should not run workers, switch profiles, or author normal work. That
is why the bootstrap script creates a seed token and why later operator work
uses a separate non-root human token.

Agent worktrees are not ceremony. They keep Codex, Claude, and any future
worker from committing another agent's dirty files or rebuilding
`.meristem/generated/meristem-bin` from the wrong ref. The primary checkout owns
`.meristem/`; worktrees link to it for local tokens and generated wrappers, but
source edits happen on isolated branches.

`scripts/provision-assistant-access.sh` mints per-assistant source=`agent`
tokens and generates wrapper scripts that read token files at runtime. The
generated MCP snippets do not embed bearer secrets. Each token row becomes the
`actor_token_id` on events the assistant causes, preserving attribution without
adding an `agent_kind` schema.

## 2. API, migrations, and readiness

`meristem migrate` applies embedded SQL migrations in numeric order. The schema
is the durable event log plus deterministic projections: work items, messages,
tokens, registry rows, named projections, active policy profile, and related
read models. The projection tables are caches over `events`, not separate
truth.

`meristem api` starts the canonical REST surface. The CLI and MCP server are
translation layers over the same services. A healthy `/readyz` means Postgres is
reachable and the deterministic safety policy validates. Since R4, readiness
also reports the active policy profile and its fingerprint, so the operator can
see whether the system is in `steady` or `bring-up`.

If `/healthz` works but `/readyz` fails, the process is alive but not ready to
coordinate durable work. Side channels can say "API down" or "API back up";
durable coordination belongs back in meristem once the API is reachable.

## 3. Operator token

The operator token is source=`human`, non-root, and scoped. For bring-up it
needs `policy_profile.switch` so it can move between `steady` and `bring-up`,
`registry.write` so the owner can define registry data when required, and the
work-item/feed scopes needed to inspect and steer the backlog.

This keeps the root deliberately weak in day-to-day operation. Root can mint,
revoke, and panic-revoke. It does not become the all-purpose operator identity.
That separation matters because every event answers "who, through which client,
with what authority" from request context, not from request bodies.

## 4. Bring-up profile

The policy profile is a declared runtime fact. `steady` preserves normal
defaults. `bring-up` relaxes patience budgets for early operation, but every
budget remains finite. Startup validates every profile, and `Validate` rejects
budgets beyond the finite cap, so switching cannot discover an invalid profile
after the system is already running.

Switching profiles appends `policy_profile.switched` and projects the singleton
`active_policy_profile` row. Re-switching to the current profile is a no-op:
the operator can retry the POST with the same idempotency key and not create
extra history. Agents cannot switch the profile because they are governed by
it.

## 5. Worker daemon and verification tick

`meristem worker` is the metronome daemon; `meristem worker --once` is the
one-tick verification path. It is deterministic noticing, not model-authored
judgment. Each tick scans non-terminal work items, resolves per-state patience
budgets from the active policy profile unless an explicit override applies,
records bounded-patience breaches, runs checklist convergence for running items
with declared checks, and routes breached states through the declared
escalation behavior.

The worker requires a source=`system` token, like `seed v1`. That gives the
audit trail a concrete worker identity while keeping root out of automation.
The one-shot command prints counters: scanned rows, emitted breach events,
already-recorded repeats, convergence verdicts, stale-input skips, accepts,
retries, and escalations. Re-running a tick should be safe because event ids and
idempotency identities collapse retries.

The daemon runs one tick immediately and then repeats serially after its
configured interval until SIGINT/SIGTERM. Operators should keep it supervised
beside the API; `worker --once` remains the smoke test and manual repair lever.

## 6. Readiness and feeds

Backlog readiness is a fold over visible `work_items`. It groups substrate
items, ready-next items, blockers, running work, and stale/noise without writing
events or inventing a second backlog.

Feeds are projections over the event log. The default feed is chronological;
named projections such as `owner-attention` and `dispatch` apply stored filters
over event kinds and taxonomy classes, then pass through the same access
reducer as the default feed. A projection can select content; it cannot grant
authority.

Feed cursors are opaque and projection-bound. A cursor from `dispatch` cannot
be replayed against `owner-attention`. That rule closed a real coordination
failure mode: cursor state now carries the feed identity it came from.

## 7. Export and rebuild

`meristem export` writes the publishable corpus: a read-only JSONL fold over
events with an allowlist and scrubber. Raw database dumps stay private because
they include the owner's planning diary, token topology, and verbatim inbox
content. The exported corpus is the shareable artifact; raw dumps are for
restore and replay.

`meristem export --validate` runs the same export in memory and compares the
result against private token names and `message.captured` bodies still present
in the database. It prints only counts and leak classes, so the report can be
used as public proof while the restored archive stays private.

`meristem rebuild` is the truth test for projections. It opens a transaction,
creates a sandbox schema, replays every event through the registered projectors,
and diffs the rebuilt projection tables against live tables. A clean rebuild
means the non-event tables are still deterministic consequences of the log. A
mismatch means the system's read model has drifted from its truth source.

Archive replay is not fully automated as a single command. The shipped helper
can create and inspect private Postgres custom-format dumps. The operator lane
is: restore a dump into scratch Postgres, point `MERISTEM_DATABASE_URL` there,
run `meristem rebuild --verbose`, then run `meristem export --validate` and
publish only the non-sensitive report.

## 8. Git forwarding

`meristem git` forwards to real `git(1)`. It does not wrap version control with
business logic; it keeps operational commands under the same binary name the
operator is already using. Fast-forwarding `footgun` from `v1` is the release
move: `v1` is the integration branch where agents collaborate; `footgun` is the
branch the owner advances only after the current head is reviewed and green.

Use `--ff-only` so a surprise divergence fails visibly. If HTTPS credentials
are not configured, push the same ref over the SSH remote URL rather than
rewriting history or changing branch shape.

## 9. Ongoing operation

Escalations and review gates show up in feed projections, especially
`owner-attention`. Full approval primitives are still v1 substrate work, so
today's owner gates are represented through work items, metadata, evented
review status, and explicit token actions. Default-deny side effects still
apply: connector writes should not ship until approval primitives exist.

R5 is now landed. Worker-proposed cultivars do not call the broad
`registry.define_cultivar` path. They propose against a scoped work item, then
go through `registry.activate_cultivar` or
`POST /v1/work-items/{id}/cultivar-activations`. Activation resolves
`profile.scopes_template`, evaluates the existing subactor-grant reducer as a
same-tree worker check, requires `human_review_status=approved` from a token
other than the proposer, and denies rootstock self-modification before any
`cultivar.defined` event. The granted path appends `cultivar.defined` but still
does not mint a token.

Xylem budgets are enforced in the substrate: delegation depth, children per
item, concurrent running items per token, and event rate by taxonomy class.
When a budget is exhausted, the correct shape is an attributed blocked or
escalation path, not silent drop or model improvisation.

Panic revoke remains the root escape hatch. It appends one `token.revoked` event
per active non-root token and leaves root active so the owner can mint
replacement credentials. Returning from `bring-up` to `steady` is a normal
profile switch with a human operator token.
