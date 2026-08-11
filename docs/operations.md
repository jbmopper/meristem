# Operations: bring-up and shutdown

Host-focused instructions for a local or single-VM deployment. Container paths reference [`README.md`](../README.md) for compose profiles.

## Prerequisites

- Go 1.25+ (for `go run ./cmd/meristem`)
- Docker (for Postgres via `docker compose`)
- A POSIX shell

## Bring-up (recommended: bootstrap script)

The script validates **resource safety** first, then Postgres, migrations, tokens, and seed. See [`scripts/bootstrap.sh`](../scripts/bootstrap.sh).

```bash
scripts/bootstrap.sh
```

Equivalent manual order:

1. **Safety (no database)**

   ```bash
   go run ./cmd/meristem safety check
   ```

2. **Postgres**

   ```bash
   docker compose up -d postgres
   ```

   Wait until the `meristem-postgres` container is healthy (or use your own DSN).

3. **Environment**

   ```bash
   cp .env.example .env
   export $(grep -v '^#' .env | xargs)
   ```

   `MERISTEM_DATABASE_URL` must point at the running database.

4. **Migrations**

   ```bash
   go run ./cmd/meristem migrate
   ```

5. **Tokens and seed** (first time only; idempotent on repeat)

   Create a root token if needed, mint a `system`-source token for `meristem seed v1`, then run seed. The bootstrap script automates this; by hand, follow [`README.md`](../README.md) Quickstart.

6. **API and worker**

   ```bash
   go run ./cmd/meristem api
   ```

   In a second supervised process, run the deterministic reconciler with a
   dedicated source=system token:

   ```bash
   MERISTEM_TOKEN="$(cat .meristem/seed.token)" go run ./cmd/meristem worker
   ```

7. **Verify**

   ```bash
   curl -sS http://127.0.0.1:8080/healthz
   curl -sS http://127.0.0.1:8080/readyz
   MERISTEM_TOKEN="$(cat .meristem/seed.token)" go run ./cmd/meristem worker --once
   ```

   `/readyz` includes `safety` and `safety_policy` when the database ping succeeds.
   `worker --once` should print `worker --once: ...`; fresh counts may be zero
   if the daemon already processed the same facts.

Optional services (each runs the same safety validation before opening the database):

- **MCP:** `MERISTEM_TOKEN=... go run ./cmd/meristem mcp`
- **Manual worker tick:** `MERISTEM_TOKEN=... go run ./cmd/meristem worker --once`

### Codex listener service

The commands in this section prepare the listener release; do not cut over the
live consumer until the authority and restart gates below are cleared. After an
independently reviewed commit is merged to `v1` and published through the
shared-build guard, the generic listener will run beside the API and worker.
Use two distinct credentials. The supervisor bearer remains the registration's
bound principal. The awakened task bearer is a separate source=agent token whose
stored scope set is exactly `mcp.profile:listener_task_v1`; that marker grants no
ordinary REST or MCP business authority. Record its token UUID as
`LISTENER_TASK_TOKEN_ID`, and install its secret only at the fixed adapter-local
path `$MERISTEM_LISTENER_CODEX_HOME/meristem-task.token` with mode 0600. Never
place the supervisor bearer there.

Set `BIN=.meristem/generated/meristem-bin`, then mint the task credential with
the root mint-only token and no other scope:

```bash
BIN=.meristem/generated/meristem-bin
MERISTEM_TOKEN="$(tr -d '\r\n' < .meristem/root.token)" "$BIN" tokens create \
  --name codex-review-task \
  --source agent \
  --scopes mcp.profile:listener_task_v1
```

Capture the printed UUID and secret without placing either in shell history.
The secret is printed only at creation; install it as shown below, then remove
the temporary capture.

```bash
export MERISTEM_TOKEN_FILE=/absolute/path/to/listener-principal.token
export LISTENER_TASK_TOKEN_ID=<separate-listener-task-token-uuid>
export CODEX_THREAD_ID=<dedicated-codex-task-uuid>
export MERISTEM_LISTENER_CODEX_HOME=/absolute/private/path/to/listener-codex-home
export MERISTEM_LISTENER_CODEX_SQLITE_HOME="$HOME/.codex"

install -d -m 700 "$MERISTEM_LISTENER_CODEX_HOME"
install -m 600 /secure/capture/of/listener-task-secret \
  "$MERISTEM_LISTENER_CODEX_HOME/meristem-task.token"
ln -s "$HOME/.codex/auth.json" "$MERISTEM_LISTENER_CODEX_HOME/auth.json"
ln -s "$HOME/.codex/thread-writer-locks" \
  "$MERISTEM_LISTENER_CODEX_HOME/thread-writer-locks"

"$BIN" listener \
  --name codex-review \
  --api http://127.0.0.1:8080 \
  --activation-adapter "$PWD/scripts/codex-thread-nudge.py" \
  --activation-security-profile=meristem-git-v1 \
  --activation-checkout-root "$PWD" \
  --activation-bundle-path=scripts/check-meristem-build-pin.sh \
  --activation-bundle-path=scripts/codex-listener-app-server.sh \
  --activation-bundle-path=scripts/codex-listener-mcp-command.sh \
  --activation-task-principal-id "$LISTENER_TASK_TOKEN_ID" \
  --activation-arg=activate \
  --activation-arg=--codex-bin \
  --activation-arg="$PWD/scripts/codex-listener-app-server.sh" \
  --activation-arg=--thread-id \
  --activation-arg="$CODEX_THREAD_ID" \
  --activation-arg=--repo-root \
  --activation-arg=/absolute/path/to/isolated/codex/worktree \
  --activation-arg=--listener-codex-home \
  --activation-arg="$MERISTEM_LISTENER_CODEX_HOME" \
  --activation-arg=--listener-codex-sqlite-home \
  --activation-arg="$MERISTEM_LISTENER_CODEX_SQLITE_HOME" \
  --activation-binding-generation=<task-binding-generation> \
  --activation-consumer-generation=<service-generation>
```

The command treats `--activation-binding-generation` as an operator generation,
then deterministically folds the exact security profile and task-token UUID into
the effective activation identity. Rotating either cannot reuse an uncertain
external client-message identity. The consumer generation changes when the
supervised listener instance is replaced. The configured task-token UUID must
differ from the listener registration's principal UUID; startup, activation
ensure, and every adapter spawn recheck that separation against current state.
Create the mode-0700 dedicated Codex home once, while the listener is stopped.
It must be a real directory different from `$HOME/.codex`, contain no
`config.toml`, and use the two exact absolute symlinks above.
`CODEX_SQLITE_HOME` remains the primary `$HOME/.codex` directory so app-server
can resolve the existing desktop task. The wrapper validates this topology
before starting Codex. Generic core does not know Codex environment names:
launchd passes both paths as the exact fixed adapter arguments above, and the
reviewed Codex adapter overwrites its child's task-id and home variables from
those arguments. The exported `CODEX_THREAD_ID` is only a shell/probe input;
generic core does not inherit it. Its value becomes the reviewed `--thread-id`
argument, from which the adapter supplies the app-server host-thread context
required by `thread/resume`. An empty argument fails validation before spawn.
The supervisor appends only activation, work-item, assignment-event, and task-
principal UUIDs to each invocation. No bearer value, bearer locator, digest, or
database URL crosses the generic Go-to-adapter boundary. The fixed reviewed
arguments carry the Codex task/home binding. The adapter starts a turn only when the
dedicated task is idle, declines unattended authority requests, and writes no
local delivery journal. Keep
`codex-thread-nudge.py`, `codex-listener-app-server.sh`, and
`codex-listener-mcp-command.sh` together under `scripts/` in the same clean,
reviewed `v1` checkout whose `.meristem/generated/meristem-bin` is published.
The wrapper resolves its guarded MCP command as a tracked sibling; do not copy
either launcher into `.meristem/generated`, where that topology would resolve a
different repository root. The pinned Go supervisor verifies its adapter plus
every declared bundle path byte-for-byte against that exact Git commit before
startup and again before each activation. This is an operator-reviewed declared
set, not a mechanical proof of every file the program could open: add every
future runtime helper to the repeated `--activation-bundle-path` arguments. The
checkout, adapter, and absolute executable adapter arguments (including
`--codex-bin`) must use exact, clean, symlink-free paths inside the reviewed
checkout. `meristem-git-v1` is the only first-release activation security
profile and requires this same-commit Git anchor. It deliberately carries no
credential contract. The explicit profile is an extension point, not a claim
that independently versioned adapters should use this packaging; those require
a reviewed manifest profile later.

The listener wrapper leaves interactive Codex unchanged but runs this
network-disabled app-server from the dedicated Codex home, so interactive MCP
servers cannot enter its configuration. It disables the app/connector runtime
with the official `features.apps=false` session flag and defines exactly one
guarded `meristem_listener` stdio entry. Codex's
`enabled_tools` filter is pinned to exactly `work_items.append_event`,
`work_items.get`, and `work_items.get_assignment`; the wake carries the exact
work-item UUID instead of using holder-only `work_items.held_assignments`. The
adapter checks global and thread-scoped app-server status before `turn/start`
and refuses any extra server, tool, or task-actor attestation. The
wrapper accepts exactly `app-server --stdio`; every other Codex mode or argument
vector exits before Codex is started, and no environment override may replace
the tracked sibling MCP launcher. Codex does not inherit arbitrary parent variables into stdio MCP
subprocesses, so the wrapper places only the fixed adapter-local task-token path
and four non-secret binding UUIDs in that server's explicit environment map.
The sibling MCP launcher requires exactly
`$CODEX_HOME/meristem-task.token`, rechecks the real mode-0700 home, mode-0600
bounded token file, and the
shared-build pin before reading it, and requires its load-bearing tracked
scripts to be clean at that same pinned commit. The launcher ignores interactive
token fallbacks, sets the local Postgres URL internally, removes token paths,
and passes the bearer value only to the reviewed `meristem mcp` child.

That child authenticates the actual token row, requires its UUID to equal the
wake's expected task UUID, requires its stored scopes to be the exact inert
listener-task marker, and authorizes the exact live activation/work-item/
assignment generation before serving JSON-RPC. It repeats the activation check
before every `tools/list` and `tools/call`. Only then does it derive the exact
tree-scoped read/write scopes in memory; attribution remains the separate task
actor. Its initialize identity includes
`meristem-actor-id-v1:<actual-task-uuid>`, which the adapter verifies globally
and again for the resumed thread. A replaced, revoked, broad, wrong-principal,
expired, yielded, or terminal task credential therefore fails closed in each
new MCP process. Verify this boundary offline with:

```bash
bash scripts/codex_listener_mcp_command_test.sh
```

Current Codex may ask the app-server host to approve MCP calls. Per the
listener control-plane contract, the adapter declines every unattended
approval, elicitation, and permission request. Do not configure unattended
`approve` mode as an operational workaround: enabling bounded MCP access is a
separate authority-design decision that must first land in `docs/spec.md` and
the live substrate. The dedicated launcher and token boundary remain in place
so that decision cannot later fall back to an interactive principal. A turn
that encounters any such declined request records a failed activation even if
Codex subsequently labels the turn completed; transport completion is not a
semantic smoke pass. On restart, a historical turn remains ambiguous because
Codex history currently cannot prove that a prior adapter process observed no
authority request; deterministic client-message identity alone is insufficient
for a positive receipt. Meristem owns the filter-bound feed cursors, assignment
lease, activation lease, receipts, retry budget, and restart derivation.

Do **not** stop the legacy consumer yet. The metadata-only wake requires the
three-tool MCP surface to resolve its assignment, while Codex 0.147 elicits for
those calls and the canonical adapter declines every elicitation. A useful
unattended activation and a positive ambiguous-admission restart smoke are
therefore intentionally blocked pending an owner-authorized, exact-tool
authority design or a deterministic non-eliciting result path. Once that gate
lands, cut over one listener at a time: stop the legacy
`meristem-codex-sse-bridge.sh` service, start exactly one generic listener
consumer, then run the restart/ambiguous-admission smoke before deleting any
legacy state files. Do not run old and new delivery consumers concurrently.
For launchd, point the service at the same guarded `$BIN` used by API/MCP and
keep the existing Postgres readiness wrapper. The adapter's environment
sanitizers do not forward `CODEX_BIN`; make the launchd `PATH` include Codex's
installed directory (for the desktop bundle,
`/Applications/ChatGPT.app/Contents/Resources`) so the tracked app-server
wrapper can resolve `codex` with `command -v`.

After a Codex or ChatGPT Desktop update and before the cutover smoke, run the
adapter's read-only `probe` against the dedicated task and record the exact
reviewed Meristem commit, `codex --version`, and probe JSON. The probe includes
the bounded printable `userAgent` returned by app-server `initialize`; that
runtime identity plus the generated `ServerRequest` inventory in
`codex-thread-nudge.py` makes the tested protocol attributable. A fixture-only
test run does not replace this live updated-app probe.

The direct probe uses the same isolated home but configures both MCP entries as
inert `/usr/bin/false` transports. It validates app-server identity, isolated
configuration, and read-only task resume without reading either Meristem bearer.
Exact task-principal attestation and the three-tool inventory are checked by
every real activation before `turn/start`:

```bash
"$PWD/scripts/codex-thread-nudge.py" probe \
  --codex-bin "$PWD/scripts/codex-listener-app-server.sh" \
  --thread-id "$CODEX_THREAD_ID" \
  --listener-codex-home "$MERISTEM_LISTENER_CODEX_HOME" \
  --listener-codex-sqlite-home "$MERISTEM_LISTENER_CODEX_SQLITE_HOME" \
  --repo-root /absolute/path/to/isolated/codex/worktree \
  --diagnostic
```

## Rebuild the shared build artifact

`.meristem/generated/meristem-bin` is the single build artifact that backs **both**
the API server and every generated agent MCP wrapper (Claude, Codex, Cursor,
Cerberus). One rebuild covers all of them. Because projection writers run
synchronously in the writing process, keeping every wrapper on one binary stops a
stale wrapper from running divergent projector code against the shared database.

Rebuild only from a clean checkout at the `v1` tip:

```bash
scripts/rebuild-meristem-bin.sh
```

The script refuses to publish the shared artifact from a dirty tree or a HEAD
that is not the freshly fetched `origin/v1`. It embeds that exact full commit in
the binary and writes the separately read
`.meristem/generated/meristem-bin.v1-pin` file with the reviewed `v1` commit.
The two values have separate jobs: the embedded value says *what code this
process is running*; the pin says *what code the owner-approved shared runtime
should be running*. Exact equality is the whole gate.

The pin is reread at API response, MCP tool-call, worker-tick, migration, and
event-append boundaries. When the pin advances, an old process reports itself
as stale at its next boundary and refuses subsequent authoritative work even
though the OS process still exists. Ordinary REST responses are held until a
post-handler check; SSE streams recheck before every query and frame. A mutation
admitted while current is allowed to return its committed response, so OAuth
rotation and delegated-token issuance cannot strand one-time credentials. MCP
`initialize` and `/readyz` remain available to explain the failure.
An ordinary unpinned `go run` development process remains usable, but reports
`unmanaged` rather than pretending to be the reviewed shared runtime.

### First activation is a coordinated restart gate

The dynamic behavior begins only after every writer is running a binary that
contains this guard. A pre-guard API, worker, or MCP process cannot read the new
pin and will keep its old mapped code authoritative even after the on-disk
binary is replaced. Therefore the first activation must remain gated by work
item `835e0dbf`: drain and stop the API and worker, close every write-capable MCP
session, publish the guarded binary and pin, then restart/relaunch all of them.
Before resuming work, verify that `/readyz` reports `build_state=current`, that
`meristem build-guard-status` returns the versioned capability with the pinned
commit, and that each MCP `initialize` reports the same current build. Pin-first
publication is not a substitute for this one-time coordinated cutover.

`--force` cannot replace the default shared artifact with dirty or off-`v1`
code. It is only accepted with an explicit alternate `MERISTEM_BIN_OUT`, and
that artifact stays pinned to reviewed `v1`, so the runtime guard refuses it.
`--no-fetch` is likewise unsafe-only: it requires `--force`, writes only an
explicit alternate artifact, and deliberately stamps that artifact as
non-authoritative. Reviewed builds disable ambient Go workspaces, persisted
Go configuration, and `GOFLAGS`, then compile an immutable `git archive` of the
fetched commit so a transient live-worktree edit cannot enter the artifact.
On macOS the script ad-hoc code-signs the temporary artifact before publishing
it (`codesign -s - --force`); a signing failure is loud but not fatal.

Caveats:

- **Running sessions are not hot-swapped.** A guarded API server or MCP client
  keeps its current in-memory code until restart. The new pin makes that
  already-guarded process fail closed; restarting is still required to serve
  work from the new binary. Pre-guard processes require the first-activation
  drain/restart above.
- **Cutover is boundary-safe, not a distributed transaction.** An operation
  admitted while the old pin was current can finish if the pin changes after
  its final check while a database transaction is already committing. The pin
  prevents new or subsequently checked work; it does not quiesce Postgres or
  revoke an in-flight commit. Use an explicit drain/restart step when a release
  requires a fully quiescent cutover.
- **The pin is a drift detector, not a signature.** Its purpose is to prevent
  two Meristem code revisions from presenting the same shared database as
  authoritative. Repository review, host access, and artifact signing remain
  separate trust controls.
- **The current Docker development image is unmanaged.** It does not mint its
  own sibling pin: per-image pins would let two image revisions both call
  themselves current. A future managed container release must verify its source
  fingerprint and read one deployment-owned reviewed pin that old containers
  can observe changing.
- **macOS firewall / code signing.** The macOS Application Firewall tracks
  inbound-connection approvals per executable identity. Any rebuild changes an
  unsigned or ad-hoc-signed binary's identity (ad-hoc signatures are hash-based),
  so expect a re-approval prompt for the API listener after a rebuild. The ad-hoc
  signing step gives the artifact a valid signature; the durable fix is a stable
  real signing identity. Verify actual prompt behavior during the 835e0dbf
  redeploy.

Rebuilding the shared artifact is repo-side (work item a9374bdd). The live
redeploy — regenerating the wrappers and restarting the API, worker, and MCP
sessions — is owner action tracked under work item 835e0dbf. See
[`docs/agent-worktrees.md`](agent-worktrees.md) for the clean-worktree rebuild
discipline.

## Shutdown

### API, MCP, or worker (foreground process)

Send **SIGINT** (Ctrl-C) or **SIGTERM**. The API shuts down gracefully
(in-flight requests get a bounded shutdown window). The worker finishes the
current tick, stops before the next interval, and exits through context
cancellation.

### Background `go run ... api &`

Find the process and signal it:

```bash
pkill -TERM -f 'cmd/meristem api'
```

(or use the PID from your shell job control).

For a background worker:

```bash
pkill -TERM -f 'cmd/meristem worker'
```

### Postgres (Docker)

Stop the stack or only Postgres:

```bash
docker compose stop postgres
# or
docker compose down
```

`docker compose down -v` **removes volumes** and deletes database data; use only when you intend to wipe the cluster.

### Full local teardown

1. Stop meristem processes (API, MCP, any worker).
2. `docker compose down` (add `-v` only if you want a clean database).
3. Optionally remove token files under `.meristem/` if you are resetting secrets.

## See also

- [`docs/safety.md`](safety.md) — resource limits and `meristem safety check`
- [`docs/network-operations.md`](network-operations.md) — queue-first two-node
  bring-up, partition behavior, and the focused networking acceptance test
- [`README.md`](../README.md) — topologies, compose profiles, integration smoke tests
- [`docs/m4-seed.md`](m4-seed.md) — resource-conscious bring-up variant for a
  16GB Apple Silicon M4 Mac (Colima sizing, Postgres resource limits, worker
  cadence); layers on top of this document rather than replacing it
