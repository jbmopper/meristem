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

After the listener release commit is independently reviewed, merged to `v1`,
and published through the shared-build guard, run the generic listener beside
the API and worker. The listener bearer belongs in one absolute mode-0600 file;
the awakened Codex task keeps its separate MCP credential file.

The first deployment has no listener projection until migrations 0037-0039
and the matching API binary are live. Bootstrap the stable address before
starting the supervisor. Registration and the initial broad policy require an
active non-root human credential with the exact `listeners.admin` scope; the
root credential may mint that administrator but cannot perform these calls.
The `principal_token_id` names the bearer stored in the listener token file,
not the administrator.

```bash
export LISTENER_ADMIN_TOKEN=<scoped-non-root-human-bearer>
export LISTENER_PRINCIPAL_ID=<uuid-of-listener-principal-token>

curl -fsS -X POST \
  -H "Authorization: Bearer $LISTENER_ADMIN_TOKEN" \
  -H 'Idempotency-Key: register-codex-review-v1' \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/v1/listeners \
  -d "{\"name\":\"codex-review\",\"principal_token_id\":\"$LISTENER_PRINCIPAL_ID\",\"provider\":\"codex\",\"capabilities\":[\"review.complementary\",\"review.exact_artifact\"]}"

export LISTENER_ID=<id-from-registration-response>
curl -fsS -X POST \
  -H "Authorization: Bearer $LISTENER_ADMIN_TOKEN" \
  -H 'Idempotency-Key: initialize-codex-review-policy-v1' \
  -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/listeners/$LISTENER_ID/policy" \
  -d '{"policy":{"predicates":[],"capabilities":["review.complementary","review.exact_artifact"],"max_concurrent_assignments":1,"focus":"claimed_work_item_tree"}}'
```

An empty predicate list means all eligible demand for those registered
capabilities. A narrower actor or work-item-tree policy is a complete
replacement and must carry the currently observed `observed_policy_event_id`.

```bash
BIN=.meristem/generated/meristem-bin
export MERISTEM_TOKEN_FILE=/absolute/path/to/listener-principal.token
export CODEX_MERISTEM_TOKEN_FILE=/absolute/path/to/assigned-task-mcp.token

"$BIN" listener \
  --name codex-review \
  --api http://127.0.0.1:8080 \
  --activation-adapter "$PWD/scripts/codex-thread-nudge.py" \
  --activation-arg=activate \
  --activation-arg=--codex-bin \
  --activation-arg=/absolute/path/to/codex-app-server-wrapper \
  --activation-arg=--thread-id \
  --activation-arg=<dedicated-codex-task-uuid> \
  --activation-arg=--repo-root \
  --activation-arg=/absolute/path/to/isolated/codex/worktree \
  --activation-binding-generation=<task-binding-generation> \
  --activation-consumer-generation=<service-generation>
```

The binding generation changes when the local Codex-task binding changes. The
consumer generation changes when the supervised listener instance is replaced.
The adapter receives only activation and assignment IDs from Meristem, starts a
turn only when the dedicated task is idle, declines unattended authority
requests, and writes no local delivery journal. Meristem owns the filter-bound
feed cursors, assignment lease, activation lease, receipts, retry budget, and
restart derivation.

Cut over one listener at a time: stop the legacy
`meristem-codex-sse-bridge.sh` service, start exactly one generic listener
consumer, then run the restart/ambiguous-admission smoke before deleting any
legacy state files. Do not run old and new delivery consumers concurrently.
For launchd, point the service at the same guarded `$BIN` used by API/MCP and
keep the existing Postgres readiness wrapper.

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
