# Operations: bring-up and shutdown

Host-focused instructions for a local or single-VM deployment. Container paths reference [`README.md`](../README.md) for compose profiles.

## Prerequisites

- Go 1.25+ (for `go run ./cmd/meristem`)
- Docker (for Postgres via `docker compose`)
- A POSIX shell

## Bring-up

On the **primary host (Slab)** meristem starts automatically at login; that is
the installed default there (**Primary host (Slab): login-time autostart**,
below). On every other host — and as the fallback/reference on Slab itself —
bring-up is manual via the bootstrap script (**Manual bring-up (bootstrap
script)**, below).

### Primary host (Slab): login-time autostart

On **Slab** (the primary host, a 24GB Apple Silicon Mac) meristem is installed
to come up automatically at login. This is the installed default there as of
2026-07-15, verified working after a real reboot. Setup is arranged by an
untracked installer, `.meristem/launchd/install-autostart.sh`; what follows
describes the resulting behavior and how to reproduce it by hand, not the
installer's internals.

**What runs.** Three launchd **user agents** live in `~/Library/LaunchAgents`,
labelled `com.jbmopper.meristem.codex.api`, `.worker`, and `.feed-watch`. Each
sets `RunAtLoad` and `KeepAlive`, so launchd starts it at login and relaunches
it if it exits. Rather than exec the service directly, every agent first runs a
**wait-for-postgres gate**: it polls the `meristem-postgres` container
healthcheck every 5s, up to a 300s timeout, and only then execs the service's
existing run script. If the gate times out, `KeepAlive` relaunches it, so
bring-up still converges on a slow boot instead of failing permanently.

**Postgres.** The database is not started by the agents. An untracked
`docker-compose.override.yml` adds `restart: unless-stopped` to the `postgres`
service, so Postgres comes back with the Docker engine on its own and gives the
gates something to wait for. The override is ignored via `.git/info/exclude`, so
it never dirties `git status`.

**colima.** The Docker engine runs as a persistent service under
`brew services start colima`, so the VM is brought up at login without a
foreground `colima start`. This has a **required companion**: the Homebrew
`docker` CLI formula (`brew install docker`). `brew services` launches colima
with a minimal PATH that cannot see a docker CLI under `/usr/local/bin`, and
colima's dependency check fails fatally without a CLI it can find. Known caveat:
that check can still fail on some boots; the post-reboot verification below
catches it, and the recovery is simply `colima start`.

**Ordering is emergent, not sequenced.** launchd fires all three agents in
parallel at login. colima brings the VM up (~60-90s), Postgres auto-restarts and
goes healthy (~10s), the gates release, and the services exec. The system is
green about 2-3 minutes after login. On a single-user FileVault Mac the
disk-unlock screen is effectively the login, so login-time ≈ boot-time.

Manual-equivalent setup (what the installer arranges, if reproducing by hand):

1. Install the Docker engine plus its CLI companion, and start colima as a
   persistent service:

   ```bash
   brew install docker            # CLI companion required by colima under brew services
   brew services start colima     # persistent VM at login
   ```

2. Make Postgres auto-restart with the engine, without dirtying git. Add an
   untracked override and exclude it locally:

   ```bash
   cat > docker-compose.override.yml <<'YAML'
   services:
     postgres:
       restart: unless-stopped
   YAML
   echo docker-compose.override.yml >> .git/info/exclude
   ```

3. Install and load the three launchd agents (`api`, `worker`, `feed-watch`),
   each with `RunAtLoad` + `KeepAlive` and exec'ing the wait-for-postgres gate
   before the service run script:

   ```bash
   launchctl load ~/Library/LaunchAgents/com.jbmopper.meristem.codex.{api,worker,feed-watch}.plist
   ```

> **zsh footnote.** The wait scripts are zsh. `status` is a **read-only reserved
> variable** in zsh (it aliases `$?`), so a wait-loop variable named `status`
> breaks the script — name it something else (e.g. `pg_health`).

Post-reboot verification, in order:

```bash
colima status                            # VM running
docker ps                                # meristem-postgres up and healthy
launchctl list | grep meristem           # three agents loaded
curl -sS http://127.0.0.1:8080/readyz    # API ready
```

If colima lost its PATH/dependency-check race on this boot, `colima status`
shows it stopped; run `colima start` and the gates release on their next poll.
Finally, a reboot purges ephemeral `/private/tmp` worktrees, so prune the stale
entries from the primary checkout afterward:

```bash
git worktree prune
```

### Manual bring-up (bootstrap script)

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

The script refuses to build from a dirty tree or a HEAD that is not the freshly
fetched `origin/v1` (pass `--force` to override). On macOS it then ad-hoc
code-signs the artifact (`codesign -s - --force`); a signing failure is loud but
not fatal.

Caveats:

- **Running sessions are not hot-swapped.** A live API server or an MCP client
  session keeps its current process — and therefore its old binary — until that
  process is restarted or the MCP client session is relaunched. Rebuilding only
  changes what the *next* launch execs; it does not disturb in-flight work.
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
