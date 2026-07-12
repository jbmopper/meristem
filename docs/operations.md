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
