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

6. **API**

   ```bash
   go run ./cmd/meristem api
   ```

7. **Verify**

   ```bash
   curl -sS http://127.0.0.1:8080/healthz
   curl -sS http://127.0.0.1:8080/readyz
   ```

   `/readyz` includes `safety` and `safety_policy` when the database ping succeeds.

Optional services (each runs the same safety validation before opening the database):

- **MCP:** `MERISTEM_TOKEN=... go run ./cmd/meristem mcp`
- **Worker:** `MERISTEM_TOKEN=... go run ./cmd/meristem worker --once`

## Shutdown

### API or MCP (foreground process)

Send **SIGINT** (Ctrl-C) or **SIGTERM**. The API shuts down gracefully (in-flight requests get a bounded shutdown window).

### Background `go run ... api &`

Find the process and signal it:

```bash
pkill -TERM -f 'cmd/meristem api'
```

(or use the PID from your shell job control).

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
- [`README.md`](../README.md) — topologies, compose profiles, integration smoke tests
