# meristem

A portable, editor-agnostic, single-operator coordination plane.

The owner gives directions in any form. `meristem` normalizes them into a graph of work items, coordinates humans and agents, brokers actions into external systems, and drives every work item to a terminal state without further intervention beyond approvals.

`meristem` itself stays light and always-on; heavy compute and inference happen in the systems it orchestrates.

## Status

v0 is shipped. Further work is tracked as `work_item`s in `meristem` itself.

Currently shipped:

- `meristem migrate` — apply embedded Postgres migrations.
- `meristem safety check` — validate deterministic resource limits (request bodies, feed long-poll cap, patience budgets); `api`, `worker`, `mcp`, and non–dry-run `seed v1` refuse to start if invalid.
- `meristem api` — HTTP server with health/readiness plus v0 inbox, signals, feed, work-item routes, and read-only Streamable HTTP MCP at `/mcp`.
- `meristem tokens {create, list, revoke}` — bearer token lifecycle.
- `meristem mcp` — JSON-RPC over stdio MCP server with parity to the v0 REST surface; this remains the write-capable compatibility transport while HTTP MCP write idempotency is specified.
- `meristem provider cursor-cli {scaffold,mcp-config,launch}` — secret-free handoff, target-workspace MCP config, and local Cursor Agent launcher for worker agents.
- `meristem seed v1` — seed the v1 substrate backlog into the running v0 system (requires a `system`-source token).
- `meristem healthcheck` — `/readyz` probe binary, used by the `meristem` container's HEALTHCHECK directive (the runtime image is distroless, so the probe ships as a subcommand).
- Deterministic error/log read views — `GET /v1/deterministic-errors` and MCP `deterministic_errors.*`, filtered by `logs.*` token scopes.
- v0 schema baseline (`tokens`, `work_items`, `work_item_relations`, `messages`, `message_parts`, `events`, `idempotency_keys`, `signals`).
- `Dockerfile` + `docker-compose.yml` profiles for in-container deploys, plus a Caddy-based TLS topology.
- `pkg/meristem` — minimal Go client for `POST /v1/signals` (handles bearer auth, idempotency-key generation, replay detection, and structured error decoding).

## Spec

The single source of truth lives at [`docs/spec.md`](docs/spec.md). The agent-facing distillation is [`AGENTS.md`](AGENTS.md). Operator notes for resource limits are in [`docs/safety.md`](docs/safety.md), and the deterministic error reporting guide is in [`docs/deterministic-errors.md`](docs/deterministic-errors.md). Bring-up and shutdown are in [`docs/operations.md`](docs/operations.md). The copy/paste bootstrap text for MCP-connected workers is in [`docs/mcp-worker-bootstrap.md`](docs/mcp-worker-bootstrap.md); Cursor CLI handoff details are in [`docs/providers/cursor-cli.md`](docs/providers/cursor-cli.md). The subactor delegation reducer is documented in [`docs/subactor-grants.md`](docs/subactor-grants.md). The signals contract that other projects integrate against lives at [`docs/signals.md`](docs/signals.md), backed by the JSON Schema at [`docs/schemas/meristem.work_spec.v1.json`](docs/schemas/meristem.work_spec.v1.json).

Project coordination now lives in meristem itself: live `work_item`s, appended
events, transitions, and `/v1/feed`. Markdown coordination notes under
`docs/coord/` are outage-only fallback or historical archive material, not the
current backlog.

## Layout

```text
cmd/meristem/       binary entry point
internal/api/      HTTP surface
internal/mcp/      MCP server (JSON-RPC over stdio and HTTP dispatch)
internal/safety/   deterministic resource limits (startup gate + HTTP enforcement)
internal/providers/ provider-specific handoff/scaffold helpers
internal/storage/  Postgres pool and migration runner
pkg/meristem/       public Go client SDK (importable by external projects)
migrations/        SQL migrations (embedded into the binary)
deploy/            reverse-proxy / TLS configuration (Caddy)
examples/          worked client examples (curl)
scripts/           operator scripts (bootstrap)
docs/              spec and operational docs
```

## Quickstart

One-shot bootstrap on the host. Requires Go 1.25+, Docker, and a POSIX shell.

```bash
scripts/bootstrap.sh
```

The script is idempotent at every step: it first runs `meristem safety check`, then brings up the Postgres container, applies migrations, mints a root token if none exists (writing the secret to `.meristem/root.token`, mode 0600), and prints the next commands you might want to run. Re-running it is safe.

After bootstrap, start the API and post a signal:

```bash
go run ./cmd/meristem safety check
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  go run ./cmd/meristem api &

MERISTEM_TOKEN=$(cat .meristem/root.token) \
  go run ./cmd/meristem tokens create --name example --source agent
# -> prints id=, name=, secret=mrs_...

MERISTEM_TOKEN=mrs_... examples/curl-signal.sh
# -> 201 Created with the signal envelope; re-run for an idempotency replay.
```

## Topologies

### Host: `go run` against containerized Postgres (default)

Fastest iteration loop. The meristem binary runs from your shell, only Postgres lives in Docker.

```bash
go run ./cmd/meristem safety check
docker compose up -d postgres
cp .env.example .env
export $(grep -v '^#' .env | xargs)

go run ./cmd/meristem migrate
go run ./cmd/meristem tokens create --root          # one-time
go run ./cmd/meristem api
```

In another shell:

```bash
curl -s http://localhost:8080/healthz   # liveness, ignores Postgres
curl -s http://localhost:8080/readyz    # readiness, pings Postgres; includes safety_policy
```

Resource-safety controls are code-owned in this slice (not environment-tunable): default caps are documented in [`docs/safety.md`](docs/safety.md).

To roll back the most recently applied migration (development only):

```bash
go run ./cmd/meristem migrate down
```

### Container: `meristem` in Docker (compose profile `app`)

Use this when another project on the same host needs a stable, always-on endpoint. Builds the meristem image from the local source and runs `meristem migrate` once as an init container before bringing up the api.

If you have an **older** local compose volume from before the Postgres role/database matched this repo, drop the stale named volume or `docker volume prune` so Postgres re-initializes with the `meristem` user and database from `docker-compose.yml`.

```bash
docker compose --profile app up -d
docker compose run --rm meristem tokens create --root --name root
# -> copy the printed secret somewhere safe; this is the only time it is shown.
```

The api is then reachable at `http://127.0.0.1:8080`. Logs:

```bash
docker compose logs -f meristem
```

### Production: `meristem` behind Caddy (compose profile `production`)

Adds TLS termination via Caddy. Caddy fetches a Let's Encrypt certificate for `MERISTEM_HOSTNAME` on first start and renews it automatically.

```bash
MERISTEM_HOSTNAME=meristem.example.com \
  docker compose --profile production up -d
```

The Caddyfile lives at [`deploy/Caddyfile`](deploy/Caddyfile); edit in place to add additional sites or tighter security headers. Postgres remains bound to loopback inside the compose network; the only public-facing ports are 80 (Caddy ACME challenge + redirect) and 443 (meristem).

## Integration smoke test

The canonical "did the deploy work?" smoke test for any integrator (jay, ns_obv, clinical-demo, your-project) is in [`examples/curl-signal.sh`](examples/curl-signal.sh). It posts a real `meristem.work_spec.v1` body, demonstrates the bearer + idempotency-key dance, and re-running it shows both the HTTP-level idempotency replay and the semantic dedupe behavior.

```bash
MERISTEM_TOKEN=mrs_... examples/curl-signal.sh        # 201, work_item created
MERISTEM_TOKEN=mrs_... examples/curl-signal.sh        # 201 + Idempotency-Replayed: true
MERISTEM_IDEMPOTENCY_KEY=$(uuidgen) \
  MERISTEM_TOKEN=mrs_... examples/curl-signal.sh      # 201, links to existing work_item via dedupe_key
```

## Assistant access

Use [`scripts/provision-assistant-access.sh`](scripts/provision-assistant-access.sh) to mint per-assistant agent tokens, store them under `.meristem/*.token` with mode 0600, and generate secret-free MCP config snippets. The script can also harden Cursor's local MCP config and register meristem with Claude Code when the `claude` CLI is installed.

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  scripts/provision-assistant-access.sh --apply-cursor --apply-claude-code --print-remote
```

When spinning up a worker manually, paste the streamlined MCP instructions from
[`docs/mcp-worker-bootstrap.md`](docs/mcp-worker-bootstrap.md) after filling in
the assigned work item, scope, and allowed areas.

For Cursor CLI workers, generate a filled handoff packet from live meristem
state, or dry-run a launch command before starting Cursor Agent:

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  go run ./cmd/meristem provider cursor-cli scaffold \
    --work-item <uuid> \
    --scope 'Implement one narrow change.' \
    --allowed-area internal/example

MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  go run ./cmd/meristem provider cursor-cli launch \
    --work-item <uuid> \
    --workspace /path/to/target-project \
    --scope 'Implement one narrow change.' \
    --allowed-area internal/example \
    --model spark \
    --apply-mcp \
    --worktree meristem-<short-id> \
    --worktree-base <target-project-base-ref> \
    --dry-run
```

The generated packet references `.meristem/cursor-cli.token` by path and does
not print bearer secrets. The current known blocker is live headless Cursor
Agent MCP exposure: `cursor-agent mcp list-tools meristem` can see meristem
tools, but `cursor-agent --print` model runs have reported `MCP unavailable`.
Use `docs/providers/cursor-cli.md` as the live runbook before assigning real
work.

## Go client

Go projects can import [`pkg/meristem`](pkg/meristem) instead of hand-rolling the bearer + idempotency-key + JSON dance:

```bash
go get github.com/jbmopper/meristem/pkg/meristem
```

```go
import "github.com/jbmopper/meristem/pkg/meristem"

client, err := meristem.New(meristem.Config{
    BaseURL: "https://meristem.example.com",
    Token:   os.Getenv("MERISTEM_TOKEN"),
})
if err != nil { log.Fatal(err) }

resp, err := client.PostSignal(ctx, meristem.SignalRequest{
    Kind:      "human_request",
    DedupeKey: "your-app:retry-budget:001",
    Source:    meristem.SignalSource{Kind: "system_event", Identifier: "your-app:job:42"},
    WorkSpec:  workSpecJSON,
}, meristem.WithIdempotencyKey("import-001"))
if err != nil {
    var apiErr *meristem.APIError
    if errors.As(err, &apiErr) {
        log.Fatalf("meristem rejected: %s (HTTP %d)", apiErr.Code, apiErr.StatusCode)
    }
    log.Fatal(err)
}
log.Printf("work_item=%s replayed=%v", resp.WorkItem.ID, resp.Replayed)
```

The client is intentionally thin: it does not validate `WorkSpec` against [`docs/schemas/meristem.work_spec.v1.json`](docs/schemas/meristem.work_spec.v1.json). The server is the single source of truth for that contract; the client transports bytes and surfaces the structured error envelope when validation fails.

## Tests

The default test suite is unit-level and does not require Postgres:

```bash
GOCACHE=/tmp/meristem-go-cache go test ./...
```

Postgres integration tests are opt-in. They create a temporary database on the same server as `MERISTEM_DATABASE_URL`, apply migrations, exercise the HTTP API through `httptest`, then drop the temporary database.

```bash
docker compose up -d postgres
export MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable'
MERISTEM_INTEGRATION=1 GOCACHE=/tmp/meristem-go-cache \
  go test ./internal/api -run TestSignalsEndpointIntegration -count=1 -v
```

Use `MERISTEM_TEST_DATABASE_URL` instead of `MERISTEM_DATABASE_URL` if you want integration tests to target a different Postgres server.

## Configuration

| Variable                    | Required | Default | Notes                                                              |
|-----------------------------|----------|---------|--------------------------------------------------------------------|
| `MERISTEM_DATABASE_URL`      | yes      | —       | Postgres DSN.                                                      |
| `MERISTEM_HTTP_ADDR`         | no       | `:8080` | Listen address for `meristem api`.                                  |
| `MERISTEM_TOKEN`             | varies   | —       | Bearer token used by `meristem tokens` (non-root ops) and `meristem mcp`. |
| `MERISTEM_HOSTNAME`          | no       | —       | Hostname Caddy issues a Let's Encrypt cert for (production profile only). |
| `MERISTEM_VERSION`           | no       | `dev`   | Version string baked into the docker image and `meristem version`.  |
| `MERISTEM_BIN`               | no       | `go run ./cmd/meristem` | Override the binary used by `scripts/bootstrap.sh`. |
| `MERISTEM_INTEGRATION`       | no       | —       | Set to `1` to run opt-in integration tests.                        |
| `MERISTEM_TEST_DATABASE_URL` | no       | —       | Optional Postgres DSN just for integration tests.                  |

Production secrets live in the host cloud's KMS. v1 wraps per-connection credentials with envelope encryption; v0 reads them from the environment.

## GitHub repository

The Go module path is `github.com/jbmopper/meristem`. Point `origin` at the canonical remote, for example:

```bash
git remote set-url origin https://github.com/jbmopper/meristem.git
git push -u origin main
```
