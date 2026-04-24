# meristem

A portable, editor-agnostic, single-operator coordination plane.

The owner gives directions in any form. `meristem` normalizes them into a graph of work items, coordinates humans and agents, brokers actions into external systems, and drives every work item to a terminal state without further intervention beyond approvals.

`meristem` itself stays light and always-on; heavy compute and inference happen in the systems it orchestrates.

## Status

v0 in development. Once v0 is up, all further work is tracked as `work_item`s in `meristem` itself.

Currently shipped:

- `meristem migrate` — apply embedded Postgres migrations.
- `meristem api` — HTTP server with health/readiness plus v0 inbox, signals, feed, and work-item routes.
- `meristem tokens {create, list, revoke}` — bearer token lifecycle.
- `meristem mcp` — JSON-RPC over stdio MCP server with parity to the v0 REST surface.
- `meristem seed v1` — seed the v1 substrate backlog into the running v0 system (requires a `system`-source token).
- `meristem healthcheck` — `/readyz` probe binary, used by the `meristem` container's HEALTHCHECK directive (the runtime image is distroless, so the probe ships as a subcommand).
- v0 schema baseline (`tokens`, `work_items`, `work_item_relations`, `messages`, `message_parts`, `events`, `idempotency_keys`, `signals`).
- `Dockerfile` + `docker-compose.yml` profiles for in-container deploys, plus a Caddy-based TLS topology.
- `pkg/meristem` — minimal Go client for `POST /v1/signals` (handles bearer auth, idempotency-key generation, replay detection, and structured error decoding).

## Spec

The single source of truth lives at [`docs/spec.md`](docs/spec.md). The agent-facing distillation is [`AGENTS.md`](AGENTS.md). The signals contract that other projects integrate against lives at [`docs/signals.md`](docs/signals.md), backed by the JSON Schema at [`docs/schemas/meristem.work_spec.v1.json`](docs/schemas/meristem.work_spec.v1.json).

## Layout

```text
cmd/meristem/       binary entry point
internal/api/      HTTP surface
internal/mcp/      MCP server (JSON-RPC over stdio)
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

The script is idempotent at every step: it brings up the Postgres container, applies migrations, mints a root token if none exists (writing the secret to `.meristem/root.token`, mode 0600), and prints the next commands you might want to run. Re-running it is safe.

After bootstrap, start the API and post a signal:

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  go run ./cmd/meristem api &

MERISTEM_TOKEN=$(cat .meristem/root.token) \
  go run ./cmd/meristem tokens create --name example --source agent
# -> prints id=, name=, secret=wln_...

MERISTEM_TOKEN=wln_... examples/curl-signal.sh
# -> 201 Created with the signal envelope; re-run for an idempotency replay.
```

## Topologies

### Host: `go run` against containerized Postgres (default)

Fastest iteration loop. The meristem binary runs from your shell, only Postgres lives in Docker.

```bash
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
curl -s http://localhost:8080/readyz    # readiness, pings Postgres
```

To roll back the most recently applied migration (development only):

```bash
go run ./cmd/meristem migrate down
```

### Container: `meristem` in Docker (compose profile `app`)

Use this when another project on the same host needs a stable, always-on endpoint. Builds the meristem image from the local source and runs `meristem migrate` once as an init container before bringing up the api.

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

The canonical "did the deploy work?" smoke test for any integrator (jay, ns_obv, clinical-demo, your-project) is in [`examples/curl-signal.sh`](examples/curl-signal.sh). It posts a real `legacy.work_spec.v1` body, demonstrates the bearer + idempotency-key dance, and re-running it shows both the HTTP-level idempotency replay and the semantic dedupe behavior.

```bash
MERISTEM_TOKEN=wln_... examples/curl-signal.sh        # 201, work_item created
MERISTEM_TOKEN=wln_... examples/curl-signal.sh        # 201 + Idempotency-Replayed: true
MERISTEM_IDEMPOTENCY_KEY=$(uuidgen) \
  MERISTEM_TOKEN=wln_... examples/curl-signal.sh      # 201, links to existing work_item via dedupe_key
```

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
