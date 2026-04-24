# wayline

A portable, editor-agnostic, single-operator coordination plane.

The owner gives directions in any form. `wayline` normalizes them into a graph of work items, coordinates humans and agents, brokers actions into external systems, and drives every work item to a terminal state without further intervention beyond approvals.

`wayline` itself stays light and always-on; heavy compute and inference happen in the systems it orchestrates.

## Status

v0 in development. Once v0 is up, all further work is tracked as `work_item`s in `wayline` itself.

Currently shipped:

- `wayline migrate` — apply embedded Postgres migrations.
- `wayline api` — HTTP server with health/readiness plus v0 inbox, signals, feed, and work-item routes.
- `wayline tokens {create, list, revoke}` — bearer token lifecycle.
- `wayline mcp` — JSON-RPC over stdio MCP server with parity to the v0 REST surface.
- `wayline seed v1` — seed the v1 substrate backlog into the running v0 system (requires a `system`-source token).
- `wayline healthcheck` — `/readyz` probe binary, used by the `wayline` container's HEALTHCHECK directive (the runtime image is distroless, so the probe ships as a subcommand).
- v0 schema baseline (`tokens`, `work_items`, `work_item_relations`, `messages`, `message_parts`, `events`, `idempotency_keys`, `signals`).
- `Dockerfile` + `docker-compose.yml` profiles for in-container deploys, plus a Caddy-based TLS topology.
- `pkg/wayline` — minimal Go client for `POST /v1/signals` (handles bearer auth, idempotency-key generation, replay detection, and structured error decoding).

## Spec

The single source of truth lives at [`docs/spec.md`](docs/spec.md). The agent-facing distillation is [`AGENTS.md`](AGENTS.md). The signals contract that other projects integrate against lives at [`docs/signals.md`](docs/signals.md), backed by the JSON Schema at [`docs/schemas/wayline.work_spec.v1.json`](docs/schemas/wayline.work_spec.v1.json).

## Layout

```text
cmd/wayline/       binary entry point
internal/api/      HTTP surface
internal/mcp/      MCP server (JSON-RPC over stdio)
internal/storage/  Postgres pool and migration runner
pkg/wayline/       public Go client SDK (importable by external projects)
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

The script is idempotent at every step: it brings up the Postgres container, applies migrations, mints a root token if none exists (writing the secret to `.wayline/root.token`, mode 0600), and prints the next commands you might want to run. Re-running it is safe.

After bootstrap, start the API and post a signal:

```bash
WAYLINE_DATABASE_URL='postgres://wayline:wayline@localhost:5432/wayline?sslmode=disable' \
  go run ./cmd/wayline api &

WAYLINE_TOKEN=$(cat .wayline/root.token) \
  go run ./cmd/wayline tokens create --name example --source agent
# -> prints id=, name=, secret=wln_...

WAYLINE_TOKEN=wln_... examples/curl-signal.sh
# -> 201 Created with the signal envelope; re-run for an idempotency replay.
```

## Topologies

### Host: `go run` against containerized Postgres (default)

Fastest iteration loop. The wayline binary runs from your shell, only Postgres lives in Docker.

```bash
docker compose up -d postgres
cp .env.example .env
export $(grep -v '^#' .env | xargs)

go run ./cmd/wayline migrate
go run ./cmd/wayline tokens create --root          # one-time
go run ./cmd/wayline api
```

In another shell:

```bash
curl -s http://localhost:8080/healthz   # liveness, ignores Postgres
curl -s http://localhost:8080/readyz    # readiness, pings Postgres
```

To roll back the most recently applied migration (development only):

```bash
go run ./cmd/wayline migrate down
```

### Container: `wayline` in Docker (compose profile `app`)

Use this when another project on the same host needs a stable, always-on endpoint. Builds the wayline image from the local source and runs `wayline migrate` once as an init container before bringing up the api.

```bash
docker compose --profile app up -d
docker compose run --rm wayline tokens create --root --name root
# -> copy the printed secret somewhere safe; this is the only time it is shown.
```

The api is then reachable at `http://127.0.0.1:8080`. Logs:

```bash
docker compose logs -f wayline
```

### Production: `wayline` behind Caddy (compose profile `production`)

Adds TLS termination via Caddy. Caddy fetches a Let's Encrypt certificate for `WAYLINE_HOSTNAME` on first start and renews it automatically.

```bash
WAYLINE_HOSTNAME=wayline.example.com \
  docker compose --profile production up -d
```

The Caddyfile lives at [`deploy/Caddyfile`](deploy/Caddyfile); edit in place to add additional sites or tighter security headers. Postgres remains bound to loopback inside the compose network; the only public-facing ports are 80 (Caddy ACME challenge + redirect) and 443 (wayline).

## Integration smoke test

The canonical "did the deploy work?" smoke test for any integrator (jay, ns_obv, clinical-demo, your-project) is in [`examples/curl-signal.sh`](examples/curl-signal.sh). It posts a real `wayline.work_spec.v1` body, demonstrates the bearer + idempotency-key dance, and re-running it shows both the HTTP-level idempotency replay and the semantic dedupe behavior.

```bash
WAYLINE_TOKEN=wln_... examples/curl-signal.sh        # 201, work_item created
WAYLINE_TOKEN=wln_... examples/curl-signal.sh        # 201 + Idempotency-Replayed: true
WAYLINE_IDEMPOTENCY_KEY=$(uuidgen) \
  WAYLINE_TOKEN=wln_... examples/curl-signal.sh      # 201, links to existing work_item via dedupe_key
```

## Go client

Go projects can import [`pkg/wayline`](pkg/wayline) instead of hand-rolling the bearer + idempotency-key + JSON dance:

```bash
go get github.com/jbmopper/wayline/pkg/wayline
```

```go
import "github.com/jbmopper/wayline/pkg/wayline"

client, err := wayline.New(wayline.Config{
    BaseURL: "https://wayline.example.com",
    Token:   os.Getenv("WAYLINE_TOKEN"),
})
if err != nil { log.Fatal(err) }

resp, err := client.PostSignal(ctx, wayline.SignalRequest{
    Kind:      "human_request",
    DedupeKey: "your-app:retry-budget:001",
    Source:    wayline.SignalSource{Kind: "system_event", Identifier: "your-app:job:42"},
    WorkSpec:  workSpecJSON,
}, wayline.WithIdempotencyKey("import-001"))
if err != nil {
    var apiErr *wayline.APIError
    if errors.As(err, &apiErr) {
        log.Fatalf("wayline rejected: %s (HTTP %d)", apiErr.Code, apiErr.StatusCode)
    }
    log.Fatal(err)
}
log.Printf("work_item=%s replayed=%v", resp.WorkItem.ID, resp.Replayed)
```

The client is intentionally thin: it does not validate `WorkSpec` against [`docs/schemas/wayline.work_spec.v1.json`](docs/schemas/wayline.work_spec.v1.json). The server is the single source of truth for that contract; the client transports bytes and surfaces the structured error envelope when validation fails.

## Tests

The default test suite is unit-level and does not require Postgres:

```bash
GOCACHE=/tmp/wayline-go-cache go test ./...
```

Postgres integration tests are opt-in. They create a temporary database on the same server as `WAYLINE_DATABASE_URL`, apply migrations, exercise the HTTP API through `httptest`, then drop the temporary database.

```bash
docker compose up -d postgres
export WAYLINE_DATABASE_URL='postgres://wayline:wayline@localhost:5432/wayline?sslmode=disable'
WAYLINE_INTEGRATION=1 GOCACHE=/tmp/wayline-go-cache \
  go test ./internal/api -run TestSignalsEndpointIntegration -count=1 -v
```

Use `WAYLINE_TEST_DATABASE_URL` instead of `WAYLINE_DATABASE_URL` if you want integration tests to target a different Postgres server.

## Configuration

| Variable                    | Required | Default | Notes                                                              |
|-----------------------------|----------|---------|--------------------------------------------------------------------|
| `WAYLINE_DATABASE_URL`      | yes      | —       | Postgres DSN.                                                      |
| `WAYLINE_HTTP_ADDR`         | no       | `:8080` | Listen address for `wayline api`.                                  |
| `WAYLINE_TOKEN`             | varies   | —       | Bearer token used by `wayline tokens` (non-root ops) and `wayline mcp`. |
| `WAYLINE_HOSTNAME`          | no       | —       | Hostname Caddy issues a Let's Encrypt cert for (production profile only). |
| `WAYLINE_VERSION`           | no       | `dev`   | Version string baked into the docker image and `wayline version`.  |
| `WAYLINE_BIN`               | no       | `go run ./cmd/wayline` | Override the binary used by `scripts/bootstrap.sh`. |
| `WAYLINE_INTEGRATION`       | no       | —       | Set to `1` to run opt-in integration tests.                        |
| `WAYLINE_TEST_DATABASE_URL` | no       | —       | Optional Postgres DSN just for integration tests.                  |

Production secrets live in the host cloud's KMS. v1 wraps per-connection credentials with envelope encryption; v0 reads them from the environment.
