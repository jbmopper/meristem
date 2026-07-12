# meristem

A portable, editor-agnostic, single-operator coordination plane.

The owner gives directions in any form. `meristem` normalizes them into a graph of work items, coordinates humans and agents, brokers actions into external systems, and drives every work item to a terminal state without further intervention beyond approvals.

`meristem` itself stays light and always-on; heavy compute and inference happen in the systems it orchestrates.

## Status

v0 is shipped. Further work is tracked as `work_item`s in `meristem` itself.

Currently shipped:

- `meristem migrate` — apply embedded Postgres migrations.
- `meristem safety check` — validate deterministic resource limits (request bodies, feed long-poll cap, patience budgets); `api`, `worker`, `mcp`, and non–dry-run `seed v1` refuse to start if invalid.
- `meristem api` — HTTP server with health/readiness plus v0 inbox, signals, feed, work-item routes, and provider-safe Streamable HTTP MCP at `/mcp`; sealed tracker profiles add coordination-only writes.
- `meristem worker` — always-on deterministic reconciler daemon; `worker --once` remains the one-tick verification path.
- `meristem tokens {create, list, revoke}` plus `POST /v1/tokens/revoke-all` — bearer token lifecycle and root-only panic revocation.
- `meristem mcp` — JSON-RPC over stdio MCP server with parity to the canonical REST surface; provider HTTP writes remain limited to the sealed tracker profile.
- `meristem seed v1` — seed the v1 substrate backlog into the running v0 system (requires a `system`-source token).
- `meristem healthcheck` — `/readyz` probe binary, used by the `meristem` container's HEALTHCHECK directive (the runtime image is distroless, so the probe ships as a subcommand).
- Deterministic error/log read views — `GET /v1/deterministic-errors` and MCP `deterministic_errors.*`, filtered by `logs.*` token scopes.
- Backlog readiness read view — `GET /v1/backlog/readiness` and MCP `backlog.readiness`, folded from visible `work_items`.
- v0 schema baseline (`tokens`, `work_items`, `work_item_relations`, `messages`, `message_parts`, `events`, `idempotency_keys`, `signals`).
- `Dockerfile` + `docker-compose.yml` profiles for in-container deploys, plus a Caddy-based TLS topology.
- `pkg/meristem` — minimal Go client for `POST /v1/signals` (handles bearer auth, idempotency-key generation, replay detection, and structured error decoding).

## Spec

The single source of truth lives at [`docs/spec.md`](docs/spec.md). The agent-facing distillation is [`AGENTS.md`](AGENTS.md). Operator notes for resource limits are in [`docs/safety.md`](docs/safety.md), and the deterministic error reporting guide is in [`docs/deterministic-errors.md`](docs/deterministic-errors.md). Bring-up and shutdown are in [`docs/operations.md`](docs/operations.md), with a resource-conscious variant for a 16GB Apple Silicon M4 Mac in [`docs/m4-seed.md`](docs/m4-seed.md). The copy/paste bootstrap text for MCP-connected workers is in [`docs/mcp-worker-bootstrap.md`](docs/mcp-worker-bootstrap.md). The subactor delegation reducer is documented in [`docs/subactor-grants.md`](docs/subactor-grants.md), and the durable human handoff path is in [`docs/escalations.md`](docs/escalations.md). The signals contract that other projects integrate against lives at [`docs/signals.md`](docs/signals.md), backed by the JSON Schema at [`docs/schemas/meristem.work_spec.v1.json`](docs/schemas/meristem.work_spec.v1.json).

Project coordination now lives in meristem itself: live `work_item`s, appended
events, transitions, `/v1/feed`, and the derived backlog-readiness view
documented in [`docs/backlog-readiness.md`](docs/backlog-readiness.md).
Markdown coordination notes under `docs/coord/` are outage-only fallback or
historical archive material, not the current backlog.

## Layout

```text
cmd/meristem/       binary entry point
internal/api/      HTTP surface
internal/mcp/      MCP server (JSON-RPC over stdio and HTTP dispatch)
internal/safety/   deterministic resource limits (startup gate + HTTP enforcement)
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
MERISTEM_TOKEN="$(cat .meristem/seed.token)" \
  MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  go run ./cmd/meristem worker &

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

In another shell, keep the reconciler running:

```bash
MERISTEM_TOKEN="$(cat .meristem/seed.token)" go run ./cmd/meristem worker
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

Use this when another project on the same host needs a stable, always-on endpoint. Builds the meristem image from the local source and runs `meristem migrate` once as an init container before bringing up the api. Run `meristem worker` beside the API with a dedicated source=system token so reconciliation is also always-on.

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

Use [`scripts/provision-assistant-access.sh`](scripts/provision-assistant-access.sh)
to mint per-assistant agent tokens, store them under `.meristem/*.token` with
mode 0600, and generate secret-free MCP config snippets. Local assistants should
run from per-agent worktrees, not the primary checkout; see
[`docs/agent-worktrees.md`](docs/agent-worktrees.md).

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  scripts/provision-assistant-access.sh --print-remote
```

When spinning up a worker manually, paste the streamlined MCP instructions from
[`docs/mcp-worker-bootstrap.md`](docs/mcp-worker-bootstrap.md) after filling in
the assigned work item, scope, and allowed areas.

Before pointing a local MCP client at a generated wrapper, prepare the matching
worktree:

```bash
scripts/prepare-agent-worktree.sh --target codex
scripts/prepare-agent-worktree.sh --target claude-code-gui
```

For MCP-native workers, generate a filled handoff packet from live meristem
state and start from the bootstrap protocol in
[`docs/mcp-worker-bootstrap.md`](docs/mcp-worker-bootstrap.md):

```bash
MERISTEM_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
  MERISTEM_TOKEN=$(cat .meristem/worker.token) \
  go run ./cmd/meristem mcp
```

This repository does not ship a dedicated worker launcher anymore. Use the
generic MCP text path above.

### Panic revoke

If an assistant token is compromised or a local agent setup gets confused, the
root token can revoke every active non-root token through the HTTP API. The
endpoint appends one `token.revoked` event per token and leaves the root token
active so the owner can mint replacements.

```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/tokens/revoke-all \
  -H "Authorization: Bearer $(cat .meristem/root.token)" \
  -H "Idempotency-Key: panic-revoke-$(date +%s)"
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

Postgres integration tests are opt-in locally and mandatory in CI. They create a temporary database on the same server as `MERISTEM_DATABASE_URL` or `MERISTEM_TEST_DATABASE_URL`, apply migrations, exercise the HTTP API through `httptest`, then drop the temporary database.

```bash
docker compose up -d postgres
export MERISTEM_TEST_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable'
MERISTEM_INTEGRATION=1 GOCACHE=/tmp/meristem-go-cache go test ./... -count=1
```

Use `MERISTEM_TEST_DATABASE_URL` instead of `MERISTEM_DATABASE_URL` if you want integration tests to target a different Postgres server. The GitHub Actions workflow also runs `meristem safety check`, migrates a seeded fixture database, runs `meristem seed v1`, and gates the build on `meristem rebuild --verbose` reporting no projection drift.

## Configuration

| Variable                    | Required | Default | Notes                                                              |
|-----------------------------|----------|---------|--------------------------------------------------------------------|
| `MERISTEM_DATABASE_URL`      | yes      | —       | Postgres DSN.                                                      |
| `MERISTEM_HTTP_ADDR`         | no       | `:8080` | Listen address for `meristem api`.                                  |
| `MERISTEM_PUBLIC_BASE_URL`   | OAuth    | —       | Explicit HTTPS issuer/resource base for remote provider OAuth; must be paired with `MERISTEM_OAUTH_SYSTEM_ACTOR_TOKEN_ID`. |
| `MERISTEM_OAUTH_SYSTEM_ACTOR_TOKEN_ID` | OAuth | — | UUID of an active non-root `source=system` token; never the bearer secret. |
| `MERISTEM_TOKEN`             | varies   | —       | Bearer token used by `meristem tokens` (non-root ops) and `meristem mcp`. |
| `MERISTEM_HOSTNAME`          | no       | —       | Hostname Caddy issues a Let's Encrypt cert for (production profile only). |
| `MERISTEM_VERSION`           | no       | `dev`   | Version string baked into the docker image and `meristem version`.  |
| `MERISTEM_BIN`               | no       | `go run ./cmd/meristem` | Override the binary used by `scripts/bootstrap.sh`. |
| `MERISTEM_INTEGRATION`       | no       | —       | Set to `1` to run opt-in integration tests.                        |
| `MERISTEM_TEST_DATABASE_URL` | no       | —       | Optional Postgres DSN just for integration tests.                  |

Production secrets live in the host cloud's KMS. v1 wraps per-connection credentials with envelope encryption; v0 reads them from the environment.

Remote provider OAuth is disabled when both OAuth variables are absent. A
partial or invalid pair fails readiness and closes the public OAuth routes.
See [`docs/provider-oauth-operations.md`](docs/provider-oauth-operations.md)
for token separation, ingress limits, client binding, consent, and smoke steps.

## GitHub repository

The Go module path is `github.com/jbmopper/meristem`. Point `origin` at the canonical remote, for example:

```bash
git remote set-url origin https://github.com/jbmopper/meristem.git
git push -u origin main
```
