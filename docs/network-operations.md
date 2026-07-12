# Network operations: queue-first stage 1

This runbook covers the currently testable pull-only path between two meristem
nodes. It does not claim that direct routing, relay routing, public TLS ingress,
provider MCP, or OAuth is complete.

## Supported acceptance boundary

A queue-first deployment has two independent Postgres databases and two
independent API processes:

- A reachable hub accepts a cross-node command, appends `command.queued`, and
  projects the command into its durable `command_queue`.
- A pull-only node runs its API on a local/private listener and runs
  `meristem spoke`. The spoke makes outbound requests to the hub, executes each
  drained command against the local API under the local agent token, and sends
  the structural result back to the hub.
- The original `Idempotency-Key` is reused for every local retry. If local
  execution succeeds but acknowledgement fails, the hub leaves the command
  pending and the next poll replays the cached local response without applying
  the state change twice.
- Loss of the hub stops cross-node drain and feed progress. It does not stop the
  pull-only node's local API, local event log, or local readiness.

The queue read and acknowledgement endpoints are authenticated in the current
slice. Per-target authorization and a command-path allowlist are separate
hardening work; do not expose these endpoints to untrusted clients before that
work lands.

## Listener and TLS expectations

Keep the pull-only node's API private. For a same-host spoke, bind it explicitly
to loopback:

```bash
export MERISTEM_HTTP_ADDR=127.0.0.1:8080
go run ./cmd/meristem api
```

`meristem api` currently serves plain HTTP. A hub reachable across a network
must sit behind a separately managed TLS reverse proxy and
`MERISTEM_HUB_URL` must use its verified `https://` URL. The present deployment
scaffold is not evidence that TLS 1.3, HSTS, public provider ingress, or tunnel
configuration has been completed; verify those controls at the deployed edge.

The spoke itself opens no listener. Its only network actions are outbound calls
to `MERISTEM_HUB_URL` and loopback/private calls to `MERISTEM_LOCAL_URL`.

## Bring up a pull-only node

Apply migrations independently to the node's local database, start its private
API, and then start the poller under a supervisor:

```bash
export MERISTEM_DATABASE_URL='postgres://.../spoke_a?sslmode=require'
go run ./cmd/meristem migrate

export MERISTEM_HTTP_ADDR=127.0.0.1:8080
go run ./cmd/meristem api
```

In the poller process, inject the two bearer values from the supervisor's secret
store rather than placing them in command arguments or logs:

```bash
export MERISTEM_HUB_URL='https://hub.example.test'
export MERISTEM_NODE_ID='spoke-a'
export MERISTEM_LOCAL_URL='http://127.0.0.1:8080'
export MERISTEM_HUB_TOKEN='...hub-minted agent bearer...'
export MERISTEM_TOKEN='...local agent bearer...'
go run ./cmd/meristem spoke --interval=30s
```

Use a distinct token row on each node. The hub bearer is authenticated by the
hub database; the local bearer is authenticated by the pull-only node's
database. Never reuse a root token for either role.

## Verify and operate

Before starting the poller, verify local readiness and hub reachability from the
pull-only host:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
curl --fail --silent --show-error https://hub.example.test/readyz
```

After enqueueing a test command, confirm the hub row moves from `pending` to
`done` or `failed`. A command that remains `pending` after more than two poll
intervals requires checking, in order:

1. spoke process health and its `hub_reachable` tick field;
2. outbound DNS, certificate validation, and connectivity to the hub;
3. hub-token validity;
4. local API readiness and local-token validity;
5. acknowledgement errors after a successful local response.

Do not manually mutate `command_queue`. Retry by restoring connectivity and
letting the spoke reuse the queued command's original idempotency key. Queue
patience/escalation is not yet complete, so operators must currently alert on
the age of pending rows rather than relying on an automatic terminal timeout.

During a hub outage, verify that the local node remains usable:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

The expected spoke behavior is a warning followed by retry on the next tick;
the process should not exit solely because the hub is unreachable.

## Automated acceptance

The focused test creates two temporary Postgres databases, serves the real hub
and local API handlers, injects one acknowledgement failure, proves the local
retry collapses, and then proves local readiness after the hub listener is
removed:

```bash
MERISTEM_INTEGRATION=1 \
MERISTEM_TEST_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
go test ./internal/spoke -run TestQueueFirstTwoNodeAcceptance -count=1 -v
```

Direct-route acceptance remains pending production route wiring. Add that proof
as a separate case once a canonical REST command is actually dispatched by the
route selector; do not treat the direct-receipt placeholder as execution.
