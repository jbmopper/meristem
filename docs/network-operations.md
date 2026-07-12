# Network operations: direct and queue stage 1

This runbook covers the currently tested direct and pull-only paths between two
meristem nodes. It does not claim that application relay routing, public TLS
ingress, provider MCP, or OAuth is complete.

## Supported acceptance boundary

A stage 1 deployment has two independent Postgres databases and two
independent API processes:

- A caller resolves an object's immutable home, loads one locally accepted
  `nodes` projection snapshot, and uses the production `crossnode.Dispatcher`
  to call the home node's canonical REST path. The home node authenticates a
  token minted in its own database and checks the selected target node again.
- Direct mutations carry the original `Idempotency-Key`. Retrying the same
  mutation therefore returns the home node's cached response without appending
  a duplicate domain event.
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

The queue write, read, attempt, and acknowledgement endpoints require their
target-specific cross-node scopes. The queued executor also admits only the
documented work-item mutation allowlist.

## Direct routing call seam

There is intentionally no second business-logic API or CLI for remote
mutations. Code that has already resolved an immutable object home constructs a
canonical `crossnode.Command` and calls `crossnode.Dispatcher.DispatchMutation`.
The dispatcher reads the local event-backed `nodes` projection once, applies
the deterministic direct-then-queue selector, and invokes the bounded delivery
policy. Qualified work-item GETs use `Dispatcher.ReadWorkItem`; reads require a
direct route and are never put in the durable mutation queue.

Credential resolution is injected per destination node. Each returned bearer
must have been minted by the node that terminates that attempt. The injected
HTTP client must use the peer-safe transport that validates and pins the
approved destination address class; the dispatcher additionally validates the
registered origin, refuses redirect handling by calling the transport directly,
and binds origin and target node ids on every request.

A direct `401`, `403`, `409`, `429`, or unclassified `500` is definitive and
is returned to the caller. Only transport errors and `502`, `503`, or `504`
consume the finite direct retry budget and permit durable queue fallback.

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

The direct-route test creates two temporary Postgres databases, loads A's real
registry projection through the production dispatcher, reads and mutates a
B-homed work item through B's real API handler, proves idempotent replay has one
B effect, and verifies accelerated direct-retry/queue ordering:

```bash
MERISTEM_INTEGRATION=1 \
MERISTEM_TEST_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
go test ./internal/crossnode -run TestDirectRouteTwoNodeAcceptance -count=1 -v
```

The queue-route test serves the real hub and local API handlers, injects one
acknowledgement failure, proves the local retry collapses, and then proves local
readiness after the hub listener is removed:

```bash
MERISTEM_INTEGRATION=1 \
MERISTEM_TEST_DATABASE_URL='postgres://meristem:meristem@localhost:5432/meristem?sslmode=disable' \
go test ./internal/spoke -run TestQueueFirstTwoNodeAcceptance -count=1 -v
```
