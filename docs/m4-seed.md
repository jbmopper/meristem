# Seed/bring-up profile: 16GB Apple Silicon M4 Mac

A resource-conscious bring-up path for a meristem instance on a 16GB M4 Mac,
layered on top of the generic bring-up in [`docs/operations.md`](operations.md).
This document does not replace that one or `scripts/bootstrap.sh`; it adds
host-sizing guidance and a Postgres resource-limit override specific to a
memory-constrained laptop running Colima, an editor, and one or more agent
processes at the same time.

Tracks work_item `575414ca` (child of owner-direction item `90359fc5`).

## Why a separate document

Generic bootstrap assumes a host with headroom to spare. On a 16GB machine,
Postgres, the meristem API, the meristem worker, Colima's Linux VM, and
whatever editor/agent processes are running all compete for the same RAM.
Left at defaults, Postgres alone (`shared_buffers` auto-tuning, up to 10
client-side connections from every meristem process per `internal/storage`)
can crowd out everything else. This doc gives a starting-point allocation;
it is not a tuned benchmark. Adjust after watching `docker stats` /
`colima status` under real load on your machine.

## 1. Runtime: Colima, not Docker Desktop

This profile assumes [Colima](https://github.com/abiosoft/colima) as the
container runtime. `scripts/snapshot-db.sh` already defaults its Docker
context to `colima` (`MERISTEM_DOCKER_CONTEXT`), so this is consistent with
existing tooling, not a new assumption. Docker Desktop works too, but its
larger baseline memory footprint eats further into a 16GB budget; Colima is
the recommended path here for that reason alone. (A generic Docker
Desktop → Colima migration for the rest of the repo's assumptions is
tracked separately in work_item `b7247c0d`; this doc does not depend on that
item landing — it just picks Colima directly for this target.)

Start a right-sized Colima VM, leaving the majority of the 16GB for macOS
and your editor/agent processes:

```bash
colima start --cpu 4 --memory 6 --disk 60
```

Adjust `--memory` down (e.g. `4`) if you routinely run additional heavy
processes (a second agent, a browser with many tabs, etc.) alongside
meristem. Verify it landed:

```bash
colima status
docker context show   # should print "colima"
```

## 2. Postgres resource limits

The base [`docker-compose.yml`](../docker-compose.yml) sets no CPU/memory
limits on the `postgres` service — reasonable when Postgres is the only
heavy thing on the box, less so on a 16GB laptop. Use the override at
[`deploy/compose.m4-16gb.yml`](../deploy/compose.m4-16gb.yml), which caps
Postgres at 2 CPUs / 1.5GB and tunes `shared_buffers`/`max_connections`
down to match.

Make every subsequent `docker compose` invocation — including the ones
`scripts/bootstrap.sh` runs internally — pick up the override automatically
by setting `COMPOSE_FILE` once per shell session, before bootstrapping:

```bash
export COMPOSE_FILE=docker-compose.yml:deploy/compose.m4-16gb.yml
```

(Compose reads `COMPOSE_FILE` as an OS-path-list of files to merge, in
order, so this is equivalent to always passing
`-f docker-compose.yml -f deploy/compose.m4-16gb.yml` by hand.)

## 3. Bring-up

With `COMPOSE_FILE` exported as above, follow the normal path:

```bash
scripts/bootstrap.sh
```

Confirm the override actually applied:

```bash
docker inspect meristem-postgres --format '{{.HostConfig.Memory}}'   # -> 1610612736 (1.5GB)
docker inspect meristem-postgres --format '{{.HostConfig.NanoCpus}}' # -> 2000000000 (2 CPUs)
```

## 4. Worker cadence

`meristem worker` defaults to `--interval=30s`. On a resource-constrained
box, a longer interval trades reconciliation latency for less steady-state
CPU/DB polling overhead. This is an ordinary CLI flag, not a policy-profile
change — bounded patience is unaffected (patience budgets, not tick
cadence, own the "how long may a state wait" invariant per
[`docs/safety.md`](safety.md)):

```bash
MERISTEM_TOKEN="$(cat .meristem/seed.token)" \
  go run ./cmd/meristem worker --interval=60s
```

60s is a reasonable starting point for a single-operator M4 instance;
tighten it back toward the 30s default if you need snappier convergence and
have headroom to spare.

## 5. Policy profile

No new policy profile is introduced for this target. Use the existing
profiles as-is (see [`docs/owner-quickstart.md`](owner-quickstart.md) step
3): switch to `bring-up` while the backlog and reconcilers are first
standing up, then back to `steady` once things settle. Profile choice is
about patience budgets, not host resources, so it is orthogonal to
everything else in this document.

## 6. Known gap: Postgres client pool size is not tunable here

`internal/storage.LoadConfigFromEnv` hardcodes the pgx pool to
`MaxConns: 10, MinConns: 1` for every meristem process (API, worker, MCP,
CLI) — see [`docs/safety.md`](safety.md): resource-safety controls are
code-owned, not environment-tunable, in this slice. On a 16GB box running
several meristem processes at once, that is up to 10 connections *per
process*, which the `max_connections=40` set in the compose override above
accounts for, but it cannot be lowered per-process without a code change.

This is deliberately **out of scope** for this document: making pool size
(and worker cadence) a real profile-governed dimension of
`internal/safety.Policy` was considered and explicitly deferred to keep
this seed item tight (coordination recorded on work_item `575414ca`). The
follow-up is tracked as a separate child work_item; do not re-derive this
decision independently — check that item's state before proposing the
`internal/safety` change again.

## 7. Verification

This document's convergence check is a live bring-up: run steps 1–4 above
end to end on real M4 hardware and confirm:

```bash
curl -sS localhost:8080/readyz
MERISTEM_TOKEN="$(cat .meristem/seed.token)" go run ./cmd/meristem worker --once
docker stats --no-stream meristem-postgres
```

`/readyz` should report `"status":"ok"`; `docker stats` should show
Postgres holding under the 1.5GB/2-CPU ceiling under normal idle load.

## See also

- [`docs/operations.md`](operations.md) — generic bring-up and shutdown.
- [`docs/safety.md`](safety.md) — resource-safety policy and why it is
  code-owned, not environment-tunable.
- [`docs/owner-quickstart.md`](owner-quickstart.md) — the full operator
  spine (tokens, policy profile, worker, reading the system) this document
  layers on top of.
- [`docs/network-layer-spec.md`](network-layer-spec.md) — the M4 node's
  eventual role as an outbound-only spoke once the network layer lands
  (out of scope here; this document only covers standalone bring-up).
