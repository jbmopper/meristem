# Dogma Conformance Map

Source: `AGENTS.md` -> `## Techniques`.

This map covers the load-bearing Techniques bullets only. Each `###` heading
must exactly match one Techniques bullet in `AGENTS.md`, and each entry must
name a concrete test or deterministic check in this repository.

### Idempotency at every layer.

- Check: `internal/idempotency/middleware_test.go` pins the lock identity; `internal/idempotency/context_test.go` pins event-discriminator derivation; `internal/api/idempotency_integration_test.go` covers HTTP replay/conflict behavior; `cmd/meristem/seed_test.go` pins deterministic seed identity.
- Evidence: HTTP, MCP/service-level event discrimination, and seeded backlog replay all have executable tests instead of relying on handler convention.

### Deterministic event ids.

- Check: `internal/events/events_test.go` pins canonical payload identity, discriminator behavior, subject/kind separation, nil payload handling, and attribution exclusion; `internal/events/canonical_test.go` pins canonical JSON; `internal/api/transition_cycle_integration_test.go` covers repeated transition payloads through the REST idempotency seam; `internal/worker/worker_integration_test.go` covers the patience attention contract where one `patience.breached` event remains the durable breach fact and later `work_item.transitioned` events resolve it by state-epoch correlation rather than by appending redundant resolution events.
- Evidence: event identity is tested at the pure reducer layer, at the API transition cycle that previously exposed payload-only collisions, and at the bounded-patience read model that distinguishes open from resolved attention after replaying the same event log into `work_items`.

### `SELECT … FOR UPDATE SKIP LOCKED`

- Check: `internal/worker/job_queue_integration_test.go` appends `dispatch.requested`, verifies one dispatch-derived `job_queue` row per event id, and starts competing claimers that lease disjoint rows through `SELECT ... FOR UPDATE SKIP LOCKED`; `cmd/meristem/seed_test.go` still pins the seeded substrate item named `Worker with job_queue and SELECT … FOR UPDATE SKIP LOCKED`.
- Evidence: dispatch enqueue is caused by durable event-log facts, while runtime leases are coordinated by Postgres row locks instead of process memory.

### Append-only enforcement on `events`

- Check: `internal/dogma/conformance_test.go` reads `migrations/0001_init.up.sql` and requires `events_no_update`, `events_no_delete`, `events_no_truncate`, and `events_reject_mutation`; `cmd/meristem/rebuild_test.go` pins the event-log projection rebuild table set.
- Evidence: the migration-level append-only guard and the replay surface both have executable drift checks.

### Migrations embedded into the binary

- Check: `internal/storage/migrate_test.go` loads the embedded migration filesystem and requires every up migration to have a matching down migration; `migrations/embed.go` owns the embedded filesystem.
- Evidence: migration presence, naming, and up/down pairing fail under `go test` before a binary can ship without committed migration files.

### `crypto/rand` 32-byte tokens, SHA-256 hash

- Check: `internal/auth/token_test.go` verifies generated token shape, uniqueness, 32-byte SHA-256 hashes, valid/invalid secret shape, and constant-time hash equality behavior; `internal/auth/token.go` owns the `crypto/rand` and SHA-256 implementation.
- Evidence: token material and hashing behavior are tested without introducing password-hash dependencies.
