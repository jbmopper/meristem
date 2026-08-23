# v2 substrate: Rust + fjall — folds and keyspace management

Analysis notes for moving the meristem substrate from Go + Postgres (v1,
current) to Rust + fjall. Focus areas per the owner: how folds over events
work without SQL, and how streams/keyspaces are managed. This is a design
study, not a commitment; nothing here changes v1 behavior.

## fjall status (as of 2026-08)

fjall 3 is the post-rewrite line — released 2026-01-02 after roughly a year
of work, with a new disk format and fully revamped APIs. Current release is
**3.1.9** (2026-08-15). Two facts shape everything below:

1. **The terminology inverted between fjall 2 and fjall 3.** fjall 2's
   `Keyspace` (the whole store) is now `Database`; fjall 2's `Partition`
   (one LSM tree / column family) is now `Keyspace`. Any 2.x-era tutorial,
   blog post, or LLM answer silently uses the old names. In this document
   "keyspace" always means the fjall 3 sense: **one named LSM tree inside
   the database**, the unit a projection or index lives in.
2. **Every release from 3.0.0 through 3.1.5 has been yanked** on crates.io;
   only 3.1.6+ stand. The rewrite line needed several months to settle.
   Consequences: pin a recent 3.1.x, watch the changelog, and keep the
   storage layer behind our own trait so an engine bug (or engine swap)
   is a module change, not a rewrite.

What the 3.0 rewrite brought, relevant to us:

- New block format: prefix truncation, sparse indexing, optional per-block
  **hash indexes** (fast point lookups — useful for the event-id dedupe
  index), partitioned filter blocks (bounded memory on large stores).
- **Versioning/snapshots**: version history across flushes and compactions,
  and a `Readable` trait unifying reads on transactions and snapshots —
  this is what replaces v1's `LOCK TABLE events IN SHARE MODE` during
  rebuild (see below).
- Integrated key-value separation that runs during compaction (no separate
  GC pass). Our payloads are small JSON; we likely never enable it.
- **Compaction filters** (3.1.0+): custom logic during compaction — the
  natural home for TTL expiry of idempotency records.
- Transactions restructured: `SingleWriterTxDatabase` (serialized writer)
  and `OptimisticTxDatabase` (multi-writer OCC), alongside plain
  `Database` + `WriteBatch` (atomic, no intermediate reads).
- Durability via `PersistMode` per write/batch (default: OS buffer);
  journal compression for large values; database file locking.
- MSRV 1.90.

## What v1 actually buys from Postgres

The migration cost is exactly the list of Postgres behaviors the substrate
leans on. Inventory, with the code that owns each:

| Postgres behavior | Where v1 uses it |
|---|---|
| Multi-table ACID transaction | `events.Writer.Append` fires every projector in the caller's tx (`internal/events/events.go`); rollback of the event rolls back its projections |
| `ON CONFLICT (id) DO NOTHING` | content-addressed event dedupe — replay collapses to one row and skips projectors |
| `seq BIGSERIAL` | the one true total order (`migrations/0006_events_seq.up.sql`); cursors, SSE tail, rebuild fold order |
| append-only triggers | UPDATE/DELETE/TRUNCATE on `events` rejected at the DB layer |
| sandbox schema + `search_path` | `meristem rebuild` folds into `meristem_rebuild.*` and diffs content hashes against live (`cmd/meristem/rebuild.go`) |
| `LOCK TABLE ... IN SHARE MODE` | freezes the log during rebuild-and-diff |
| SQL secondary indexes | every projection read view (state filters, `updated_at DESC`, partial indexes) |
| `SELECT ... FOR UPDATE SKIP LOCKED` | job_queue lease claims across worker processes (`migrations/0007_job_queue.up.sql`) |
| `expires_at` + sweep | idempotency key TTL |
| **shared server across processes** | `api`, `worker`, and `mcp` are separate OS processes sharing one Postgres |

Everything above has a fjall answer except the last row, which is not a
storage question at all — see "The process-topology fork".

## Keyspace layout

Proposed layout. Names are illustrative; the point is the shape.

**The log (the system):**

- `events` — key: `seq` as 8-byte big-endian u64 → value: canonical event
  envelope (id, occurred_at, actor, source, subject_kind, subject_id, kind,
  payload). This is the append-only heart. Monotonically increasing keys
  are the best case for an LSM tree: memtables flush into non-overlapping
  runs, compaction has almost nothing to merge, and range scans
  (`seq > cursor`) are sequential reads.
- `event_ids` — key: 16-byte event id → value: seq. The dedupe index that
  replaces `ON CONFLICT DO NOTHING`: the appender does a point `get`
  before writing; hit ⇒ replay ⇒ return `fresh=false`, skip projectors.
  Enable the hash-index block option here; it is a pure point-lookup
  keyspace.
- `events_by_subject` — key: `subject_kind ∥ 0x00 ∥ subject_id ∥ seq` →
  empty value. Replaces the `(subject_kind, subject_id, occurred_at)`
  index; per-subject folds and audits become prefix scans.
- (optional) `events_by_kind` — only if feed-filter scans over the main log
  prove too slow. v1 filters ~35 kinds in SQL; v2 can filter in Rust while
  scanning `events`, which for our volumes is likely fine. Don't build
  indexes speculatively; each keyspace has fixed memtable/flush overhead.
- `meta` — schema/format version, engine epoch, anything that must be read
  before folds run. The seq head does NOT live here: recover it at startup
  from `events.last_key_value()`. One source of truth.

**Projections (derived, droppable):**

- One keyspace per projection table: `proj_work_items` (id → document),
  `proj_tokens`, `proj_signals`, `proj_approvals`, …
- One keyspace per secondary index the read paths actually use:
  `proj_work_items_by_state` (`state ∥ updated_at_desc ∥ id` → empty), etc.
  Every SQL index v1 relies on must be enumerated and hand-encoded; this is
  the single biggest silent cost of leaving Postgres. Key-encoding
  discipline (order-preserving encodings, careful separators, big-endian
  integers, inverted timestamps for DESC) becomes a small in-house library
  with property tests.
- Dynamic feed projections (`internal/projectiondefs`) stay *virtual* —
  they are named filters over the log, not materialized state — so they
  need no keyspaces. Their definitions remain events; the `projections`
  registry keyspace is just another projection. If a projection type ever
  becomes materialized, its name+version maps to a keyspace
  (`proj_feed_<name>_v<N>`), and version bumps get the pleasant LSM
  property that dropping the old version is file deletion, not a
  tombstone-and-compact churn.

**Operational (event-caused, not diffed — v1's "scratch tables"):**

- `job_queue`, `job_queue_pending` (claim index), `idempotency` — see
  "Non-fold state" below.

Naming convention worth pinning early: `log_*` / `idx_*` / `proj_*` /
`ops_*` prefixes, a registry module that owns the full list (the sibling of
v1's `projectionTables` list in `rebuild.go`), and a startup check that the
on-disk keyspace set matches the registry — orphaned keyspaces from
removed projections should be flagged (or auto-dropped, since they are
derived state).

## The fold engine

v1's law (AGENTS.md principles 1–2): every state change is an event, and
every non-`events` row is written by a projector **in the same transaction
as the append**. Keep the law; change the mechanism.

**Write path.** Run all appends through a single-writer appender — one
task/thread owning the write half of the store (either literally, with
plain `Database` + `WriteBatch` behind an mpsc channel, or via
`SingleWriterTxDatabase` which serializes writers for us). Per append:

1. compute the deterministic event id (port of `events.DeterministicID` —
   same SHA-256 over `subject_kind : subject_id : kind : canonical(payload)
   [: discriminator]`, same v5-shaped stamping, so v1 ids replay
   byte-identically);
2. point-lookup `event_ids`; hit ⇒ return `fresh=false`, write nothing;
3. allocate `seq = head + 1` (in-memory counter, recovered at startup);
4. open one `WriteBatch`; insert into `events`, `event_ids`,
   `events_by_subject`; run every registered projector for the kind, each
   contributing its puts/deletes **to the same batch**;
5. commit the batch with the chosen `PersistMode`.

The batch is the transaction: all keyspaces it touches commit atomically
through the shared journal, so "a projection row exists only because an
event caused it" survives crashes exactly as it does under Postgres. (The
one API caveat to verify on current 3.1.x: batches don't support reading
your own uncommitted writes, so a projector that reads current projection
state must read the pre-batch store plus an in-batch overlay, or we use the
single-writer transaction type which does read-your-own-writes. v1
projectors already read current rows via `tx`, so this is a real
requirement, and it nudges toward `SingleWriterTxDatabase`.)

The single-writer discipline is not a concession — it *simplifies* v1:
seq allocation needs no sequence object, dedupe check-then-write has no
race window, and projector ordering is trivially deterministic. Postgres
gave us concurrency we then spent effort re-serializing (SHARE locks,
`FOR UPDATE`, BIGSERIAL ordering caveats). An embedded engine lets the
appender own order outright. Throughput is not a concern at meristem's
scale (a coordination plane, not a firehose); if it ever is, group-commit
inside the appender (drain the channel, one batch, one fsync) is the
standard answer.

**Durability.** Decide `PersistMode` policy explicitly: default OS-buffer
persistence means a power loss can drop the last events *after* callers saw
201. v1 inherits fsync-on-commit from Postgres. The honest equivalent is
sync-per-append (or group-commit sync); anything weaker must be a
documented, deliberate downgrade.

**Folds themselves.** A projector becomes a pure Rust function:
`fn apply(&self, view: &impl Readable, out: &mut Batch, event: &Event)`.
The registry stays a map from event kind to an ordered projector list —
a direct port of `internal/projections`. The three v1 invariants
(idempotent, pure w.r.t. payload, rebuild-safe) carry over verbatim and
get *stronger*: no `now()` column defaults, no timestamptz session
formatting, no JSONB key-reordering surprises. But that also means v2 must
pin a **canonical value encoding** for projection documents — the rebuild
diff compares bytes, so map iteration order and float/integer formatting
must be deterministic. Options: keep canonical JSON (port of
`internal/events/canonical.go`, one codebase already speaks it) or move to
deterministic CBOR. Either way: one encoder, owned by the storage layer,
property-tested.

**Rebuild (the honesty check).** v1: fold `events` in seq order into a
sandbox schema, hash-compare against live, roll back
(`cmd/meristem/rebuild.go`). v2 equivalent, cleaner in two ways:

- *Isolation*: fold into fresh `rebuild_*` keyspaces (cheap to create,
  cheap to drop — dropping a keyspace is file deletion) or into an
  entirely separate temporary `Database` in a scratch directory. No
  `search_path` redirection trick, no risk of a projector "escaping the
  sandbox" via a qualified table name — the projector writes wherever the
  handed-in keyspace handles point.
- *Consistency*: instead of SHARE-locking the log, take a **snapshot** /
  read at a pinned version, fold everything ≤ head-at-snapshot, and diff
  projection state at the same snapshot. Writers keep writing; the
  verdict is about a consistent point in time. The diff itself: iterate
  live and rebuilt keyspaces in lockstep (both are sorted — this is a
  merge-join, not a hash-the-world), reporting the first divergent key;
  strictly better diagnostics than v1's table-level md5 verdict.

Read-time folds (e.g. backlog readiness, folded from visible work items at
request time) port as ordinary Rust folds over a keyspace scan — no SQL
lost there, since v1 already computes them in Go.

## Streams and cursors

The feed contract survives almost unchanged, because v1 already refused to
lean on SQL for it:

- The cursor is opaque(seq) — 8 BE bytes, base64url (`internal/feed/
  cursor.go`), with projection-scoped envelope cursors on top. Both
  encodings port byte-for-byte. Cursor existence check = point lookup of
  seq in `events`.
- `queryAfter` = range scan `events` over `(cursor, ..]`, decode, filter by
  kind allowlist / projection filter in code, `limit+1` for HasMore. The
  taxonomy (Included/Excluded kinds partition, kind classes) is pure Go
  today and ports as pure Rust.
- At-least-once + consumer dedupe by event id: unchanged.
- **Long-poll and SSE get better.** v1 polls every 250ms because separate
  processes can't share a wakeup (`feed.Page`, `feed.Tail`). In-process,
  the appender publishes head-seq on a `tokio::sync::watch`; feed waiters
  await a head change past their cursor and then range-scan. The 250ms
  latency floor and idle query load disappear. Keep the bounded-wait cap
  and the "from now" head-bootstrap semantics as-is.

## In-process reads and pushes

The mpsc appender serializes *writes only* — seq allocation, dedupe
check-then-write, fold atomicity. Reads and pushes have their own story:

**Reads never queue behind the appender.** fjall handles are thread-safe
and cheaply clonable; handlers read directly against memtable + SSTs while
the appender commits. Two disciplines replace what Postgres did silently:

- *Snapshot rule*: a single point-get reads live; any read touching two or
  more keys that must agree (document + its index, feed page assembly, a
  fold-at-read view like backlog readiness) takes a snapshot at the
  current instant and reads through it (the `Readable` trait). Postgres
  gave every SQL statement one MVCC snapshot; naive multi-gets can
  interleave with an appender commit. Snapshots pin a sequence number and
  are cheap, so the rule can be universal.
- *Blocking rule*: fjall reads are synchronous and can touch disk; route
  all storage access through `spawn_blocking` (or a dedicated read pool)
  rather than tokio's async executor. Uniformity beats per-call-site
  judgment; overhead is noise at our volumes.

Read-your-writes holds across the API for free: the appender's oneshot
reply arrives only after batch commit, so a 201 happens-before any
subsequent read by the caller.

**Pushes: channels are doorbells, the log is the data.** No events travel
through channels; consumers own a cursor, a push only says "head moved."
The appender publishes head-seq on a `tokio::sync::watch` after each
commit. `watch` (not `broadcast`) is correct precisely because it
coalesces to the latest value — consumers re-scan from their own cursor,
so merged or missed wakeups are harmless, and the at-least-once +
dedupe-by-event-id contract is untouched. Feed long-poll: scan after
cursor; if empty, `select!` on `watch.changed()` vs deadline; re-scan on
wake (subscribe before concluding emptiness — standard condition-variable
discipline against lost wakeups). SSE: same loop, streaming; both v1 poll
ticks (`pollTick`, `ssePollInterval`) die. The worker selects over the
same head watch plus its own deadline timer (next patience budget or
lease expiry), keeping a coarse periodic full `ScanOnce` as self-healing
and `worker --once` as the unchanged verification path. MCP assistants
still pull `feed.read` with a cursor — only the server side of that poll
changes from ticking to waiting.

**Group commit, per-request semantics.** Batching amortizes fsync, but
replies are per-request and fold errors isolate per request: each append
folds into its own sub-batch before merging into the shared journal
write, so one bad projector rejects one request (v1's per-tx abort
semantics), while an I/O failure at commit fails the whole group
consistently — nothing durable, everyone errors.

## The process-topology fork (the real decision)

fjall is embedded: one process owns the files (3.0 even enforces it with
file locking). v1's topology is **three processes sharing Postgres** —
`api`, `worker`, `mcp` (stdio). The merge is less dramatic than it
sounds: all three are thin transports over one shared, transport-agnostic
service layer (`internal/mcp/server.go` documents its deps as "the same
services the HTTP transport calls into … one more translation layer,
never an alternate execution path"). `api` is the HTTP driver, `worker`
is a timer driver around `ScanOnce`, `mcp` is a stdio driver — three
drivers, one core. In v2 they become three tokio tasks over the same
Rust service layer; the factoring ports directly. v2 must pick:

1. **One process** — api + worker + MCP-over-HTTP as tasks in one tokio
   runtime; the stdio MCP transport becomes a thin proxy speaking to the
   HTTP surface instead of opening the database. This is the natural
   shape for "light and always-on", it is what makes the in-process
   notify/fold wins above real, and it collapses v1 machinery that exists
   only because of multi-process coordination (SKIP LOCKED leasing across
   workers, poll ticks). Cost: worker misbehavior shares a failure domain
   with the API; mitigate with task supervision, not process separation.
2. Keep multi-process and put a gRPC/HTTP shim in front of storage —
   re-inventing a database server, which forfeits most of the reason to
   embed.

Recommendation: (1). The `worker --once` verification path survives as a
subcommand that either runs against the API or takes the (unlocked) store
exclusively.

## Non-fold state

- **job_queue.** Enqueue rows remain event-caused (dispatch.requested);
  lease state remains operational scratch, excluded from the rebuild diff
  (v1 already drew this line — `rebuildScratchTables`). Single-process v2
  can hold leases in memory over a durable pending set: claim = mutate the
  in-process scheduler; only enqueue/done/failed transitions touch the
  store. `SKIP LOCKED` was solving cross-process contention that topology
  (1) deletes.
- **idempotency.** Key: `token_id ∥ scope ∥ key` → recorded response +
  `expires_at`. Enforce TTL by filtering on read, and reclaim space with a
  compaction filter (3.1.0+) dropping entries past expiry — the LSM-native
  replacement for v1's `expires_at` index sweep. Until the compaction
  filter is wired, a periodic range-scan sweep is fine at our volumes.
- **tokens.** Hashes are a projection like any other; nothing special.

## LSM study guide, keyed to our decisions

What actually matters for meristem, in dependency order:

1. **Write path**: WAL/journal → memtable → flush → immutable SSTs →
   compaction into levels. Maps to: why the append batch is cheap, why
   durability is a `PersistMode` decision, why a keyspace has fixed
   overhead (own memtable) and we shouldn't mint them speculatively.
2. **Monotonic keys**: appending strictly increasing seqs produces
   non-overlapping SSTs — minimal compaction, near-sequential disk layout.
   Our `events` keyspace is the textbook best case. (Contrast: the
   content-addressed `event_ids` keyspace is uniformly random — that one
   *does* compact and is why it exists as its own keyspace instead of a
   second key shape mixed into `events`.)
3. **Read paths**: point reads go through filters (bloom/hash) per SST —
   cheap misses matter for the dedupe check; range scans merge iterators
   across levels — cheap because sorted. Sparse indexes + prefix
   truncation (the 3.0 block format) are why big scans got faster.
4. **Deletes are writes**: tombstones cost until compaction removes them.
   Consequence: never mass-delete a derived keyspace key-by-key; drop the
   keyspace. This is why versioned materialized projections and rebuild
   sandboxes are keyspace-lifecycle operations.
5. **The amplification triangle** (write/read/space) and leveled vs tiered
   compaction — enough to understand fjall's level-based config policies,
   not to tune them prematurely.
6. **Snapshots/MVCC**: how sequence-number-based visibility gives
   consistent reads without locks — the rebuild design above depends on it.

Reading: the original LSM-tree paper (O'Neil et al.) for vocabulary; the
RocksDB wiki (Compaction, MemTable, WAL pages) for the engineering
reality; skyzh's *mini-lsm* course to build one in Rust (directly relevant
muscle); the fjall author's blog for engine-specific behavior post-3.0.

## Migration path (v1 log → v2 store)

The event log's content-addressing is the migration insurance policy:

1. Freeze v1 writes. Stream `SELECT ... FROM events ORDER BY seq ASC`.
2. Append each into the v2 store preserving seq verbatim (imported ids
   must reproduce — same hash inputs, same canonical JSON — so replay
   collapses if run twice; the import is idempotent for free).
3. Fold projections by running rebuild — the importer writes only the log;
   projections are derived on arrival or in one batch fold after.
4. Run v2's rebuild diff against a v1 `meristem rebuild --verbose` table
   signature run for the same event set as the cross-engine honesty check.

Cursor compatibility: v1 cursors encode seq; imported seqs are preserved;
outstanding consumer cursors survive the migration unchanged. That is a
direct payoff of 0006's decision to make seq the only ordering primitive.

## Open questions

- Verify on current 3.1.x docs: cross-keyspace atomicity of `WriteBatch`
  (expected: yes, shared journal), snapshot API shape, whether
  `SingleWriterTxDatabase` transactions span keyspaces with
  read-your-own-writes (expected: yes), compaction-filter API surface.
- Canonical encoding: keep canonical JSON or adopt deterministic CBOR for
  event payloads and projection documents (rebuild diff depends on it).
- Backup story: Postgres gave pg_dump; fjall needs a snapshot-plus-copy or
  export-the-log discipline. The log export (events in seq order) may
  simply *be* the backup format — it can rebuild everything else.
- The append-only triggers have no engine-level equivalent — v2 enforces
  append-only by construction (nothing but the appender holds write
  handles to `log_*` keyspaces). Decide whether that discipline needs a
  runtime guard (e.g. debug assertions in the storage layer) or code
  review suffices.
- Whether `events_by_kind` is needed at all, measured against real feed
  filter latency after the port.
