-- 0037_rename_relay_via_to_queue_via: expand phase of the relay_via ->
-- queue_via rename (cold audit finding G3).
--
-- The column always meant "ordered queue-host allowlist", never a relay chain:
-- crossnode.Select emits KindQueue for these hops and never KindRelay. The
-- name misled readers into believing relay routing exists.
--
-- This is the EXPAND half, not a rename. `ALTER TABLE ... RENAME COLUMN` would
-- be a breaking change under the fleet's known binary drift (see
-- docs/network-operations.md): a stale meristem process on the same database
-- still selects relay_via and would fail on every node read the moment this
-- migration lands. So queue_via is added alongside and backfilled.
--
-- IMPORTANT — the expand release WRITES BOTH COLUMNS BUT STILL READS relay_via.
-- queue_via exists and is kept current, but nothing reads it yet. That is the
-- middle step of expand/contract, and skipping it is a live defect rather than
-- a style question: a drifted pre-0037 binary updates only relay_via, so a
-- reader on queue_via would serve the stale backfilled value and route through
-- a queue host the operator had already removed. Reads flip to queue_via in the
-- contract release, once no old writer can still be running; that release also
-- drops relay_via and the dual write.
--
-- The mirroring is deliberately in the projectors, not a trigger. `nodes` is a
-- projection whose every column must stay a deterministic fold of the event
-- log — `meristem rebuild` verifies exactly that by replaying events into a
-- sandbox schema cloned with LIKE ... INCLUDING ALL, which does not copy
-- triggers. A trigger-maintained relay_via would diverge in the sandbox and
-- fail the rebuild check for no real defect.

ALTER TABLE nodes ADD COLUMN queue_via JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE nodes SET queue_via = relay_via;
