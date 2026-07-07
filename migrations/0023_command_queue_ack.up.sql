-- 0023_command_queue_ack: drain/ack bookkeeping for the per-target command queue.
--
-- Stage 1 spoke drain (work item bc1da2c5). A target node polls its queue
-- outbound (GET /v1/crossnode/commands?target=...), executes each command
-- locally under its own agent token with the original idempotency key, and
-- acknowledges the structural outcome back to the hub
-- (POST /v1/crossnode/commands/{event_id}/ack). Truth stays in events: the ack
-- appends a command.acked event whose projector folds the outcome onto the
-- queued row here (see docs/network-layer-spec.md §2 "Commands to nodes without
-- inbound reachability").
--
-- state advances pending -> done (ok) / failed (not ok) exactly once per row;
-- outcome_status_code / outcome_ok record the structural outcome the spoke
-- observed executing the command locally; acked_at is the ack event's
-- occurred_at (projector reads the event clock, never wall time, so a rebuild
-- reproduces the row).
--
-- Expand-safe: adds nullable/defaulted columns to an existing projection table;
-- no existing writer is forced to change and the pending default keeps every
-- already-projected row in its pre-ack state.

ALTER TABLE command_queue
    ADD COLUMN state               TEXT        NOT NULL DEFAULT 'pending',
    ADD COLUMN outcome_status_code INTEGER,
    ADD COLUMN outcome_ok          BOOLEAN,
    ADD COLUMN acked_at            TIMESTAMPTZ;

ALTER TABLE command_queue
    ADD CONSTRAINT command_queue_state_check
    CHECK (state IN ('pending', 'done', 'failed'));

-- The drain read filters a single target's pending rows oldest-first.
CREATE INDEX command_queue_pending_idx
    ON command_queue (target_node_id, queued_at)
    WHERE state = 'pending';
