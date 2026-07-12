-- 0027_network_patience: bounded command-queue patience and event-backed
-- spoke cursors (work item e34cd621-bd63-5a20-a85f-4cf0e2c7d372).
--
-- Queue rows expire after 24 hours or five recorded local attempts. Attempt
-- and terminal event ids keep replay deterministic; terminal_event_id records
-- the first acknowledgement/expiry event that won the pending -> terminal
-- reduction. spoke_state remains the read projection, but its writes now come
-- only from spoke_cursor.advanced events.

ALTER TABLE command_queue
    ADD COLUMN expires_at        TIMESTAMPTZ,
    ADD COLUMN attempt_count     INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN last_attempt_at   TIMESTAMPTZ,
    ADD COLUMN terminal_event_id UUID,
    ADD COLUMN terminal_reason   TEXT;

UPDATE command_queue
SET expires_at = queued_at + INTERVAL '24 hours'
WHERE expires_at IS NULL;

ALTER TABLE command_queue
    ALTER COLUMN expires_at SET NOT NULL;

-- Existing terminal rows were caused by command.acked under migration 0023.
-- Backfill their winning event identity from the immutable log before adding
-- the terminal-state invariant.
UPDATE command_queue AS cq
SET terminal_event_id = (
    SELECT e.id
    FROM events AS e
    WHERE e.kind = 'command.acked'
      AND e.payload->>'command_queue_id' = cq.id::text
    ORDER BY e.seq
    LIMIT 1
)
WHERE cq.state <> 'pending'
  AND cq.terminal_event_id IS NULL;

ALTER TABLE command_queue
    DROP CONSTRAINT command_queue_state_check,
    ADD CONSTRAINT command_queue_state_check
        CHECK (state IN ('pending', 'done', 'refused', 'failed', 'expired')),
    ADD CONSTRAINT command_queue_attempt_count_check
        CHECK (attempt_count BETWEEN 0 AND 5),
    ADD CONSTRAINT command_queue_terminal_event_check
        CHECK (
            (state = 'pending' AND terminal_event_id IS NULL)
            OR (state <> 'pending' AND terminal_event_id IS NOT NULL)
        );

CREATE INDEX command_queue_due_idx
    ON command_queue (expires_at, id)
    WHERE state = 'pending';

-- Rows written under migration 0024 have no causing event and therefore
-- cannot survive an honest replay. Drop those best-effort bookmarks once as
-- the write path becomes event-backed; the next spoke poll establishes a
-- durable cursor through spoke_cursor.advanced.
DELETE FROM spoke_state;
