DROP INDEX IF EXISTS command_queue_due_idx;

ALTER TABLE command_queue
    DROP CONSTRAINT IF EXISTS command_queue_terminal_event_check,
    DROP CONSTRAINT IF EXISTS command_queue_attempt_count_check,
    DROP CONSTRAINT IF EXISTS command_queue_state_check;

-- Best-effort development rollback: the old schema has no refused/expired
-- states, so retain their terminality as failed.
UPDATE command_queue
SET state = 'failed', outcome_ok = false
WHERE state IN ('refused', 'expired');

ALTER TABLE command_queue
    ADD CONSTRAINT command_queue_state_check
        CHECK (state IN ('pending', 'done', 'failed')),
    DROP COLUMN IF EXISTS terminal_reason,
    DROP COLUMN IF EXISTS terminal_event_id,
    DROP COLUMN IF EXISTS last_attempt_at,
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS expires_at;
