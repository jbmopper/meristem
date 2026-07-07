DROP INDEX IF EXISTS command_queue_pending_idx;
ALTER TABLE command_queue DROP CONSTRAINT IF EXISTS command_queue_state_check;
ALTER TABLE command_queue
    DROP COLUMN IF EXISTS acked_at,
    DROP COLUMN IF EXISTS outcome_ok,
    DROP COLUMN IF EXISTS outcome_status_code,
    DROP COLUMN IF EXISTS state;
