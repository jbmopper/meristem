DROP INDEX IF EXISTS events_assigned_assignee_idx;
DROP INDEX IF EXISTS review_launch_live_idx;
DROP TABLE IF EXISTS review_launch;
ALTER TABLE job_queue
    DROP COLUMN IF EXISTS lease_generation,
    DROP COLUMN IF EXISTS lease_owner;
ALTER TABLE work_item_assignment_state
    DROP CONSTRAINT IF EXISTS work_item_assignment_state_release_terminal_check,
    ADD CONSTRAINT work_item_assignment_state_check1 CHECK (
        (last_release_reason IS NULL AND terminal_state IS NULL)
        OR (last_release_reason = 'done' AND terminal_state IS NOT NULL)
        OR (last_release_reason IN ('yield', 'expired') AND terminal_state IS NULL)
    ),
    DROP CONSTRAINT IF EXISTS work_item_assignment_state_last_release_reason_check,
    ADD CONSTRAINT work_item_assignment_state_last_release_reason_check CHECK (
        last_release_reason IS NULL OR last_release_reason IN ('done', 'yield', 'expired')
    );
ALTER TABLE tokens
    DROP COLUMN IF EXISTS expires_at;
