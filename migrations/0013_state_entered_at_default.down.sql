-- 0013_state_entered_at_default rollback.

ALTER TABLE work_items
    ALTER COLUMN state_entered_at DROP DEFAULT;
