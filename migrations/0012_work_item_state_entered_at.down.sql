-- 0012_work_item_state_entered_at rollback.

DROP INDEX IF EXISTS work_items_state_entered_at_idx;

ALTER TABLE work_items
    DROP COLUMN IF EXISTS state_entered_at;
