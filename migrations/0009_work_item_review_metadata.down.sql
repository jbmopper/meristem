-- 0009_work_item_review_metadata rollback.

ALTER TABLE work_items
    DROP CONSTRAINT IF EXISTS work_items_suggested_convergence_checks_array,
    DROP CONSTRAINT IF EXISTS work_items_human_review_status_check,
    DROP COLUMN IF EXISTS suggested_convergence_checks,
    DROP COLUMN IF EXISTS human_review_status;
