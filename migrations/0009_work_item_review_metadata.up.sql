-- 0009_work_item_review_metadata: convergence and review metadata.
--
-- These columns are still a projection of work_item.* events. They give
-- agents a durable checklist for deterministic convergence reduction and a
-- small human-review signal without introducing approval rows yet.

ALTER TABLE work_items
    ADD COLUMN suggested_convergence_checks JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN human_review_status TEXT NOT NULL DEFAULT 'waved_through',
    ADD CONSTRAINT work_items_suggested_convergence_checks_array
        CHECK (jsonb_typeof(suggested_convergence_checks) = 'array'),
    ADD CONSTRAINT work_items_human_review_status_check
        CHECK (human_review_status IN ('blocked', 'waved_through', 'approved'));
