-- 0019_approvals rollback.

DROP INDEX IF EXISTS approvals_status_expires_idx;
DROP INDEX IF EXISTS approvals_work_item_idx;
DROP TABLE IF EXISTS approvals;
