-- 0007_job_queue down: drop in dependency order (indexes, then table).

DROP INDEX IF EXISTS job_queue_lease_reclaim_idx;
DROP INDEX IF EXISTS job_queue_pending_claim_idx;

DROP TABLE IF EXISTS job_queue;
