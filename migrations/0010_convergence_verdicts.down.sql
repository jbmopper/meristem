-- 0010_convergence_verdicts down: drop convergence verdict projection.

DROP INDEX IF EXISTS convergence_verdicts_disposition_idx;
DROP INDEX IF EXISTS convergence_verdicts_work_item_occurred_idx;
DROP INDEX IF EXISTS convergence_verdicts_work_item_attempt_idx;
DROP TABLE IF EXISTS convergence_verdicts;
