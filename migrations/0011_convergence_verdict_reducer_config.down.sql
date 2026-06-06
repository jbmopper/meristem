-- 0011_convergence_verdict_reducer_config down: remove reducer configuration
-- from the convergence verdict projection.

ALTER TABLE convergence_verdicts
    DROP CONSTRAINT IF EXISTS convergence_verdicts_reducer_config_object,
    DROP COLUMN IF EXISTS reducer_config;
