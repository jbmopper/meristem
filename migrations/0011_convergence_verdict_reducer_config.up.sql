-- 0011_convergence_verdict_reducer_config: persist reducer configuration in
-- the convergence verdict projection so parameterized reductions remain
-- inspectable without refolding event payload JSON.

ALTER TABLE convergence_verdicts
    ADD COLUMN reducer_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT convergence_verdicts_reducer_config_object
        CHECK (jsonb_typeof(reducer_config) = 'object');
