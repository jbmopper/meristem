-- 0008_deterministic_errors down: drop projection indexes/table.

DROP INDEX IF EXISTS deterministic_errors_component_code_idx;
DROP INDEX IF EXISTS deterministic_errors_masked_idx;
DROP INDEX IF EXISTS deterministic_errors_active_idx;
DROP TABLE IF EXISTS deterministic_errors;
