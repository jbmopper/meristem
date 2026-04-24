-- 0005_active_root_token_unique down: restore the stricter historical index.

DROP INDEX IF EXISTS tokens_single_active_root_idx;

CREATE UNIQUE INDEX tokens_single_root_idx
    ON tokens ((is_root))
    WHERE is_root;
