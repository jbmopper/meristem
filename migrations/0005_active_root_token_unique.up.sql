-- 0005_active_root_token_unique: allow root rotation while preserving the
-- invariant that only one active root token may exist.

DROP INDEX IF EXISTS tokens_single_root_idx;

CREATE UNIQUE INDEX tokens_single_active_root_idx
    ON tokens ((is_root))
    WHERE is_root AND revoked_at IS NULL;
