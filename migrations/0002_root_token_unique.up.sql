-- 0002_root_token_unique: enforce the single-root-token invariant at the
-- projection layer. Root creation still goes through token.created events.

CREATE UNIQUE INDEX tokens_single_root_idx ON tokens ((is_root)) WHERE is_root;
