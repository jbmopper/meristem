-- 0004_token_source: classify each client credential by authority source.
--
-- Event attribution uses the token row as authority. Request bodies may
-- describe upstream provenance, but only tokens.source decides whether an
-- appended event came from a human, agent, or system actor.

ALTER TABLE tokens
    ADD COLUMN source TEXT NOT NULL DEFAULT 'human'
    CHECK (source IN ('human', 'agent', 'system'));
