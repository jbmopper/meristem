-- 0004_token_source down: remove token source classification.

ALTER TABLE tokens
    DROP COLUMN source;
