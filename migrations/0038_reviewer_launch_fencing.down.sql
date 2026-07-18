DROP TABLE IF EXISTS review_launch_capacity;
DROP INDEX IF EXISTS review_launch_live_idx;
CREATE INDEX review_launch_live_idx
    ON review_launch (deadline)
    WHERE state IN ('reserved', 'handled', 'abandoned');
ALTER TABLE review_launch
    DROP CONSTRAINT review_launch_state_check,
    ADD CONSTRAINT review_launch_state_check CHECK (
        state IN ('reserved', 'handled', 'succeeded', 'failed', 'abandoned')
    );
ALTER TABLE review_launch
    DROP COLUMN IF EXISTS termination_event_id,
    DROP COLUMN IF EXISTS resolved_event_id,
    DROP COLUMN IF EXISTS handle_event_id,
    DROP COLUMN IF EXISTS termination_due,
    DROP COLUMN IF EXISTS lease_generation,
    DROP COLUMN IF EXISTS lease_owner,
    DROP COLUMN IF EXISTS issuer_token_id;
