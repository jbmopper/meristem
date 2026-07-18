-- 0038_reviewer_launch_fencing: forward-only delta over 0037 (ee916614
-- slice 3a round 3). 0037 shipped with the exact parent commit and may
-- already be applied; every round-2/3 schema change lands here instead of
-- rewriting an applied migration.
--
-- issuer_token_id / lease_owner / lease_generation fence handles and
-- outcomes to the exact provisioning incarnation. They are nullable because
-- v1 reservation events predate them; fencing fails closed on NULL, so a
-- v1 row can never take a handle or a success.
--
-- handle/resolved/termination event ids preserve per-step causal identity:
-- replaying ANY already-applied exact event is a no-op even after later
-- transitions, while a distinct event on the same lifecycle key still fails.
--
-- succeeded means the reviewer process is RUNNING (capacity stays held);
-- exited is the confirmed-death/normal-exit terminal; termination_due is
-- the deadline mark that frees nothing without confirmed death.

ALTER TABLE review_launch
    ADD COLUMN issuer_token_id UUID REFERENCES tokens(id),
    ADD COLUMN lease_owner UUID,
    ADD COLUMN lease_generation BIGINT,
    ADD COLUMN termination_due BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN handle_event_id UUID,
    ADD COLUMN resolved_event_id UUID,
    ADD COLUMN termination_event_id UUID;

ALTER TABLE review_launch
    DROP CONSTRAINT review_launch_state_check,
    ADD CONSTRAINT review_launch_state_check CHECK (
        state IN ('reserved', 'handled', 'succeeded', 'exited', 'failed', 'abandoned')
    );

DROP INDEX IF EXISTS review_launch_live_idx;
CREATE INDEX review_launch_live_idx
    ON review_launch (deadline)
    WHERE state IN ('reserved', 'handled', 'succeeded', 'abandoned');

-- Portable capacity serialization: provisioning locks this singleton row
-- (SELECT ... FOR UPDATE) before counting live launches. No advisory locks:
-- the storage contract must survive a future SQLite-per-node mode.
CREATE TABLE review_launch_capacity (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton)
);
INSERT INTO review_launch_capacity (singleton) VALUES (TRUE);
