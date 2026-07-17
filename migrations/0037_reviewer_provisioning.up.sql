-- 0037_reviewer_provisioning: durable state for atomic reviewer provisioning
-- (ee916614 slice 3a, accepted lifecycle design revision 4).
--
-- tokens.expires_at: ordinary tokens gain a hard durable expiry that the
-- authenticator enforces. NULL means no expiry (every pre-0037 token).
--
-- job_queue.lease_owner / lease_generation: a lease becomes a concrete fenced
-- fact (who holds it, which incarnation) instead of state='leased' alone.
-- Lease fields remain operational coordination, the narrow direct-update
-- exception; they are never a domain projection.
--
-- review_launch: the durable reservation/handle/outcome projection for one
-- spawned-review launch attempt, keyed by (work_item, review round, attempt).
-- Rows are caused by work_item.review_launch_* events and projected in the
-- same transaction; capacity accounting counts live rows here, never
-- supervisor memory.

ALTER TABLE tokens
    ADD COLUMN expires_at TIMESTAMPTZ;

-- launch_failed joins the closed release-reason vocabulary: a terminally
-- failed spawned-review launch releases its exact binding generation without
-- manufacturing a terminal work-item sentinel (it pairs with terminal_state
-- NULL exactly like yield/expired).
ALTER TABLE work_item_assignment_state
    DROP CONSTRAINT work_item_assignment_state_last_release_reason_check,
    ADD CONSTRAINT work_item_assignment_state_last_release_reason_check CHECK (
        last_release_reason IS NULL
        OR last_release_reason IN ('done', 'yield', 'expired', 'launch_failed')
    ),
    DROP CONSTRAINT work_item_assignment_state_check1,
    ADD CONSTRAINT work_item_assignment_state_release_terminal_check CHECK (
        (last_release_reason IS NULL AND terminal_state IS NULL)
        OR (last_release_reason = 'done' AND terminal_state IS NOT NULL)
        OR (last_release_reason IN ('yield', 'expired', 'launch_failed') AND terminal_state IS NULL)
    );

ALTER TABLE job_queue
    ADD COLUMN lease_owner UUID,
    ADD COLUMN lease_generation BIGINT NOT NULL DEFAULT 0;

-- succeeded means the reviewer PROCESS IS RUNNING for the exact binding; it
-- is not terminal and keeps holding capacity. exited is the terminal
-- confirmed-death/normal-exit record (Wait, or adopted pid+start-time
-- absence). termination_due is the server-side deadline mark: the deadline
-- pass may demand termination but must never terminally free a handled or
-- succeeded run without confirmed death.
CREATE TABLE review_launch (
    work_item_id UUID NOT NULL REFERENCES work_items(id),
    round_seq BIGINT NOT NULL,
    attempt INTEGER NOT NULL,
    job_id UUID NOT NULL,
    assignment_event_id UUID NOT NULL,
    reviewer_token_id UUID NOT NULL REFERENCES tokens(id),
    issuer_token_id UUID NOT NULL REFERENCES tokens(id),
    lease_owner UUID NOT NULL,
    lease_generation BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('reserved', 'handled', 'succeeded', 'exited', 'failed', 'abandoned')
    ),
    stage TEXT,
    termination_due BOOLEAN NOT NULL DEFAULT FALSE,
    handle_pid BIGINT,
    handle_pgid BIGINT,
    handle_start_token TEXT,
    deadline TIMESTAMPTZ NOT NULL,
    created_event_id UUID NOT NULL,
    updated_event_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (work_item_id, round_seq, attempt)
);

-- Live-capacity scans and deadline reconciliation both filter on state and
-- deadline; keep them off a sequential scan as launches accumulate.
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

-- Single-use enforcement: a reviewer identity binds at most once, ever.
-- AssignSpawned refuses an assignee that appears in any prior
-- work_item.assigned event; this expression index makes that lookup exact.
CREATE INDEX events_assigned_assignee_idx
    ON events ((payload->>'assignee_token_id'))
    WHERE kind = 'work_item.assigned';
