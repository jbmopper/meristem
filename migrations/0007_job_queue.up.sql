-- 0007_job_queue: durable job rows for the v1 worker queue.
--
-- Rationale: `internal/worker` currently scans `work_items` in-process;
-- a separate queue lets multiple worker processes claim disjoint units
-- of work with SELECT … FOR UPDATE SKIP LOCKED in a follow-on slice.
--
-- This migration is schema-only. No writer enqueues yet; the table is
-- empty until enqueue semantics land (separate work_items). Rebuild
-- does not include job_queue: it is not a deterministic projection of
-- `events` in this phase.

CREATE TABLE job_queue (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    kind          TEXT         NOT NULL,
    work_item_id  UUID         NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    state         TEXT         NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'leased', 'done', 'failed', 'canceled')),
    payload       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    attempts      INT          NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_until   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Claim path: only pending jobs are directly eligible for SKIP LOCKED.
CREATE INDEX job_queue_pending_claim_idx
    ON job_queue (created_at ASC)
    WHERE state = 'pending';

-- Reclaim / expiry: find leased rows past lease_until when implementing
-- lease renewal or background sweep.
CREATE INDEX job_queue_lease_reclaim_idx
    ON job_queue (lease_until ASC)
    WHERE state = 'leased';
