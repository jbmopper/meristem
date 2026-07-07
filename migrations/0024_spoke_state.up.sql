-- 0024_spoke_state: spoke-local outbound-poll bookmarks.
--
-- Stage 1 spoke drain (work item bc1da2c5). The `meristem spoke` loop polls a
-- hub feed with a persisted cursor so a restart resumes where it left off
-- instead of re-scanning from the hub head (docs/network-layer-spec.md §2
-- Reads / remote-ref pull, §6 stage 1 outbound polling).
--
-- This is deliberately NOT a projection and NOT on the events path: the cursor
-- is derived, best-effort, per-node operational state (a poll bookmark), never
-- domain truth — losing it only re-bootstraps the feed cursor "from now". It is
-- the "process state belongs in Postgres; memory is best-effort" case AGENTS.md
-- calls out, kept out of `events` precisely so it never enters replay/rebuild.
-- Written directly by the spoke loop, one row per opaque key.

CREATE TABLE spoke_state (
    key        TEXT        PRIMARY KEY,
    value      TEXT        NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
