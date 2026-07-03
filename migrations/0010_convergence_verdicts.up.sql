-- 0010_convergence_verdicts: projection for deterministic convergence reductions.
--
-- Each row is derived from one convergence.verdict_recorded event. The event
-- is the durable truth; this table is the indexed read view for "why did this
-- work_item converge, retry, or escalate?"

CREATE TABLE convergence_verdicts (
    event_id          UUID        PRIMARY KEY REFERENCES events(id),
    work_item_id      UUID        NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    reducer_identity  TEXT        NOT NULL,
    reducer_version   INTEGER     NOT NULL CHECK (reducer_version > 0),
    attempt           INTEGER     NOT NULL CHECK (attempt > 0),
    inputs_digest     TEXT        NOT NULL CHECK (length(inputs_digest) = 64),
    disposition       TEXT        NOT NULL CHECK (disposition IN ('accept', 'reject', 'escalate')),
    reason            TEXT        NOT NULL,
    signals           JSONB       NOT NULL DEFAULT '[]'::jsonb,
    actor_token_id    UUID        REFERENCES tokens(id),
    source            TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system')),
    occurred_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX convergence_verdicts_work_item_attempt_idx
    ON convergence_verdicts (work_item_id, attempt);

CREATE INDEX convergence_verdicts_work_item_occurred_idx
    ON convergence_verdicts (work_item_id, occurred_at DESC);

CREATE INDEX convergence_verdicts_disposition_idx
    ON convergence_verdicts (disposition, occurred_at DESC);
