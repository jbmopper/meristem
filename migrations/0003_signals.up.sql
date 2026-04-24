-- 0003_signals: signal projection table.
--
-- Signals are the bridge from non-human structured inputs (review findings,
-- repairable runtime failures, webhook reports) into work_items. Per
-- docs/signals.md, signals are content (like messages from non-human
-- sources) but are explicitly converted to work_items under policy by the
-- /v1/signals handler.
--
-- Each row is the deterministic projection of one signal.received event.
-- The handler resolves the target work_item id (via dedupe lookup against
-- this table or fresh creation) before appending the event, so the
-- projector writes a fully linked row with no second-pass updates.

CREATE TABLE signals (
    id                UUID        PRIMARY KEY,
    received_at       TIMESTAMPTZ NOT NULL,
    actor_token_id    UUID        REFERENCES tokens(id),
    source            TEXT        NOT NULL CHECK (source IN ('human', 'agent', 'system')),
    signal_kind       TEXT        NOT NULL,
    dedupe_key        TEXT,
    fingerprint       BYTEA       NOT NULL,
    work_spec         JSONB       NOT NULL,
    work_item_id      UUID        REFERENCES work_items(id) ON DELETE SET NULL,
    created_work_item BOOLEAN     NOT NULL
);

-- Non-unique on dedupe_key: multiple signal receptions may share a
-- dedupe_key over time (different Idempotency-Key windows, different
-- callers, retries past the cache horizon). The dedupe contract says they
-- collapse to one work_item, not one signal row.
CREATE INDEX signals_dedupe_key_idx ON signals (dedupe_key) WHERE dedupe_key IS NOT NULL;

CREATE INDEX signals_work_item_idx   ON signals (work_item_id);
CREATE INDEX signals_received_at_idx ON signals (received_at DESC);
CREATE INDEX signals_kind_idx        ON signals (signal_kind);
