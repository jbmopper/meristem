-- Record the event-log boundary at which the terminal human-review projector
-- invariant becomes authoritative. The migration runner stores max(events.seq)
-- on this migration's schema_migrations row in the same transaction. This is
-- schema compatibility metadata, not domain state: events remain the truth.

ALTER TABLE schema_migrations
    ADD COLUMN IF NOT EXISTS event_seq_boundary BIGINT;
