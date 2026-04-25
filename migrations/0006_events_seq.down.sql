-- 0006_events_seq down: drop the seq column. The BIGSERIAL sequence is
-- OWNED BY the column and will be dropped with it.
--
-- WARNING: rolling back invalidates every issued cursor (the v1 cursor
-- encodes seq). Watcher consumers will get 400 invalid_cursor on their
-- next call and recover via re-bootstrap.

ALTER TABLE events DROP COLUMN seq;
