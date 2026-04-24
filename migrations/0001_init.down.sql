-- 0001_init down: drop in reverse dependency order.

DROP TABLE IF EXISTS idempotency_keys;

DROP TRIGGER IF EXISTS events_no_truncate ON events;
DROP TRIGGER IF EXISTS events_no_delete   ON events;
DROP TRIGGER IF EXISTS events_no_update   ON events;
DROP FUNCTION IF EXISTS events_reject_mutation();
DROP TABLE IF EXISTS events;

DROP TABLE IF EXISTS message_parts;
DROP TABLE IF EXISTS messages;

DROP TABLE IF EXISTS work_item_relations;
DROP TABLE IF EXISTS work_items;

DROP TABLE IF EXISTS tokens;

-- pgcrypto is left in place; other migrations may rely on it.
