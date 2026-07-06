-- 0020_http_connector_stub rollback.

DROP INDEX IF EXISTS outbox_events_action_idx;
DROP INDEX IF EXISTS outbox_events_ready_idx;
DROP TABLE IF EXISTS outbox_events;

DROP INDEX IF EXISTS http_connector_actions_status_idx;
DROP INDEX IF EXISTS http_connector_actions_approval_idx;
DROP INDEX IF EXISTS http_connector_actions_work_item_idx;
DROP TABLE IF EXISTS http_connector_actions;
