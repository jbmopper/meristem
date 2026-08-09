DROP INDEX IF EXISTS work_item_assignment_state_listener_idx;
ALTER TABLE work_item_assignment_state
    DROP COLUMN IF EXISTS listener_id,
    DROP COLUMN IF EXISTS demand_event_id,
    DROP COLUMN IF EXISTS policy_event_id;
