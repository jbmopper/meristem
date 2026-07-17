DROP INDEX IF EXISTS work_item_assignment_terminal_addressee_event_idx;

ALTER TABLE work_item_assignment_state
    DROP CONSTRAINT IF EXISTS work_item_assignment_terminal_addressee_check,
    DROP COLUMN IF EXISTS terminal_addressee_token_id;
